#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections

from oslo_db import api as oslo_db_api
from oslo_log import log as logging

from placement.db import graph_db as db
from placement import db_api
from placement import exception
from placement.objects import consumer as consumer_obj
from placement.objects import project as project_obj
from placement.objects import resource_provider as rp_obj
from placement.objects import user as user_obj


LOG = logging.getLogger(__name__)

# The number of times to retry set_allocations if there has
# been a resource provider (not consumer) generation coflict.
RP_CONFLICT_RETRY_COUNT = 10


class Allocation(object):

    def __init__(self, resource_provider=None, consumer=None,
                 resource_class=None, used=0, updated_at=None,
                 created_at=None):
        self.resource_provider = resource_provider
        self.resource_class = resource_class
        self.consumer = consumer
        self.used = used
        self.updated_at = updated_at
        self.created_at = created_at


@db_api.placement_context_manager.writer
def _delete_allocations_for_consumer(context, consumer_uuid):
    """Deletes any existing allocations that correspond to the allocations to
    be written. This is wrapped in a transaction, so if the write subsequently
    fails, the deletion will also be rolled back.
    """
    query = """
            MATCH p=(:CONSUMER {uuid: '%s'})-[:USES]->()
            WITH relationships(p)[0] AS usages
            DELETE usages
    """ % consumer_uuid
    context.tx.run(query)


def _check_capacity_exceeded(context, allocs):
    """Checks to see if the supplied allocation records would result in any of
    the inventories involved having their capacity exceeded.

    Raises an InvalidAllocationCapacityExceeded exception if any inventory
    would be exhausted by the allocation. Raises an
    InvalidAllocationConstraintsViolated exception if any of the `step_size`,
    `min_unit` or `max_unit` constraints in an inventory will be violated
    by any one of the allocations.

    If no inventories would be exceeded or violated by the allocations, the
    function returns a list of `ResourceProvider` objects that contain the
    generation at the time of the check.

    :param ctx: `placement.context.RequestContext` that has an oslo_db
                Session
    :param allocs: List of `Allocation` objects to check
    """
    rc_names = set([a.resource_class for a in allocs])
    provider_uuids = set([a.resource_provider.uuid for a in allocs])
    query = """
            MATCH (rp:RESOURCE_PROVIDER)-[:PROVIDES]->(rc)
            WHERE rp.uuid IN %s
            AND labels(rc)[0] IN %s
            WITH rp, rc
            OPTIONAL MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH rp, rc, relationships(p)[0] AS allocs
            WITH rp, rc, sum(allocs.amount) AS total_usages
            RETURN rp, rc, labels(rc)[0] AS rc_name, total_usages
    """ % (list(provider_uuids), list(rc_names))
    result = context.tx.run(query).data()

    # Create a map keyed by (rp_uuid, res_class) for the records in the DB
    usage_map = {}
    provs_with_inv = set()
    for record in result:
        map_key = (record["rp"]["uuid"], record["rc_name"])
        if map_key in usage_map:
            raise KeyError("%s already in usage_map, bad query" % str(map_key))
        usage_map[map_key] = record
        provs_with_inv.add(record["rp"]["uuid"])
    # Ensure that all providers have existing inventory
    missing_provs = provider_uuids - provs_with_inv
    if missing_provs:
        class_str = ", ".join(rc_names)
        provider_str = ", ".join(missing_provs)
        raise exception.InvalidInventory(
            resource_class=class_str, resource_provider=provider_str)

    res_providers = {}
    rp_resource_class_sum = collections.defaultdict(
        lambda: collections.defaultdict(int))
    for alloc in allocs:
        rc_name = alloc.resource_class
        rp_uuid = alloc.resource_provider.uuid
        if rp_uuid not in res_providers:
            res_providers[rp_uuid] = alloc.resource_provider
        amount_needed = alloc.used
        # No use checking usage if we're not asking for anything
        if amount_needed == 0:
            continue
        rp_resource_class_sum[rp_uuid][rc_name] += amount_needed
        key = (rp_uuid, rc_name)
        try:
            usage_record = usage_map[key]
        except KeyError:
            # The resource class at rc_name is not in the usage map.
            raise exception.InvalidInventory(
                resource_class=alloc.resource_class,
                resource_provider=rp_uuid)
        rc_record = usage_record["rc"]
        allocation_ratio = rc_record["allocation_ratio"]
        min_unit = rc_record["min_unit"]
        max_unit = rc_record["max_unit"]
        step_size = rc_record["step_size"]
        total = rc_record["total"]
        reserved = rc_record["reserved"]

        # check min_unit, max_unit, step_size
        if ((amount_needed < min_unit) or (amount_needed > max_unit) or
                (amount_needed % step_size)):
            LOG.warning(
                    "Allocation for %(rc)s on resource provider %(rp)s "
                    "violates min_unit, max_unit, or step_size. "
                    "Requested: %(requested)s, min_unit: %(min_unit)s, "
                    "max_unit: %(max_unit)s, step_size: %(step_size)s",
                    {"rc": alloc.resource_class,
                     "rp": rp_uuid,
                     "requested": amount_needed,
                     "min_unit": min_unit,
                     "max_unit": max_unit,
                     "step_size": step_size})
            raise exception.InvalidAllocationConstraintsViolated(
                    resource_class=alloc.resource_class,
                    resource_provider=rp_uuid)

        # Should never be null, but just in case...
        used = usage_record["total_usages"] or 0
        capacity = (total - reserved) * allocation_ratio
        if (capacity < (used + amount_needed) or
                capacity < (used + rp_resource_class_sum[rp_uuid][rc_name])):
            LOG.warning(
                "Over capacity for %(rc)s on resource provider %(rp)s. "
                "Needed: %(needed)s, Used: %(used)s, Capacity: %(cap)s",
                {"rc": alloc.resource_class,
                 "rp": rp_uuid,
                 "needed": amount_needed,
                 "used": used,
                 "cap": capacity})
            raise exception.InvalidAllocationCapacityExceeded(
                resource_class=alloc.resource_class,
                resource_provider=rp_uuid)
    return res_providers


@db_api.placement_context_manager.reader
def _get_allocations_by_provider_uuid(context, rp_uuid):
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            MATCH (rp)-[:PROVIDES]->(rc)
            WITH rp, rc
            MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH rp, rc, labels(rc)[0] AS rc_name, cs,
                relationships(p)[0] AS usages
            MATCH (pj:PROJECT)-[:OWNS]->(user:USER)-[:OWNS]->(cs)
            RETURN rp, rc, rc_name, cs, usages, pj, user
    """ % rp_uuid
    result = context.tx.run(query).data()
    allocs = []
    for record in result:
        allocs.append({
            "resource_class_name": record["rc_name"],
            "used": record["usages"]["amount"],
            "consumer_uuid": record["cs"]["uuid"],
            "consumer_generation": record["cs"]["generation"],
            "project_uuid": record["pj"]["uuid"],
            "user_uuid": record["user"]["uuid"],
        })
    return allocs


@db_api.placement_context_manager.reader
def _get_allocations_by_consumer_uuid(context, consumer_uuid):
    query = """
            MATCH p=(cs:CONSUMER {uuid: '%s'})-[:USES]->(rc)
            WITH cs, rc, labels(rc)[0] AS rc_name,
                relationships(p)[0] AS usages
            OPTIONAL MATCH (pj:PROJECT)-[:OWNS]->(user:USER)-[:OWNS]->(cs)
            WITH rc, rc_name, cs, usages, pj, user
            MATCH (rp:RESOURCE_PROVIDER)-[:PROVIDES]->(rc)
            RETURN rp, rc, rc_name, cs, usages, pj, user
    """ % consumer_uuid
    result = context.tx.run(query).data()
    allocs = []
    for record in result:
        pj_uuid = record["pj"].get("uuid") if record["pj"] else None
        user_uuid = record["user"].get("uuid") if record["user"] else None
        allocs.append({
            "resource_provider_name": record["rp"]["name"],
            "resource_provider_uuid": record["rp"]["uuid"],
            "resource_provider_generation": record["rp"]["generation"],
            "resource_class_name": record["rc_name"],
            "used": record["usages"]["amount"],
            "consumer_uuid": record["cs"]["uuid"],
            "consumer_generation": record["cs"]["generation"],
            "project_uuid": pj_uuid,
            "user_uuid": user_uuid,
        })
    return allocs


@db_api.placement_context_manager.writer.independent
def _create_incomplete_consumers_for_provider(context, rp_id):
    # TODO(jaypipes): Remove in Stein after a blocker migration is added.
    """Creates consumer record if consumer relationship between allocations ->
    consumers table is missing for any allocation on the supplied provider
    internal ID, using the "incomplete consumer" project and user CONF options.

    NOTE: using a graph database, an allocation is a relation between a
    consumer and a resource. There cannot be an allocation without a consumer,
    so this method is not necessary.
    """
    return


@db_api.placement_context_manager.writer.independent
def _create_incomplete_consumer(context, consumer_id):
    # TODO(jaypipes): Remove in Stein after a blocker migration is added.
    """Creates consumer record if consumer relationship between allocations ->
    consumers table is missing for the supplied consumer UUID, using the
    "incomplete consumer" project and user CONF options.

    NOTE: using a graph database, an allocation is a relation between a
    consumer and a resource. There cannot be an allocation without a consumer,
    so this method is not necessary.
    """
    return


@db_api.placement_context_manager.writer
def _set_allocations(context, allocs):
    """Write a set of allocations.

    We must check that there is capacity for each allocation.
    If there is not we roll back the entire set.

    :raises `exception.ResourceClassNotFound` if any resource class in any
            allocation in allocs cannot be found in either the DB.
    :raises `exception.InvalidAllocationCapacityExceeded` if any inventory
            would be exhausted by the allocation.
    :raises `InvalidAllocationConstraintsViolated` if any of the
            `step_size`, `min_unit` or `max_unit` constraints in an
            inventory will be violated by any one of the allocations.
    :raises `ConcurrentUpdateDetected` if a generation for a resource
            provider or consumer failed its increment check.
    """
    # First delete any existing allocations for any consumers. This
    # provides a clean slate for the consumers mentioned in the list of
    # allocations being manipulated.
    consumer_uuids = set(alloc.consumer.uuid for alloc in allocs)
    for consumer_uuid in consumer_uuids:
        _delete_allocations_for_consumer(context, consumer_uuid)

    # Before writing any allocation records, we check that the submitted
    # allocations do not cause any inventory capacity to be exceeded for
    # any resource provider and resource class involved in the allocation
    # transaction. _check_capacity_exceeded() raises an exception if any
    # inventory capacity is exceeded. If capacity is not exceeeded, the
    # function returns a list of ResourceProvider objects containing the
    # generation of the resource provider at the time of the check. These
    # objects are used at the end of the allocation transaction as a guard
    # against concurrent updates.
    #
    # Don't check capacity when alloc.used is zero. Zero is not a valid
    # amount when making an allocation (the minimum consumption of a
    # resource is one) but is used in this method to indicate a need for
    # removal. Providing 0 is controlled at the HTTP API layer where PUT
    # /allocations does not allow empty allocations. When POST /allocations
    # is implemented it will for the special case of atomically setting and
    # removing different allocations in the same request.
    # _check_capacity_exceeded will raise a ResourceClassNotFound if any
    # allocation is using a resource class that does not exist.
    visited_consumers = {}
    visited_rps = _check_capacity_exceeded(context, allocs)
    for alloc in allocs:
        if alloc.consumer.uuid not in visited_consumers:
            visited_consumers[alloc.consumer.uuid] = alloc.consumer

        # If alloc.used is set to zero that is a signal that we don't want
        # to (re-)create any allocations for this resource class.
        # _delete_current_allocs has already wiped out allocations so just
        # continue
        if alloc.used == 0:
            continue
        consumer_uuid = alloc.consumer.uuid
        rp = alloc.resource_provider
        rc_name = alloc.resource_class
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp
                MATCH (rp)-[:PROVIDES]->(rc:%s)
                WITH rp, rc
                MATCH (cs:CONSUMER {uuid: '%s'})
                WITH rp, rc, cs
                CREATE p=(cs)-[:USES {amount: %s}]->(rc)
                RETURN p
        """ % (rp.uuid, rc_name, consumer_uuid, alloc.used)
        result = context.tx.run(query)

    # Generation checking happens here. If the inventory for this resource
    # provider changed out from under us, this will raise a
    # ConcurrentUpdateDetected which can be caught by the caller to choose
    # to try again. It will also rollback the transaction so that these
    # changes always happen atomically.
    for rp in visited_rps.values():
        rp.increment_generation()
    for consumer in visited_consumers.values():
        consumer.increment_generation()
    # If any consumers involved in this transaction ended up having no
    # allocations, delete the consumer records. Exclude consumers that had
    # *some resource* in the allocation list with a total > 0 since clearly
    # those consumers have allocations...
    cons_with_allocs = set(a.consumer.uuid for a in allocs if a.used > 0)
    all_cons = set(c.uuid for c in visited_consumers.values())
    consumers_to_check = all_cons - cons_with_allocs
    consumer_obj.delete_consumers_if_no_allocations(context,
            consumers_to_check)


def get_all_by_resource_provider(context, rp):
    _create_incomplete_consumers_for_provider(context, rp.uuid)
    db_allocs = _get_allocations_by_provider_uuid(context, rp.uuid)
    # Build up a list of Allocation objects, setting the Allocation object
    # fields to the same-named database record field we got from
    # _get_allocations_by_provider_uuid(). We already have the
    # ResourceProvider object so we just pass that object to the Allocation
    # object constructor as-is
    objs = []
    for rec in db_allocs:
        consumer = consumer_obj.Consumer(
                context, uuid=rec["consumer_uuid"],
                generation=rec["consumer_generation"],
                project=project_obj.Project(uuid=rec["project_uuid"]),
                user=user_obj.User(context, uuid=rec["user_uuid"]))
        objs.append(
            Allocation(
                resource_provider=rp,
                resource_class=rec["rc_name"],
                consumer=consumer,
                used=rec["used"],
                created_at=rec["created_at"],
                updated_at=rec["updated_at"]))
    return objs


def get_all_by_consumer_id(context, consumer_id):
    _create_incomplete_consumer(context, consumer_id)
    db_allocs = _get_allocations_by_consumer_uuid(context, consumer_id)

    if not db_allocs:
        return []

    # Build up the Consumer object (it's the same for all allocations
    # since we looked up by consumer ID)
    db_first = db_allocs[0]
    consumer = consumer_obj.Consumer(
        context, uuid=db_first['consumer_uuid'],
        generation=db_first['consumer_generation'],
        project=project_obj.Project(context, uuid=db_first['project_uuid']),
        user=user_obj.User(context, uuid=db_first['user_uuid']))

    # Build up a list of Allocation objects, setting the Allocation object
    # fields to the same-named database record field we got from
    # _get_allocations_by_consumer_id().
    #
    # NOTE(jaypipes):  Unlike with get_all_by_resource_provider(), we do
    # NOT already have the ResourceProvider object so we construct a new
    # ResourceProvider object below by looking at the resource provider
    # fields returned by _get_allocations_by_consumer_id().
    alloc_list = [
        Allocation(
            resource_provider=rp_obj.ResourceProvider(
                context,
                uuid=rec["resource_provider_uuid"],
                name=rec["resource_provider_name"],
                generation=rec["resource_provider_generation"]),
            resource_class=rec["resource_class_name"],
            consumer=consumer,
            used=rec["used"],
            created_at=rec.get("created_at"),
            updated_at=rec.get("updated_at"))
        for rec in db_allocs
    ]
    return alloc_list


def replace_all(context, alloc_list):
    """Replace the supplied allocations.

    :note: This method always deletes all allocations for all consumers
           referenced in the list of Allocation objects and then replaces
           the consumer's allocations with the Allocation objects. In doing
           so, it will end up setting the Allocation.id attribute of each
           Allocation object.
    """
    # Retry _set_allocations server side if there is a
    # ResourceProviderConcurrentUpdateDetected. We don't care about
    # sleeping, we simply want to reset the resource provider objects
    # and try again. For sake of simplicity (and because we don't have
    # easy access to the information) we reload all the resource
    # providers that may be present.
    retries = RP_CONFLICT_RETRY_COUNT
    while retries:
        retries -= 1
        try:
            _set_allocations(context, alloc_list)
            break
        except exception.ResourceProviderConcurrentUpdateDetected:
            LOG.debug("Retrying allocations write on resource provider "
                      "generation conflict")
            # We only want to reload each unique resource provider once.
            alloc_rp_uuids = set(
                    alloc.resource_provider.uuid for alloc in alloc_list)
            seen_rps = {}
            for rp_uuid in alloc_rp_uuids:
                seen_rps[rp_uuid] = rp_obj.ResourceProvider.get_by_uuid(
                        context, rp_uuid)
            for alloc in alloc_list:
                rp_uuid = alloc.resource_provider.uuid
                alloc.resource_provider = seen_rps[rp_uuid]
    else:
        # We ran out of retries so we need to raise again.
        # The log will automatically have request id info associated with
        # it that will allow tracing back to specific allocations.
        # Attempting to extract specific consumer or resource provider
        # information from the allocations is not coherent as this
        # could be multiple consumers and providers.
        LOG.warning("Exceeded retry limit of %d on allocations write",
                    RP_CONFLICT_RETRY_COUNT)
        raise exception.ResourceProviderConcurrentUpdateDetected()


def delete_all(context, alloc_list):
    consumer_uuids = set(alloc.consumer.uuid for alloc in alloc_list)
    for consumer_uuid in consumer_uuids:
        _delete_allocations_for_consumer(context, consumer_uuid)
    consumer_obj.delete_consumers_if_no_allocations(context, consumer_uuids)
