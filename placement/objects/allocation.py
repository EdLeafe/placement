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
import sqlalchemy as sa
from sqlalchemy import sql

from placement.db import graph_db as db
from placement.db.sqlalchemy import models
from placement import db_api
from placement import exception
from placement.objects import consumer as consumer_obj
from placement.objects import project as project_obj
from placement.objects import resource_provider as rp_obj
from placement.objects import user as user_obj
from placement import resource_class_cache as rc_cache


_ALLOC_TBL = models.Allocation.__table__
_CONSUMER_TBL = models.Consumer.__table__
_INV_TBL = models.Inventory.__table__
_PROJECT_TBL = models.Project.__table__
_RP_TBL = models.ResourceProvider.__table__
_USER_TBL = models.User.__table__

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
def _delete_allocations_for_consumer(ctx, consumer_uuid):
    """Deletes any existing allocations that correspond to the allocations to
    be written. This is wrapped in a transaction, so if the write subsequently
    fails, the deletion will also be rolled back.
    """
    query = """
            MATCH p=(:CONSUMER {uuid: '%s'})-[:USES]->()
            WITH relationships(p)[0] AS usages
            DELETE usages
    """ % consumer_uuid
    db.execute(query)


def _check_capacity_exceeded(ctx, allocs):
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
            MATCH (rp:RESOURCE_PROVIDER)
            WHERE rp.uuid IN %s
            WITH rp
            MATCH (rp)-[*]->(rc)
            WHERE labels(rc)[0] IN %s
            WITH rp, rc
            OPTIONAL MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH rp, rc, relationships(p)[0] AS allocs
            WITH rp, rc, sum(allocs.amount) AS total_usages
            RETURN rp, rc, labels(rc)[0] AS rc_name, total_usages
    """ % (provider_uuids, rc_names)
    result = db.execute(query)

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
                capacity < (used + rp_resource_class_sum[rp_uuid][rc_id])):
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
def _get_allocations_by_provider_id(ctx, rp_id):
    allocs = sa.alias(_ALLOC_TBL, name="a")
    consumers = sa.alias(_CONSUMER_TBL, name="c")
    projects = sa.alias(_PROJECT_TBL, name="p")
    users = sa.alias(_USER_TBL, name="u")
    cols = [
        allocs.c.id,
        allocs.c.resource_class_id,
        allocs.c.used,
        allocs.c.updated_at,
        allocs.c.created_at,
        consumers.c.id.label("consumer_id"),
        consumers.c.generation.label("consumer_generation"),
        sql.func.coalesce(
            consumers.c.uuid, allocs.c.consumer_id).label("consumer_uuid"),
        projects.c.id.label("project_id"),
        projects.c.external_id.label("project_external_id"),
        users.c.id.label("user_id"),
        users.c.external_id.label("user_external_id"),
    ]
    # TODO(jaypipes): change this join to be on ID not UUID
    consumers_join = sa.join(
        allocs, consumers, allocs.c.consumer_id == consumers.c.uuid)
    projects_join = sa.join(
        consumers_join, projects, consumers.c.project_id == projects.c.id)
    users_join = sa.join(
        projects_join, users, consumers.c.user_id == users.c.id)
    sel = sa.select(cols).select_from(users_join)
    sel = sel.where(allocs.c.resource_provider_id == rp_id)

    return [dict(r) for r in ctx.session.execute(sel)]


@db_api.placement_context_manager.reader
def _get_allocations_by_consumer_uuid(ctx, consumer_uuid):
    allocs = sa.alias(_ALLOC_TBL, name="a")
    rp = sa.alias(_RP_TBL, name="rp")
    consumer = sa.alias(_CONSUMER_TBL, name="c")
    project = sa.alias(_PROJECT_TBL, name="p")
    user = sa.alias(_USER_TBL, name="u")
    cols = [
        allocs.c.id,
        allocs.c.resource_provider_id,
        rp.c.name.label("resource_provider_name"),
        rp.c.uuid.label("resource_provider_uuid"),
        rp.c.generation.label("resource_provider_generation"),
        allocs.c.resource_class_id,
        allocs.c.used,
        consumer.c.id.label("consumer_id"),
        consumer.c.generation.label("consumer_generation"),
        sql.func.coalesce(
            consumer.c.uuid, allocs.c.consumer_id).label("consumer_uuid"),
        project.c.id.label("project_id"),
        project.c.external_id.label("project_external_id"),
        user.c.id.label("user_id"),
        user.c.external_id.label("user_external_id"),
        allocs.c.created_at,
        allocs.c.updated_at,
    ]
    # Build up the joins of the five tables we need to interact with.
    rp_join = sa.join(allocs, rp, allocs.c.resource_provider_id == rp.c.id)
    consumer_join = sa.join(rp_join, consumer,
                            allocs.c.consumer_id == consumer.c.uuid)
    project_join = sa.join(consumer_join, project,
                           consumer.c.project_id == project.c.id)
    user_join = sa.join(project_join, user,
                        consumer.c.user_id == user.c.id)

    sel = sa.select(cols).select_from(user_join)
    sel = sel.where(allocs.c.consumer_id == consumer_uuid)

    return [dict(r) for r in ctx.session.execute(sel)]


@db_api.placement_context_manager.writer.independent
def _create_incomplete_consumers_for_provider(ctx, rp_id):
    # TODO(jaypipes): Remove in Stein after a blocker migration is added.
    """Creates consumer record if consumer relationship between allocations ->
    consumers table is missing for any allocation on the supplied provider
    internal ID, using the "incomplete consumer" project and user CONF options.
    """
    alloc_to_consumer = sa.outerjoin(
        _ALLOC_TBL, consumer_obj.CONSUMER_TBL,
        _ALLOC_TBL.c.consumer_id == consumer_obj.CONSUMER_TBL.c.uuid)
    sel = sa.select([_ALLOC_TBL.c.consumer_id])
    sel = sel.select_from(alloc_to_consumer)
    sel = sel.where(
        sa.and_(
            _ALLOC_TBL.c.resource_provider_id == rp_id,
            consumer_obj.CONSUMER_TBL.c.id.is_(None)))
    missing = ctx.session.execute(sel).fetchall()
    if missing:
        # Do a single INSERT for all missing consumer relationships for the
        # provider
        incomplete_proj_id = project_obj.ensure_incomplete_project(ctx)
        incomplete_user_id = user_obj.ensure_incomplete_user(ctx)

        cols = [
            _ALLOC_TBL.c.consumer_id,
            incomplete_proj_id,
            incomplete_user_id,
        ]
        sel = sa.select(cols)
        sel = sel.select_from(alloc_to_consumer)
        sel = sel.where(
            sa.and_(
                _ALLOC_TBL.c.resource_provider_id == rp_id,
                consumer_obj.CONSUMER_TBL.c.id.is_(None)))
        # NOTE(mnaser): It is possible to have multiple consumers having many
        #               allocations to the same resource provider, which would
        #               make the INSERT FROM SELECT fail due to duplicates.
        sel = sel.group_by(_ALLOC_TBL.c.consumer_id)
        target_cols = ['uuid', 'project_id', 'user_id']
        ins_stmt = consumer_obj.CONSUMER_TBL.insert().from_select(
            target_cols, sel)
        res = ctx.session.execute(ins_stmt)
        if res.rowcount > 0:
            LOG.info("Online data migration to fix incomplete consumers "
                     "for resource provider %s has been run. Migrated %d "
                     "incomplete consumer records on the fly.", rp_id,
                     res.rowcount)


@db_api.placement_context_manager.writer.independent
def _create_incomplete_consumer(ctx, consumer_id):
    # TODO(jaypipes): Remove in Stein after a blocker migration is added.
    """Creates consumer record if consumer relationship between allocations ->
    consumers table is missing for the supplied consumer UUID, using the
    "incomplete consumer" project and user CONF options.
    """
    alloc_to_consumer = sa.outerjoin(
        _ALLOC_TBL, consumer_obj.CONSUMER_TBL,
        _ALLOC_TBL.c.consumer_id == consumer_obj.CONSUMER_TBL.c.uuid)
    sel = sa.select([_ALLOC_TBL.c.consumer_id])
    sel = sel.select_from(alloc_to_consumer)
    sel = sel.where(
        sa.and_(
            _ALLOC_TBL.c.consumer_id == consumer_id,
            consumer_obj.CONSUMER_TBL.c.id.is_(None)))
    missing = ctx.session.execute(sel).fetchall()
    if missing:
        incomplete_proj_id = project_obj.ensure_incomplete_project(ctx)
        incomplete_user_id = user_obj.ensure_incomplete_user(ctx)

        ins_stmt = consumer_obj.CONSUMER_TBL.insert().values(
            uuid=consumer_id, project_id=incomplete_proj_id,
            user_id=incomplete_user_id)
        res = ctx.session.execute(ins_stmt)
        if res.rowcount > 0:
            LOG.info("Online data migration to fix incomplete consumers "
                     "for consumer %s has been run. Migrated %d incomplete "
                     "consumer records on the fly.", consumer_id, res.rowcount)


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
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
        rc_name =alloc.resource_class
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp
                MATCH (rp)-[:PROVIDES]->(rc:%s)
                WITH rp, rc
                MATCH (cs:CONSUMER {uuid: '%s'})
                WITH rp, rc, cs
                CREATE p=(cs)-[:USES {amount: %s}->(rc)
                RETURN p
        """ % (rp.uuid, rc_name, consumer_uuid)
        result = db.execute(query)

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
            resource_class=rec["resource_class"],
            consumer=consumer,
            used=rec["used"],
            created_at=rec["created_at"],
            updated_at=rec["updated_at"])
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
