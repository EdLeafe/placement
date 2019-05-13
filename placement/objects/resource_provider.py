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
import copy

# NOTE(cdent): The resource provider objects are designed to never be
# used over RPC. Remote manipulation is done with the placement HTTP
# API. The 'remotable' decorators should not be used, the objects should
# not be registered and there is no need to express VERSIONs nor handle
# obj_make_compatible.

import os_traits
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_log import log as logging
from oslo_utils import excutils
import six
import sqlalchemy as sa
from sqlalchemy import exc as sqla_exc
from sqlalchemy import func
from sqlalchemy import sql

from placement.db import graph_db as db
from placement.db.sqlalchemy import models
from placement import db_api
from placement import exception
from placement.objects import inventory as inv_obj
from placement.objects import rp_candidates
from placement.objects import trait as trait_obj
from placement import resource_class_cache as rc_cache

_TRAIT_TBL = models.Trait.__table__
_ALLOC_TBL = models.Allocation.__table__
_INV_TBL = models.Inventory.__table__
_RP_TBL = models.ResourceProvider.__table__
_AGG_TBL = models.PlacementAggregate.__table__
_RP_AGG_TBL = models.ResourceProviderAggregate.__table__
_RP_TRAIT_TBL = models.ResourceProviderTrait.__table__

LOG = logging.getLogger(__name__)


def _usage_select(rc_ids):
    usage = sa.select([_ALLOC_TBL.c.resource_provider_id,
                       _ALLOC_TBL.c.resource_class_id,
                       sql.func.sum(_ALLOC_TBL.c.used).label('used')])
    usage = usage.where(_ALLOC_TBL.c.resource_class_id.in_(rc_ids))
    usage = usage.group_by(_ALLOC_TBL.c.resource_provider_id,
                           _ALLOC_TBL.c.resource_class_id)
    return sa.alias(usage, name='usage')


def _capacity_check_clause(amount, usage, inv_tbl=_INV_TBL):
    return sa.and_(
        sql.func.coalesce(usage.c.used, 0) + amount <= (
            (inv_tbl.c.total - inv_tbl.c.reserved) *
            inv_tbl.c.allocation_ratio),
        inv_tbl.c.min_unit <= amount,
        inv_tbl.c.max_unit >= amount,
        amount % inv_tbl.c.step_size == 0,
    )


def _get_current_inventory_resources(ctx, rp):
    """Returns a set() containing the names of the resource classes for all
    resources currently having an inventory record for the supplied resource
    provider.

    :param ctx: `placement.context.RequestContext` that may be used to grab a
                DB connection.
    :param rp: Resource provider to query inventory for.
    """
    query = """
            MATCH (rp {uuid: '%s'})-[:PROVIDES]->(rc)
            RETURN labels(rc) as rc_name
            """ % rp.uuid
    result = db.execute()
    return set([rec["rc_name"] for rec in result])

def has_allocations(self, rp, rcs=None):
    """Returns True if there are any allocations against resources from the
    specified provider. If `rcs` is specified, the check is limited to
    allocations against just the resource classes specified.
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-->(inv)<-[:USES]-(cs)
            RETURN labels(inv)[0] AS rc
            """ % rp.uuid
    result = db.execute(query)
    if not result:
        return False
    if rcs:
        allocs = [rec["rc"] for rec in result]
        for rc in rc:
            if rc in allocs:
                return True
        return False
    return True

def get_allocated_inventory(self, rp, rcs=None):
    """Returns the list of resource classes for any inventory that has
    allocations against it, along with the total amount allocated. If `rcs` is
    specified, the returned list is limited to only those resource classes in
    `rcs` that have allocations.
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-->(inv)<-[u:USES]-(cs)
            WITH labels(inv)[0] AS rc
            RETURN rc, sum(u.total) AS used
            """ % rp.uuid
    result = db.execute(query)
    if not result:
        return {}
    if rcs:
        filt = set(rcs)
    else:
        filt = set()
    return {rec["rc"]: rec[:used] for rec in result
            if rec["rc"] in filt}

def _delete_inventory_from_provider(ctx, rp, to_delete):
    """Deletes any inventory records from the supplied provider and set() of
    resource class identifiers.

    If there are allocations for any of the inventories to be deleted raise
    InventoryInUse exception.

    :param ctx: `placement.context.RequestContext` that contains an oslo_db
                Session
    :param rp: Resource provider from which to delete inventory.
    :param to_delete: set() containing resource class names to delete.
    """
    allocs = rp.get_allocated_inventory(rcs=to_delete)
    if allocs:
        raise exception.InventoryInUse(resource_classes=allocs,
                                       resource_provider=rp.uuid)
    for rc in to_delete:
        # Delete the providing relationship first
        query = """
                MATCH (rp)-[rel:PROVIDES]->(rc:%s)
                WITH rel, rc
                DELETE rel
                RETURN id(rc) AS rcid
                """ % rc
        result = db.execute()
        rcid = result[0]["rcid"]
        # Now delete the inventory
        query = "MATCH (inv) WHERE id(inv) = %s WITH inv DELETE inv" % rcid
        result = db.execute()
    return len(to_delete)


def _add_inventory_to_provider(ctx, rp, inv_list):
    """Inserts new inventory records for the supplied resource provider.

    :param ctx: `placement.context.RequestContext` that contains an oslo_db
                Session
    :param rp: Resource provider to add inventory to.
    :param inv_list: List of Inventory objects
    """
    inv_adds = []
    for inv_rec in inv_list:
        rc_name = inv_rec.resource_class
        rc_atts = ["allocation_ratio: %s" % inv_rec.allocation_ratio,
                "total: %s" % inv_rec.total,
                "max_unit: %s" % inv_rec.max_unit,
                "min_unit: %s" % inv_rec.min_unit,
                "reserved: %s" % inv_rec.reserved,
                "step_size: %s" % inv_rec.step_size,
                ]
        rc_att_str = ", ".join(rc_atts)
        inv_adds.append("CREATE (rp)-[:PROVIDES]-(:%s {%s})" %
                (rc_name, rc_att_str))
    creates = "\n".join(inv_adds)
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            %s
            """ % (rp.uuid, creates)
    result = db.execute(query)

def _update_inventory_for_provider(ctx, rp, inv_list, to_update):
    """Updates existing inventory records for the supplied resource provider.

    :param ctx: `placement.context.RequestContext` that contains an oslo_db
                Session
    :param rp: Resource provider on which to update inventory.
    :param inv_list: List of Inventory objects
    :param to_update: set() containing resource class IDs to search inv_list
                      for updating in resource provider.
    :returns: A list of (uuid, class) tuples that have exceeded their
              capacity after this inventory update.
    """
    current_allocs = get_allocated_inventory(rp)
    exceeded = []
    for rc in to_update:
        inv_record = inv_obj.find(inv_list, rc)
        # Get the current inventory
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-->(rc:%s)
                RETURN id(rc) as rcid
                """ % (rp.uuid, rc)
        result = db.execute(query)
        if not result:
            raise exception.InventoryWithResourceClassNotFound(
                resource_class=rc)
        rcid = result[0]["rcid"]
        # Check if the new total - reserved is exceeded
        used = current_allocs.get(rc, 0)
        if inv_record.capacity < used:
            exceeded.append((rp.uuid, rc))
        # Create the update clause
        upds = []
        for att in ("total", "reserved", "min_unit", "max_unit", "step_size",
                "allocation_ratio"):
            upds.append("%s=%s" % (att, getattr(inv_record, att)))
        update_clause = ", ".join(upds)
        # Updated the inventory of the RC node
        query = """
                MATCH (rc)
                WHERE id(rc) = %s
                SET %s
                RETURN rc
                """ % (rcid, upd_clause)
        result = db.execute(query)
    return exceeded


@db_api.placement_context_manager.writer
def _add_inventory(context, rp, inventory):
    """Add one Inventory that wasn't already on the provider.

    :raises `exception.ResourceClassNotFound` if inventory.resource_class
            cannot be found in the DB.
    """
    rc = inventory.resource_class
    query = """
            MATCH (rc:RESOURCE_CLASS {name: '%s'})
            RETURN rc
            """ % rc
    result = db.execute(query)
    if not result:
        raise exception.ResourceClassNotFound(resource_class=rc)
    _add_inventory_to_provider(context, rp, [inventory])
    rp.increment_generation()


@db_api.placement_context_manager.writer
def _update_inventory(context, rp, inventory):
    """Update an inventory already on the provider.

    :raises `exception.ResourceClassNotFound` if inventory.resource_class
            cannot be found in the DB.
    """
    exceeded = _update_inventory_for_provider(
        context, rp, [inventory], set([inventory.resource_class]))
    rp.increment_generation()
    return exceeded


@db_api.placement_context_manager.writer
def _delete_inventory(context, rp, resource_class):
    """Delete up to one Inventory of the given resource_class string.

    :raises `exception.ResourceClassNotFound` if resource_class
            cannot be found in the DB.
    """
    if not _delete_inventory_from_provider(context, rp, [resource_class]):
        raise exception.NotFound(
            'No inventory of class %s found for delete'
            % resource_class)
    rp.increment_generation()


@db_api.placement_context_manager.writer
def _set_inventory(context, rp, inv_list):
    """Given a list of Inventory objects, replaces the inventory of the
    resource provider in a safe, atomic fashion using the resource
    provider's generation as a consistent view marker.

    :param context: Nova RequestContext.
    :param rp: `ResourceProvider` object upon which to set inventory.
    :param inv_list: A list of `Inventory` objects to save to backend storage.
    :returns: A list of (uuid, class) tuples that have exceeded their
              capacity after this inventory update.
    :raises placement.exception.ConcurrentUpdateDetected: if another thread
            updated the same resource provider's view of its inventory or
            allocations in between the time when this object was originally
            read and the call to set the inventory.
    :raises `exception.ResourceClassNotFound` if any resource class in any
            inventory in inv_list cannot be found in the DB.
    :raises `exception.InventoryInUse` if we attempt to delete inventory
            from a provider that has allocations for that resource class.
    """
    existing_resources = _get_current_inventory_resources(context, rp)
    these_resources = set([r.resource_class for r in inv_list])

    # Determine which resources we should be adding, deleting and/or
    # updating in the resource provider's inventory by comparing sets
    # of resource class identifiers.
    to_add = these_resources - existing_resources
    to_delete = existing_resources - these_resources
    to_update = these_resources & existing_resources
    exceeded = []

    if to_delete:
        _delete_inventory_from_provider(context, rp, to_delete)
    if to_add:
        _add_inventory_to_provider(context, rp, to_add)
    if to_update:
        exceeded = _update_inventory_for_provider(context, rp, inv_list,
                                                  to_update)

    # Here is where we update the resource provider's generation value.  If
    # this update updates zero rows, that means that another thread has updated
    # the inventory for this resource provider between the time the caller
    # originally read the resource provider record and inventory information
    # and this point. We raise an exception here which will rollback the above
    # transaction and return an error to the caller to indicate that they can
    # attempt to retry the inventory save after reverifying any capacity
    # conditions and re-reading the existing inventory information.
    rp.increment_generation()

    return exceeded


@db_api.placement_context_manager.reader
def _get_provider_by_uuid(context, uuid):
    """Given a UUID, return a dict of information about the resource provider
    from the database.

    :raises: NotFound if no such provider was found
    :param uuid: The UUID to look up
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            RETURN rp
            """ % uuid
    result = db.execute(query)
    if not result:
        raise exception.NotFound(
            "No resource provider with uuid %s found" % uuid)
    rp = db.pythonize(result[0]["rp"])
    provider_ids = provider_ids_from_uuid(uuid)
    return {"uuid": rp.uuid,
            "name": rp.name,
            "generation": rp.generation,
            "root_provider_uuid": provider_ids.root_provider_uuid,
            "parent_provider_uuid": provider_ids.parent_provider_uuid,
            "updated_at": rp.updated_at,
            "created_at": rp.created_at,
    }


@db_api.placement_context_manager.reader
def _get_aggregates_by_provider(context, rp):
    """Returns a list of UUIDs of any aggregates for the supplied resource
    provider.
    """
#    """Returns a dict, keyed by internal aggregate ID, of aggregate UUIDs
#    associated with the supplied internal resource provider ID.
#    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:ASSOCIATED]->
                (agg:RESOURCE_PROVIDER)
            RETURN agg.uuid AS agg_uuid
            """ % rp.uuid
    result = db.execute(query)
    return [rec[agg_uuid] for rec in result]

@db_api.placement_context_manager.reader
def anchors_for_sharing_providers(context, rp_ids, get_id=False):
    """Given a list of internal IDs of sharing providers, returns a set of
    tuples of (sharing provider UUID, anchor provider UUID), where each of
    anchor is the unique root provider of a tree associated with the same
    aggregate as the sharing provider. (These are the providers that can
    "anchor" a single AllocationRequest.)

    The sharing provider may or may not itself be part of a tree; in either
    case, an entry for this root provider is included in the result.

    If the sharing provider is not part of any aggregate, the empty list is
    returned.

    If get_id is True, it returns a set of tuples of (sharing provider ID,
    anchor provider ID) instead.
    """
    # SELECT sps.uuid, COALESCE(rps.uuid, shr_with_sps.uuid)
    # FROM resource_providers AS sps
    # INNER JOIN resource_provider_aggregates AS shr_aggs
    #   ON sps.id = shr_aggs.resource_provider_id
    # INNER JOIN resource_provider_aggregates AS shr_with_sps_aggs
    #   ON shr_aggs.aggregate_id = shr_with_sps_aggs.aggregate_id
    # INNER JOIN resource_providers AS shr_with_sps
    #   ON shr_with_sps_aggs.resource_provider_id = shr_with_sps.id
    # LEFT JOIN resource_providers AS rps
    #   ON shr_with_sps.root_provider_id = rps.id
    # WHERE sps.id IN $(RP_IDs)
    rps = sa.alias(_RP_TBL, name='rps')
    sps = sa.alias(_RP_TBL, name='sps')
    shr_aggs = sa.alias(_RP_AGG_TBL, name='shr_aggs')
    shr_with_sps_aggs = sa.alias(_RP_AGG_TBL, name='shr_with_sps_aggs')
    shr_with_sps = sa.alias(_RP_TBL, name='shr_with_sps')
    join_chain = sa.join(
        sps, shr_aggs, sps.c.id == shr_aggs.c.resource_provider_id)
    join_chain = sa.join(
        join_chain, shr_with_sps_aggs,
        shr_aggs.c.aggregate_id == shr_with_sps_aggs.c.aggregate_id)
    join_chain = sa.join(
        join_chain, shr_with_sps,
        shr_with_sps_aggs.c.resource_provider_id == shr_with_sps.c.id)
    if get_id:
        # TODO(yikun): Change `func.coalesce(shr_with_sps.c.root_provider_id,
        # shr_with_sps.c.id)` to `shr_with_sps.c.root_provider_id` when we are
        # sure all root_provider_id values are NOT NULL
        sel = sa.select([sps.c.id, func.coalesce(
            shr_with_sps.c.root_provider_id, shr_with_sps.c.id)])
    else:
        # TODO(efried): Change this to an inner join and change
        # 'func.coalesce(rps.c.uuid, shr_with_sps.c.uuid)' to `rps.c.uuid`
        # when we are sure all root_provider_id values are NOT NULL
        join_chain = sa.outerjoin(
            join_chain, rps, shr_with_sps.c.root_provider_id == rps.c.id)
        sel = sa.select([sps.c.uuid, func.coalesce(rps.c.uuid,
                                                   shr_with_sps.c.uuid)])
    sel = sel.select_from(join_chain)
    sel = sel.where(sps.c.id.in_(rp_ids))
    return set([(r[0], r[1]) for r in context.session.execute(sel).fetchall()])


def _ensure_aggregate(ctx, agg_uuid):
    """Finds an aggregate and returns its internal ID. If not found, creates
    the aggregate and returns the new aggregate's internal ID.

    If there is a race to create the aggregate (which can happen under rare
    high load conditions), retry up to 10 times.
    """
    sel = sa.select([_AGG_TBL.c.id]).where(_AGG_TBL.c.uuid == agg_uuid)
    res = ctx.session.execute(sel).fetchone()
    if res:
        return res[0]

    LOG.debug("_ensure_aggregate() did not find aggregate %s. "
              "Attempting to create it.", agg_uuid)
    try:
        ins_stmt = _AGG_TBL.insert().values(uuid=agg_uuid)
        res = ctx.session.execute(ins_stmt)
        agg_id = res.inserted_primary_key[0]
        LOG.debug("_ensure_aggregate() created new aggregate %s (id=%d).",
                  agg_uuid, agg_id)
        return agg_id
    except db_exc.DBDuplicateEntry:
        # Something else added this agg_uuid in between our initial
        # fetch above and when we tried flushing this session.
        with excutils.save_and_reraise_exception():
            LOG.debug("_ensure_provider() failed to create new aggregate %s. "
                      "Another thread already created an aggregate record. ",
                      agg_uuid)


# _ensure_aggregate() can raise DBDuplicateEntry. Then we must start a new
# transaction because the new aggregate entry can't be found in the old
# transaction if the isolation level is set to "REPEATABLE_READ"
@oslo_db_api.wrap_db_retry(
    max_retries=10, inc_retry_interval=False,
    exception_checker=lambda exc: isinstance(exc, db_exc.DBDuplicateEntry))
@db_api.placement_context_manager.writer
def _set_aggregates(context, resource_provider, provided_aggregates,
                    increment_generation=False):
    # When aggregate uuids are persisted no validation is done
    # to ensure that they refer to something that has meaning
    # elsewhere. It is assumed that code which makes use of the
    # aggregates, later, will validate their fitness.
    # TODO(cdent): At the moment we do not delete
    # a PlacementAggregate that no longer has any associations
    # with at least one resource provider. We may wish to do that
    # to avoid bloat if it turns out we're creating a lot of noise.
    # Not doing now to move things along.
    provided_aggregates = set(provided_aggregates)
    existing_aggregates = set(_get_aggregates_by_provider(context,
            resource_provider))
    # These are the changes to existing agg associations
    agg_uuids_to_add = provided_aggregates - existing_aggregates
    aggs_uuids_to_disassociate = [agg_uuid for agg_uuid in existing_aggregates
            if agg_uuid not in provided_aggregates]
    # Create any new associations. This will also create the agg RP if it
    # doesn't already exist.
    for agg_uuid in agg_uuids_to_add:
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                MERGE (agg:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp, agg
                MERGE p=(rp)-[:ASSOCIATES]->(agg)
                RETURN p
        """ % (resource_provider.uuid, agg_uuid)
        db.execute(query)
        LOG.debug("Setting aggregates for provider %s. Successfully "
                  "associated aggregate %s.",
                  resource_provider.uuid, agg_uuid)
    # Remove any unneeded associations
    for agg_uuid in aggs_uuids_to_disassociate:
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[rel:ASSOCIATES]->
                    (agg:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp, agg, rel
                DELETE rel
                RETURN rp, agg
        """ % (resource_provider.uuid, agg_uuid)
        db.execute(query)
        LOG.debug("Setting aggregates for provider %s. Successfully "
                  "disassociated aggregate %s.",
                  resource_provider.uuid, agg_uuid)
    if increment_generation:
        resource_provider.increment_generation()


@db_api.placement_context_manager.writer
def _set_traits(context, rp, traits):
    """Given a ResourceProvider object and a list of Trait objects, replaces
    the set of traits associated with the resource provider.

    :raises: ConcurrentUpdateDetected if the resource provider's traits or
             inventory was changed in between the time when we first started to
             set traits and the end of this routine.

    :param rp: The ResourceProvider object to set traits against
    :param traits: List of Trait objects
    """
    # Get the list of all trait names
    query = """
            MATCH (t:TRAIT)
            RETURN t.name AS trait_name
    """
    result = db.execute(query)
    trait_names = [rec["trait_name"] for rec in result]
    # Get the traits for this RP
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            MATCH (t:TRAIT)
            WHERE t.name IN keys(properties(rp))
            RETURN t.name AS trait_name
    """ % rp.uuid
    result = db.execute(query)
    existing_traits = set([rec["trait_name"] for rec in result])
    new_traits = set([trait.name for trait in traits])
    to_add = new_traits - existing_traits
    to_delete = existing_traits - new_traits
    if not to_add and not to_delete:
        return
    # Remove the traits no longer needed
    if to_delete:
        del_list = []
        for del_trait in to_delete:
            del_list.append("rp.%s" % del_trait)
        del_clause = ", ".join(del_list)
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                REMOVE %s
                RETURN rp
        """ % (rp.uuid, del_clause)
        result = db.execute(query)
    # Add the new traits, if any
    if to_add:
        add_list = []
        for add_trait in to_add:
            add_list.append("rp.%s = true" % add_trait)
        add_clause = ", ".join(add_list)
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                SET %s
                RETURN rp
        """ % (rp.uuid, add_clause)
        result = db.execute(query)
    rp.increment_generation()


@db_api.placement_context_manager.reader
def _has_child_providers(context, rp_id):
    """Returns True if the supplied resource provider has any child providers,
    False otherwise
    """
    query = """
MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:CONTAINS]-(child:RESOURCE_PROVIDER)
RETURN count(child) AS num
"""
    result = db.execute(query)
    return bool(result[0]["num"])


@db_api.placement_context_manager.writer
def _set_root_provider_id(context, rp_id, root_id):
    """Simply sets the root_provider_id value for a provider identified by
    rp_id. Used in implicit online data migration via REST API getting
    resource providers.

    :param rp_id: Internal ID of the provider to update
    :param root_id: Value to set root provider to
    """
    upd = _RP_TBL.update().where(_RP_TBL.c.id == rp_id)
    upd = upd.values(root_provider_id=root_id)
    context.session.execute(upd)


@db_api.placement_context_manager.writer
def set_root_provider_ids(context, batch_size):
    """Simply sets the root_provider_id value for a provider identified by
    rp_id. Used in explicit online data migration via CLI.

    :param rp_id: Internal ID of the provider to update
    :param root_id: Value to set root provider to
    """
    # UPDATE resource_providers
    # SET root_provider_id=resource_providers.id
    # WHERE resource_providers.id
    # IN (SELECT subq_1.id
    #     FROM (SELECT resource_providers.id AS id
    #           FROM resource_providers
    #           WHERE resource_providers.root_provider_id IS NULL
    #           LIMIT :param_1)
    #     AS subq_1)

    subq_1 = context.session.query(_RP_TBL.c.id)
    subq_1 = subq_1.filter(_RP_TBL.c.root_provider_id.is_(None))
    subq_1 = subq_1.limit(batch_size)
    subq_1 = sa.alias(subq_1.as_scalar(), name="subq_1")

    subq_2 = sa.select([subq_1.c.id]).select_from(subq_1)

    upd = _RP_TBL.update().where(_RP_TBL.c.id.in_(subq_2.as_scalar()))
    upd = upd.values(root_provider_id=_RP_TBL.c.id)
    res = context.session.execute(upd)

    return res.rowcount, res.rowcount


ProviderIds = collections.namedtuple(
        "ProviderIds", "uuid parent_uuid root_uuid")


def provider_ids_from_rp_ids(context, rp_ids):
    """Given an iterable of internal resource provider IDs, returns a dict,
    keyed by internal provider Id, of ProviderIds namedtuples describing those
    providers.

    :returns: dict, keyed by internal provider Id, of ProviderIds namedtuples
    :param rp_ids: iterable of internal provider IDs to look up
    """
    # SELECT
    #   rp.id, rp.uuid,
    #   parent.id AS parent_id, parent.uuid AS parent_uuid,
    #   root.id AS root_id, root.uuid AS root_uuid
    # FROM resource_providers AS rp
    # LEFT JOIN resource_providers AS parent
    #   ON rp.parent_provider_id = parent.id
    # LEFT JOIN resource_providers AS root
    #   ON rp.root_provider_id = root.id
    # WHERE rp.id IN ($rp_ids)
    me = sa.alias(_RP_TBL, name="me")
    parent = sa.alias(_RP_TBL, name="parent")
    root = sa.alias(_RP_TBL, name="root")
    cols = [
        me.c.id,
        me.c.uuid,
        parent.c.id.label('parent_id'),
        parent.c.uuid.label('parent_uuid'),
        root.c.id.label('root_id'),
        root.c.uuid.label('root_uuid'),
    ]
    # TODO(jaypipes): Change this to an inner join when we are sure all
    # root_provider_id values are NOT NULL
    me_to_root = sa.outerjoin(me, root, me.c.root_provider_id == root.c.id)
    me_to_parent = sa.outerjoin(
        me_to_root, parent,
        me.c.parent_provider_id == parent.c.id)
    sel = sa.select(cols).select_from(me_to_parent)
    sel = sel.where(me.c.id.in_(rp_ids))

    ret = {}
    for r in context.session.execute(sel):
        # Use its id/uuid for the root id/uuid if the root id/uuid is None
        # TODO(tetsuro): Remove this to when we are sure all root_provider_id
        # values are NOT NULL
        d = dict(r)
        if d['root_id'] is None:
            d['root_id'] = d['id']
            d['root_uuid'] = d['uuid']
        ret[d['id']] = ProviderIds(**d)
    return ret


def provider_ids_from_uuid(context, uuid):
    """Given the UUID of a resource provider, returns a namedtuple
    (ProviderIds) with the UUID, parent provider's UUID, and the root provider
    UUID.

    :returns: ProviderIds object containing the UUIDs of the provider
              identified by the supplied UUID, or None if there is no
              ResourceProvider with a matching uuid.
    :param uuid: The UUID of the provider to look up
    """
    query = """
            MATCH (me:RESOURCE_PROVIDER {uuid: '%s'})
            WITH me
            OPTIONAL MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS]->(me)
            OPTIONAL MATCH (root:RESOURCE_PROVIDER)-[:CONTAINS*2..99]->(me)
            WHERE NOT ()-[:CONTAINS]->(root)
            RETURN parent.uuid AS p_uuid, root.uuid AS r_uuid
            """ % uuid
    result = db.execute(query)
    if not result:
        return None
    parent_uuid = result[0]["p_uuid"]
    root_uuid = result[0]["r_uuid"]
    return ProviderIds(uuid=uuid, parent_uuid=parent_uuid, root_uuid=root_uuid)


def provider_ids_matching_aggregates(context, member_of, rp_ids=None):
    """Given a list of lists of aggregate UUIDs, return the internal IDs of all
    resource providers associated with the aggregates.

    :param member_of: A list containing lists of aggregate UUIDs. Each item in
        the outer list is to be AND'd together. If that item contains multiple
        values, they are OR'd together.

        For example, if member_of is::

            [
                ['agg1'],
                ['agg2', 'agg3'],
            ]

        we will return all the resource providers that are
        associated with agg1 as well as either (agg2 or agg3)
    :param rp_ids: When present, returned resource providers are limited
        to only those in this value

    :returns: A set of internal resource provider IDs having all required
        aggregate associations
    """
    # Given a request for the following:
    #
    # member_of = [
    #   [agg1],
    #   [agg2],
    #   [agg3, agg4]
    # ]
    #
    # we need to produce the following SQL expression:
    #
    # SELECT
    #   rp.id
    # FROM resource_providers AS rp
    # JOIN resource_provider_aggregates AS rpa1
    #   ON rp.id = rpa1.resource_provider_id
    #   AND rpa1.aggregate_id IN ($AGG1_ID)
    # JOIN resource_provider_aggregates AS rpa2
    #   ON rp.id = rpa2.resource_provider_id
    #   AND rpa2.aggregate_id IN ($AGG2_ID)
    # JOIN resource_provider_aggregates AS rpa3
    #   ON rp.id = rpa3.resource_provider_id
    #   AND rpa3.aggregate_id IN ($AGG3_ID, $AGG4_ID)
    # # Only if we have rp_ids...
    # WHERE rp.id IN ($RP_IDs)

    # First things first, get a map of all the aggregate UUID to internal
    # aggregate IDs
    agg_uuids = set()
    for members in member_of:
        for member in members:
            agg_uuids.add(member)
    agg_tbl = sa.alias(_AGG_TBL, name='aggs')
    agg_sel = sa.select([agg_tbl.c.uuid, agg_tbl.c.id])
    agg_sel = agg_sel.where(agg_tbl.c.uuid.in_(agg_uuids))
    agg_uuid_map = {
        r[0]: r[1] for r in context.session.execute(agg_sel).fetchall()
    }

    rp_tbl = sa.alias(_RP_TBL, name='rp')
    join_chain = rp_tbl

    for x, members in enumerate(member_of):
        rpa_tbl = sa.alias(_RP_AGG_TBL, name='rpa%d' % x)

        agg_ids = [agg_uuid_map[member] for member in members
                   if member in agg_uuid_map]
        if not agg_ids:
            # This member_of list contains only non-existent aggregate UUIDs
            # and therefore we will always return 0 results, so short-circuit
            return set()

        join_cond = sa.and_(
            rp_tbl.c.id == rpa_tbl.c.resource_provider_id,
            rpa_tbl.c.aggregate_id.in_(agg_ids))
        join_chain = sa.join(join_chain, rpa_tbl, join_cond)
    sel = sa.select([rp_tbl.c.id]).select_from(join_chain)
    if rp_ids:
        sel = sel.where(rp_tbl.c.id.in_(rp_ids))
    return set(r[0] for r in context.session.execute(sel))


@db_api.placement_context_manager.writer
def _delete_rp_record(context, _id):
    query = context.session.query(models.ResourceProvider)
    query = query.filter(models.ResourceProvider.id == _id)
    return query.delete(synchronize_session=False)


class ResourceProvider(object):
    SETTABLE_FIELDS = ('name', 'parent_provider_uuid')

    def __init__(self, context, id=None, uuid=None, name=None,
                 generation=None, parent_provider_uuid=None,
                 root_provider_uuid=None, updated_at=None, created_at=None):
        self._context = context
        self.id = id
        self.uuid = uuid
        self.name = name
        self.generation = generation
        # UUID of the root provider in a hierarchy of providers. Will be equal
        # to the uuid field if this provider is the root provider of a
        # hierarchy. This field is never manually set by the user. Instead, it
        # is automatically set to either the root provider UUID of the parent
        # or the UUID of the provider itself if there is no parent. This field
        # is an optimization field that allows us to very quickly query for all
        # providers within a particular tree without doing any recursive
        # querying.
        self.root_provider_uuid = root_provider_uuid
        # UUID of the direct parent provider, or None if this provider is a
        # "root" provider.
        self.parent_provider_uuid = parent_provider_uuid
        self.updated_at = updated_at
        self.created_at = created_at

    def create(self):
        if self.id is not None:
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        if self.uuid is None:
            raise exception.ObjectActionError(action='create',
                                              reason='uuid is required')
        if not self.name:
            raise exception.ObjectActionError(action='create',
                                              reason='name is required')

        # These are the only fields we are willing to create with.
        # If there are others, ignore them.
        updates = {
            'name': self.name,
            'uuid': self.uuid,
            'parent_provider_uuid': self.parent_provider_uuid,
        }
        self._create_in_db(self._context, updates)

    def destroy(self):
        self._delete(self._context, self.uuid)

    def save(self):
        # These are the only fields we are willing to save with.
        # If there are others, ignore them.
        updates = {
            'name': self.name,
            'parent_provider_uuid': self.parent_provider_uuid,
        }
        self._update_in_db(updates)

    @classmethod
    def get_by_uuid(cls, context, uuid):
        """Returns a new ResourceProvider object with the supplied UUID.

        :raises NotFound if no such provider could be found
        :param uuid: UUID of the provider to search for
        """
        rp_rec = _get_provider_by_uuid(context, uuid)
        return cls._from_db_object(context, cls(context), rp_rec)

    def add_inventory(self, inventory):
        """Add one new Inventory to the resource provider.

        Fails if Inventory of the provided resource class is
        already present.
        """
        _add_inventory(self._context, self, inventory)

    def delete_inventory(self, resource_class):
        """Delete Inventory of provided resource_class."""
        _delete_inventory(self._context, self, resource_class)

    def set_inventory(self, inv_list):
        """Set all resource provider Inventory to be the provided list."""
        exceeded = _set_inventory(self._context, self, inv_list)
        for uuid, rclass in exceeded:
            LOG.warning('Resource provider %(uuid)s is now over-'
                        'capacity for %(resource)s',
                        {'uuid': uuid, 'resource': rclass})

    def update_inventory(self, inventory):
        """Update one existing Inventory of the same resource class.

        Fails if no Inventory of the same class is present.
        """
        exceeded = _update_inventory(self._context, self, inventory)
        for uuid, rclass in exceeded:
            LOG.warning('Resource provider %(uuid)s is now over-'
                        'capacity for %(resource)s',
                        {'uuid': uuid, 'resource': rclass})

    def get_aggregates(self):
        """Get the aggregate uuids associated with this resource provider."""
        return _get_aggregates_by_provider(self._context, self)

    def set_aggregates(self, aggregate_uuids, increment_generation=False):
        """Set the aggregate uuids associated with this resource provider.

        If an aggregate does not exist, one will be created using the
        provided uuid.

        The resource provider generation is incremented if and only if the
        increment_generation parameter is True.
        """
        _set_aggregates(self._context, self, aggregate_uuids,
                        increment_generation=increment_generation)

    def set_traits(self, traits):
        """Replaces the set of traits associated with the resource provider
        with the given list of Trait objects.

        :param traits: A list of Trait objects representing the traits to
                       associate with the provider.
        """
        _set_traits(self._context, self, traits)

    def increment_generation(self):
        """Increments this provider's generation value, supplying the
        currently-known generation.

        :raises placement.exception.ConcurrentUpdateDetected: if another thread
                updated the resource provider's view of its inventory or
                allocations in between the time when this object was originally
                read and the call to set the inventory.
        """
        rp_gen = self.generation
        new_generation = rp_gen + 1
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                WHERE rp.generation = %s
                WITH rp
                SET rp.generation = %s
                RETURN rp
        """ % (self.uuid, rp_gen, new_generation)
        result = db.execute(query)
        if not result:
            raise exception.ResourceProviderConcurrentUpdateDetected()
        self.generation = new_generation

    @db_api.placement_context_manager.writer
    def _create_in_db(self, context, updates):
        # User supplied a parent, let's make sure it exists
        parent_uuid = updates.pop('parent_provider_uuid')
        parent_create_clause = ""
        if parent_uuid is not None:
            # Setting parent to ourselves doesn't make any sense
            if parent_uuid == self.uuid:
                raise exception.ObjectActionError(
                    action='create',
                    reason='parent provider UUID cannot be same as UUID. '
                           'Please set parent provider UUID to None if '
                           'there is no parent.')

            # Verify that the parent exists

            query = """
                    MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                    RETURN parent
                    """ % parent_uuid
            result = db.execute(query)
            if not result:
                raise exception.ObjectActionError(
                    action='create',
                    reason='parent provider UUID does not exist.')
            parent_create_clause = """
                    WITH rp
                    MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                    WITH rp, parent
                    CREATE (parent)-[:CONTAINS]->(rp)
                    """
        # Create the RP, and if there is a parent, create the relationship
        query = """
                CREATE (rp:RESOURCE_PROVIDER {uuid: '%s', generation: 0,
                    created_at: timestamp(), updated_at: timestamp()})
                %s
                RETURN rp
                """ % (uuid, parent_create_clause)
        result = db.execute(query)
        rp = db.pythonize(result[0]["p"])
        self.generation = rp.generation

    @staticmethod
    @db_api.placement_context_manager.writer
    def _delete(context, uuid):
        # Do a quick check to see if the provider is a parent. If it is, don't
        # allow deleting the provider. Note that the foreign key constraint on
        # resource_providers.parent_provider_id will prevent deletion of the
        # parent within the transaction below. This is just a quick
        # short-circuit outside of the transaction boundary.
        if _has_child_providers(context, _id):
            raise exception.CannotDeleteParentResourceProvider()

        # Delete any inventory associated with the resource provider. This will
        # fail if the inventory has any allocations against it.
        query = """
                MATCH (me:RESOURCE_PROVIDER {uuid: '%s')-[:PROVIDES]->(inv)
                WITH me, inv
                DELETE inv
                """ % uuid
        try:
            result = db.execute(query)
        except db.ClientError:
            raise exception.ResourceProviderInUse()

        # Now delete the RP record
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                DETACH DELETE rp
                """ % uuid
        result = db.execute(query)

    @db_api.placement_context_manager.writer
    def _update_in_db(self, updates):
        # A list of resource providers in the same tree with the
        # resource provider to update
        context = self._context
        same_tree = []
        if 'parent_provider_uuid' in updates:
            # TODO(jaypipes): For now, "re-parenting" and "un-parenting" are
            # not possible. If the provider already had a parent, we don't
            # allow changing that parent due to various issues, including:
            #
            # * if the new parent is a descendant of this resource provider, we
            #   introduce the possibility of a loop in the graph, which would
            #   be very bad
            # * potentially orphaning heretofore-descendants
            #
            # So, for now, let's just prevent re-parenting...
            my_ids = provider_ids_from_uuid(context, self.uuid)
            parent_uuid = updates.pop('parent_provider_uuid')
            if parent_uuid is not None:
                # Get the current parent and the node with the new parent_uuid
                query = """
                        OPTIONAL MATCH (new:RESOURCE_PROVIDER {uuid: '%s'})
                        OPTIONAL MATCH (curr:RESOURCE_PROVIDER)-[:CONTAINS]->
                            (:RESOURCE_PROVIDER {uuid: '%s'})
                        RETURN new.uuid as new_uuid, curr.uuid as curr_uuid
                        """ % (parent_uuid, self.uuid)
                result = db.execute(query)
                rec = result[0]
                curr_parent_uuid = result[0]["curr_uuid"]
                new_parent_uuid = result[0]["new_uuid"]
                if not new_parent_uuid:
                    raise exception.ObjectActionError(
                        action='create',
                        reason='parent provider UUID does not exist.')
                if curr_parent_uuid != parent_provider_uuid:
                    raise exception.ObjectActionError(
                        action='update',
                        reason='re-parenting a provider is not currently '
                               'allowed.')
                if curr_parent_uuid is None:
                    # We can set the parent relationship, but first we need to
                    # check that this parent isn't already related to this
                    # node, or else we can get a circular relationship instead
                    # of a tree.
                    query = """
                            // Get all the inbound relations
                            MATCH (rp:RESOURCE_PROVIDER)-[*]->
                                (:RESOURCE_PROVIDER {uuid:'%s'})
                            RETURN rp
                            UNION
                            // Get all the outbound relations
                            MATCH (:RESOURCE_PROVIDER {uuid:'%s'})-[*]->
                                (rp:RESOURCE_PROVIDER)
                            RETURN rp
                            UNION
                            // Get this node
                            MATCH (rp:RESOURCE_PROVIDER {uuid:'%s'})
                            RETURN rp
                            """ (self.uuid, self.uuid, self.uuid)
                    result = db.execute(query)
                    tree_uuids = [rec["rp"]["uuid"] for rec in result]
                    if parent_uuid in tree_uuids:
                        raise exception.ObjectActionError(
                            action='update',
                            reason='creating loop in the provider tree is '
                                   'not allowed.')
                # Everything checks out; set the parent relationship
                query = """
                        MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                        WITH parent
                        CREATE (parent)-[:CONTAINS]->
                            (me:RESOURCE_PROVIDER uuid: '%s')
                        """ % (parent_uuid, self.uuid)
                result = db.execute(query)

            else:
                # Ensure that a null parent uuid is not being passed when there
                # already is a parent to this node.
                query = """
                        MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS]->
                            (:RESOURCE_PROVIDER {uuid: '%s'})
                        """ % self.uuid
                result = db.execute(query)
                if result:
                    raise exception.ObjectActionError(
                        action='update',
                        reason='un-parenting a provider is not currently '
                               'allowed.')

        update_text = ["%s = %s" % (k, v) for k, v in updates.items()]
        update_clause = ", ".join(update_text)
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp
                SET %s
                RETURN rp
                """ % (self.uuid, update_clause)
        result = db.execute(query)

        ##### WORK
        # We should also update the root providers of resource providers
        # originally in the same tree. If re-parenting is supported,
        # this logic should be changed to update only descendents of the
        # re-parented resource providers, not all the providers in the tree.
        for rp in same_tree:
            # If the parent is not updated, this clause is skipped since the
            # `same_tree` has no element.
            rp.root_provider_uuid = parent_ids.root_uuid
            db_rp = context.session.query(
                models.ResourceProvider).filter_by(id=rp.id).first()
            data = {'root_provider_id': parent_ids.root_id}
            db_rp.update(data)
            context.session.add(db_rp)

        try:
            context.session.flush()
        except sqla_exc.IntegrityError:
            # NOTE(jaypipes): Another thread snuck in and deleted the parent
            # for this resource provider in between the above check for a valid
            # parent provider and here...
            raise exception.ObjectActionError(
                action='update',
                reason='parent provider UUID does not exist.')

    @staticmethod
    @db_api.placement_context_manager.writer  # For online data migration
    def _from_db_object(context, resource_provider, db_resource_provider):
        # Online data migration to populate root_provider_id
        for field in ['uuid', 'name', 'generation',
                      'updated_at', 'created_at']:
            setattr(resource_provider, field, db_resource_provider[field])
        return resource_provider


@db_api.placement_context_manager.reader
def get_providers_with_shared_capacity(ctx, rc_id, amount, member_of=None):
    """Returns a list of resource provider IDs (internal IDs, not UUIDs)
    that have capacity for a requested amount of a resource and indicate that
    they share resource via an aggregate association.

    Shared resource providers are marked with a standard trait called
    MISC_SHARES_VIA_AGGREGATE. This indicates that the provider allows its
    inventory to be consumed by other resource providers associated via an
    aggregate link.

    For example, assume we have two compute nodes, CN_1 and CN_2, each with
    inventory of VCPU and MEMORY_MB but not DISK_GB (in other words, these are
    compute nodes with no local disk). There is a resource provider called
    "NFS_SHARE" that has an inventory of DISK_GB and has the
    MISC_SHARES_VIA_AGGREGATE trait. Both the "CN_1" and "CN_2" compute node
    resource providers and the "NFS_SHARE" resource provider are associated
    with an aggregate called "AGG_1".

    The scheduler needs to determine the resource providers that can fulfill a
    request for 2 VCPU, 1024 MEMORY_MB and 100 DISK_GB.

    Clearly, no single provider can satisfy the request for all three
    resources, since neither compute node has DISK_GB inventory and the
    NFS_SHARE provider has no VCPU or MEMORY_MB inventories.

    However, if we consider the NFS_SHARE resource provider as providing
    inventory of DISK_GB for both CN_1 and CN_2, we can include CN_1 and CN_2
    as potential fits for the requested set of resources.

    To facilitate that matching query, this function returns all providers that
    indicate they share their inventory with providers in some aggregate and
    have enough capacity for the requested amount of a resource.

    To follow the example above, if we were to call
    get_providers_with_shared_capacity(ctx, "DISK_GB", 100), we would want to
    get back the ID for the NFS_SHARE resource provider.

    :param rc_id: Internal ID of the requested resource class.
    :param amount: Amount of the requested resource.
    :param member_of: When present, contains a list of lists of aggregate
                      uuids that are used to filter the returned list of
                      resource providers that *directly* belong to the
                      aggregates referenced.
    """
    # The SQL we need to generate here looks like this:
    #
    # SELECT rp.id
    # FROM resource_providers AS rp
    #   INNER JOIN resource_provider_traits AS rpt
    #     ON rp.id = rpt.resource_provider_id
    #   INNER JOIN traits AS t
    #     ON rpt.trait_id = t.id
    #     AND t.name = "MISC_SHARES_VIA_AGGREGATE"
    #   INNER JOIN inventories AS inv
    #     ON rp.id = inv.resource_provider_id
    #     AND inv.resource_class_id = $rc_id
    #   LEFT JOIN (
    #     SELECT resource_provider_id, SUM(used) as used
    #     FROM allocations
    #     WHERE resource_class_id = $rc_id
    #     GROUP BY resource_provider_id
    #   ) AS usage
    #     ON rp.id = usage.resource_provider_id
    # WHERE COALESCE(usage.used, 0) + $amount <= (
    #   inv.total - inv.reserved) * inv.allocation_ratio
    # ) AND
    #   inv.min_unit <= $amount AND
    #   inv.max_unit >= $amount AND
    #   $amount % inv.step_size = 0
    # GROUP BY rp.id

    rp_tbl = sa.alias(_RP_TBL, name='rp')
    inv_tbl = sa.alias(_INV_TBL, name='inv')
    t_tbl = sa.alias(_TRAIT_TBL, name='t')
    rpt_tbl = sa.alias(_RP_TRAIT_TBL, name='rpt')

    rp_to_rpt_join = sa.join(
        rp_tbl, rpt_tbl,
        rp_tbl.c.id == rpt_tbl.c.resource_provider_id,
    )

    rpt_to_t_join = sa.join(
        rp_to_rpt_join, t_tbl,
        sa.and_(
            rpt_tbl.c.trait_id == t_tbl.c.id,
            # The traits table wants unicode trait names, but os_traits
            # presents native str, so we need to cast.
            t_tbl.c.name == six.text_type(os_traits.MISC_SHARES_VIA_AGGREGATE),
        ),
    )

    rp_to_inv_join = sa.join(
        rpt_to_t_join, inv_tbl,
        sa.and_(
            rpt_tbl.c.resource_provider_id == inv_tbl.c.resource_provider_id,
            inv_tbl.c.resource_class_id == rc_id,
        ),
    )

    usage = _usage_select([rc_id])

    inv_to_usage_join = sa.outerjoin(
        rp_to_inv_join, usage,
        inv_tbl.c.resource_provider_id == usage.c.resource_provider_id,
    )

    where_conds = _capacity_check_clause(amount, usage, inv_tbl=inv_tbl)

    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if member_of:
        rps_in_aggs = provider_ids_matching_aggregates(ctx, member_of)
        if not rps_in_aggs:
            # Short-circuit. The user either asked for a non-existing
            # aggregate or there were no resource providers that matched
            # the requirements...
            return []
        where_conds.append(rp_tbl.c.id.in_(rps_in_aggs))

    sel = sa.select([rp_tbl.c.id]).select_from(inv_to_usage_join)
    sel = sel.where(where_conds)
    sel = sel.group_by(rp_tbl.c.id)

    return [r[0] for r in ctx.session.execute(sel)]


@db_api.placement_context_manager.reader
def _get_all_by_filters_from_db(context, filters):
    # Eg. filters can be:
    #  filters = {
    #      'name': <name>,
    #      'uuid': <uuid>,
    #      'member_of': [[<aggregate_uuid>, <aggregate_uuid>],
    #                    [<aggregate_uuid>]]
    #      'forbidden_aggs': [<aggregate_uuid>, <aggregate_uuid>]
    #      'resources': {
    #          'VCPU': 1,
    #          'MEMORY_MB': 1024
    #      },
    #      'in_tree': <uuid>,
    #      'required': [<trait_name>, ...]
    #  }
    if not filters:
        filters = {}
    else:
        # Since we modify the filters, copy them so that we don't modify
        # them in the calling program.
        filters = copy.deepcopy(filters)
    name = filters.pop('name', None)
    uuid = filters.pop('uuid', None)
    member_of = filters.pop('member_of', [])
    forbidden_aggs = filters.pop('forbidden_aggs', [])
    required = set(filters.pop('required', []))
    forbidden = set([trait for trait in required
                     if trait.startswith('!')])
    required = required - forbidden
    forbidden = set([trait.lstrip('!') for trait in forbidden])
    resources = filters.pop('resources', {})

    rp = sa.alias(_RP_TBL, name="rp")
    root_rp = sa.alias(_RP_TBL, name="root_rp")
    parent_rp = sa.alias(_RP_TBL, name="parent_rp")

    cols = [
        rp.c.id,
        rp.c.uuid,
        rp.c.name,
        rp.c.generation,
        rp.c.updated_at,
        rp.c.created_at,
        root_rp.c.uuid.label("root_provider_uuid"),
        parent_rp.c.uuid.label("parent_provider_uuid"),
    ]

    # TODO(jaypipes): Convert this to an inner join once all
    # root_provider_id values are NOT NULL
    rp_to_root = sa.outerjoin(
        rp, root_rp,
        rp.c.root_provider_id == root_rp.c.id)
    rp_to_parent = sa.outerjoin(
        rp_to_root, parent_rp,
        rp.c.parent_provider_id == parent_rp.c.id)

    query = sa.select(cols).select_from(rp_to_parent)

    if name:
        query = query.where(rp.c.name == name)
    if uuid:
        query = query.where(rp.c.uuid == uuid)
    if 'in_tree' in filters:
        # The 'in_tree' parameter is the UUID of a resource provider that
        # the caller wants to limit the returned providers to only those
        # within its "provider tree". So, we look up the resource provider
        # having the UUID specified by the 'in_tree' parameter and grab the
        # root_provider_id value of that record. We can then ask for only
        # those resource providers having a root_provider_id of that value.
        tree_uuid = filters.pop('in_tree')
        tree_ids = provider_ids_from_uuid(context, tree_uuid)
        if tree_ids is None:
            # List operations should simply return an empty list when a
            # non-existing resource provider UUID is given.
            return []
        root_id = tree_ids.root_id
        # TODO(jaypipes): Remove this OR condition when root_provider_id
        # is not nullable in the database and all resource provider records
        # have populated the root provider ID.
        where_cond = sa.or_(
            rp.c.id == root_id,
            rp.c.root_provider_id == root_id)
        query = query.where(where_cond)

    # Get the provider IDs matching any specified traits and/or aggregates
    rp_ids, forbidden_rp_ids = get_provider_ids_for_traits_and_aggs(
        context, required, forbidden, member_of, forbidden_aggs)
    if rp_ids is None:
        # If no providers match the traits/aggs, we can short out
        return []
    if rp_ids:
        query = query.where(rp.c.id.in_(rp_ids))
    # forbidden providers, if found, are mutually exclusive with matching
    # providers above, so we only need to include this clause if we didn't
    # use the positive filter above.
    elif forbidden_rp_ids:
        query = query.where(~rp.c.id.in_(forbidden_rp_ids))

    for rc_name, amount in resources.items():
        rc_id = rc_cache.RC_CACHE.id_from_string(rc_name)
        rps_with_resource = get_providers_with_resource(context, rc_id, amount)
        rps_with_resource = (rp[0] for rp in rps_with_resource)
        query = query.where(rp.c.id.in_(rps_with_resource))

    res = context.session.execute(query).fetchall()
    return [dict(r) for r in res]


def get_all_by_filters(context, filters=None):
    """Returns a list of `ResourceProvider` objects that have sufficient
    resources in their inventories to satisfy the amounts specified in the
    `filters` parameter.

    If no resource providers can be found, the function will return an
    empty list.

    :param context: `placement.context.RequestContext` that may be used to
                    grab a DB connection.
    :param filters: Can be `name`, `uuid`, `member_of`, `in_tree` or
                    `resources` where `member_of` is a list of list of
                    aggregate UUIDs, `in_tree` is a UUID of a resource
                    provider that we can use to find the root provider ID
                    of the tree of providers to filter results by and
                    `resources` is a dict of amounts keyed by resource
                    classes.
    :type filters: dict
    """
    resource_providers = _get_all_by_filters_from_db(context, filters)
    return [ResourceProvider(context, **rp) for rp in resource_providers]


@db_api.placement_context_manager.reader
def get_provider_ids_having_any_trait(ctx, traits):
    """Returns a set of resource provider internal IDs that have ANY of the
    supplied traits.

    :param ctx: Session context to use
    :param traits: A map, keyed by trait string name, of trait internal IDs, at
                   least one of which each provider must have associated with
                   it.
    :raise ValueError: If traits is empty or None.
    """
    if not traits:
        raise ValueError('traits must not be empty')

    rptt = sa.alias(_RP_TRAIT_TBL, name="rpt")
    sel = sa.select([rptt.c.resource_provider_id])
    sel = sel.where(rptt.c.trait_id.in_(traits.values()))
    sel = sel.group_by(rptt.c.resource_provider_id)
    return set(r[0] for r in ctx.session.execute(sel))


@db_api.placement_context_manager.reader
def _get_provider_ids_having_all_traits(ctx, required_traits):
    """Returns a set of resource provider internal IDs that have ALL of the
    required traits.

    NOTE: Don't call this method with no required_traits.

    :param ctx: Session context to use
    :param required_traits: A map, keyed by trait string name, of required
                            trait internal IDs that each provider must have
                            associated with it
    :raise ValueError: If required_traits is empty or None.
    """
    if not required_traits:
        raise ValueError('required_traits must not be empty')

    rptt = sa.alias(_RP_TRAIT_TBL, name="rpt")
    sel = sa.select([rptt.c.resource_provider_id])
    sel = sel.where(rptt.c.trait_id.in_(required_traits.values()))
    sel = sel.group_by(rptt.c.resource_provider_id)
    # Only get the resource providers that have ALL the required traits, so we
    # need to GROUP BY the resource provider and ensure that the
    # COUNT(trait_id) is equal to the number of traits we are requiring
    num_traits = len(required_traits)
    cond = sa.func.count(rptt.c.trait_id) == num_traits
    sel = sel.having(cond)
    return set(r[0] for r in ctx.session.execute(sel))


@db_api.placement_context_manager.reader
def has_provider_trees(ctx):
    """Simple method that returns whether provider trees (i.e. nested resource
    providers) are in use in the deployment at all. This information is used to
    switch code paths when attempting to retrieve allocation candidate
    information. The code paths are eminently easier to execute and follow for
    non-nested scenarios...

    NOTE(jaypipes): The result of this function can be cached extensively.
    """
    sel = sa.select([_RP_TBL.c.id])
    sel = sel.where(_RP_TBL.c.parent_provider_id.isnot(None))
    sel = sel.limit(1)
    res = ctx.session.execute(sel).fetchall()
    return len(res) > 0


def get_provider_ids_for_traits_and_aggs(ctx, required_traits,
                                         forbidden_traits, member_of,
                                         forbidden_aggs):
    """Get internal IDs for all providers matching the specified traits/aggs.

    :return: A tuple of:
        filtered_rp_ids: A set of internal provider IDs matching the specified
            criteria. If None, work was done and resulted in no matching
            providers. This is in contrast to the empty set, which indicates
            that no filtering was performed.
        forbidden_rp_ids: A set of internal IDs of providers having any of the
            specified forbidden_traits.
    """
    filtered_rps = set()
    if required_traits:
        trait_map = _normalize_trait_map(ctx, required_traits)
        trait_rps = _get_provider_ids_having_all_traits(ctx, trait_map)
        filtered_rps = trait_rps
        LOG.debug("found %d providers after applying required traits filter "
                  "(%s)",
                  len(filtered_rps), list(required_traits))
        if not filtered_rps:
            return None, []

    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if member_of:
        rps_in_aggs = provider_ids_matching_aggregates(ctx, member_of)
        if filtered_rps:
            filtered_rps &= rps_in_aggs
        else:
            filtered_rps = rps_in_aggs
        LOG.debug("found %d providers after applying required aggregates "
                  "filter (%s)", len(filtered_rps), member_of)
        if not filtered_rps:
            return None, []

    forbidden_rp_ids = set()
    if forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(ctx, [forbidden_aggs])
        forbidden_rp_ids |= rps_bad_aggs
        if filtered_rps:
            filtered_rps -= rps_bad_aggs
            LOG.debug("found %d providers after applying forbidden aggregates "
                      "filter (%s)", len(filtered_rps), forbidden_aggs)
            if not filtered_rps:
                return None, []

    if forbidden_traits:
        trait_map = _normalize_trait_map(ctx, forbidden_traits)
        rps_bad_traits = get_provider_ids_having_any_trait(ctx, trait_map)
        forbidden_rp_ids |= rps_bad_traits
        if filtered_rps:
            filtered_rps -= rps_bad_traits
            LOG.debug("found %d providers after applying forbidden traits "
                      "filter (%s)", len(filtered_rps), list(forbidden_traits))
            if not filtered_rps:
                return None, []

    return filtered_rps, forbidden_rp_ids


def _normalize_trait_map(ctx, traits):
    if not isinstance(traits, dict):
        return trait_obj.ids_from_names(ctx, traits)
    return traits


@db_api.placement_context_manager.reader
def get_provider_ids_matching(ctx, resources, required_traits,
                              forbidden_traits, member_of, forbidden_aggs,
                              tree_root_id):
    """Returns a list of tuples of (internal provider ID, root provider ID)
    that have available inventory to satisfy all the supplied requests for
    resources. If no providers match, the empty list is returned.

    :note: This function is used to get results for (a) a RequestGroup with
           use_same_provider=True in a granular request, or (b) a short cut
           path for scenarios that do NOT involve sharing or nested providers.
           Each `internal provider ID` represents a *single* provider that
           can satisfy *all* of the resource/trait/aggregate criteria. This is
           in contrast with get_trees_matching_all(), where each provider
           might only satisfy *some* of the resources, the rest of which are
           satisfied by other providers in the same tree or shared via
           aggregate.

    :param ctx: Session context to use
    :param resources: A dict, keyed by resource class ID, of the amount
                      requested of that resource class.
    :param required_traits: A map, keyed by trait string name, of required
                            trait internal IDs that each provider must have
                            associated with it
    :param forbidden_traits: A map, keyed by trait string name, of forbidden
                             trait internal IDs that each provider must not
                             have associated with it
    :param member_of: An optional list of list of aggregate UUIDs. If provided,
                      the allocation_candidates returned will only be for
                      resource providers that are members of one or more of the
                      supplied aggregates of each aggregate UUID list.
    :param forbidden_aggs: An optional list of aggregate UUIDs. If provided,
                           the allocation_candidates returned will only be for
                           resource providers that are NOT members of supplied
                           aggregates.
    :param tree_root_id: An optional root resource provider ID. If provided,
                         the result will be restricted to providers in the tree
                         with this root ID.
    """
    # The iteratively filtered set of resource provider internal IDs that match
    # all the constraints in the request
    filtered_rps, forbidden_rp_ids = get_provider_ids_for_traits_and_aggs(
        ctx, required_traits, forbidden_traits, member_of, forbidden_aggs)
    if filtered_rps is None:
        # If no providers match the traits/aggs, we can short out
        return []

    # Instead of constructing a giant complex SQL statement that joins multiple
    # copies of derived usage tables and inventory tables to each other, we do
    # one query for each requested resource class. This allows us to log a
    # rough idea of which resource class query returned no results (for
    # purposes of rough debugging of a single allocation candidates request) as
    # well as reduce the necessary knowledge of SQL in order to understand the
    # queries being executed here.
    #
    # NOTE(jaypipes): The efficiency of this operation may be improved by
    # passing the trait_rps and/or forbidden_ip_ids iterables to the
    # get_providers_with_resource() function so that we don't have to process
    # as many records inside the loop below to remove providers from the
    # eventual results list
    provs_with_resource = set()
    first = True
    for rc_id, amount in resources.items():
        rc_name = rc_cache.RC_CACHE.string_from_id(rc_id)
        provs_with_resource = get_providers_with_resource(
            ctx, rc_id, amount, tree_root_id=tree_root_id)
        LOG.debug("found %d providers with available %d %s",
                  len(provs_with_resource), amount, rc_name)
        if not provs_with_resource:
            return []

        rc_rp_ids = set(p[0] for p in provs_with_resource)
        # The branching below could be collapsed code-wise, but is in place to
        # make the debug logging clearer.
        if first:
            first = False
            if filtered_rps:
                filtered_rps &= rc_rp_ids
                LOG.debug("found %d providers after applying initial "
                          "aggregate and trait filters", len(filtered_rps))
            else:
                filtered_rps = rc_rp_ids
                # The following condition is not necessary for the logic; just
                # prevents the message from being logged unnecessarily.
                if forbidden_rp_ids:
                    # Forbidden trait/aggregate filters only need to be applied
                    # a) on the first iteration; and
                    # b) if not already set up before the loop
                    # ...since any providers in the resulting set are the basis
                    # for intersections, and providers with forbidden traits
                    # are already absent from that set after we've filtered
                    # them once.
                    filtered_rps -= forbidden_rp_ids
                    LOG.debug("found %d providers after applying forbidden "
                              "traits/aggregates", len(filtered_rps))
        else:
            filtered_rps &= rc_rp_ids
            LOG.debug("found %d providers after filtering by previous result",
                      len(filtered_rps))

        if not filtered_rps:
            return []

    # provs_with_resource will contain a superset of providers with IDs still
    # in our filtered_rps set. We return the list of tuples of
    # (internal provider ID, root internal provider ID)
    return [rpids for rpids in provs_with_resource if rpids[0] in filtered_rps]


@db_api.placement_context_manager.reader
def get_providers_with_resource(ctx, rc_id, amount, tree_root_id=None):
    """Returns a set of tuples of (provider ID, root provider ID) of providers
    that satisfy the request for a single resource class.

    :param ctx: Session context to use
    :param rc_id: Internal ID of resource class to check inventory for
    :param amount: Amount of resource being requested
    :param tree_root_id: An optional root provider ID. If provided, the results
                         are limited to the resource providers under the given
                         root resource provider.
    """
    # SELECT rp.id, rp.root_provider_id
    # FROM resource_providers AS rp
    # JOIN inventories AS inv
    #  ON rp.id = inv.resource_provider_id
    #  AND inv.resource_class_id = $RC_ID
    # LEFT JOIN (
    #  SELECT
    #    alloc.resource_provider_id,
    #    SUM(allocs.used) AS used
    #  FROM allocations AS alloc
    #  WHERE allocs.resource_class_id = $RC_ID
    #  GROUP BY allocs.resource_provider_id
    # ) AS usage
    #  ON inv.resource_provider_id = usage.resource_provider_id
    # WHERE
    #  used + $AMOUNT <= ((total - reserved) * inv.allocation_ratio)
    #  AND inv.min_unit <= $AMOUNT
    #  AND inv.max_unit >= $AMOUNT
    #  AND $AMOUNT % inv.step_size == 0
    rpt = sa.alias(_RP_TBL, name="rp")
    inv = sa.alias(_INV_TBL, name="inv")
    usage = _usage_select([rc_id])
    rp_to_inv = sa.join(
        rpt, inv, sa.and_(
            rpt.c.id == inv.c.resource_provider_id,
            inv.c.resource_class_id == rc_id))
    inv_to_usage = sa.outerjoin(
        rp_to_inv, usage,
        inv.c.resource_provider_id == usage.c.resource_provider_id)
    sel = sa.select([rpt.c.id, rpt.c.root_provider_id])
    sel = sel.select_from(inv_to_usage)
    where_conds = _capacity_check_clause(amount, usage, inv_tbl=inv)
    if tree_root_id is not None:
        where_conds = sa.and_(
            # TODO(tetsuro): Bug#1799892: Remove this "or" condition in Train
            sa.or_(rpt.c.root_provider_id == tree_root_id,
                   rpt.c.id == tree_root_id),
            where_conds)
    sel = sel.where(where_conds)
    res = ctx.session.execute(sel).fetchall()
    res = set((r[0], r[1]) for r in res)
    # TODO(tetsuro): Bug#1799892: We could have old providers with no root
    # provider set and they haven't undergone a data migration yet,
    # so we need to set the root_id explicitly here. We remove
    # this and when all root_provider_id values are NOT NULL
    ret = []
    for rp_tuple in res:
        rp_id = rp_tuple[0]
        root_id = rp_id if rp_tuple[1] is None else rp_tuple[1]
        ret.append((rp_id, root_id))
    return ret


@db_api.placement_context_manager.reader
def _get_trees_with_traits(ctx, rp_ids, required_traits, forbidden_traits):
    """Given a list of provider IDs, filter them to return a set of tuples of
    (provider ID, root provider ID) of providers which belong to a tree that
    can satisfy trait requirements.

    :param ctx: Session context to use
    :param rp_ids: a set of resource provider IDs
    :param required_traits: A map, keyed by trait string name, of required
                            trait internal IDs that each provider TREE must
                            COLLECTIVELY have associated with it
    :param forbidden_traits: A map, keyed by trait string name, of trait
                             internal IDs that a resource provider must
                             not have.
    """
    # We now want to restrict the returned providers to only those provider
    # trees that have all our required traits.
    #
    # The SQL we want looks like this:
    #
    # SELECT outer_rp.id, outer_rp.root_provider_id
    # FROM resource_providers AS outer_rp
    # JOIN (
    #   SELECT rp.root_provider_id
    #   FROM resource_providers AS rp
    #   # Only if we have required traits...
    #   INNER JOIN resource_provider_traits AS rptt
    #   ON rp.id = rptt.resource_provider_id
    #   AND rptt.trait_id IN ($REQUIRED_TRAIT_IDS)
    #   # Only if we have forbidden_traits...
    #   LEFT JOIN resource_provider_traits AS rptt_forbid
    #   ON rp.id = rptt_forbid.resource_provider_id
    #   AND rptt_forbid.trait_id IN ($FORBIDDEN_TRAIT_IDS)
    #   WHERE rp.id IN ($RP_IDS)
    #   # Only if we have forbidden traits...
    #   AND rptt_forbid.resource_provider_id IS NULL
    #   GROUP BY rp.root_provider_id
    #   # Only if have required traits...
    #   HAVING COUNT(DISTINCT rptt.trait_id) == $NUM_REQUIRED_TRAITS
    # ) AS trees_with_traits
    #  ON outer_rp.root_provider_id = trees_with_traits.root_provider_id
    rpt = sa.alias(_RP_TBL, name="rp")
    cond = [rpt.c.id.in_(rp_ids)]
    subq = sa.select([rpt.c.root_provider_id])
    subq_join = None
    if required_traits:
        rptt = sa.alias(_RP_TRAIT_TBL, name="rptt")
        rpt_to_rptt = sa.join(
            rpt, rptt, sa.and_(
                rpt.c.id == rptt.c.resource_provider_id,
                rptt.c.trait_id.in_(required_traits.values())))
        subq_join = rpt_to_rptt
        # Only get the resource providers that have ALL the required traits,
        # so we need to GROUP BY the root provider and ensure that the
        # COUNT(trait_id) is equal to the number of traits we are requiring
        num_traits = len(required_traits)
        having_cond = sa.func.count(sa.distinct(rptt.c.trait_id)) == num_traits
        subq = subq.having(having_cond)

    # Tack on an additional LEFT JOIN clause inside the derived table if we've
    # got forbidden traits in the mix.
    if forbidden_traits:
        rptt_forbid = sa.alias(_RP_TRAIT_TBL, name="rptt_forbid")
        join_to = rpt
        if subq_join is not None:
            join_to = subq_join
        rpt_to_rptt_forbid = sa.outerjoin(
            join_to, rptt_forbid, sa.and_(
                rpt.c.id == rptt_forbid.c.resource_provider_id,
                rptt_forbid.c.trait_id.in_(forbidden_traits.values())))
        cond.append(rptt_forbid.c.resource_provider_id == sa.null())
        subq_join = rpt_to_rptt_forbid

    subq = subq.select_from(subq_join)
    subq = subq.where(sa.and_(*cond))
    subq = subq.group_by(rpt.c.root_provider_id)
    trees_with_traits = sa.alias(subq, name="trees_with_traits")

    outer_rps = sa.alias(_RP_TBL, name="outer_rps")
    outer_to_subq = sa.join(
        outer_rps, trees_with_traits,
        outer_rps.c.root_provider_id == trees_with_traits.c.root_provider_id)
    sel = sa.select([outer_rps.c.id, outer_rps.c.root_provider_id])
    sel = sel.select_from(outer_to_subq)
    res = ctx.session.execute(sel).fetchall()

    return [(rp_id, root_id) for rp_id, root_id in res]


@db_api.placement_context_manager.reader
def get_trees_matching_all(ctx, resources, required_traits, forbidden_traits,
                           sharing, member_of, forbidden_aggs, tree_root_id):
    """Returns a RPCandidates object representing the providers that satisfy
    the request for resources.

    If traits are also required, this function only returns results where the
    set of providers within a tree that satisfy the resource request
    collectively have all the required traits associated with them. This means
    that given the following provider tree:

    cn1
     |
     --> pf1 (SRIOV_NET_VF:2)
     |
     --> pf2 (SRIOV_NET_VF:1, HW_NIC_OFFLOAD_GENEVE)

    If a user requests 1 SRIOV_NET_VF resource and no required traits will
    return both pf1 and pf2. However, a request for 2 SRIOV_NET_VF and required
    trait of HW_NIC_OFFLOAD_GENEVE will return no results (since pf1 is the
    only provider with enough inventory of SRIOV_NET_VF but it does not have
    the required HW_NIC_OFFLOAD_GENEVE trait).

    :note: This function is used for scenarios to get results for a
    RequestGroup with use_same_provider=False. In this scenario, we are able
    to use multiple providers within the same provider tree including sharing
    providers to satisfy different resources involved in a single RequestGroup.

    :param ctx: Session context to use
    :param resources: A dict, keyed by resource class ID, of the amount
                      requested of that resource class.
    :param required_traits: A map, keyed by trait string name, of required
                            trait internal IDs that each provider TREE must
                            COLLECTIVELY have associated with it
    :param forbidden_traits: A map, keyed by trait string name, of trait
                             internal IDs that a resource provider must
                             not have.
    :param sharing: dict, keyed by resource class ID, of lists of resource
                    provider IDs that share that resource class and can
                    contribute to the overall allocation request
    :param member_of: An optional list of lists of aggregate UUIDs. If
                      provided, the allocation_candidates returned will only be
                      for resource providers that are members of one or more of
                      the supplied aggregates in each aggregate UUID list.
    :param forbidden_aggs: An optional list of aggregate UUIDs. If provided,
                           the allocation_candidates returned will only be for
                           resource providers that are NOT members of supplied
                           aggregates.
    :param tree_root_id: An optional root provider ID. If provided, the results
                         are limited to the resource providers under the given
                         root resource provider.
    """
    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if member_of:
        rps_in_aggs = provider_ids_matching_aggregates(ctx, member_of)
        if not rps_in_aggs:
            # Short-circuit. The user either asked for a non-existing
            # aggregate or there were no resource providers that matched
            # the requirements...
            return rp_candidates.RPCandidateList()

    if forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(ctx, [forbidden_aggs])

    # To get all trees that collectively have all required resource,
    # aggregates and traits, we use `RPCandidateList` which has a list of
    # three-tuples with the first element being resource provider ID, the
    # second element being the root provider ID and the third being resource
    # class ID.
    provs_with_inv = rp_candidates.RPCandidateList()

    for rc_id, amount in resources.items():
        rc_name = rc_cache.RC_CACHE.string_from_id(rc_id)

        provs_with_inv_rc = rp_candidates.RPCandidateList()
        rc_provs_with_inv = get_providers_with_resource(
            ctx, rc_id, amount, tree_root_id=tree_root_id)
        provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_id)
        LOG.debug("found %d providers under %d trees with available %d %s",
                  len(provs_with_inv_rc), len(provs_with_inv_rc.trees),
                  amount, rc_name)
        if not provs_with_inv_rc:
            # If there's no providers that have one of the resource classes,
            # then we can short-circuit returning an empty RPCandidateList
            return rp_candidates.RPCandidateList()

        sharing_providers = sharing.get(rc_id)
        if sharing_providers and tree_root_id is None:
            # There are sharing providers for this resource class, so we
            # should also get combinations of (sharing provider, anchor root)
            # in addition to (non-sharing provider, anchor root) we've just
            # got via get_providers_with_resource() above. We must skip this
            # process if tree_root_id is provided via the ?in_tree=<rp_uuid>
            # queryparam, because it restricts resources from another tree.
            rc_provs_with_inv = anchors_for_sharing_providers(
                ctx, sharing_providers, get_id=True)
            provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_id)
            LOG.debug(
                "considering %d sharing providers with %d %s, "
                "now we've got %d provider trees",
                len(sharing_providers), amount, rc_name,
                len(provs_with_inv_rc.trees))

        if member_of:
            # Aggregate on root spans the whole tree, so the rp itself
            # *or its root* should be in the aggregate
            provs_with_inv_rc.filter_by_rp_or_tree(rps_in_aggs)
            LOG.debug("found %d providers under %d trees after applying "
                      "aggregate filter %s",
                      len(provs_with_inv_rc.rps), len(provs_with_inv_rc.trees),
                      member_of)
            if not provs_with_inv_rc:
                # Short-circuit returning an empty RPCandidateList
                return rp_candidates.RPCandidateList()
        if forbidden_aggs:
            # Aggregate on root spans the whole tree, so the rp itself
            # *and its root* should be outside the aggregate
            provs_with_inv_rc.filter_by_rp_nor_tree(rps_bad_aggs)
            LOG.debug("found %d providers under %d trees after applying "
                      "negative aggregate filter %s",
                      len(provs_with_inv_rc.rps), len(provs_with_inv_rc.trees),
                      forbidden_aggs)
            if not provs_with_inv_rc:
                # Short-circuit returning an empty RPCandidateList
                return rp_candidates.RPCandidateList()

        # Adding the resource providers we've got for this resource class,
        # filter provs_with_inv to have only trees with enough inventories
        # for this resource class. Here "tree" includes sharing providers
        # in its terminology
        provs_with_inv.merge_common_trees(provs_with_inv_rc)
        LOG.debug(
            "found %d providers under %d trees after filtering by "
            "previous result",
            len(provs_with_inv.rps), len(provs_with_inv.trees))
        if not provs_with_inv:
            return rp_candidates.RPCandidateList()

    if (not required_traits and not forbidden_traits) or (
            any(sharing.values())):
        # If there were no traits required, there's no difference in how we
        # calculate allocation requests between nested and non-nested
        # environments, so just short-circuit and return. Or if sharing
        # providers are in play, we check the trait constraints later
        # in _alloc_candidates_multiple_providers(), so skip.
        return provs_with_inv

    # Return the providers where the providers have the available inventory
    # capacity and that set of providers (grouped by their tree) have all
    # of the required traits and none of the forbidden traits
    rp_tuples_with_trait = _get_trees_with_traits(
        ctx, provs_with_inv.rps, required_traits, forbidden_traits)
    provs_with_inv.filter_by_rp(rp_tuples_with_trait)
    LOG.debug("found %d providers under %d trees after applying "
              "traits filter - required: %s, forbidden: %s",
              len(provs_with_inv.rps), len(provs_with_inv.trees),
              list(required_traits), list(forbidden_traits))

    return provs_with_inv
