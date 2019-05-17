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
            MATCH p=(rp:RESOURCE_PROVIDER {uuid: '%s'})-->(inv)<-[:USES]-(cs)
            WITH relationships(p)[0] AS usages, labels(inv)[0] AS rcname
            RETURN rcname, sum(usages.amount) AS used
    """ % rp.uuid
    result = db.execute(query)
    if not result:
        return {}
    allocs = {rec["rc"]: rec["used"] for rec in result}
    if rcs:
        for rc in set(rcs):
            allocs.pop(rc, None)
    return allocs


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
                MATCH p=(rp {uuid: '%s'})-[:PROVIDES]->(rc:%s)
                WITH relationships(p)[0] AS rel, rc
                DELETE rel
                RETURN id(rc) AS rcid
        """ % (rp.uuid, rc)
        result = db.execute()
        rcid = result[0]["rcid"]
        # Now delete the inventory
        query = """
                MATCH (inv)
                WHERE id(inv) = %s
                WITH inv
                DELETE inv
        """ % rcid
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
            "No inventory of class %s found for delete" % resource_class)
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
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:ASSOCIATED]-> (agg)
            RETURN agg.uuid AS agg_uuid
    """ % rp.uuid
    result = db.execute(query)
    return [rec[agg_uuid] for rec in result]

@db_api.placement_context_manager.reader
def anchors_for_sharing_providers(context, rp_uuids):
    """Given a list of UUIDs of sharing providers, returns a set of
    tuples of (sharing provider UUID, anchor provider UUID), where each of
    anchor is the unique root provider of a tree associated with the same
    aggregate as the sharing provider. (These are the providers that can
    "anchor" a single AllocationRequest.)

    If the sharing provider is not part of any aggregate, the empty list is
    returned.
    """
    query = """
            MATCH (anchor:RESOURCE_PROVIDER)-[*]->()-[:ASSOCIATED]->
                (shared:RESOURCE_PROVIDER)
            WHERE shared.uuid in %s
            RETURN shared.uuid as s_uuid, anchor.uuid AS a_uuid 
    """ % rp_uuids
    result = db.execute(query)
    return set((rec["s_uuid"], rec["a_uuid"]) for rec in result)


def _ensure_aggregate(ctx, agg_uuid):
    """Finds an aggregate and returns its UUID (which is the same as the
    supplied parameter). If not found, creates the aggregate with the supplied
    UUID and returns the new aggregate's UUID.
    """
    query = """
            MERGE (agg:AGGREGATE {uuid: '%s'})
            RETURN agg
    """ % agg_uuid
    result = db.execute(query)
    return agg_uuid


@db_api.placement_context_manager.writer
def _set_aggregates(context, resource_provider, provided_aggregates,
                    increment_generation=False):
    """When aggregate uuids are persisted no validation is done to ensure that
    they refer to something that has meaning elsewhere. It is assumed that code
    which makes use of the aggregates, later, will validate their fitness.
    TODO(cdent): At the moment we do not delete a PlacementAggregate that no
    longer has any associations with at least one resource provider. We may
    wish to do that to avoid bloat if it turns out we're creating a lot of
    noise. Not doing now to move things along.
    """
    provided_aggregates = set(provided_aggregates)
    existing_aggregates = _get_aggregates_by_provider(context,
            resource_provider)
    agg_uuids_to_add = provided_aggregates - set(existing_aggregates)
    # A list of aggregate UUIDs that will be associated with the provider
    aggs_to_associate = []
    # Same list for those aggregates to remove the association with this
    # provider
    aggs_to_disassociate = [agg_uuid for agg_uuid in existing_aggregates
            if agg_uuid not in provided_aggregates]

    stmnt = "MERGE (rp)-[:ASSOCIATED]->(agg:AGGREGATE {uuid: '%s'})"
    creates = [stmnt % agg_uuid for agg_uuid in agg_uuids_to_add]
    create_clause = "\n".join(creates)
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            %s
            RETURN rp
    """ % (resource_provider.uuid, create_clause)
    result = db.execute(query)

    # Delete the agg relationships no longer needed
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[a:ASSOCIATED]->(agg:AGGREGATE)
            WITH rp, a, agg
            WHERE agg.uuid in %s
            DELETE a
    """ % (resource_provider.uuid, aggs_to_disassociate)
    result = db.execute(query)
    if increment_generation:
        resource_provider.increment_generation()
    return


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
    trait_names = trait_obj.Trait.get_all_names(context)
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
    del_list = []
    for del_trait in to_delete:
        del_list.append("rp.%s" % del_trait)
    del_clause = ", ".join(del_list)
    if del_clause:
        del_clause = "REMOVE " + del_clause
    # Add the new traits, if any
    add_list = []
    for add_trait in to_add:
        add_list.append("rp.%s = true" % add_trait)
    add_clause = ", ".join(add_list)
    if add_clause:
        add_clause = "SET " + add_clause
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            %s
            %s
            RETURN rp
    """ % (rp.uuid, add_clause, del_clause)
    result = db.execute(query)
    rp.increment_generation()


@db_api.placement_context_manager.reader
def _has_child_providers(context, rp_id):
    """Returns True if the supplied resource provider has any child providers,
    False otherwise
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:CONTAINS]-(child)
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

    NOTE (edleafe): This is not needed with a graph DB, and should be removed.
    """
    return


@db_api.placement_context_manager.writer
def set_root_provider_ids(context, batch_size):
    """Simply sets the root_provider_id value for a provider identified by
    rp_id. Used in explicit online data migration via CLI.

    :param rp_id: Internal ID of the provider to update
    :param root_id: Value to set root provider to

    NOTE (edleafe): This is not needed with a graph DB, and should be removed.
    """
    return


ProviderIds = collections.namedtuple(
        "ProviderIds", "uuid parent_uuid root_uuid")


def provider_ids_from_rp_uuids(context, rp_uuids):
    """Given an iterable of resource provider UUIDs, returns a dict,
    keyed by provider UUID, of ProviderIds namedtuples describing those
    providers.

    :returns: dict, keyed by internal provider Id, of ProviderIds namedtuples
    :param rp_iuuids: iterable of provider UUIDs to look up
    """
    query = """
	    MATCH (rp:RESOURCE_PROVIDER)
	    WHERE rp.uuid IN %s
	    WITH rp
	    OPTIONAL MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS*1]->(rp)
	    WITH rp, parent
	    OPTIONAL MATCH (root:RESOURCE_PROVIDER)-[:CONTAINS*1..99]->(parent)
            WITH rp.uuid AS uuid, parent.uuid AS parent_uuid,
		 root.uuid AS root_uuid
            ORDER BY uuid, parent_uuid, root_uuid
            RETURN uuid, parent_uuid, root_uuid
    """ % rp_uuids
    result = db.execute(query)
    return [ProviderIds(**rec) for rec in result]


def provider_ids_from_uuid(context, uuid):
    """Given the UUID of a resource provider, returns a namedtuple
    (ProviderIds) with the UUID, parent provider's UUID, and the root provider
    UUID.

    :returns: ProviderIds object containing the UUIDs of the provider
              identified by the supplied UUID, or None if there is no
              ResourceProvider with a matching uuid.
    :param uuid: The UUID of the provider to look up
    """
    return provider_ids_from_rp_uuids([uuid])[0]


def provider_ids_matching_aggregates(context, member_of, rp_uuids=None):
    """Given a list of lists of aggregate UUIDs, return the UUIDs of all
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

    :returns: A set of resource provider UUIDs having all required
              aggregate associations
    """
    rp_clause = "rp.uuid IN %s" % rp_uuids if rp_uuids else ""
    agg_filters = ["agg.uuid in %s" % agg_uuid_list for agg in member_of]
    agg_clause = " AND ".join(agg_filters)
    query = """
            MATCH (rp:RESOURCE_PROVIDER)-[:ASSOCIATED]->(agg:AGGREGATE)
            WHERE
            {rp_clause}
            {agg_clause}
            RETURN rp.uuid AS rp_uuid
    """.format("rp_clause": rp_clause, "agg_clause": agg_clause)
    result = db.execute(query)
    return set([rec["rp_uuid"] for rec in result])


class ResourceProvider(object):
    SETTABLE_FIELDS = ('name', 'parent_provider_uuid')

    def __init__(self, context, uuid=None, name=None,
                 generation=None, parent_provider_uuid=None,
                 root_provider_uuid=None, updated_at=None, created_at=None):
        self._context = context
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
        # NOTE(edleafe): Neither of the above fields are used in a graph DB,
        # and will be removed.
        self.updated_at = updated_at
        self.created_at = created_at

    def create(self):
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

    @staticmethod
    @db_api.placement_context_manager.writer  # For online data migration
    def _from_db_object(context, resource_provider, db_resource_provider):
        # Online data migration to populate root_provider_id
        for field in ['uuid', 'name', 'generation',
                      'updated_at', 'created_at']:
            setattr(resource_provider, field, db_resource_provider[field])
        return resource_provider


##### WORK
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
    query = """
            MATCH ()-[:ASSOCIATES]->(rp:RESOURCE_PROVIDER)
            WITH rp
            MATCH (rp)-[:PROVIDES]->(rc:%s)
            WITH rp, rc
            OPTIONAL MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH rp, rc, relationships(p)[0] AS usages
            WITH rp, rc, sum(usages.amount) AS total_used
            MATCH (rc)
            WHERE rc.total - total_used >= %s
            RETURN rp.uuid AS rp_uuid
    """
    result = db.execute(query)
    return [rec["rp_uuid"] for rec in result]


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
	# TODO: EGL: Traits no longer have IDs
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
def get_providers_with_resource(ctx, rc_name, amount, tree_root_uuid=None):
    """Returns a set of tuples of (provider UUID, root provider UUID) of
    providers that satisfy the request for a single resource class.

    :param ctx: Session context to use
    :param rc_name: Name of the resource class to check inventory for
    :param amount: Amount of resource being requested
    :param tree_root_uuid: An optional root provider UUID. If provided, the
                           results are limited to the resource providers under
                           the given root resource provider.
    """
    tree_root_clause = ""
    if tree_root_uuid:
        tree_root_clause = " {uuid: '%s'}" % tree_root_uuid
    query = """
            MATCH (root:RESOURCE_PROVIDER{rt})-[*0..99]->
                (rp:RESOURCE_PROVIDER)-[:PROVIDES]->(rc:{rc_name})
            WITH root, rp, rc, ((rc.total - rc.reserved) * rc.allocation_ratio)
                AS capacity
            OPTIONAL MATCH par=(parent:RESOURCE_PROVIDER)-[*]->(root)
            WITH root, size(relationships(par)) AS numrel, rp, rc, capacity
            OPTIONAL MATCH p=(:CONSUMER)-[:USES]->(rc)
            WITH root, numrel, rp, rc, capacity, last(relationships(p)) AS uses
            WITH root, numrel, rp, rc, capacity - sum(uses.amount) AS avail
            WHERE avail >= {amount}
            AND rc.min_unit <= {amount}
            AND rc.max_unit >= {amount}
            AND {amount} % rc.step_size = 0
            AND numrel IS null
            RETURN rp, root
    """.format(rt=tree_root_clause, rc_name=rc_name, amount=amount)
    result = db.execute(query)
    return = set((rec["rp"], rec["root"]) for rec in result)


@db_api.placement_context_manager.reader
def _get_trees_with_traits(ctx, rp_uuids, required_traits, forbidden_traits):
    """Given a list of provider UUIDs, filter them to return a set of tuples of
    (provider UUID, root provider UUID) of providers which belong to a tree
    that can satisfy trait requirements.

    :param ctx: Session context to use
    :param rp_uuids: a set of resource provider UUIDs
    :param required_traits: A list of trait string names of required traits
                            that each provider TREE must COLLECTIVELY have
                            associated with it
    :param forbidden_traits: A list of trait string names of required traits
                             that each resource provider must not have.
    """
    if not required_traits or forbidden_traits:
        # Nothing to do
        return rp_uuids
    good = ["exists(rp.%s)" % t for t in required_traits]
    bad = ["not exists(rp.%s)" % t for t in forbidden_traits]
    req_clause = " AND ".join(good)
    forbid_clause = " AND ".join(bad)
    trait_clause = ""
    if req_clause:
        trait_clause += " AND %s" % req_clause
    if forbid_clause:
        trait_clause += " AND %s" % forbid_clause
    query = """
            MATCH (rp:RESOURCE_PROVIDER)
            WHERE rp.uuid IN {rp_uuids}
            {trait_clause}
            WITH rp
            MATCH p=()-[]->(root:RESOURCE_PROVIDER)-->(rp)
	    WITH p, relationships(p) AS relp, root, rp
	    WITH p, relp, size(relp) AS numrel, root, rp
	    WITH rp, root, numrel, max(numrel) AS maxrel
            WHERE numrel = maxrel
            ORDER BY rp, root
            RETURN rp, root
    """
    result = db.execute(query)
    return ((rec["rp"], rec["root"]) for rec in result)


@db_api.placement_context_manager.reader
def get_trees_matching_all(ctx, resources, required_traits, forbidden_traits,
        sharing, member_of, forbidden_aggs, tree_root_uuid):
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
