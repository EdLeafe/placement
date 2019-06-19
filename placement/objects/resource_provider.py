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

from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_log import log as logging
from oslo_utils import excutils
import six

from placement.db import graph_db as db
from placement import db_api
from placement import exception
from placement.objects import inventory as inv_obj
from placement.objects import research_context as res_ctx
from placement.objects import trait as trait_obj
from placement import resource_class_cache as rc_cache


LOG = logging.getLogger(__name__)


def _get_current_inventory_resources(ctx, rp):
    """Returns a set() containing the names of the resource classes for all
    resources currently having an inventory record for the supplied resource
    provider.

    :param ctx: `placement.context.RequestContext` that may be used to grab a
                DB connection.
    :param rp: Resource provider to query inventory for.
    """
    rp_uuid = rp if isinstance(rp, six.string_types) else rp.uuid
    query = """
            MATCH (rp {uuid: '%s'})-[:PROVIDES]->(rc)
            RETURN labels(rc)[0] AS rc_name
    """ % rp_uuid
    result = ctx.tx.run(query).data()
    resources = [rec["rc_name"] for rec in result]
    return set(resources)


def has_allocations(ctx, rp, rcs=None):
    """Returns True if there are any allocations against resources from the
    specified provider. If `rcs` is specified, the check is limited to
    allocations against just the resource classes specified.
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-->(inv)<-[:USES]-(cs)
            RETURN labels(inv)[0] AS rc
    """ % rp.uuid
    result = ctx.tx.run(query).data()
    if not result:
        return False
    if rcs:
        allocs = [rec["rc"] for rec in result]
        for rc in rc:
            if rc in allocs:
                return True
        return False
    return True


def get_allocated_inventory(ctx, rp, rcs=None):
    """Returns the list of resource classes for any inventory that has
    allocations against it, along with the total amount allocated. If `rcs` is
    specified, the returned list is limited to only those resource classes in
    `rcs` that have allocations.
    """
    rp_uuid = rp if isinstance(rp, six.string_types) else rp.uuid
    query = """
            MATCH p=(rp:RESOURCE_PROVIDER {uuid: '%s'})-->(inv)<-[:USES]-(cs)
            WITH last(relationships(p)) AS usages, labels(inv)[0] AS rcname
            RETURN rcname, sum(usages.amount) AS used
    """ % rp_uuid
    result = ctx.tx.run(query).data()
    if not result:
        return {}
    allocs = {rec["rcname"]: rec["used"] for rec in result}
    if rcs:
        rcs = set(rcs)
        alloc_keys = set(allocs.keys())
        for alloc_key in alloc_keys - rcs:
            allocs.pop(alloc_key)
    return allocs


def _delete_inventory_from_provider(ctx, rp, to_delete=None):
    """Deletes any inventory records from the supplied provider and set() of
    resource class identifiers.

    If there are allocations for any of the inventories to be deleted raise
    InventoryInUse exception.

    :param ctx: `placement.context.RequestContext` that contains an oslo_db
                Session
    :param rp: Resource provider from which to delete inventory.
    :param to_delete: set() containing resource class names to delete. If
                      to_delete is None, all inventory will be deleted.
    """
    rp_uuid = rp if isinstance(rp, six.string_types) else rp.uuid
    allocs = get_allocated_inventory(ctx, rp_uuid, rcs=to_delete)
    if allocs:
        alloc_keys = ", ".join(allocs.keys())
        raise exception.InventoryInUse(resource_classes=alloc_keys,
                                       resource_provider=rp_uuid)
    if to_delete is None:
        to_delete = _get_current_inventory_resources(ctx, rp_uuid)
    for rc in to_delete:
        # Delete the providing relationship first
        query = """
                MATCH p=(rp {uuid: '%s'})-[:PROVIDES]->(rc:%s)
                WITH relationships(p)[0] AS rel, rc
                DELETE rel
                RETURN id(rc) AS rcid
        """ % (rp_uuid, rc)
        result = ctx.tx.run(query).data()
        if not result:
            return 0
        else: 
            rcid = result[0]["rcid"]
            # Now delete the inventory
            query = """
                    MATCH (inv)
                    WHERE id(inv) = %s
                    WITH inv
                    DELETE inv
            """ % rcid
            result = ctx.tx.run(query)
    return len(to_delete)


def _add_inventory_to_provider(ctx, rp, inv_list):
    """Inserts new inventory records for the supplied resource provider.

    :param ctx: `placement.context.RequestContext` that contains an oslo_db
                Session
    :param rp: Resource provider to add inventory to.
    :param inv_list: List of Inventory objects
    """
    # First ensure that the RP doesn't contain any inventory for the resources
    # classes to be added.
    rc_to_add = [inv.resource_class for inv in inv_list]
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:PROVIDES]->(rc)
            WHERE labels(rc)[0] IN %s
            RETURN labels(rc)[0] AS rc_name
    """ % (rp.uuid, rc_to_add)
    result = ctx.tx.run(query).data()
    if result:
        raise db_exc.DBDuplicateEntry()
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
        inv_adds.append("CREATE (rp)-[:PROVIDES]->(:%s {%s})" %
                (rc_name, rc_att_str))
    creates = "\n".join(inv_adds)
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            %s
    """ % (rp.uuid, creates)
    result = ctx.tx.run(query).data()

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
    current_allocs = get_allocated_inventory(ctx, rp)
    exceeded = []
    for rc in to_update:
        inv_record = inv_obj.find(inv_list, rc)
        # Get the current inventory
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-->(rc:%s)
                RETURN id(rc) as rcid
        """ % (rp.uuid, rc)
        result = ctx.tx.run(query)
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
            upds.append("rc.%s=%s" % (att, getattr(inv_record, att)))
        update_clause = ", ".join(upds)
        # Updated the inventory of the RC node
        query = """
                MATCH (rc)
                WHERE id(rc) = %s
                SET %s
                RETURN rc
        """ % (rcid, update_clause)
        result = ctx.tx.run(query).data()
    return exceeded


@db_api.placement_context_manager.writer
def _add_inventory(ctx, rp, inventory):
    """Add one Inventory that wasn't already on the provider.

    :raises `exception.ResourceClassNotFound` if inventory.resource_class
            cannot be found in the DB.
    """
    rc = inventory.resource_class
    query = """
            MATCH (rc:RESOURCE_CLASS {name: '%s'})
            RETURN rc
            """ % rc
    result = ctx.tx.run(query).data()
    if not result:
        raise exception.ResourceClassNotFound(resource_class=rc)
    _add_inventory_to_provider(ctx, rp, [inventory])
    rp.increment_generation()


@db_api.placement_context_manager.writer
def _update_inventory(ctx, rp, inventory):
    """Update an inventory already on the provider.

    :raises `exception.ResourceClassNotFound` if inventory.resource_class
            cannot be found in the DB.
    """
    exceeded = _update_inventory_for_provider(ctx, rp, [inventory],
            set([inventory.resource_class]))
    rp.increment_generation()
    return exceeded


@db_api.placement_context_manager.writer
def _delete_inventory(ctx, rp, resource_class):
    """Delete up to one Inventory of the given resource_class string.

    :raises `exception.ResourceClassNotFound` if resource_class
            cannot be found in the DB.
    """
    if not _delete_inventory_from_provider(ctx, rp, [resource_class]):
        raise exception.NotFound(
            "No inventory of class %s found for delete" % resource_class)
    rp.increment_generation()


@db_api.placement_context_manager.writer
def _set_inventory(ctx, rp, inv_list):
    """Given a list of Inventory objects, replaces the inventory of the
    resource provider in a safe, atomic fashion using the resource
    provider's generation as a consistent view marker.

    :param ctx: Session context to use
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
    existing_resources = _get_current_inventory_resources(ctx, rp)
    these_resources = set([r.resource_class for r in inv_list])

    # Determine which resources we should be adding, deleting and/or
    # updating in the resource provider's inventory by comparing sets
    # of resource class identifiers.
    to_add = these_resources - existing_resources
    to_delete = existing_resources - these_resources
    to_update = these_resources & existing_resources
    exceeded = []

    if to_delete:
        _delete_inventory_from_provider(ctx, rp, to_delete)
    if to_add:
        inv_to_add = [inv for inv in inv_list if inv.resource_class in to_add]
        _add_inventory_to_provider(ctx, rp, inv_to_add)
    if to_update:
        exceeded = _update_inventory_for_provider(ctx, rp, inv_list,
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
def _get_provider_by_uuid(ctx, uuid):
    """Given a UUID, return a dict of information about the resource provider
    from the database.

    :raises: NotFound if no such provider was found
    :param uuid: The UUID to look up
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            RETURN rp
    """ % uuid
    result = ctx.tx.run(query).data()
    if not result:
        raise exception.NotFound(
                "No resource provider with uuid %s found" % uuid)
    rp = db.pythonize(result[0]["rp"])
    return {"uuid": rp.uuid,
            "name": rp.name,
            "generation": rp.generation,
            "updated_at": rp.updated_at,
            "created_at": rp.created_at,
    }


@db_api.placement_context_manager.reader
def _get_aggregates_by_provider(ctx, rp):
    """Returns a list of UUIDs of any aggregates for the supplied resource
    provider.
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:ASSOCIATED]-> (agg)
            RETURN agg.uuid AS agg_uuid
    """ % rp.uuid
    result = ctx.tx.run(query).data()
    return [rec["agg_uuid"] for rec in result]

def _ensure_aggregate(ctx, agg_uuid):
    """Finds an aggregate and returns its UUID (which is the same as the
    supplied parameter). If not found, creates the aggregate with the supplied
    UUID and returns the new aggregate's UUID.
    """
    query = """
            MERGE (agg:AGGREGATE {uuid: '%s'})
            RETURN agg
    """ % agg_uuid
    result = ctx.tx.run(query).data()
    return agg_uuid


def associate(ctx, resource_provider, rp_uuids):
    """Associates one or more resource providers with the specified resource
    provider. Note that the relationship is from RP to shared entity, as this
    is needed for resolving :PROVIDES relationships; e.g.:
        (rp)-[:ASSOCIATED]->(share)-[:PROVIDES]->(resource)
    """
    query = """
            MATCH (share:RESOURCE_PROVIDER {uuid: '%s'})
            WITH share
            MATCH (rp:RESOURCE_PROVIDER)
            WHERE rp.uuid IN %s
            WITH share, rp
            MERGE (rp)-[:ASSOCIATED]->(share)
            RETURN share
    """ % (resource_provider.uuid, rp_uuids)
    result = ctx.tx.run(query).data()

@db_api.placement_context_manager.writer
def _set_aggregates(ctx, resource_provider, provided_aggregates,
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
    existing_aggregates = _get_aggregates_by_provider(ctx,
            resource_provider)
    # A list of aggregate UUIDs that will be associated with the provider
    aggs_to_associate = provided_aggregates - set(existing_aggregates)
    # Same list for those aggregates to remove the association with this
    # provider
    aggs_to_disassociate = [agg_uuid for agg_uuid in existing_aggregates
            if agg_uuid not in provided_aggregates]

    if aggs_to_associate:
        stmnt = "MERGE (agg%s:AGGREGATE {uuid: '%s'})"
        creates = [stmnt % (num, agg_uuid)
                for num, agg_uuid in enumerate(aggs_to_associate)]
        create_clause = "\n".join(creates)
        agg_withs = ", ".join(["agg%s" % num
            for num in range(len(aggs_to_associate))])
        assoc_lines = ["MERGE (rp)-[:ASSOCIATED]->(agg%s)" % num
            for num in range(len(aggs_to_associate))]
        assoc_clause = "\n".join(assoc_lines)
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                %s
                WITH rp, %s
                %s
                RETURN rp
        """ % (resource_provider.uuid, create_clause, agg_withs, assoc_clause)
        result = ctx.tx.run(query).data()

    if aggs_to_disassociate:
        # Delete the agg relationships no longer needed
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[a:ASSOCIATED]->
                    (agg:AGGREGATE)
                WITH rp, a, agg
                WHERE agg.uuid in %s
                DELETE a
        """ % (resource_provider.uuid, aggs_to_disassociate)
        result = ctx.tx.run(query).data()
        if increment_generation:
            resource_provider.increment_generation()
        return


@db_api.placement_context_manager.writer
def _set_traits(ctx, rp, traits):
    """Given a ResourceProvider object and a list of Trait objects, replaces
    the set of traits associated with the resource provider.

    :raises: ConcurrentUpdateDetected if the resource provider's traits or
             inventory was changed in between the time when we first started to
             set traits and the end of this routine.

    :param rp: The ResourceProvider object to set traits against
    :param traits: List of Trait objects
    """
    # Get the list of all trait names
    trait_names = trait_obj.Trait.get_all_names(ctx)
    # Get the traits for this RP
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            WITH rp
            MATCH (t:TRAIT)
            WHERE t.name IN keys(properties(rp))
            RETURN t.name AS trait_name
    """ % rp.uuid
    result = ctx.tx.run(query).data()
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
    result = ctx.tx.run(query).data()
    rp.increment_generation()


@db_api.placement_context_manager.reader
def _has_child_providers(ctx, rp_uuid):
    """Returns True if the supplied resource provider has any child providers,
    False otherwise
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})-[:CONTAINS]->(child)
            RETURN count(child) AS num
    """ % rp_uuid
    result = ctx.tx.run(query).data()
    return bool(result[0]["num"])


@db_api.placement_context_manager.writer
def _set_root_provider_id(ctx, rp_id, root_id):
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


class ResourceProvider(object):
    SETTABLE_FIELDS = ('name', 'parent_provider_uuid')

    def __init__(self, ctx, uuid=None, name=None,
                 generation=None, parent_provider_uuid=None, updated_at=None,
                 created_at=None):
        self._context = ctx
        self.uuid = uuid
        self.name = name
        self.generation = generation
        self.updated_at = updated_at
        self.created_at = created_at
        # Hold this for setting relationships at create() time.
        self._parent_provider_uuid = parent_provider_uuid

    @property
    def root_provider_uuid(self):
        return _root_provider_for_rp(self._context, self)

    @property
    def parent_provider_uuid(self):
        return _parent_provider_for_rp(self._context, self)

    @parent_provider_uuid.setter
    def parent_provider_uuid(self, pp_uuid):
        self._parent_provider_uuid = pp_uuid

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
            'parent_provider_uuid': self._parent_provider_uuid,
        }
        self._create_in_db(self._context, updates)

    def destroy(self):
        self._delete(self._context, self.uuid)

    def save(self):
        # These are the only fields we are willing to save with.
        # If there are others, ignore them.
        updates = {
            'name': self.name,
            'parent_provider_uuid': self._parent_provider_uuid,
        }
        self._update_in_db(self._context, updates)

    @classmethod
    def get_by_uuid(cls, ctx, uuid):
        """Returns a new ResourceProvider object with the supplied UUID.

        :raises NotFound if no such provider could be found
        :param uuid: UUID of the provider to search for
        """
        rp_rec = _get_provider_by_uuid(ctx, uuid)
        return cls._from_db_object(ctx, cls(ctx), rp_rec)

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
        result = self._context.tx.run(query).data()
        if not result:
            raise exception.ResourceProviderConcurrentUpdateDetected()
        self.generation = new_generation

    @db_api.placement_context_manager.writer
    def _create_in_db(self, ctx, updates):
        # User supplied a parent, let's make sure it exists
        parent_uuid = updates.pop('parent_provider_uuid')
        parent_create_clause = ""
        atts = ["uuid: '%s'" % self.uuid, "name: '%s'" % self.name,
                "generation: 0", "created_at: timestamp()",
                "updated_at: timestamp()"]
        if parent_uuid is not None:
            # Setting parent to ourselves doesn't make any sense
            if parent_uuid == self.uuid:
                raise exception.ObjectActionError(
                    action="create",
                    reason="parent provider UUID cannot be same as UUID. "
                           "Please set parent provider UUID to None if "
                           "there is no parent.")

            # Verify that the parent exists
            query = """
                    MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                    RETURN parent
                    """ % parent_uuid
            result = ctx.tx.run(query).data()
            if not result:
                raise exception.ObjectActionError(
                    action='create',
                    reason='parent provider UUID does not exist.')
            parent_create_clause = """
                    WITH rp
                    MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                    WITH rp, parent
                    MERGE (parent)-[:CONTAINS]->(rp)
                    """ % parent_uuid
        # Create the RP, and if there is a parent, create the relationship
        att_clause = ", ".join(atts)
        query = """
                CREATE (rp:RESOURCE_PROVIDER {%s})
                %s
                RETURN rp
                """ % (att_clause, parent_create_clause)
        result = ctx.tx.run(query).data()
        rp_db = db.pythonize(result[0]["rp"])
        self.generation = rp_db.generation
        self.created_at = rp_db.created_at
        self.updated_at = rp_db.updated_at

    @staticmethod
    @db_api.placement_context_manager.writer
    def _delete(ctx, uuid):
        # First, we want to make sure that the RP exists
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                RETURN rp
        """ % uuid
        result = ctx.tx.run(query).data()
        if not result:
            raise exception.NotFound(
                    "No resource provider with uuid %s found" % uuid)
        # Do a quick check to see if the provider is a parent. If it is, don't
        # allow deleting the provider. Note that the foreign key constraint on
        # resource_providers.parent_provider_id will prevent deletion of the
        # parent within the transaction below. This is just a quick
        # short-circuit outside of the transaction boundary.
        if _has_child_providers(ctx, uuid):
            raise exception.CannotDeleteParentResourceProvider()

        # Delete any inventory associated with the resource provider. This will
        # fail if the inventory has any allocations against it.
        _delete_inventory_from_provider(ctx, uuid)
        query = """
                MATCH p=(me:RESOURCE_PROVIDER {uuid: '%s'})-[:PROVIDES]->(inv)
                WITH me, inv, last(relationships(p)) AS provisions
                DELETE provisions
                """ % uuid
        try:
            result = ctx.tx.run(query).data()
        except db.ClientError:
            raise exception.ResourceProviderInUse()
        query = """
                MATCH (me:RESOURCE_PROVIDER {uuid: '%s'})-[:PROVIDES]->(inv)
                WITH inv
                DELETE inv
                """ % uuid
        try:
            result = ctx.tx.run(query).data()
        except db.ClientError:
            raise exception.ResourceProviderInUse()

        # Now delete the RP record
        query = """
                MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
                DETACH DELETE rp
                RETURN rp
                """ % uuid
        result = ctx.tx.run(query).data()

    @db_api.placement_context_manager.writer
    def _update_in_db(self, ctx, updates):
        # A list of resource providers in the same tree with the
        # resource provider to update
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
            my_ids = provider_uuids_from_uuid(ctx, self.uuid)
            curr_parent_uuid = my_ids.parent_uuid if my_ids else None
            parent_uuid = updates.get("parent_provider_uuid")
            if parent_uuid is not None:
                if (curr_parent_uuid is not None and
                        (curr_parent_uuid != parent_uuid)):
                    raise exception.ObjectActionError(
                        action="update",
                        reason="re-parenting a provider is not currently "
                               "allowed.")
                # Make sure that the parent node exists
                query = """
                        MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                        RETURN parent.uuid AS parent_uuid
                        """ % parent_uuid
                result = ctx.tx.run(query).data()
                if not result:
                    raise exception.ObjectActionError(
                        action="create",
                        reason="parent provider UUID does not exist.")
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
                            """ % (self.uuid, self.uuid, self.uuid)
                    result = ctx.tx.run(query).data()
                    tree_uuids = [rec["rp"]["uuid"] for rec in result]
                    if parent_uuid in tree_uuids:
                        raise exception.ObjectActionError(
                            action="update",
                            reason="creating loop in the provider tree is "
                                   "not allowed.")
                # Everything checks out; set the parent relationship
                query = """
                        MATCH (parent:RESOURCE_PROVIDER {uuid: '%s'})
                        MATCH (me:RESOURCE_PROVIDER {uuid: '%s'})
                        WITH parent, me
                        CREATE (parent)-[:CONTAINS]->(me)
                        RETURN parent
                        """ % (parent_uuid, self.uuid)
                result = ctx.tx.run(query).data()
            else:
                # Ensure that a null parent uuid is not being passed when there
                # already is a parent to this node.
                query = """
                        MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS]->
                            (:RESOURCE_PROVIDER {uuid: '%s'})
                        RETURN parent
                        """ % self.uuid
                result = ctx.tx.run(query).data()
                if result:
                    raise exception.ObjectActionError(
                        action='update',
                        reason='un-parenting a provider is not currently '
                               'allowed.')

        update_lines = []
        for key, val in updates.items():
            if isinstance(val, six.string_types):
                txt = "rp.%s = '%s'" % (key, val)
            else:
                if val is None:
                    val = "null"
                txt = "rp.%s = %s" % (key, val)
            update_lines.append(txt)
        if not "generation" in updates and not hasattr(self, "generation"):
            update_lines.append("rp.generation = 0")
        if not "created_at" in updates and not hasattr(self, "created_at"):
            update_lines.append("rp.created_at = timestamp()")
        if not "updated_at" in updates:
            update_lines.append("rp.updated_at = timestamp()")
        update_clause = ", ".join(update_lines)
        query = """
                MERGE (rp:RESOURCE_PROVIDER {uuid: '%s'})
                WITH rp
                SET %s
                RETURN rp
                """ % (self.uuid, update_clause)
        result = ctx.tx.run(query).data()
        self._from_db_object(ctx, self, db.pythonize(result[0]["rp"]))

    @staticmethod
    @db_api.placement_context_manager.writer  # For online data migration
    def _from_db_object(ctx, resource_provider, db_resource_provider):
        for field in ["uuid", "name", "generation", "updated_at",
                "created_at"]:
            setattr(resource_provider, field, db_resource_provider.get(field))
        return resource_provider

    @classmethod
    def create_tree(cls, ctx, tree):
        """This method accepts a nested dict that describes a tree-like
        relationship among resource providers. Each node on the tree should
        contain the following keys:
            name: the name given to the resource. If not supplied, no name is
                set.
            uuid: the resource's UUID. If not supplied, one will be generated.
            resources: a dict of resources that this node provides directly.
                Each member should be of the form `resource_class: amount`
            traits: a list of traits to apply to this node.
            children: a list of nodes representing the children of this node.
                Each child node should be the same format dict as described
                here.

        There is no limit to the level of nesting for child resource providers.
        """
        rp_rec = _create_tree(ctx, tree)
        return cls._from_db_object(ctx, cls(ctx), rp_rec)


@db_api.placement_context_manager.reader
def get_providers_with_shared_capacity(ctx, rc_name, amount, member_of=None):
    """Returns a list of resource provider UUIDs that have capacity for a
    requested amount of a resource and indicate that they share resource via an
    aggregate association.

    For example, assume we have two compute nodes, CN_1 and CN_2, each with
    inventory of VCPU and MEMORY_MB but not DISK_GB (in other words, these are
    compute nodes with no local disk). There is a resource provider called
    "NFS_SHARE" that has an inventory of DISK_GB. Both the "CN_1" and "CN_2"
    compute node resource providers are related to the "NFS_SHARE" resource
    provider with an [:ASSOCIATED] relationship.

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

    :param rc_name: Name of the requested resource class.
    :param amount: Amount of the requested resource.
    :param member_of: When present, contains a list of lists of aggregate
                      uuids that are used to filter the returned list of
                      resource providers that *directly* belong to the
                      aggregates referenced.
    """
    query = """
            MATCH ()-[:ASSOCIATED]->(rp:RESOURCE_PROVIDER)
            WITH rp
            MATCH (rp)-[:PROVIDES]->(rc:%s)
            WITH rp, rc
            OPTIONAL MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH rp, rc, relationships(p)[0] AS usages
            WITH rp, rc, sum(usages.amount) AS total_used,
                ((rc.total - rc.reserved) * rc.allocation_ratio) AS capacity
            MATCH (rc)
            WHERE capacity -  total_used >= %s
            RETURN rp.uuid AS rp_uuid
    """ % (rc_name, amount)
    result = ctx.tx.run(query).data()
    return [rec["rp_uuid"] for rec in result]


@db_api.placement_context_manager.reader
def _get_all_by_filters_from_db(ctx, filters):
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
    in_tree = filters.pop('in_tree', None)

    rp_props = []
    if name:
        rp_props.append("name: '%s'" % name)
    if uuid:
        rp_props.append("uuid: '%s'" % uuid)
    if rp_props:
        rp_prop_str = " {%s}" % ", ".join(rp_props)
    else:
        rp_prop_str = ""
    rp_str ="rp:RESOURCE_PROVIDER%s" % rp_prop_str

    # Build the query line-by-line, incorporating all the filters
    query_lines = []
    if in_tree:
        tree_query = """
                MATCH (root:RESOURCE_PROVIDER)-[:CONTAINS*0..99]->
                    (tree:RESOURCE_PROVIDER {uuid: '%s'})
                OPTIONAL MATCH r=()-->(root)
                WITH root, relationships(r) AS rootrel
                WHERE rootrel IS null
                MATCH (root)-[:CONTAINS*0..99]->(%s)
                WITH rp
        """ % (in_tree, rp_str)
        query_lines.append(tree_query)
    else:
        query_lines.append("MATCH (%s)" % rp_str)
        query_lines.append("WITH rp")

    if resources:
        rps_with_rsrcs = []
        for rc_name, amount in resources.items():
            rps = get_providers_with_resource(ctx, rc_name, amount)
            rps_with_rsrcs.append(set([rp[0] for rp in rps]))
        good_rps = rps_with_rsrcs[0]
        for rsrc_set in rps_with_rsrcs[1:]:
            good_rps.intersection_update(rsrc_set)
        # Now create the query lines to limit RPs to just those with sufficient
        # resources.
        query_lines.append("WHERE rp.uuid IN %s" % list(good_rps))
        query_lines.append("WITH rp")

    if member_of:
        for agg in member_of:
            query_lines.append("MATCH (rp)-[:ASSOCIATED]->(agg)")
            query_lines.append("WHERE agg.uuid IN %s" % agg)
        query_lines.append("WITH rp")
    if forbidden_aggs:
        for agg in forbidden_aggs:
            query_lines.append("MATCH (rp)-[:ASSOCIATED]->(agg)")
            query_lines.append("WHERE agg.uuid NOT IN %s" % agg)
        query_lines.append("WITH rp")

    trait_filters = []
    for trait in required:
        trait_filters.append("EXISTS rp.%s" % trait)
    for trait in forbidden:
        trait_filters.append("NOT EXISTS rp.%s" % trait)
    trait_str = " AND ".join(trait_filters)
    if trait_str:
        query_lines.append("WHERE %s" % trait_str)
        query_lines.append("WITH rp")
    query_lines.append("RETURN rp")
    query = "\n".join(query_lines)
    result = ctx.tx.run(query).data()
    return [db.pythonize(rec["rp"]) for rec in result]


def get_all_by_filters(ctx, filters=None):
    """Returns a list of `ResourceProvider` objects that have sufficient
    resources in their inventories to satisfy the amounts specified in the
    `filters` parameter.

    If no resource providers can be found, the function will return an
    empty list.

    :param ctx: `placement.context.RequestContext` that may be used to
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
    resource_providers = _get_all_by_filters_from_db(ctx, filters)
    return [ResourceProvider(ctx, **rp) for rp in resource_providers]


@db_api.placement_context_manager.reader
def _parent_provider_for_rp(ctx, rp):
    """Given a resource provider, returns the UUID of its parent. If there is
    no parent for this node, returns None.
    """
    rp_uuid = rp.uuid if isinstance(rp, ResourceProvider) else rp
    query = """
            MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS*1]->
                (rp:RESOURCE_PROVIDER {uuid: '%s'})
            RETURN parent.uuid AS parent_uuid
    """ % rp_uuid
    result = ctx.tx.run(query).data()
    if result:
        return result[0]["parent_uuid"]
    else:
        return None


@db_api.placement_context_manager.reader
def _root_provider_for_rp(ctx, rp):
    """Given a resource provider, returns the UUID of its root. If there is no
    parent for this node, returns its own UUID.
    """
    rp_uuid = rp.uuid if isinstance(rp, ResourceProvider) else rp
    query = """
            MATCH p=(root:RESOURCE_PROVIDER)-[:CONTAINS*0..99]->
                (nd:RESOURCE_PROVIDER {uuid: '%s'})
            WITH root, p, size(relationships(p)) AS relsize
            ORDER BY relsize DESC
            RETURN root.uuid AS root_uuid
    """ % rp_uuid
    result = ctx.tx.run(query).data()
    if result:
        return result[0]["root_uuid"]
    else:
        return rp_uuid


def is_nested(ctx, rp1_uuid, rp2_uuid):
    """Returns True if the two resource providers are related with a :CONTAINS
    relationship. The direction of the relationship doesn't matter.
    """
    query = """
            MATCH p=(rp1 {uuid: '%s'})-[:CONTAINS*]-(rp2 {uuid: '%s'})
            RETURN p
    """ % (rp1_uuid, rp2_uuid)
    result = ctx.tx.run(query).data()
    return bool(result)


@db_api.placement_context_manager.writer
def _create_tree(ctx, tree, parent_uuid=None):
    atts = []
    if "name" in tree:
        nm = tree["name"]
        atts.append("name: '{name}'".format(name=nm))
    if "type" in tree:
        tp = tree["type"]
        atts.append("type: '{tp}'".format(tp=tp))
    uuid = tree["uuid"] if "uuid" in tree else db.gen_uuid()
    atts.append("uuid: '{uuid}'".format(uuid=uuid))
    for trait in tree["traits"]:
        atts.append("{trait}: True".format(trait=trait))
    atts.append("generation: 0")
    atts.append("created_at: timestamp()")
    atts.append("updated_at: timestamp()")
    atts_clause = ", ".join(atts)
    
    provides = []
    for rsrc in tree["resources"]:
        rc_name = rsrc.get("name")
        total = rsrc.get("total")
        reserved = rsrc.get("reserved", 0)
        min_unit= rsrc.get("min_unit", 1)
        max_unit = rsrc.get("max_unit", total)
        step_size = rsrc.get("step_size", 1)
        allocation_ratio = rsrc.get("allocation_ratio", 1)
        prov_stmnt = """
WITH nd
CREATE (nd)-[:PROVIDES]->(:{rc_name}
{{total: {total}, reserved: {reserved}, min_unit: {min_unit},
max_unit: {max_unit}, allocation_ratio: {allocation_ratio},
step_size: {step_size} }})""".format(rc_name=rc_name, total=total,
        reserved=reserved, min_unit=min_unit, max_unit=max_unit,
        allocation_ratio=allocation_ratio, step_size=step_size)
        provides.append(prov_stmnt)

    prov_clause = "\n".join(provides)
    if parent_uuid:
        query = """
MATCH (parent:RESOURCE_PROVIDER {{ uuid: '{parent_uuid}' }})
WITH parent
CREATE (parent)-[:CONTAINS]->(nd:RESOURCE_PROVIDER {{ {atts_clause} }})
{prov_clause}
RETURN nd AS rp""".format(parent_uuid=parent_uuid,
        atts_clause=atts_clause, prov_clause=prov_clause)
    else:
        # Primary node
        query = """
CREATE (nd:RESOURCE_PROVIDER {{ {atts_clause} }})
{prov_clause}
RETURN nd AS rp""".format(atts_clause=atts_clause,
        prov_clause=prov_clause)

    result = ctx.tx.run(query).data()
    rp_rec = result[0]["rp"]
    # Call recursively to add child nodes, if any
    child_nodes = tree.get("children", [])
    for child in child_nodes:
        _create_tree(ctx, child, rp_rec["uuid"])
    return rp_rec
