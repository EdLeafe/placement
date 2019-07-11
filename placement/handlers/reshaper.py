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
"""Placement API handler for the reshaper.

The reshaper provides for atomically migrating resource provider inventories
and associated allocations when some of the inventory moves from one resource
provider to another, such as when a class of inventory moves from a parent
provider to a new child provider.
"""

from collections import defaultdict
import copy
import itertools

from oslo_utils import excutils
import webob

from placement import errors
from placement import exception
# TODO(cdent): That we are doing this suggests that there's stuff to be
# extracted from the handler to a shared module.
from placement.handlers import allocation
from placement.handlers import inventory
from placement import microversion
from placement.objects import reshaper
from placement.objects import resource_provider as rp_obj
from placement.policies import reshaper as policies
from placement.schemas import reshaper as schema
from placement import util
from placement import wsgi_wrapper


@wsgi_wrapper.PlacementWsgify
@microversion.version_handler('1.30')
@util.require_content('application/json')
def reshape(req):
    context = req.environ['placement.context']
    want_version = req.environ[microversion.MICROVERSION_ENVIRON]
    context.can(policies.RESHAPE)
    data = util.extract_json(req.body, schema.POST_RESHAPER_SCHEMA)
    inventories = data['inventories']
    allocations = data['allocations']

    # Get the current inventory for all the RPs included. Compare it to the
    # passed inventories; if they don't have the same resources, then something
    # is off, and reject the reshape.
    curr_resources = []
    proposed_resources = []
    for rp_uuid, inv_data in inventories.items():
        curr_resources += list(rp_obj.get_current_inventory_resources(context,
                rp_uuid, include_total=True))
        for rc_name, rc_vals in inv_data["inventories"].items():
            proposed_resources.append((rc_name, rc_vals["total"]))
    curr_resources.sort()
    proposed_resources.sort()
    if not curr_resources == proposed_resources:
        raise webob.exc.HTTPBadRequest("Proposed reshaped inventory does not "
                "match existing inventory.")

    # Now check the allocations to make sure that they match
    curr_allocs = defaultdict(int)
    proposed_allocs = defaultdict(int)

    alloc_data = [itm["allocations"] for itm in allocations.values()]
    # First, get all the unique RPs
    rp_dict_keys = [itm.keys() for itm in alloc_data]
    # Convert dict_keys data to lists
    rp_lists = [list(itm) for itm in rp_dict_keys]
    # Collapse the lists of lists to a single list, and get the unique values
    rp_set = set(list(itertools.chain(*rp_lists)))
    # Get the current allocations for each RP
    for rp in rp_set:
        alloc_dict = rp_obj.get_allocated_inventory(context, rp)
        for key, val in alloc_dict.items():
            curr_allocs[key] += val
    # Now sum up the proposed allocations. The RC name and amount are in a
    # deeply-nested dict.
    for rdict in alloc_data:
        for _, rsrc in rdict.items():
            for _, rc in rsrc.items():
                for rc_name, amt in rc.items():
                    proposed_allocs[rc_name] += amt

    if not curr_allocs == proposed_allocs:
        raise webob.HTTPBadRequest("Proposed reshaped allocations do not "
                "match existing allocations.")

    # The inventories and allocations match up, so now just switch the
    # connections among the entities.
    rp_obj.reshape(context, inventories, allocations)
    # ^^^ This needs to be written




    # We're going to create several lists of Inventory objects, keyed by rp
    # uuid.
    inventory_by_rp = {}

    # TODO(cdent): this has overlaps with inventory:set_inventories
    # and is a mess of bad names and lack of method extraction.
    for rp_uuid, inventory_data in inventories.items():
        try:
            resource_provider = rp_obj.ResourceProvider.get_by_uuid(
                context, rp_uuid)
        except exception.NotFound as exc:
            raise webob.exc.HTTPBadRequest(
                'Resource provider %(rp_uuid)s in inventories not found: '
                '%(error)s' % {'rp_uuid': rp_uuid, 'error': exc},
                comment=errors.RESOURCE_PROVIDER_NOT_FOUND)

        # Do an early generation check.
        generation = inventory_data['resource_provider_generation']
        if generation != resource_provider.generation:
            raise webob.exc.HTTPConflict(
                'resource provider generation conflict for provider %(rp)s: '
                'actual: %(actual)s, given: %(given)s' %
                {'rp': rp_uuid,
                 'actual': resource_provider.generation,
                 'given': generation},
                comment=errors.CONCURRENT_UPDATE)

        inv_list = []
        for res_class, raw_inventory in inventory_data['inventories'].items():
            inv_data = copy.copy(inventory.INVENTORY_DEFAULTS)
            inv_data.update(raw_inventory)
            inv_object = inventory.make_inventory_object(
                resource_provider, res_class, **inv_data)
            inv_list.append(inv_object)
        inventory_by_rp[resource_provider] = inv_list

    # Make the consumer objects associated with the allocations.
    consumers, new_consumers_created = allocation.inspect_consumers(
        context, allocations, want_version)

    # Nest exception handling so that any exception results in new consumer
    # objects being deleted, then reraise for translating to HTTP exceptions.
    try:
        try:
            # When these allocations are created they get resource provider
            # objects which are different instances (usually with the same
            # data) from those loaded above when creating inventory objects.
            # The reshape method below is responsible for ensuring that the
            # resource providers and their generations do not conflict.
            allocation_objects = allocation.create_allocation_list(
                context, allocations, consumers)

            reshaper.reshape(context, inventory_by_rp, allocation_objects)
        except Exception:
            with excutils.save_and_reraise_exception():
                allocation.delete_consumers(new_consumers_created)
    # Generation conflict is a (rare) possibility in a few different
    # places in reshape().
    except exception.ConcurrentUpdateDetected as exc:
        raise webob.exc.HTTPConflict(
            'update conflict: %(error)s' % {'error': exc},
            comment=errors.CONCURRENT_UPDATE)
    # A NotFound here means a resource class that does not exist was named
    except exception.NotFound as exc:
        raise webob.exc.HTTPBadRequest(
            'malformed reshaper data: %(error)s' % {'error': exc})
    # Distinguish inventory in use (has allocations on it)...
    except exception.InventoryInUse as exc:
        raise webob.exc.HTTPConflict(
            'update conflict: %(error)s' % {'error': exc},
            comment=errors.INVENTORY_INUSE)
    # ...from allocations which won't fit for a variety of reasons.
    except exception.InvalidInventory as exc:
        raise webob.exc.HTTPConflict(
            'Unable to allocate inventory: %(error)s' % {'error': exc})

    req.response.status = 204
    req.response.content_type = None
    return req.response
