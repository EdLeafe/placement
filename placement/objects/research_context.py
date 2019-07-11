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
"""Utility methods for getting allocation candidates."""

import collections
import os_traits
from oslo_log import log as logging
import sqlalchemy as sa

from placement.db import graph_db as db
from placement import db_api
from placement import exception
from placement.objects import rp_candidates
from placement.objects import trait as trait_obj
from placement import resource_class_cache as rc_cache
from placement import util


LOG = logging.getLogger(__name__)

QUERY_TEMPLATE = """
MATCH (root:RESOURCE_PROVIDER)-[*0..99]->
    (rp:RESOURCE_PROVIDER)-[:PROVIDES]->(rc{num}:{rc_name})
WITH root, rp, rc{num}, ((rc{num}.total - rc{num}.reserved) *
	 rc{num}.allocation_ratio) AS capacity
OPTIONAL MATCH par=(parent:RESOURCE_PROVIDER)- [:CONTAINS|ASSOCIATED*]->(root)
WITH root, size(relationships(par)) AS numrel, rp, rc{num}, capacity
OPTIONAL MATCH p=(:CONSUMER)-[:USES]->(rc{num})
WITH root, numrel, rp, rc{num}, capacity, last(relationships(p)) AS uses
WITH root, numrel, rp, rc{num}, capacity - sum(uses.amount) AS avail
WHERE avail >= {amount}
AND rc{num}.min_unit <= {amount}
AND rc{num}.max_unit >= {amount}
AND {amount} % rc{num}.step_size = 0
AND numrel IS null
RETURN rp.uuid AS rp_uuid, root.uuid AS root_uuid,
    labels(rc{num})[0] AS rc_name
"""

ProviderIds = collections.namedtuple("ProviderIds",
        "uuid parent_uuid root_uuid")


class RequestGroupSearchContext(object):
    """An adapter object that represents the search for allocation candidates
    for a single request group.
    """
    def __init__(self, context, request, has_trees, sharing):
        """Initializes the object retrieving and caching matching providers
        for each conditions like resource and aggregates from database.

        :raises placement.exception.ResourceProviderNotFound if there is no
                provider found which satisfies the request.
        """
        # TODO(tetsuro): split this into smaller functions reordering
        self.context = context

        # A dict, keyed by resource class internal ID, of the amounts of that
        # resource class being requested by the group.
        self.resources = request.resources

        # A list of lists of aggregate UUIDs that the providers matching for
        # that request group must be members of
        self.member_of = request.member_of

        # A list of aggregate UUIDs that the providers matching for
        # that request group must not be members of
        self.forbidden_aggs = request.forbidden_aggs

        # A set of provider ids that matches the requested positive aggregates
        self.rps_in_aggs = set()
        if self.member_of:
            x = request.resources
            self.rps_in_aggs = provider_ids_matching_aggregates(
                context, self.member_of)
            if not self.rps_in_aggs:
                raise exception.ResourceProviderNotFound()

        # If True, this RequestGroup represents requests which must be
        # satisfied by a single resource provider.  If False, represents a
        # request for resources in any resource provider in the same tree,
        # or a sharing provider.
        self.use_same_provider = request.use_same_provider

        # maps the trait name to the trait internal ID
        self.required_traits = request.required_traits
        self.forbidden_traits = request.forbidden_traits

        # Internal id of a root provider. If provided, this RequestGroup must
        # be satisfied by resource provider(s) under the root provider.
        self.tree_root_uuid = None
        if request.in_tree:
            tree_ids = provider_uuids_from_uuid(context, request.in_tree)
            if tree_ids is None:
                raise exception.ResourceProviderNotFound()
            self.tree_root_uuid = tree_ids.root_uuid
            LOG.debug("getting allocation candidates in the same tree "
                      "with the root provider %s", tree_ids.root_uuid)

        self._rps_with_resource = {}
        for rc_id, amount in self.resources.items():
            # NOTE(tetsuro): We could pass rps in requested aggregates to
            # get_providers_with_resource here once we explicitly put
            # aggregates to nested (non-root) providers (the aggregate
            # flows down feature) rather than applying later the implicit rule
            # that aggregate on root spans the whole tree
            provs_with_resource = get_providers_with_resource(
                context, rc_id, amount, tree_root_uuid=self.tree_root_uuid)
            if not provs_with_resource:
                raise exception.ResourceProviderNotFound()
            self._rps_with_resource[rc_id] = provs_with_resource

        # a set of resource provider IDs that share some inventory for some
        # resource class.
        self._sharing_providers = sharing

        # bool indicating there is some level of nesting in the environment
        self.has_trees = has_trees

    @property
    def exists_sharing(self):
        """bool indicating there is sharing providers in the environment for
        the requested resource class (if there isn't, we take faster, simpler
        code paths)
        """
        # NOTE: This could be refactored to see the requested resources
        return bool(self._sharing_providers)

    @property
    def exists_nested(self):
        """bool indicating there is some level of nesting in the environment
        (if there isn't, we take faster, simpler code paths)
        """
        # NOTE: This could be refactored to see the requested resources
        return self.has_trees

    def get_rps_with_shared_capacity(self, rc_name):
        sharing_in_aggs = set(self._sharing_providers)
        if self.rps_in_aggs:
            sharing_in_aggs &= self.rps_in_aggs
        if not sharing_in_aggs:
            return set()
        rps_with_resource = set(p[0] for p in self._rps_with_resource[rc_name])
        return sharing_in_aggs & rps_with_resource

    def get_rps_with_resource(self, rc_name):
        return self._rps_with_resourcerp.get(rc_name)


def provider_uuids_from_rp_uuids(ctx, rp_uuids):
    """Given an iterable of resource provider UUIDs, returns a dict,
    keyed by provider UUID, of ProviderIds namedtuples describing those
    providers.

    :returns: dict, keyed by provider UUID, of ProviderIds namedtuples
    :param rp_uuids: iterable of provider UUIDs to look up
    """
    query = """
        MATCH (rp:RESOURCE_PROVIDER)
        WHERE rp.uuid IN {rp_uuids}
        WITH rp
        OPTIONAL MATCH (parent:RESOURCE_PROVIDER)-[:CONTAINS*1]->(rp)
        WITH rp, parent
        OPTIONAL MATCH (root:RESOURCE_PROVIDER)-[:CONTAINS*1..99]->(parent)
        WITH rp.uuid AS uuid, parent.uuid AS parent_uuid,
             coalesce(root.uuid, rp.uuid) AS root_uuid
        ORDER BY uuid, parent_uuid, root_uuid
        RETURN uuid, parent_uuid, root_uuid
    """.format(rp_uuids=util.makelist(rp_uuids))
    result = ctx.tx.run(query).data()
    return {rec["uuid"]: ProviderIds(**rec) for rec in result}


def provider_uuids_from_uuid(ctx, uuid):
    """Given the UUID of a resource provider, returns a namedtuple
    (ProviderIds) with the UUID, parent provider's UUID, and the root provider
    UUID.

    :returns: ProviderIds object containing the UUIDs of the provider
              identified by the supplied UUID, or None if there is no
              ResourceProvider with a matching uuid.
    :param uuid: The UUID of the provider to look up
    """
    return provider_uuids_from_rp_uuids(ctx, [uuid]).get(uuid)


@db_api.placement_context_manager.reader
def validate_resources(ctx, rc_names):
    """Ensure that all the resource classes requested are valid."""
    query = """
            MATCH (rc:RESOURCE_CLASS)
            RETURN DISTINCT rc.name AS rc_name
    """
    result = ctx.tx.run(query).data()
    valid_names = set([rec["rc_name"] for rec in result])
    invalid_rc_names = set(rc_names) - valid_names
    if invalid_rc_names:
        resource_class = ",".join(invalid_rc_names)
        raise exception.ResourceClassNotFound(resource_class)


@db_api.placement_context_manager.reader
def validate_traits(ctx, traits):
    """Ensure that all the traits requested are valid."""
    query = """
            MATCH (t:TRAIT)
            RETURN DISTINCT t.name AS trait
    """
    result = ctx.tx.run(query).data()
    valid_names = set([rec["trait"] for rec in result])
    invalid_names = set(traits) - valid_names
    if invalid_names:
        trait_names = ",".join(invalid_names)
        raise exception.TraitNotFound(trait_names)


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
            OPTIONAL MATCH par=(parent:RESOURCE_PROVIDER)-
                [:CONTAINS|ASSOCIATED*]->(root)
            WITH root, size(relationships(par)) AS numrel, rp, rc, capacity
            OPTIONAL MATCH p=(:CONSUMER)-[:USES]->(rc)
            WITH root, numrel, rp, rc, capacity, last(relationships(p)) AS uses
            WITH root, numrel, rp, rc, capacity - sum(uses.amount) AS avail
            WHERE avail >= {amount}
            AND rc.min_unit <= {amount}
            AND rc.max_unit >= {amount}
            AND {amount} % rc.step_size = 0
            AND numrel IS null
            RETURN rp.uuid AS rp_uuid, root.uuid AS root_uuid
    """.format(rt=tree_root_clause, rc_name=rc_name, amount=amount)
    result = ctx.tx.run(query).data()
    return set((rec["rp_uuid"], rec["root_uuid"]) for rec in result)


@db_api.placement_context_manager.reader
def get_provider_uuids_matching(rg_ctx):
    """Returns a list of tuples of (provider UUID, root provider UUID)
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

    :param rg_ctx: Session context to use
    """
    # The iteratively filtered set of resource provider internal IDs that match
    # all the constraints in the request
    filtered_rps, forbidden_rp_uuids = get_provider_ids_for_traits_and_aggs(
            rg_ctx)
    # 'filtered_rps' will be None if there were traits or aggs in the request,
    # but no RPs matched those requirements. It will be an empty set if there
    # was no filtering done because no traits or aggs were specified.
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
    parts = []
    for num, (rc_name, amount) in enumerate(rg_ctx.resources.items()):
        parts.append( QUERY_TEMPLATE.format(num=num, rc_name=rc_name,
                amount=amount))
    query = "\nUNION\n".join(parts)
    result = rg_ctx.tx.run(query).data()
    # There will be a record for each (rp, root) that satisfies each requested
    # resource. Only keep those where the root appears for all the resources.
    provs_with_resource = set([(rec["rp_uuid"], rec["root_uuid"]) for rec in result])
#        provs_with_resource = get_providers_with_resource(rg_ctx, rc_name,
#                amount, tree_root_uuid=rg_ctx.tree_root_uuid)
#        LOG.debug("found %d providers with available %d %s",
#                  len(provs_with_resource), amount, rc_name)
#        if not provs_with_resource:
#            return []
#
#        rc_rp_uuids = set(p[0] for p in provs_with_resource)
#        # The branching below could be collapsed code-wise, but is in place to
#        # make the debug logging clearer.
#        if filtered_rps:
#            filtered_rps &= rc_rp_uuids
#        else:
#            filtered_rps = rc_rp_uuids
#        LOG.debug("found %d providers after filtering by previous result",
#                len(filtered_rps))
#        if not filtered_rps:
#            return []
    return [prov for prov in provs_with_resource
            if prov[0] not in forbidden_rp_uuids]


@db_api.placement_context_manager.reader
def get_trees_matching_all(rg_ctx):
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

    :param rg_ctx: Session context to use
    """
    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if rg_ctx.member_of:
        rps_in_aggs = provider_ids_matching_aggregates(rg_ctx,
                rg_ctx.member_of)
        if not rps_in_aggs:
            # Short-circuit. The user either asked for a non-existing
            # aggregate or there were no resource providers that matched
            # the requirements...
            return rp_candidates.RPCandidateList()

    if rg_ctx.forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(rg_ctx,
                [rg_ctx.forbidden_aggs])

    # To get all trees that collectively have all required resource,
    # aggregates and traits, we use `RPCandidateList` which has a list of
    # three-tuples with the first element being resource provider UUID, the
    # second element being the root provider UUID and the third being resource
    # class name.
    provs_with_inv = rp_candidates.RPCandidateList()

    for rc_name, amount in rg_ctx.resources.items():
        provs_with_inv_rc = rp_candidates.RPCandidateList()
        rc_provs_with_inv = get_providers_with_resource(rg_ctx, rc_name,
                amount, tree_root_uuid=rg_ctx.tree_root_uuid)
        provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_name)
        LOG.debug("found %d providers under %d trees with available %d %s",
                  len(provs_with_inv_rc), len(provs_with_inv_rc.trees),
                  amount, rc_name)
        if not provs_with_inv_rc:
            # If there's no providers that have one of the resource classes,
            # then we can short-circuit returning an empty RPCandidateList
            return rp_candidates.RPCandidateList()

        sharing_providers = rg_ctx.get_rps_with_shared_capacity(rc_name)
        if sharing_providers and rg_ctx.tree_root_uuid is None:
            # There are sharing providers for this resource class, so we
            # should also get combinations of (sharing provider, anchor root)
            # in addition to (non-sharing provider, anchor root) we've just
            # got via get_providers_with_resource() above. We must skip this
            # process if tree_root_uuid is provided via the ?in_tree=<rp_uuid>
            # queryparam, because it restricts resources from another tree.
            rc_provs_with_inv = anchors_for_sharing_providers(rg_ctx,
                    sharing_providers)
            provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_name)
            LOG.debug(
                    "considering %d sharing providers with %d %s, "
                    "now we've got %d provider trees",
                    len(sharing_providers), amount, rc_name,
                    len(provs_with_inv_rc.trees))

        if rg_ctx.member_of:
            # Aggregate on root spans the whole tree, so the rp itself
            # *or its root* should be in the aggregate
            provs_with_inv_rc.filter_by_rp_or_tree(rps_in_aggs)
            LOG.debug("found %d providers under %d trees after applying "
                      "aggregate filter %s",
                      len(provs_with_inv_rc.rps), len(provs_with_inv_rc.trees),
                      rg_ctx.member_of)
            if not provs_with_inv_rc:
                # Short-circuit returning an empty RPCandidateList
                return rp_candidates.RPCandidateList()
        if rg_ctx.forbidden_aggs:
            # Aggregate on root spans the whole tree, so the rp itself
            # *and its root* should be outside the aggregate
            provs_with_inv_rc.filter_by_rp_nor_tree(rps_bad_aggs)
            LOG.debug("found %d providers under %d trees after applying "
                      "negative aggregate filter %s",
                      len(provs_with_inv_rc.rps), len(provs_with_inv_rc.trees),
                      rg_ctx.forbidden_aggs)
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

    if (not rg_ctx.required_traits and not rg_ctx.forbidden_traits) or (
            any(rg_ctx._sharing_providers)):
        # If there were no traits required, there's no difference in how we
        # calculate allocation requests between nested and non-nested
        # environments, so just short-circuit and return. Or if sharing
        # providers are in play, we check the trait constraints later
        # in _alloc_candidates_multiple_providers(), so skip.
        return provs_with_inv

    # Return the providers where the providers have the available inventory
    # capacity and that set of providers (grouped by their tree) have all
    # of the required traits and none of the forbidden traits
    rp_tuples_with_trait = _get_trees_with_traits(rg_ctx, provs_with_inv.rps,
            rg_ctx.required_traits, rg_ctx.forbidden_traits)
    provs_with_inv.filter_by_rp(rp_tuples_with_trait)
    LOG.debug("found %d providers under %d trees after applying "
              "traits filter - required: %s, forbidden: %s",
              len(provs_with_inv.rps), len(provs_with_inv.trees),
              list(rg_ctx.required_traits), list(rg_ctx.forbidden_traits))
    return provs_with_inv


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
            MATCH p=()-[*0]->(root:RESOURCE_PROVIDER)-[:CONTAINS*0..99]->(rp)
            WITH p, relationships(p) AS relp, root, rp
            WITH p, relp, size(relp) AS numrel, root, rp
            WITH rp, root, numrel, max(numrel) AS maxrel
            WHERE numrel = maxrel
            RETURN rp.uuid AS rp_uuid, root.uuid AS root_uuid
            ORDER BY rp_uuid, root_uuid
    """.format(rp_uuids=util.makelist(rp_uuids), trait_clause=trait_clause)
    result = ctx.tx.run(query).data()
    return [(rec["rp_uuid"], rec["root_uuid"]) for rec in result]


@db_api.placement_context_manager.reader
def provider_ids_matching_aggregates(ctx, member_of, rp_uuids=None):
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
    agg_lines = []
    for num, agg_list in enumerate(member_of):
        agg_lines.append("MATCH (rp)-[:ASSOCIATED]->(agg%s:AGGREGATE)" % num)
        agg_lines.append("WHERE agg%s.uuid IN %s" % (num, util.makelist(agg_list)))
    agg_clause = "\n".join(agg_lines)
    query = """
            MATCH (rp:RESOURCE_PROVIDER)
            {rp_clause}
            WITH rp
            {agg_clause}
            RETURN rp.uuid AS rp_uuid
    """.format(rp_clause=rp_clause, agg_clause=agg_clause)
    result = ctx.tx.run(query).data()
    return set([rec["rp_uuid"] for rec in result])


@db_api.placement_context_manager.reader
def _filter_rps_by_traits(ctx, traits, any_or_all):
    if not traits:
        raise ValueError("traits must not be empty")
    op = " OR " if any_or_all == "any" else " AND "
    trait_str = op.join(["EXISTS(rp.%s)" % trait for trait in traits])
    query = """
        MATCH (rp)
        WHERE %s
        RETURN rp.uuid AS rp_uuid
    """ % trait_str
    result = ctx.tx.run(query).data()
    return set([rec["rp_uuid"] for rec in result])


def get_provider_ids_having_any_trait(ctx, traits):
    """Returns a set of resource provider UUIDs that have ANY of the supplied
    traits.

    :param ctx: Session context to use
    :param traits: A list of trait names, at least one of which each provider
                   must have associated with it.
    :raise ValueError: If traits is empty or None.
    """
    return _filter_rps_by_traits(ctx, traits, "any")


def _get_provider_ids_having_all_traits(ctx, traits):
    """Returns a set of resource provider internal IDs that have ALL of the
    required traits.

    NOTE: Don't call this method with no required_traits.

    :param ctx: Session context to use
    :param traits: A list of trait names, all of which each provider must have
                   associated with it.
    """
    return _filter_rps_by_traits(ctx, traits, "all")


def get_provider_ids_for_traits_and_aggs(res_ctx):
    """Get UUIDs for all providers matching the specified traits/aggs.

    :return: A tuple of:
        filtered_rp_uuids: A set of provider UUIDs matching the specified
            criteria. If None, work was done and resulted in no matching
            providers. This is in contrast to the empty set, which indicates
            that no filtering was performed.
        forbidden_rp_uuids: A set of UUIDs of providers having any of the
            specified forbidden_traits.
    """
    filtered_rps = set()
    if res_ctx.required_traits:
        trait_rps = _get_provider_ids_having_all_traits(res_ctx,
                res_ctx.required_traits)
        filtered_rps = trait_rps
        LOG.debug("found %d providers after applying required traits filter "
                  "(%s)",
                  len(filtered_rps), list(res_ctx.required_traits))
        if not filtered_rps:
            return None, []

    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if res_ctx.member_of:
        rps_in_aggs = provider_ids_matching_aggregates(res_ctx,
                res_ctx.member_of)
        if filtered_rps:
            filtered_rps &= rps_in_aggs
        else:
            filtered_rps = rps_in_aggs
        LOG.debug("found %d providers after applying required aggregates "
                  "filter (%s)", len(filtered_rps), res_ctx.member_of)
        if not filtered_rps:
            return None, []

    forbidden_rp_uuids = set()
    if res_ctx.forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(res_ctx,
                [res_ctx.forbidden_aggs])
        forbidden_rp_uuids |= rps_bad_aggs
        if filtered_rps:
            filtered_rps -= rps_bad_aggs
            LOG.debug("found %d providers after applying forbidden aggregates "
                      "filter (%s)", len(filtered_rps), res_ctx.forbidden_aggs)
            if not filtered_rps:
                return None, []

    if res_ctx.forbidden_traits:
        rps_bad_traits = get_provider_ids_having_any_trait(res_ctx,
                res_ctx.forbidden_traits)
        forbidden_rp_uuids |= rps_bad_traits
        if filtered_rps:
            filtered_rps -= rps_bad_traits
            LOG.debug("found %d providers after applying forbidden traits "
                      "filter (%s)", len(filtered_rps),
                      list(res_ctx.forbidden_traits))
            if not filtered_rps:
                return None, []

    return filtered_rps, forbidden_rp_uuids


@db_api.placement_context_manager.reader
def get_sharing_providers(ctx, rp_uuids=None):
    """Returns a list of resource provider UUIDs that indicate that they share
    resource via an aggregate association.

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
    indicate they share their inventory with providers in some aggregate.

    :param rp_uuids: When present, returned resource providers are limited to
                     only those in this value
    """
    query = """
            MATCH (rp:RESOURCE_PROVIDER)
            WHERE exists(rp.MISC_SHARES_VIA_AGGREGATE)
            RETURN rp.uuid AS rp_uuid
    """
    result = db.execute(query)
    return [rec["rp_uuid"] for rec in result]


@db_api.placement_context_manager.reader
def anchors_for_sharing_providers(ctx, rp_uuids):
    """Given a list of UUIDs of sharing providers, returns a set of
    tuples of (sharing provider UUID, anchor provider UUID), where each of
    anchor is the unique root provider of a tree associated with the same
    aggregate as the sharing provider. (These are the providers that can
    "anchor" a single AllocationRequest.)

    If the sharing provider is not part of any aggregate, the empty list is
    returned.
    """
    query = """
            MATCH (anchor:RESOURCE_PROVIDER)-[*0..99]->()-[:ASSOCIATED]->
                (shared:RESOURCE_PROVIDER)
            WHERE shared.uuid IN {rp_uuids}
            RETURN shared.uuid as s_uuid, anchor.uuid AS a_uuid 
    """.format(rp_uuids=util.makelist(rp_uuids))
    result = ctx.tx.run(query).data()
    return set((rec["s_uuid"], rec["a_uuid"]) for rec in result)


@db_api.placement_context_manager.reader
def has_provider_trees(ctx):
    """Simple method that returns whether provider trees (i.e. nested resource
    providers) are in use in the deployment at all. This information is used to
    switch code paths when attempting to retrieve allocation candidate
    information. The code paths are eminently easier to execute and follow for
    non-nested scenarios...

    NOTE(jaypipes): The result of this function can be cached extensively.
    """
    query = """
            MATCH p=()-[:CONTAINS]->()
            RETURN sum(size(relationships(p))) AS nest_count
    """
    result = ctx.tx.run(query).data()
    return result[0]["nest_count"] > 0
