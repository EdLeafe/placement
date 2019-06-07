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
from sqlalchemy import sql

from placement.db.sqlalchemy import models
from placement import db_api
from placement import exception
from placement.objects import rp_candidates
from placement.objects import trait as trait_obj
from placement import resource_class_cache as rc_cache


# TODO(tetsuro): Move these public symbols in a central place.
_TRAIT_TBL = models.Trait.__table__
_ALLOC_TBL = models.Allocation.__table__
_INV_TBL = models.Inventory.__table__
_RP_TBL = models.ResourceProvider.__table__
_AGG_TBL = models.PlacementAggregate.__table__
_RP_AGG_TBL = models.ResourceProviderAggregate.__table__
_RP_TRAIT_TBL = models.ResourceProviderTrait.__table__


LOG = logging.getLogger(__name__)


ProviderIds = collections.namedtuple(
    'ProviderIds', 'id uuid parent_id parent_uuid root_id root_uuid')


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
        self.resources = {
            rc_cache.RC_CACHE.id_from_string(key): value
            for key, value in request.resources.items()
        }

        # A list of lists of aggregate UUIDs that the providers matching for
        # that request group must be members of
        self.member_of = request.member_of

        # A list of aggregate UUIDs that the providers matching for
        # that request group must not be members of
        self.forbidden_aggs = request.forbidden_aggs

        # A set of provider ids that matches the requested positive aggregates
        self.rps_in_aggs = set()
        if self.member_of:
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
        self.required_trait_map = {}
        self.forbidden_trait_map = {}
        for trait_map, traits in (
                (self.required_trait_map, request.required_traits),
                (self.forbidden_trait_map, request.forbidden_traits)):
            if traits:
                trait_map.update(trait_obj.ids_from_names(context, traits))

        # Internal id of a root provider. If provided, this RequestGroup must
        # be satisfied by resource provider(s) under the root provider.
        self.tree_root_id = None
        if request.in_tree:
            tree_ids = provider_ids_from_uuid(context, request.in_tree)
            if tree_ids is None:
                raise exception.ResourceProviderNotFound()
            self.tree_root_id = tree_ids.root_id
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
                context, rc_id, amount, tree_root_id=self.tree_root_id)
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

    def get_rps_with_shared_capacity(self, rc_id):
        sharing_in_aggs = self._sharing_providers
        if self.rps_in_aggs:
            sharing_in_aggs &= self.rps_in_aggs
        if not sharing_in_aggs:
            return set()
        rps_with_resource = set(p[0] for p in self._rps_with_resource[rc_id])
        return sharing_in_aggs & rps_with_resource

    def get_rps_with_resource(self, rc_id):
        return self._rps_with_resource.get(rc_id)


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
    # INNER JOIN resource_providers AS root
    #   ON rp.root_provider_id = root.id
    # LEFT JOIN resource_providers AS parent
    #   ON rp.parent_provider_id = parent.id
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
    me_to_root = sa.join(me, root, me.c.root_provider_id == root.c.id)
    me_to_parent = sa.outerjoin(
        me_to_root, parent,
        me.c.parent_provider_id == parent.c.id)
    sel = sa.select(cols).select_from(me_to_parent)
    sel = sel.where(me.c.id.in_(rp_ids))

    ret = {}
    for r in context.session.execute(sel):
        ret[r['id']] = ProviderIds(**r)
    return ret


@db_api.placement_context_manager.reader
def provider_ids_from_uuid(context, uuid):
    """Given the UUID of a resource provider, returns a namedtuple
    (ProviderIds) with the internal ID, the UUID, the parent provider's
    internal ID, parent provider's UUID, the root provider's internal ID and
    the root provider UUID.

    :returns: ProviderIds object containing the internal IDs and UUIDs of the
              provider identified by the supplied UUID
    :param uuid: The UUID of the provider to look up
    """
    # SELECT
    #   rp.id, rp.uuid,
    #   parent.id AS parent_id, parent.uuid AS parent_uuid,
    #   root.id AS root_id, root.uuid AS root_uuid
    # FROM resource_providers AS rp
    # INNER JOIN resource_providers AS root
    #   ON rp.root_provider_id = root.id
    # LEFT JOIN resource_providers AS parent
    #   ON rp.parent_provider_id = parent.id
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
    me_to_root = sa.join(me, root, me.c.root_provider_id == root.c.id)
    me_to_parent = sa.outerjoin(
        me_to_root, parent,
        me.c.parent_provider_id == parent.c.id)
    sel = sa.select(cols).select_from(me_to_parent)
    sel = sel.where(me.c.uuid == uuid)
    res = context.session.execute(sel).fetchone()
    if not res:
        return None
    return ProviderIds(**dict(res))


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
            rpt.c.root_provider_id == tree_root_id,
            where_conds)
    sel = sel.where(where_conds)
    res = ctx.session.execute(sel).fetchall()
    res = set((r[0], r[1]) for r in res)
    return res


@db_api.placement_context_manager.reader
def get_provider_ids_matching(rg_ctx):
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

    :param rg_ctx: RequestGroupSearchContext
    """
    filtered_rps, forbidden_rp_ids = get_provider_ids_for_traits_and_aggs(
        rg_ctx)

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
    for rc_id, amount in rg_ctx.resources.items():
        rc_name = rc_cache.RC_CACHE.string_from_id(rc_id)
        provs_with_resource = rg_ctx.get_rps_with_resource(rc_id)
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

    :param rg_ctx: RequestGroupSearchContext
    """
    if rg_ctx.forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(
            rg_ctx.context, [rg_ctx.forbidden_aggs])

    # To get all trees that collectively have all required resource,
    # aggregates and traits, we use `RPCandidateList` which has a list of
    # three-tuples with the first element being resource provider ID, the
    # second element being the root provider ID and the third being resource
    # class ID.
    provs_with_inv = rp_candidates.RPCandidateList()

    for rc_id, amount in rg_ctx.resources.items():
        rc_name = rc_cache.RC_CACHE.string_from_id(rc_id)

        provs_with_inv_rc = rp_candidates.RPCandidateList()
        rc_provs_with_inv = rg_ctx.get_rps_with_resource(rc_id)
        provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_id)
        LOG.debug("found %d providers under %d trees with available %d %s",
                  len(provs_with_inv_rc), len(provs_with_inv_rc.trees),
                  amount, rc_name)
        if not provs_with_inv_rc:
            # If there's no providers that have one of the resource classes,
            # then we can short-circuit returning an empty RPCandidateList
            return rp_candidates.RPCandidateList()

        sharing_providers = rg_ctx.get_rps_with_shared_capacity(rc_id)
        if sharing_providers and rg_ctx.tree_root_id is None:
            # There are sharing providers for this resource class, so we
            # should also get combinations of (sharing provider, anchor root)
            # in addition to (non-sharing provider, anchor root) we've just
            # got via get_providers_with_resource() above. We must skip this
            # process if tree_root_id is provided via the ?in_tree=<rp_uuid>
            # queryparam, because it restricts resources from another tree.
            rc_provs_with_inv = anchors_for_sharing_providers(
                rg_ctx.context, sharing_providers, get_id=True)
            provs_with_inv_rc.add_rps(rc_provs_with_inv, rc_id)
            LOG.debug(
                "considering %d sharing providers with %d %s, "
                "now we've got %d provider trees",
                len(sharing_providers), amount, rc_name,
                len(provs_with_inv_rc.trees))

        if rg_ctx.member_of:
            # Aggregate on root spans the whole tree, so the rp itself
            # *or its root* should be in the aggregate
            provs_with_inv_rc.filter_by_rp_or_tree(rg_ctx.rps_in_aggs)
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

    if (not rg_ctx.required_trait_map and not rg_ctx.forbidden_trait_map) or (
            rg_ctx.exists_sharing):
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
        rg_ctx.context, provs_with_inv.rps, rg_ctx.required_trait_map,
        rg_ctx.forbidden_trait_map)
    provs_with_inv.filter_by_rp(rp_tuples_with_trait)
    LOG.debug("found %d providers under %d trees after applying "
              "traits filter - required: %s, forbidden: %s",
              len(provs_with_inv.rps), len(provs_with_inv.trees),
              list(rg_ctx.required_trait_map),
              list(rg_ctx.forbidden_trait_map))

    return provs_with_inv


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


def get_provider_ids_for_traits_and_aggs(rg_ctx):
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
    if rg_ctx.required_trait_map:
        trait_rps = _get_provider_ids_having_all_traits(
            rg_ctx.context, rg_ctx.required_trait_map)
        filtered_rps = trait_rps
        LOG.debug("found %d providers after applying required traits filter "
                  "(%s)",
                  len(filtered_rps), list(rg_ctx.required_trait_map))
        if not filtered_rps:
            return None, []

    # If 'member_of' has values, do a separate lookup to identify the
    # resource providers that meet the member_of constraints.
    if rg_ctx.member_of:
        if filtered_rps:
            filtered_rps &= rg_ctx.rps_in_aggs
        else:
            filtered_rps = rg_ctx.rps_in_aggs
        LOG.debug("found %d providers after applying required aggregates "
                  "filter (%s)", len(filtered_rps), rg_ctx.member_of)
        if not filtered_rps:
            return None, []

    forbidden_rp_ids = set()
    if rg_ctx.forbidden_aggs:
        rps_bad_aggs = provider_ids_matching_aggregates(
            rg_ctx.context, [rg_ctx.forbidden_aggs])
        forbidden_rp_ids |= rps_bad_aggs
        if filtered_rps:
            filtered_rps -= rps_bad_aggs
            LOG.debug("found %d providers after applying forbidden aggregates "
                      "filter (%s)", len(filtered_rps), rg_ctx.forbidden_aggs)
            if not filtered_rps:
                return None, []

    if rg_ctx.forbidden_trait_map:
        rps_bad_traits = get_provider_ids_having_any_trait(
            rg_ctx.context, rg_ctx.forbidden_trait_map)
        forbidden_rp_ids |= rps_bad_traits
        if filtered_rps:
            filtered_rps -= rps_bad_traits
            LOG.debug("found %d providers after applying forbidden traits "
                      "filter (%s)", len(filtered_rps),
                      list(rg_ctx.forbidden_trait_map))
            if not filtered_rps:
                return None, []

    return filtered_rps, forbidden_rp_ids


@db_api.placement_context_manager.reader
def get_sharing_providers(ctx, rp_ids=None):
    """Returns a list of resource provider IDs (internal IDs, not UUIDs)
    that indicate that they share resource via an aggregate association.

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

    :param rp_ids: When present, returned resource providers are limited to
                   only those in this value
    """
    # The SQL we need to generate here looks like this:
    #
    # SELECT rp.id
    # FROM resource_providers AS rp
    #   INNER JOIN resource_provider_traits AS rpt
    #     ON rp.id = rpt.resource_provider_id
    #     AND rpt.trait_id = ${"MISC_SHARES_VIA_AGGREGATE" trait id}
    # WHERE rp.id IN $(RP_IDs)

    sharing_trait = trait_obj.Trait.get_by_name(
        ctx, os_traits.MISC_SHARES_VIA_AGGREGATE)

    rp_tbl = sa.alias(_RP_TBL, name='rp')
    rpt_tbl = sa.alias(_RP_TRAIT_TBL, name='rpt')

    rp_to_rpt_join = sa.join(
        rp_tbl, rpt_tbl,
        sa.and_(rp_tbl.c.id == rpt_tbl.c.resource_provider_id,
                rpt_tbl.c.trait_id == sharing_trait.id)
    )

    sel = sa.select([rp_tbl.c.id]).select_from(rp_to_rpt_join)
    if rp_ids:
        sel = sel.where(rp_tbl.c.id.in_(rp_ids))

    return set(r[0] for r in ctx.session.execute(sel))


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
        sel = sa.select([sps.c.id, shr_with_sps.c.root_provider_id])
    else:
        join_chain = sa.join(
            join_chain, rps, shr_with_sps.c.root_provider_id == rps.c.id)
        sel = sa.select([sps.c.uuid, rps.c.uuid])
    sel = sel.select_from(join_chain)
    sel = sel.where(sps.c.id.in_(rp_ids))
    return set([(r[0], r[1]) for r in context.session.execute(sel).fetchall()])


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
