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

import os_traits
from oslo_concurrency import lockutils
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_log import log as logging
import six

from placement.db import graph_db as db
from placement.db.sqlalchemy import models
from placement import db_api
from placement import exception


_RP_TBL = models.ResourceProvider.__table__
_RP_TRAIT_TBL = models.ResourceProviderTrait.__table__
_TRAIT_TBL = models.Trait.__table__
_TRAIT_LOCK = 'trait_sync'
_TRAITS_SYNCED = False

LOG = logging.getLogger(__name__)


class Trait(object):

    # All the user-defined traits must begin with this prefix.
    CUSTOM_NAMESPACE = 'CUSTOM_'

    def __init__(self, context, id=None, name=None, updated_at=None,
                 created_at=None):
        self._context = context
        self.id = id
        self.name = name
        self.updated_at = updated_at
        self.created_at = created_at

    # FIXME(cdent): Duped from resource_class.
    @staticmethod
    def _from_db_object(context, target, source):
        target._context = context
        target.name = source['name']
        target.updated_at = source['updated_at']
        target.created_at = source['created_at']
        return target

    @staticmethod
    @db_api.placement_context_manager.writer
    def _create_in_db(context, updates):
        upd_list = []
        for key, val in updates.items():
            if isinstance(val, six.string_types):
                upd_list.append("%s = '%s'" % (key, val))
            else:
                upd_list.append("%s = %s" % (key, val))
        upd_clause = ", ".join(upd_list)
        query = """
        CREATE (t:TRAIT {%s})
        RETURN t
        """ % upd_clause
        try:
            result = db.execute(query)
        except db.ClientError as e:
            raise db_exc.DBDuplicateEntry(e)
        return db.pythonize(result[0])

    def create(self):
        if not self.name:
            raise exception.ObjectActionError(action='create',
                                              reason='name is required')

        # FIXME(cdent): duped from resource class
        updates = {}
        for field in ['name', 'updated_at', 'created_at']:
            value = getattr(self, field, None)
            if value:
                updates[field] = value

        try:
            db_trait = self._create_in_db(self._context, updates)
        except db_exc.DBDuplicateEntry:
            raise exception.TraitExists(name=self.name)

        self._from_db_object(self._context, self, db_trait)

    @staticmethod
    @db_api.placement_context_manager.reader
    def _get_by_name_from_db(context, name):
        query = """
                MATCH (t:TRAIT {name: '%s'})
                RETURN t
        """
        result = db.execute(query)
        if not result:
            raise exception.TraitNotFound(names=name)
        return db.pythonize(result[0])

    @classmethod
    def get_by_name(cls, context, name):
        db_trait = cls._get_by_name_from_db(context, six.text_type(name))
        return cls._from_db_object(context, cls(context), db_trait)

    @classmethod
    def get_all_names(cls, context):
        query = """
                MATCH (t:TRAIT)
                RETURN t.name AS trait_name
        """
        result = db.execute(query)
        trait_names = [rec["trait_name"] for rec in result]
        return trait_names

    @staticmethod
    @db_api.placement_context_manager.writer
    def _destroy_in_db(context, name):
        query = """
                MATCH (rp:RESOURCE_PROVIDER)
                WHERE exists(rp.%s)
        """ % name
        result = db.execute(query)
        if result:
            raise exception.TraitInUse(name=name)
        query = """
                MATCH (t:TRAIT {name: '%s'})
                WITH t
                DELETE t
                RETURN t
        """ % name
        result = db.execute(query)
        if not result:
            raise exception.TraitNotFound(names=name)

    def destroy(self):
        if not self.name:
            raise exception.ObjectActionError(action='destroy',
                                              reason='name is required')
        if not self.name.startswith(self.CUSTOM_NAMESPACE):
            raise exception.TraitCannotDeleteStandard(name=self.name)

        self._destroy_in_db(self._context, self.name)


def ensure_sync(ctx):
    """Ensures that the os_traits library is synchronized to the traits db.

    If _TRAITS_SYNCED is False then this process has not tried to update the
    traits db. Do so by calling _trait_sync. Since the placement API server
    could be multi-threaded, lock around testing _TRAITS_SYNCED to avoid
    duplicating work.

    Different placement API server processes that talk to the same database
    will avoid issues through the power of transactions.

    :param ctx: `placement.context.RequestContext` that may be used to grab a
                DB connection.
    """
    global _TRAITS_SYNCED
    # If another thread is doing this work, wait for it to complete.
    # When that thread is done _TRAITS_SYNCED will be true in this
    # thread and we'll simply return.
    with lockutils.lock(_TRAIT_LOCK):
        if not _TRAITS_SYNCED:
            _trait_sync(ctx)
            _TRAITS_SYNCED = True


def get_all(context, filters=None):
    db_traits = _get_all_from_db(context, filters)
    return [Trait(context, **data) for data in db_traits]


def get_all_by_resource_provider(context, rp):
    """Returns a list containing Trait objects for any trait
    associated with the supplied resource provider.
    """
    db_traits = get_traits_by_provider_uuid(context, rp.uuid)
    return [Trait(context, **data) for data in db_traits]


@db_api.placement_context_manager.reader
def get_traits_by_provider_uuid(context, rp_uuid):
    query = """
            MATCH (rp:RESOURCE_PROVIDER {uuid: '%s'})
            RETURN properties(rp) as props
    """ % rp_uuid
    result = db.execute(query)
    props = [rec["props"] for rec in result]
    if not props:
        return []
    return _traits_from_props(props)


def _traits_from_props(props):
    all_traits = Trait.get_all_names()
    return [prop for p in props if p in all_traits]


@db_api.placement_context_manager.reader
def get_traits_by_provider_tree(ctx, root_uuids):
    """Returns a dict, keyed by provider UUIDs for all resource providers
    in all trees indicated in the ``root_uuids``, of string trait names
    associated with that provider.

    :raises: ValueError when root_uuids is empty.

    :param ctx: placement.context.RequestContext object
    :param root_ids: list of root resource provider UUIDs
    """
    if not root_uuids:
        raise ValueError("Expected root_uuids to be a list of root resource "
                         "provider UUIDs, but got an empty list.")
    query = """
            MATCH (root:RESOURCE_PROVIDER {uuid: '%s'})
            WITH root
            OPTIONAL MATCH (root)-[*]->(rp:RESOURCE_PROVIDER)
            RETURN root, rp
    """
    rp_traits = collections.defaultdict(set)
    for root_uuid in root_uuids:
        result = db.execute(query % root_uuid)
        if result:
            root_rp = db.pythonize(result[0]["root"])
            root_props = root_rp.keys()
            root_traits = _traits_from_props(root_props)
            rp_traits[root_uuid].update(root_traits)
        for rec in result:
            rp = rp_traits[rec["rp"]]
            rp_traits = _traits_from_props(rp_props)
            rp_traits[rp_uuid].update(rp_traits)
    # Convert the sets back to lists
    rp_traits = {k: list(v) for k, v in rp_traits.items()}
    return rp_traits


@db_api.placement_context_manager.reader
def ids_from_names(ctx, names):
    """This method is no longer needed"""
    return


@db_api.placement_context_manager.reader
def _get_all_from_db(context, filters):
    if not filters:
        filters = {}

    name_where = ""
    assoc = ""
    if 'name_in' in filters:
        name_list = ["'%s'" % six.text_type(n) for n in filters['name_in']]
        name_where = "WHERE t.name IN [%s]" % name_list
    if 'prefix' in filters:
        prefix_where = "t.name STARTS WITH %s" % prefix
        if name_where:
            name_where += "AND %s" % prefix_where
        else:
            name_where = "WHERE %s" % prefix_where
    if 'associated' in filters:
        # This will be either True or False
        assoc = """
                WITH t
                MATCH (rp)
                WHERE %s exists(rp.%s)
        """
        add_not = "" if filters["associated"] else "not"
    query = """
            MATCH (t:TRAIT)
            %(name_where)s
            %(assoc)s
            RETURN t
    """ % {"name_where": name_where, "assoc": assoc}
    result = db.execute(query)
    return [db.pythonize(rec) for rec in result]


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
# Bug #1760322: If the caller raises an exception, we don't want the trait
# sync rolled back; so use an .independent transaction
@db_api.placement_context_manager.writer
def _trait_sync(ctx):
    """Sync the os_traits symbols to the database.

    Reads all symbols from the os_traits library, checks if any of them do
    not exist in the database and bulk-inserts those that are not. This is
    done once per web-service process, at startup.

    :param ctx: `placement.context.RequestContext` that may be used to grab a
                 DB connection.
    """
    # Create a set of all traits in the os_traits library.
    std_traits = set(os_traits.get_traits())
    # Get the traits in the database
    trait_names = Trait.get_all_names(ctx)
    db_traits = set(name for name in trait_names
            if not os.traits.is_custom(name))
    # Determine those traits which are in os_traits but not
    # currently in the database, and insert them.
    need_sync = std_traits - db_traits
    if not need_sync:
        return
    for trait_name in need_sync:
        query = """
                CREATE (t:TRAIT {name: '%s', created_at: timestamp(),
                    updated_at: timestamp()})
                RETURN t
        """ % trait_name
        try:
            result = db.execute(query)
        except db.ClientError:
            pass  # some other process sync'd, just ignore
