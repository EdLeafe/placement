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


import os_resource_classes as orc
from oslo_concurrency import lockutils
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_log import log as logging
from oslo_utils import timeutils
import six

from placement.db import graph_db as db
from placement import db_api
from placement import exception
from placement import resource_class_cache as rc_cache

_RESOURCE_CLASSES_LOCK = 'resource_classes_sync'
_RESOURCE_CLASSES_SYNCED = False

LOG = logging.getLogger(__name__)


class ResourceClass(object):

    MIN_CUSTOM_RESOURCE_CLASS_ID = 10000
    """Any user-defined resource classes must have an identifier greater than
    or equal to this number.
    """

    # Retry count for handling possible race condition in creating resource
    # class. We don't ever want to hit this, as it is simply a race when
    # creating these classes, but this is just a stopgap to prevent a potential
    # infinite loop.
    RESOURCE_CREATE_RETRY_COUNT = 100

    def __init__(self, context, name=None, updated_at=None, created_at=None):
        self._context = context
        self.name = name
        self.updated_at = updated_at
        self.created_at = created_at

    @staticmethod
    def _from_db_object(context, target, source):
        target._context = context
        target.name = source['name']
        target.updated_at = source['updated_at']
        target.created_at = source['created_at']
        return target

    @classmethod
    def get_by_name(cls, context, name):
        """Return a ResourceClass object with the given string name.

        :param name: String name of the resource class to find

        :raises: ResourceClassNotFound if no such resource class was found
        """
        query = """
                MATCH (rc:RESOURCE_CLASS {name: '%s'})
                RETURN rc
        """ % name
        result = context.tx.run(query).data()
        if not result:
            raise exception.ResourceClassNotFound(resource_class=name)
        rec = db.pythonize(result[0])
        obj = cls(context, name=name, updated_at=rec.get("updated_at"),
                created_at=rec.get("created_at"))
        return obj

    @staticmethod
    @db_api.placement_context_manager.reader
    def _get_next_id(context):
        """Utility method to grab the next resource class identifier to use for
         user-defined resource classes.

        NOTE: to be removed once graph conversion is complete. Resource classes
        are identified by name, not IDs.
        """
        return -1

    def create(self):
        if not self.name:
            raise exception.ObjectActionError(action="create",
                                              reason="name is required")
        if self.name in orc.STANDARDS:
            raise exception.ResourceClassExists(resource_class=self.name)
        if not self.name.startswith(orc.CUSTOM_NAMESPACE):
            raise exception.ObjectActionError(
                action="create",
                reason="name must start with " + orc.CUSTOM_NAMESPACE)
        updates = {}
        for field in ["name", "updated_at", "created_at"]:
            value = getattr(self, field, None)
            if value:
                updates[field] = value

        # There is the possibility of a race when adding resource classes, as
        # the ID is generated locally. This loop catches that exception, and
        # retries until either it succeeds, or a different exception is
        # encountered.
        retries = self.RESOURCE_CREATE_RETRY_COUNT
        while retries:
            retries -= 1
            try:
                rc = self._create_in_db(self._context, updates)
                self._from_db_object(self._context, self, rc)
                break
            except db_exc.DBDuplicateEntry as e:
                # The duplication is on the other unique column, "name". So do
                # not retry; raise the exception immediately.
                raise exception.ResourceClassExists(resource_class=self.name)
        else:
            # We have no idea how common it will be in practice for the retry
            # limit to be exceeded. We set it high in the hope that we never
            # hit this point, but added this log message so we know that this
            # specific situation occurred.
            LOG.warning("Exceeded retry limit on ID generation while "
                        "creating ResourceClass %(name)s",
                        {"name": self.name})
            msg = "creating resource class %s" % self.name
            raise exception.MaxDBRetriesExceeded(action=msg)

    @staticmethod
    @db_api.placement_context_manager.writer
    def _create_in_db(context, updates):
        created_at = updates.get("created_at", "timestamp()")
        updated_at = updates.get("updated_at", "timestamp()")
        query = """
                CREATE (rc:RESOURCE_CLASS {name: '%s', created_at: %s,
                    updated_at: %s})
                RETURN rc
        """ % (updates["name"], created_at, updated_at)
        try:
            result = context.tx.run(query).data()
        except db.ClientError:
            raise db_exc.DBDuplicateEntry()
        return result[0]["rc"]

    def destroy(self):
        # Never delete any standard resource class.
        if not self.name.startswith(orc.CUSTOM_NAMESPACE):
            raise exception.ResourceClassCannotDeleteStandard(
                    resource_class=self.name)
        self._destroy(self._context, self.name)

    @staticmethod
    @db_api.placement_context_manager.writer
    def _destroy(context, name):
        # Don't delete the resource class if it exists as being provided by a
        # resource provider.
        query = """
                MATCH ()-[:PROVIDES]->(rc)
                WHERE labels(rc)[0] = '%s'
                RETURN rc
        """ % name
        result = context.tx.run(query).data()
        if result:
            raise exception.ResourceClassInUse(resource_class=name)

        query = """
                MATCH (rc:RESOURCE_CLASS {name: '%s'})
                WITH rc
                DELETE rc
        """ % name
        result = context.tx.run(query).data()

    def save(self):
        # Never update any standard resource class.
        if not self.name.startswith(orc.CUSTOM_NAMESPACE):
            raise exception.ResourceClassCannotUpdateStandard(
                resource_class=self.name)
        updates = {}
        for field in ["name", "updated_at", "created_at"]:
            value = getattr(self, field, None)
            if value:
                updates[field] = value
        self._save(self._context, self.name, updates)

    @staticmethod
    @db_api.placement_context_manager.writer
    def _save(context, name, updates):
        update_list = ["rc.%s = '%s'" % (k, v) for k, v in updates.items()]
        update_str = ", ".join(update_list)
        query = """
                MATCH (rc:RESOURCE_CLASS {name: '%s'})
                WITH rc
                SET %s
                RETURN rc
        """ % (name, update_str)
        try:
            result = context.tx.run(query).data()
        except db.ClientError:
            raise exception.ResourceClassExists(resource_class=name)


def ensure_sync(ctx):
    global _RESOURCE_CLASSES_SYNCED
    # If another thread is doing this work, wait for it to complete.
    # When that thread is done _RESOURCE_CLASSES_SYNCED will be true in this
    # thread and we'll simply return.
    with lockutils.lock(_RESOURCE_CLASSES_LOCK):
        if not _RESOURCE_CLASSES_SYNCED:
            _resource_classes_sync(ctx)
            _RESOURCE_CLASSES_SYNCED = True


@db_api.placement_context_manager.reader
def get_all(context):
    """Get a list of all the resource classes in the database."""
    query = """
            MATCH (rc:RESOURCE_CLASS)
            RETURN rc
    """
    result = context.tx.run(query).data()
    result_objs = [db.pythonize(rec["rc"]) for rec in result]
    return [ResourceClass(context, **obj) for obj in result_objs]


@db_api.placement_context_manager.writer
def _resource_classes_sync(context):
    # Create a set of all resource class in the os_resource_classes library.

    query = """
            MATCH (rc:RESOURCE_CLASS)
            RETURN rc.name AS name
    """
    result = context.tx.run(query).data()
    db_std_classes = []
    if result:
        db_std_classes = [res["name"] for res in result
                if not orc.is_custom(res["name"])]
    LOG.debug("Found existing resource classes in db: %s", db_std_classes)
    # Determine those resource clases which are in os_resource_classes but not
    # currently in the database, and insert them.
    missing_rc_names = [name for name in orc.STANDARDS
            if name not in db_std_classes]
    for rc_name in missing_rc_names:
        query = """
                MERGE (rc:RESOURCE_CLASS {name: '%s', created_at: timestamp(),
                    updated_at: timestamp()})
        """ % rc_name
        try:
            result = context.tx.run(query).data()
        except db.ClientError:
            pass  # some other process sync'd, just ignore
        except db.TransientError as e:
            LOG.error("Transient errror creating Resource Class '%s': %s" %
                    (rc_name, e))
                
