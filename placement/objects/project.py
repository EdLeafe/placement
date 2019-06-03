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

from oslo_db import exception as db_exc

from placement.db import graph_db as db
from placement import db_api
from placement import exception


@db_api.placement_context_manager.writer
def ensure_incomplete_project(context):
    """Ensures that a project record is created for the "incomplete consumer
    project". Returns the internal ID of that record.
    """
    incomplete_uuid = context.config.placement.incomplete_consumer_project_id
    query = """
            MERGE (pj:PROJECT {uuid: '%s'})
            RETURN pj
    """ % incomplete_uuid
    context.tx.run(query)
    return incomplete_uuid


@db_api.placement_context_manager.reader
def _get_project_by_uuid(context, uuid):
    query = """
            MATCH (pj:PROJECT {uuid: '%s'})
            RETURN pj
    """ % uuid
    result = context.tx.run(query).data()
    if not result:
        raise exception.ProjectNotFound(uuid=uuid)
    rec = db.pythonize(result[0]["pj"])
    return {"uuid": rec.uuid,
            "updated_at": rec.updated_at,
            "created_at": rec.created_at,
            }


class Project(object):
    def __init__(self, context, uuid=None, updated_at=None, created_at=None):
        self._context = context
        self.uuid = uuid
        self.updated_at = updated_at
        self.created_at = created_at

    @staticmethod
    def _from_db_object(context, target, source):
        target._context = context
        target.uuid = source['uuid']
        target.updated_at = source['updated_at']
        target.created_at = source['created_at']
        return target

    @classmethod
    def get_by_uuid(cls, context, uuid):
        res = _get_project_by_uuid(context, uuid)
        return cls._from_db_object(context, cls(context), res)

    def create(self):
        @db_api.placement_context_manager.writer
        def _create_in_db(context):
            query = """
                    CREATE (pj:PROJECT {uuid: '%s', created_at: timestamp(),
                        updated_at: timestamp()})
                    RETURN pj
            """ % self.uuid
            try:
                result = context.tx.run(query).data()
            except db.ClientError:
                raise exception.ProjectExists(uuid=self.uuid)
            db_obj = db.pythonize(result[0]["pj"])
            self._from_db_object(context, self, db_obj)
        _create_in_db(self._context)
