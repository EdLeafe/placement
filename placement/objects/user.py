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
def ensure_incomplete_user(ctx):
    """Ensures that a user node is created for the "incomplete consumer
    user". Returns the UUID of that record.
    """
    # First, make sure the incomplete project exists
    project_uuid = project_obj.ensure_incomplete_project(ctx)
    incomplete_uuid = ctx.config.placement.incomplete_consumer_user_id
    # Now create the user if it doesn't exist, and the relationship to the
    # incomplete project node.
    query = """
            MERGE (u:USER {uuid: '%s'})
            MERGE (pj:PROJECT {uuid: '%s'})
            WITH u, pj
            MERGE (pj)-[:OWNS]->(u)
    """ % (incomplete_uuid, project_uuid)
    return incomplete_uuid


@db_api.placement_context_manager.reader
def _get_user_by_uuid(context, uuid):
    query = """
            MATCH (u:USER {uuid: '%s'})
            RETURN u
    """ % uuid
    result = context.tx.run(query).data()
    if not result:
        raise exception.UserNotFound(uuid=uuid)
    rec = db.pythonize(result[0]["u"])
    return {"uuid": rec.uuid,
            "updated_at": rec.updated_at,
            "created_at": rec.created_at,
            }


class User(object):
    def __init__(self, context, uuid=None, updated_at=None, created_at=None):
        self._context = context
        self.uuid = uuid
        self.updated_at = updated_at
        self.created_at = created_at

    @staticmethod
    def _from_db_object(ctx, target, source):
        target._context = ctx
        target.uuid = source['uuid']
        target.updated_at = source['updated_at']
        target.created_at = source['created_at']
        return target

    @classmethod
    def get_by_uuid(cls, ctx, uuid):
        res = _get_user_by_uuid(ctx, uuid)
        return cls._from_db_object(ctx, cls(ctx), res)

    def create(self):
        @db_api.placement_context_manager.writer
        def _create_in_db(context):
            query = """
                    CREATE (u:USER {uuid: '%s', created_at: timestamp(),
                        updated_at: timestamp()})
                    RETURN u
            """ % self.uuid
            try:
                result = context.tx.run(query).data()
            except db.ClientError:
                raise exception.UserExists(uuid=self.uuid)
            db_obj = db.pythonize(result[0]["u"])
            self._from_db_object(context, self, db_obj)
        _create_in_db(self._context)
