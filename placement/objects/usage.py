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

from placement.db import graph_db as db
from placement import db_api


class Usage(object):
    def __init__(self, resource_class=None, usage=0):
        self.resource_class = resource_class
        self.usage = int(usage)


def get_all_by_resource_provider_uuid(context, rp_uuid):
    """Get a list of Usage objects filtered by one resource provider."""
    usage_list = _get_all_by_resource_provider_uuid(context, rp_uuid)
    return [Usage(**db_item) for db_item in usage_list]


def get_all_by_project_user(context, project_id, user_id=None):
    """Get a list of Usage objects filtered by project and (optional) user."""
    usage_list = _get_all_by_project_user(context, project_id, user_id=user_id)
    return [Usage(**db_item) for db_item in usage_list]


@db_api.placement_context_manager.reader
def _get_all_by_resource_provider_uuid(context, rp_uuid):
    query = """
            MATCH (rp {uuid: '%s'})-[*0..99]->(:RESOURCE_PROVIDER)-
                [:PROVIDES]->(rc)
            WITH rc
            OPTIONAL MATCH p=(cs:CONSUMER)-[:USES]->(rc)
            WITH labels(rc)[0] AS rcname, relationships(p)[0] AS usage
            RETURN rcname, sum(usage.amount) AS used
    """ % rp_uuid
    result = context.tx.run(query).data()
    return [{"resource_class": rec["rcname"], "usage": rec["used"]}
            for rec in result]


@db_api.placement_context_manager.reader
def _get_all_by_project_user(context, project_id, user_id=None):
    if user_id:
        match = "MATCH p=(:USER {uuid: '%s'})-[*]->()-[:USES]->(rc)" % user_id
    else:
        match = "MATCH p=(:PROJECT {uuid: '%s'})-[*]->()-[:USES]->(rc)"
        match = match % project_id
    query = """
            %s
            WITH labels(rc)[0] AS rcname, relationships(p)[-1] AS used
            RETURN rcname, sum(used.amount) AS used
    """ % match
    result = context.tx.run(query).data()
    return [{"resource_class": rec["rcname"], "usage": rec["used"]}
            for rec in result]
