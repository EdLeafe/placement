# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
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

"""Fixtures for Placement tests."""
from __future__ import absolute_import


from oslo_config import cfg
from oslo_db.sqlalchemy import test_fixtures

from placement.db import graph_db
from placement.db.sqlalchemy import migration
from placement import db_api as placement_db
from placement import deploy
from placement.objects import resource_class
from placement.objects import trait
from placement import resource_class_cache as rc_cache


class Database(test_fixtures.GeneratesSchema, test_fixtures.AdHocDbFixture):
    def __init__(self, conf_fixture, set_config=False):
        """Create a database fixture."""
        super(Database, self).__init__()

        if set_config:
            try:
                conf_fixture.register_opt(
                    cfg.StrOpt('connection'), group='placement_database')
            except cfg.DuplicateOptError:
                # already registered
                pass
            conf_fixture.config(connection='sqlite://',
                                group='placement_database')
        self.conf_fixture = conf_fixture
        self.get_engine = placement_db.get_placement_engine
        placement_db.configure(self.conf_fixture.conf)

    def get_enginefacade(self):
        return placement_db.placement_context_manager

    def generate_schema_create_all(self, engine):
        # note: at this point in oslo_db's fixtures, the incoming
        # Engine has **not** been associated with the global
        # context manager yet.
        migration.create_schema(engine)

        # Clear the graph DB
        graph_db.delete_all()
        # Create the constraints. Eventually this should be moved to somewhere
        # higher-level, but for now this is good enough for tests.
        constraints = [
                "CREATE CONSTRAINT ON (rp:RESOURCE_PROVIDER) ASSERT rp.uuid "
                    "IS UNIQUE",
                "CREATE CONSTRAINT ON (rp:RESOURCE_PROVIDER) ASSERT rp.name "
                    "IS UNIQUE",
                "CREATE CONSTRAINT ON (rc:RESOURCE_CLASS) ASSERT rc.name "
                    "IS UNIQUE",
                "CREATE CONSTRAINT ON (t:TRAIT) ASSERT t.name IS UNIQUE",
                "CREATE CONSTRAINT ON (pj:PROJECT) ASSERT pj.uuid IS UNIQUE",
                "CREATE CONSTRAINT ON (u:USER) ASSERT u.uuid IS UNIQUE",
                "CREATE CONSTRAINT ON (cs:CONSUMER) ASSERT cs.uuid IS UNIQUE",
        ]
        for constraint in constraints:
            graph_db.execute(constraint)

        # Make sure db flags are correct at both the start and finish
        # of the test.
        self.addCleanup(self.cleanup)
        self.cleanup()

        # Sync traits and resource classes.
        deploy.update_database(self.conf_fixture.conf)

    def cleanup(self):
        trait._TRAITS_SYNCED = False
        resource_class._RESOURCE_CLASSES_SYNCED = False
        rc_cache.RC_CACHE = None
