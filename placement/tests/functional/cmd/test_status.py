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

from oslo_config import cfg
from oslo_upgradecheck import upgradecheck
from oslo_utils.fixture import uuidsentinel

from placement.cmd import status
from placement import conf
from placement.db import graph_db as db
from placement import db_api
from placement.objects import consumer
from placement.objects import resource_provider
from placement.tests.functional import base
from placement.tests.functional.db import test_consumer


class UpgradeCheckIncompleteConsumersTestCase(
        base.TestCase, test_consumer.CreateIncompleteAllocationsMixin):
    """Tests the "Incomplete Consumers" check for the
    "placement-status upgrade check" command.
    """
    def setUp(self):
        super(UpgradeCheckIncompleteConsumersTestCase, self).setUp()
        config = cfg.ConfigOpts()
        conf.register_opts(config)
        self.checks = status.Checks(config)

    def test_check_root_provider_ids(self):

        @db_api.placement_context_manager.writer
        def _create_old_rp(ctx):
            query = """
                    CREATE (rp:RESOURCE_PROVIDER {name: 'rp-1', uuid: '%s',
                        generation: 42, created_at: timestamp(), updated_at:
                        timestamp()})
                    RETURN rp
            """ % uuidsentinel.rp1
            result = db.execute(query)

        # Create a resource provider with no root provider id.
        _create_old_rp(self.context)
        result = self.checks._check_root_provider_ids()
        # Since there is a missing root id, there should be a warning.
        self.assertEqual(upgradecheck.Code.WARNING, result.code)
        # Check the details for the consumer count.
        self.assertIn('There is at least one resource provider table record '
                      'which misses its root provider id. ', result.details)
        # Run the online data migration as recommended from the check output.
        resource_provider.set_root_provider_ids(self.context, batch_size=50)
        # Run the check again and it should be successful.
        result = self.checks._check_root_provider_ids()
        self.assertEqual(upgradecheck.Code.SUCCESS, result.code)
