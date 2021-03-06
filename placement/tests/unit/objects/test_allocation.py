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


import mock
from oslo_utils.fixture import uuidsentinel as uuids
from oslo_utils import timeutils
import six

from placement.objects import allocation as alloc_obj
from placement.objects import resource_provider as rp_obj
from placement.tests.unit.objects import base


_RESOURCE_PROVIDER_UUID = uuids.resource_provider
_RESOURCE_PROVIDER_NAME = six.text_type(uuids.resource_name)
_RESOURCE_CLASS_NAME = "STUFF"
_ALLOCATION_uuid = uuids.allocation
_ALLOCATION_DB = {
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_class_name': _RESOURCE_CLASS_NAME,
    'consumer_uuid': uuids.fake_instance,
    'consumer_uuid': uuids.consumer,
    'consumer_generation': 0,
    'used': 8,
    'user_uuid': uuids.user_uuid,
    'project_uuid': uuids.project_uuid,
    'updated_at': timeutils.utcnow(with_timezone=True),
    'created_at': timeutils.utcnow(with_timezone=True),
}

_ALLOCATION_BY_CONSUMER_DB = {
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_class_name': _RESOURCE_CLASS_NAME,
    'consumer_uuid': uuids.fake_instance,
    'consumer_uuid': uuids.consumer,
    'consumer_generation': 0,
    'used': 8,
    'user_uuid': uuids.user_uuid,
    'project_uuid': uuids.project_uuid,
    'updated_at': timeutils.utcnow(with_timezone=True),
    'created_at': timeutils.utcnow(with_timezone=True),
    'resource_provider_name': _RESOURCE_PROVIDER_NAME,
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_provider_generation': 0,
}


class TestAllocationListNoDB(base.TestCase):

    def setUp(self):
        super(TestAllocationListNoDB, self).setUp()
        base.fake_ensure_cache(self.context)

    @mock.patch('placement.objects.allocation.'
                '_create_incomplete_consumers_for_provider')
    @mock.patch('placement.objects.allocation.'
                '_get_allocations_by_provider_uuid',
                return_value=[_ALLOCATION_DB])
    def test_get_all_by_resource_provider(
            self, mock_get_allocations_from_db, mock_create_consumers):
        rp = rp_obj.ResourceProvider(self.context,
                                     uuid=uuids.resource_provider)
        allocations = alloc_obj.get_all_by_resource_provider(self.context, rp)

        self.assertEqual(1, len(allocations))
        mock_get_allocations_from_db.assert_called_once_with(
            self.context, rp.uuid)
        self.assertEqual(_ALLOCATION_DB['used'], allocations[0].used)
        self.assertEqual(_ALLOCATION_DB['created_at'],
                         allocations[0].created_at)
        self.assertEqual(_ALLOCATION_DB['updated_at'],
                         allocations[0].updated_at)
        mock_create_consumers.assert_called_once_with(
            self.context, _RESOURCE_PROVIDER_UUID)

    @mock.patch('placement.objects.allocation.'
                '_create_incomplete_consumer')
    @mock.patch('placement.objects.allocation.'
                '_get_allocations_by_consumer_uuid',
                return_value=[_ALLOCATION_BY_CONSUMER_DB])
    def test_get_all_by_consumer_uuid(self, mock_get_allocations_from_db,
                                    mock_create_consumer):
        allocations = alloc_obj.get_all_by_consumer_uuid(
            self.context, uuids.consumer)

        self.assertEqual(1, len(allocations))
        mock_create_consumer.assert_called_once_with(self.context,
                                                     uuids.consumer)
        mock_get_allocations_from_db.assert_called_once_with(self.context,
                                                             uuids.consumer)
        self.assertEqual(_ALLOCATION_BY_CONSUMER_DB['used'],
                         allocations[0].used)
        self.assertEqual(_ALLOCATION_BY_CONSUMER_DB['created_at'],
                         allocations[0].created_at)
        self.assertEqual(_ALLOCATION_BY_CONSUMER_DB['updated_at'],
                         allocations[0].updated_at)
