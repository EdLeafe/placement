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

from oslo_utils.fixture import uuidsentinel as uuids
from oslo_utils import timeutils
import six

from placement import exception
from placement.objects import resource_provider
from placement.tests.unit.objects import base


_RESOURCE_CLASS_NAME = "TEST_RC"

_RESOURCE_PROVIDER_UUID = uuids.resource_provider
_RESOURCE_PROVIDER_NAME = six.text_type(uuids.resource_name)
_RESOURCE_PROVIDER_DB = {
    'uuid': _RESOURCE_PROVIDER_UUID,
    'name': _RESOURCE_PROVIDER_NAME,
    'generation': 0,
    'root_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'parent_provider_uuid': None,
    'updated_at': None,
    'created_at': timeutils.utcnow(with_timezone=True),
}

_RESOURCE_PROVIDER_UUID2 = uuids.resource_provider2
_RESOURCE_PROVIDER_NAME2 = uuids.resource_name2
_RESOURCE_PROVIDER_DB2 = {
    'uuid': _RESOURCE_PROVIDER_UUID2,
    'name': _RESOURCE_PROVIDER_NAME2,
    'generation': 0,
    'root_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'parent_provider_uuid': _RESOURCE_PROVIDER_UUID,
}


_ALLOCATION_DB = {
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_class_name': _RESOURCE_CLASS_NAME,
    'consumer_uuid': uuids.fake_instance,
    'consumer_generation': 0,
    'used': 8,
    'user_uuid': uuids.user_id,
    'project_uuid': uuids.project_id,
    'updated_at': timeutils.utcnow(with_timezone=True),
    'created_at': timeutils.utcnow(with_timezone=True),
}

_ALLOCATION_BY_CONSUMER_DB = {
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_class_name': _RESOURCE_CLASS_NAME,
    'consumer_uuid': uuids.fake_instance,
    'consumer_generation': 0,
    'used': 8,
    'user_uuid': uuids.user_id,
    'project_uuid': uuids.project_id,
    'updated_at': timeutils.utcnow(with_timezone=True),
    'created_at': timeutils.utcnow(with_timezone=True),
    'resource_provider_name': _RESOURCE_PROVIDER_NAME,
    'resource_provider_uuid': _RESOURCE_PROVIDER_UUID,
    'resource_provider_generation': 0,
}


class TestResourceProviderNoDB(base.TestCase):

    def test_create_id_fail(self):
        obj = resource_provider.ResourceProvider(context=self.context,
                                                 uuid=_RESOURCE_PROVIDER_UUID,
        self.assertRaises(exception.ObjectActionError,
                          obj.create)

    def test_create_no_uuid_fail(self):
        obj = resource_provider.ResourceProvider(context=self.context)
        self.assertRaises(exception.ObjectActionError,
                          obj.create)
