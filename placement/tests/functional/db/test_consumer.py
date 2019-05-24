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
from oslo_utils.fixture import uuidsentinel as uuids

from placement import db_api
from placement import exception
from placement.objects import allocation as alloc_obj
from placement.objects import consumer as consumer_obj
from placement.objects import project as project_obj
from placement.objects import resource_provider as rp_obj
from placement.objects import user as user_obj
from placement.tests.functional import base
from placement.tests.functional.db import test_base as tb


class ConsumerTestCase(tb.PlacementDbBaseTestCase):
    def test_non_existing_consumer(self):
        self.assertRaises(
            exception.ConsumerNotFound,
            consumer_obj.Consumer.get_by_uuid, self.ctx,
            uuids.non_existing_consumer)

    def test_create_and_get(self):
        u = user_obj.User(self.ctx, uuid='another-user')
        u.create()
        p = project_obj.Project(self.ctx, uuid='another-project')
        p.create()
        c = consumer_obj.Consumer(
            self.ctx, uuid=uuids.consumer, user=u, project=p)
        c.create()
        c = consumer_obj.Consumer.get_by_uuid(self.ctx, uuids.consumer)
        self.assertRaises(exception.ConsumerExists, c.create)

    def test_update(self):
        """Tests the scenario where a user supplies a different project/user ID
        for an allocation's consumer and we call Consumer.update() to save that
        information to the consumers table.
        """
        # First, create the consumer with the "fake-user" and "fake-project"
        # user/project in the base test class's setUp
        c = consumer_obj.Consumer(
            self.ctx, uuid=uuids.consumer, user=self.user_obj,
            project=self.project_obj)
        c.create()
        c = consumer_obj.Consumer.get_by_uuid(self.ctx, uuids.consumer)

        # Now change the consumer's project and user to a different project
        another_user = user_obj.User(self.ctx, uuid='another-user')
        another_user.create()
        another_proj = project_obj.Project(self.ctx, uuid='another-project')
        another_proj.create()

        c.project = another_proj
        c.user = another_user
        c.update()
        c = consumer_obj.Consumer.get_by_uuid(self.ctx, uuids.consumer)


class CreateIncompleteAllocationsMixin(object):
    """Mixin for test setup to create some allocations with missing consumers
    """

class DeleteConsumerIfNoAllocsTestCase(tb.PlacementDbBaseTestCase):
    def test_delete_consumer_if_no_allocs(self):
        """alloc_obj.replace_all() should attempt to delete consumers that
        no longer have any allocations. Due to the REST API not having any way
        to query for consumers directly (only via the GET
        /allocations/{consumer_uuid} endpoint which returns an empty dict even
        when no consumer record exists for the {consumer_uuid}) we need to do
        this functional test using only the object layer.
        """
        # We will use two consumers in this test, only one of which will get
        # all of its allocations deleted in a transaction (and we expect that
        # consumer record to be deleted)
        c1 = consumer_obj.Consumer(
            self.ctx, uuid=uuids.consumer1, user=self.user_obj,
            project=self.project_obj)
        c1.create()
        c2 = consumer_obj.Consumer(
            self.ctx, uuid=uuids.consumer2, user=self.user_obj,
            project=self.project_obj)
        c2.create()

        # Create some inventory that we will allocate
        cn1 = self._create_provider('cn1')
        tb.add_inventory(cn1, orc.VCPU, 8)
        tb.add_inventory(cn1, orc.MEMORY_MB, 2048)
        tb.add_inventory(cn1, orc.DISK_GB, 2000)

        # Now allocate some of that inventory to two different consumers
        allocs = [
            alloc_obj.Allocation(
                consumer=c1, resource_provider=cn1,
                resource_class=orc.VCPU, used=1),
            alloc_obj.Allocation(
                consumer=c1, resource_provider=cn1,
                resource_class=orc.MEMORY_MB, used=512),
            alloc_obj.Allocation(
                consumer=c2, resource_provider=cn1,
                resource_class=orc.VCPU, used=1),
            alloc_obj.Allocation(
                consumer=c2, resource_provider=cn1,
                resource_class=orc.MEMORY_MB, used=512),
        ]
        alloc_obj.replace_all(self.ctx, allocs)

        # Validate that we have consumer records for both consumers
        for c_uuid in (uuids.consumer1, uuids.consumer2):
            c_obj = consumer_obj.Consumer.get_by_uuid(self.ctx, c_uuid)
            self.assertIsNotNone(c_obj)

        # OK, now "remove" the allocation for consumer2 by setting the used
        # value for both allocated resources to 0 and re-running the
        # alloc_obj.replace_all(). This should end up deleting the
        # consumer record for consumer2
        allocs = [
            alloc_obj.Allocation(
                consumer=c2, resource_provider=cn1,
                resource_class=orc.VCPU, used=0),
            alloc_obj.Allocation(
                consumer=c2, resource_provider=cn1,
                resource_class=orc.MEMORY_MB, used=0),
        ]
        alloc_obj.replace_all(self.ctx, allocs)

        # consumer1 should still exist...
        c_obj = consumer_obj.Consumer.get_by_uuid(self.ctx, uuids.consumer1)
        self.assertIsNotNone(c_obj)

        # but not consumer2...
        self.assertRaises(
            exception.NotFound, consumer_obj.Consumer.get_by_uuid,
            self.ctx, uuids.consumer2)

        # DELETE /allocations/{consumer_uuid} is the other place where we
        # delete all allocations for a consumer. Let's delete all for consumer1
        # and check that the consumer record is deleted
        alloc_list = alloc_obj.get_all_by_consumer_id(
            self.ctx, uuids.consumer1)
        alloc_obj.delete_all(self.ctx, alloc_list)

        # consumer1 should no longer exist in the DB since we just deleted all
        # of its allocations
        self.assertRaises(
            exception.NotFound, consumer_obj.Consumer.get_by_uuid,
            self.ctx, uuids.consumer1)
