#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from google.datacatalog_connectors.commons import utils


class ValuesComparableObjectTestCase(unittest.TestCase):

    def test_equal_entries_should_return_true(self):
        object_1 = utils.ValuesComparableObject()

        object_1.user_specified_system = 'source_system'
        object_1.user_specified_type = 'source_type'
        object_1.display_name = 'display_name'
        object_1.description = 'description'
        object_1.linked_resource = 'linked_resource'

        object_2 = utils.ValuesComparableObject()
        object_2.user_specified_system = 'source_system'
        object_2.user_specified_type = 'source_type'
        object_2.display_name = 'display_name'
        object_2.description = 'description'
        object_2.linked_resource = 'linked_resource'

        self.assertEqual(object_1, object_2)

    def test_different_entries_should_return_false(self):
        object_1 = utils.ValuesComparableObject()

        object_1.user_specified_system = 'source_system'
        object_1.user_specified_type = 'source_type'
        object_1.display_name = 'display_name'
        object_1.description = 'description'
        object_1.linked_resource = 'linked_resource'

        object_2 = utils.ValuesComparableObject()
        object_2.user_specified_system = 'source_system different'
        object_2.user_specified_type = 'source_type'
        object_2.display_name = 'display_name'
        object_2.description = 'description'
        object_2.linked_resource = 'linked_resource'

        self.assertNotEqual(object_1, object_2)

    def test_comparable_entry_private_fields(self):
        object_1 = utils.ValuesComparableObject()

        expected_value = 'desc'

        getattr(object_1, '__setitem__')('description', expected_value)
        returned_value = getattr(object_1, '__getitem__')('description')

        self.assertEqual(returned_value, expected_value)
