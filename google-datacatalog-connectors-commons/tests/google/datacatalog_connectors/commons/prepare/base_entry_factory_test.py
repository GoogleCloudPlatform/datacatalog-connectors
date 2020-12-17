#!/usr/bin/python
# coding=utf-8
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

from google.datacatalog_connectors.commons import prepare


class BaseEntryFactoryTestCase(unittest.TestCase):

    def test_format_id_should_normalize_non_compliant_id(self):
        formatted_id = prepare.BaseEntryFactory._format_id(u'ã123 - b456  ')
        self.assertEqual('a123_b456', formatted_id)

    def test_format_id_with_hashing_should_normalize_non_compliant_id(self):
        long_str = 'organization_warehouse7192ecb2__personsc3a8d512_' \
                   'business_area_and_segment_of_marketing'

        expected_str = 'organization_warehouse7192ecb2_personsc3a8d512_' \
                       'business_7074c286'

        formatted_id = prepare.BaseEntryFactory._format_id_with_hashing(
            long_str, 8)
        self.assertEqual(expected_str, formatted_id)

    def test_format_display_name_should_normalize_non_compliant_name(self):
        formatted_name = prepare.BaseEntryFactory._format_display_name(
            u'ã123 :?: b456  ')
        self.assertEqual('a123 _ b456', formatted_name)
