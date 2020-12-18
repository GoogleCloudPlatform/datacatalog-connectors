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

import mock

from google.datacatalog_connectors.commons import prepare


class BaseEntryFactoryTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __PREPARE_PACKAGE = '{}.prepare'.format(__COMMONS_PACKAGE)

    def test_format_id_should_normalize_non_compliant_id(self):
        formatted_id = prepare.BaseEntryFactory._format_id(u'ã123 - b456  ')
        self.assertEqual('a123_b456', formatted_id)

    def test_format_id_with_hashing_should_normalize_non_compliant_id(self):
        long_str = 'organization_warehouse7192ecb2__personsc3a8d512_' \
                   'business_area_and_segment_of_marketing'

        expected_str = 'organization_warehouse7192ecb2_personsc3a8d512_' \
                       'business_7074c286'

        formatted_id = prepare.BaseEntryFactory._format_id_with_hashing(
            long_str, hash_length=8)
        self.assertEqual(expected_str, formatted_id)

    def test_format_id_with_provided_pattern_should_normalize_non_compliant_id(  # noqa: E501
            self):
        long_str = 'organization__warehouse7192ecb2__personsc3a8d512_' \
                   'business_area_and_segment_of_marketing'

        expected_str = 'organization__warehouse7192ecb2_' \
                       '_personsc3a8d512_businesa4f7e655'

        formatted_id = prepare.BaseEntryFactory._format_id_with_hashing(
            long_str, regex_pattern=r'[^a-zA-Z0-9_]+')
        self.assertEqual(expected_str, formatted_id)

    def test_format_display_name_should_normalize_non_compliant_name(self):
        formatted_name = prepare.BaseEntryFactory._format_display_name(
            u'ã123 :?: b456  ')
        self.assertEqual('a123 _ b456', formatted_name)

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_format_linked_resource_should_not_normalize_compliant_string(
            self, mock_truncate_string):
        # Return same value received.
        mock_truncate_string.side_effect = (lambda *args: args[0])

        formatted_linked_resource = prepare.BaseEntryFactory.\
            _format_linked_resource(
                'hdfs://namenode:8020/user/hive/warehouse/table_company'
                '_names_from_department_that_keeps_records_with_'
                'historical_data_from_every_single_member')

        self.assertEqual(
            'hdfs://namenode:8020/user/hive/warehouse/'
            'table_company_names_from_department_that_'
            'keeps_records_with_historical_data_'
            'from_every_single_member', formatted_linked_resource)

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_format_linked_resource_should_normalize_non_compliant_string(
            self, mock_truncate_string):
        # Return same value received.
        mock_truncate_string.side_effect = (lambda *args: args[0])

        formatted_linked_resource = prepare.BaseEntryFactory. \
            _format_linked_resource(
                'hdfs://[namenode]:8020/user/{hive}/[warehouse]/table_company'
                '_names_from_?department?_that_;keeps;_records_with_'
                'historical_data_from_every_single_member')

        self.assertEqual(
            'hdfs://_namenode_:8020/user/'
            '_hive_/_warehouse_/table_company_names_from'
            '__department__that__keeps__records_with_'
            'historical_data_from_every_single_member',
            formatted_linked_resource)

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_format_linked_resource_should_truncate_non_compliant_string(
            self, mock_truncate_string):
        expected_value = 'truncated_str...'
        mock_truncate_string.return_value = expected_value

        formatted_linked_resource = prepare.BaseEntryFactory. \
            _format_linked_resource(
                'hdfs://[namenode]:8020/user/{hive}/[warehouse]/table_company'
                '_names_from_?department?_that_;keeps;_records_with_'
                'historical_data_from_every_single_member')

        self.assertEqual(expected_value, formatted_linked_resource)
