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

import calendar
import unittest

from dateutil import parser
from google.cloud import datacatalog
import mock

from google.datacatalog_connectors.commons import prepare


class BaseTagFactoryTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __PREPARE_PACKAGE = '{}.prepare'.format(__COMMONS_PACKAGE)

    def test_set_bool_field_should_skip_none_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_bool_field(tag, 'bool', None)

        self.assertNotIn('bool', tag.fields)

    def test_set_bool_field_should_set_given_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_bool_field(tag, 'bool', False)

        self.assertFalse(tag.fields['bool'].bool_value)

    def test_set_double_field_should_skip_none_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_double_field(tag, 'double', None)

        self.assertNotIn('double', tag.fields)

    def test_set_double_field_should_set_given_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_double_field(tag, 'double', 2.5)

        self.assertEqual(2.5, tag.fields['double'].double_value)

    def test_set_double_zero_field_should_set_given_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_double_field(tag, 'double', 0)

        self.assertEqual(0, tag.fields['double'].double_value)

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_set_string_field_should_skip_none_value(self,
                                                     mock_truncate_string):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', None)

        self.assertNotIn('string', tag.fields)
        mock_truncate_string.assert_not_called()

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_set_string_field_should_skip_empty_value(self,
                                                      mock_truncate_string):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', '')

        self.assertNotIn('string', tag.fields)
        mock_truncate_string.assert_not_called()

    @mock.patch(
        '{}.DataCatalogStringsHelper.truncate_string'.format(__PREPARE_PACKAGE)
    )
    def test_set_string_field_should_set_given_value(self,
                                                     mock_truncate_string):
        expected_value = '{}...'.format('a' * 1997)
        mock_truncate_string.return_value = expected_value

        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', 'a' * 2001)

        self.assertEqual(expected_value, tag.fields['string'].string_value)

    def test_set_timestamp_field_should_skip_none_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_timestamp_field(tag, 'timestamp-field',
                                                    None)

        self.assertNotIn('timestamp-field', tag.fields)

    def test_set_timestamp_field_should_set_given_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_timestamp_field(
            tag, 'timestamp-field',
            parser.isoparse('2019-09-12T16:30:00+0000'))

        date = parser.isoparse('2019-09-12T16:30:00+0000')
        self.assertEqual(
            int(calendar.timegm(date.utctimetuple())),
            tag.fields['timestamp-field'].timestamp_value.timestamp())
