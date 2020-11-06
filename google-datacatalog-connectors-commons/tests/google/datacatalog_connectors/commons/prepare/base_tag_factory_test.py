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

from google.datacatalog_connectors.commons import prepare
import dateutil.parser

from google.cloud import datacatalog


class BaseTagFactoryTestCase(unittest.TestCase):

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

    def test_set_string_field_should_skip_none_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', None)

        self.assertNotIn('string', tag.fields)

    def test_set_string_field_should_skip_empty_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', '')

        self.assertNotIn('string', tag.fields)

    def test_set_string_field_should_truncate_uft8_bytes_size(self):
        """
         - Input string: 2001 'a' chars;
         - Expected field value: '1997 "a" chars + ...' since each char needs
         1 byte when encoded in UTF-8.
        """

        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_string_field(tag, 'string', 'a' * 2001)

        self.assertEqual(2000, len(tag.fields['string'].string_value))
        self.assertEqual('{}...'.format('a' * 1997),
                         tag.fields['string'].string_value)
        self.assertEqual(
            2000, len(tag.fields['string'].string_value.encode('UTF-8')))

    def test_set_string_field_should_truncate_uft8_bytes_size_i18n(self):
        """
         - Input string: 1010 'ã' chars;
         - Expected field value: '998 "ã" chars + ...' since each 'ã' char
         needs 2 bytes and periods need 1 byte when encoded in UTF-8.
        """

        tag = datacatalog.Tag()
        str_value = u''
        for _ in range(1010):
            str_value += u'ã'

        prepare.BaseTagFactory._set_string_field(tag, 'string', str_value)

        self.assertEqual(1001, len(tag.fields['string'].string_value))

        str_value = u''
        for _ in range(998):
            str_value += u'ã'

        self.assertEqual(u'{}...'.format(str_value),
                         tag.fields['string'].string_value)
        self.assertEqual(
            1999, len(tag.fields['string'].string_value.encode('UTF-8')))

    def test_set_string_field_should_truncate_uft8_bytes_size_mixed(self):
        """
         - Input string: 1990 'a' chars + 10 'ã' chars;
         - Expected field value: '1990 "a" chars + 3 "ã" chars + ...' since
         each 'ã' char needs 2 bytes, 'a' chars and periods need 1 byte when
         encoded in UTF-8.
        """

        tag = datacatalog.Tag()

        str_value = u''
        for _ in range(10):
            str_value += u'ã'

        prepare.BaseTagFactory._set_string_field(
            tag, 'string', u'{}{}'.format('a' * 1990, str_value))

        str_value = u''
        for _ in range(3):
            str_value += u'ã'

        self.assertEqual(1996, len(tag.fields['string'].string_value))
        self.assertEqual(u'{}{}...'.format('a' * 1990, str_value),
                         tag.fields['string'].string_value)
        self.assertEqual(
            1999, len(tag.fields['string'].string_value.encode('UTF-8')))

    def test_set_timestamp_field_should_skip_none_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_timestamp_field(tag, 'timestamp-field',
                                                    None)

        self.assertNotIn('timestamp-field', tag.fields)

    def test_set_timestamp_field_should_set_given_value(self):
        tag = datacatalog.Tag()
        prepare.BaseTagFactory._set_timestamp_field(
            tag, 'timestamp-field',
            dateutil.parser.isoparse('2019-09-12T16:30:00+0000'))

        date = dateutil.parser.isoparse('2019-09-12T16:30:00+0000')
        self.assertEqual(
            int(calendar.timegm(date.utctimetuple())),
            tag.fields['timestamp-field'].timestamp_value.timestamp())
