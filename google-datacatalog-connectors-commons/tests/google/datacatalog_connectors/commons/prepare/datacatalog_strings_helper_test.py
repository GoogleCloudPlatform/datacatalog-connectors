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


class DataCatalogStringsHelperTestCase(unittest.TestCase):
    __STRING_VALUE_UTF8_MAX_LENGTH = 2000

    def test_truncate_string_should_skip_none_value(self):
        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            None, self.__STRING_VALUE_UTF8_MAX_LENGTH)

        self.assertEqual(None, truncated_string)

    def test_truncate_string_should_return_empty_value(self):
        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            '', self.__STRING_VALUE_UTF8_MAX_LENGTH)

        self.assertEqual('', truncated_string)

    def test_truncate_string_should_truncate_on_max_length(self):
        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            'a' * 201, 200)

        self.assertEqual('{}...'.format('a' * 197), truncated_string)

    def test_truncate_string_should_truncate_uft8_bytes_size(self):
        """
         - Input string: 2001 'a' chars;
         - Expected field value: '1997 "a" chars + ...' since each char needs
         1 byte when encoded in UTF-8.
        """

        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            'a' * 2001, self.__STRING_VALUE_UTF8_MAX_LENGTH)
        self.assertEqual(2000, len(truncated_string))
        self.assertEqual('{}...'.format('a' * 1997), truncated_string)
        self.assertEqual(2000, len(truncated_string.encode('UTF-8')))

    def test_truncate_string_should_truncate_uft8_bytes_size_i18n(self):
        """
         - Input string: 1010 'ã' chars;
         - Expected field value: '998 "ã" chars + ...' since each 'ã' char
         needs 2 bytes and periods need 1 byte when encoded in UTF-8.
        """

        string = u''
        for _ in range(1010):
            string += u'ã'

        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            string, self.__STRING_VALUE_UTF8_MAX_LENGTH)

        self.assertEqual(1001, len(truncated_string))

        string = u''
        for _ in range(998):
            string += u'ã'

        self.assertEqual(u'{}...'.format(string), truncated_string)
        self.assertEqual(1999, len(truncated_string.encode('UTF-8')))

    def test_truncate_string_should_truncate_uft8_bytes_size_mixed(self):
        """
         - Input string: 1990 'a' chars + 10 'ã' chars;
         - Expected field value: '1990 "a" chars + 3 "ã" chars + ...' since
         each 'ã' char needs 2 bytes, 'a' chars and periods need 1 byte when
         encoded in UTF-8.
        """

        string = u''
        for _ in range(10):
            string += u'ã'

        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            u'{}{}'.format('a' * 1990, string),
            self.__STRING_VALUE_UTF8_MAX_LENGTH)

        string = u''
        for _ in range(3):
            string += u'ã'

        self.assertEqual(1996, len(truncated_string))
        self.assertEqual(u'{}{}...'.format('a' * 1990, string),
                         truncated_string)
        self.assertEqual(1999, len(truncated_string.encode('UTF-8')))
