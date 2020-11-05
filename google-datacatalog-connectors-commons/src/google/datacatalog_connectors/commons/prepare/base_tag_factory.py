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

from google.cloud import datacatalog
from google.protobuf import timestamp_pb2
import six


class BaseTagFactory:
    __UTF8_CHARACTER_ENCODING = 'UTF-8'
    # String field values are limited to 2000 bytes size when encoded in UTF-8.
    __STRING_VALUE_UTF8_MAX_LENGTH = 2000
    __SUFFIX_CHARS_LENGTH = 3

    @classmethod
    def _set_bool_field(cls, tag, field_id, value):
        if value is not None:
            bool_field = datacatalog.TagField()
            bool_field.bool_value = value
            tag.fields[field_id] = bool_field

    @classmethod
    def _set_double_field(cls, tag, field_id, value):
        if value is not None:
            double_field = datacatalog.TagField()
            double_field.double_value = value
            tag.fields[field_id] = double_field

    @classmethod
    def _set_string_field(cls, tag, field_id, value):
        """
        String field values are limited by Data Catalog API at 2000 chars
        length when encoded in UTF-8. UTF-8 chars may need from 1 to 4 bytes
        (https://en.wikipedia.org/wiki/UTF-8 for details):
        - the first 128 characters (US-ASCII) need one byte;
        - the next 1,920 characters need two bytes to encode, which covers the
          remainder of almost all Latin-script alphabets, and also Greek,
          Cyrillic, Coptic, Armenian, Hebrew, Arabic, Syriac, Thaana and N'Ko
          alphabets, as well as Combining Diacritical Marks;
        - three bytes are needed for characters in the rest of the Basic
          Multilingual Plane, which contains virtually all characters in common
          use, including most Chinese, Japanese and Korean characters;
        - four bytes are needed for characters in the other planes of Unicode,
          which include less common CJK characters, various historic scripts,
          mathematical symbols, and emoji (pictographic symbols).

        Given a value and a string Tag Field, this method assigns the field the
        value. Before assigning it checks the value's UTF-8 byte-size and
        truncates if needed. When it happens, 3 periods are appended to the
        result string so users will know it's different from the original
        value.
        """
        if not (value and isinstance(value, six.string_types)):
            return

        encoding = cls.__UTF8_CHARACTER_ENCODING
        max_length = cls.__STRING_VALUE_UTF8_MAX_LENGTH
        suffix_length = cls.__SUFFIX_CHARS_LENGTH

        encoded = value.encode(encoding)

        # the max length supported is stored at max_length
        # we leave some chars as the suffix_length to be used when
        # creating the new string, so this line truncates the existing string.
        truncated_string_field = encoded[:max_length - suffix_length]

        decoded = u'{}...'.format(
            truncated_string_field.decode(
                encoding,
                'ignore')) if len(encoded) > max_length else encoded.decode(
                    encoding, 'ignore')

        string_field = datacatalog.TagField()
        string_field.string_value = decoded
        tag.fields[field_id] = string_field

    @classmethod
    def _set_timestamp_field(cls, tag, field_id, value):
        if value:
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(value)

            timestamp_field = datacatalog.TagField()
            timestamp_field.timestamp_value = timestamp
            tag.fields[field_id] = timestamp_field
