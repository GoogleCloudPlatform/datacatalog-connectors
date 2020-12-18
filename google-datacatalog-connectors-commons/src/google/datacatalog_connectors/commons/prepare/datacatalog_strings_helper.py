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

import six


class DataCatalogStringsHelper:
    __TRUNCATED_STRING_SUFFIX_CHARS_LENGTH = 3
    __UTF8_CHARACTER_ENCODING = 'UTF-8'

    @classmethod
    def truncate_string(cls, string, max_length):
        """
        Given a string, this method checks its UTF-8 byte-size and
        truncates if needed. When it happens, 3 periods are appended to the
        result string so users will know it's different from the original
        value.

        UTF-8 chars may need from 1 to 4 bytes
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
        """
        if not (string is not None and isinstance(string, six.string_types)):
            return

        encoding = cls.__UTF8_CHARACTER_ENCODING
        suffix_length = cls.__TRUNCATED_STRING_SUFFIX_CHARS_LENGTH

        encoded = string.encode(encoding)

        # The max length supported is stored at max_length.
        # We leave some chars as the suffix_length to be used when
        # creating the new string, so this line truncates the existing string.
        truncated_string_field = encoded[:max_length - suffix_length]

        decoded = u'{}...'.format(
            truncated_string_field.decode(encoding, 'ignore')) \
            if len(encoded) > max_length \
            else encoded.decode(encoding, 'ignore')

        return decoded
