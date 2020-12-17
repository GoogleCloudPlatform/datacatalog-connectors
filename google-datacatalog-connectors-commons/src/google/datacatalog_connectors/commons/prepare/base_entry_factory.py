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

import hashlib
import re
import six
import unicodedata


class BaseEntryFactory:
    __ASCII_CHARACTER_ENCODING = 'ASCII'
    __ID_MAX_LENGTH = 64

    @classmethod
    def _format_id(cls, source_id):
        formatted_id = cls.__normalize_string(r'[^a-zA-Z0-9]+', source_id)

        return formatted_id[:cls.__ID_MAX_LENGTH] if \
            len(formatted_id) > cls.__ID_MAX_LENGTH else formatted_id

    @classmethod
    def _format_id_with_hashing(cls, source_id, hash_length):
        formatted_id = cls.__normalize_string(r'[^a-zA-Z0-9]+', source_id)

        if len(formatted_id) <= cls.__ID_MAX_LENGTH:
            return formatted_id

        hash = hashlib.sha1()
        hash.update(formatted_id.encode(cls.__ASCII_CHARACTER_ENCODING))

        return formatted_id[:cls.__ID_MAX_LENGTH - hash_length] + \
            hash.hexdigest()[:hash_length]

    @classmethod
    def _format_display_name(cls, source_name):
        return cls.__normalize_string(r'[^\w\- ]+', source_name)

    @classmethod
    def __normalize_string(cls, regex_pattern, source_string):
        formatted_str = re.sub(
            regex_pattern, '_',
            cls.__normalize_ascii_chars(source_string.strip()))
        return formatted_str

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
