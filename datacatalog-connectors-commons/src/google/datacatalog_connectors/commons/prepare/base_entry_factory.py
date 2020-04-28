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

import re
import unicodedata
import six


class BaseEntryFactory:
    __ASCII_CHARACTER_ENCODING = 'ASCII'

    @classmethod
    def _format_id(cls, source_id):
        formatted_id = re.sub(r'[^a-zA-Z0-9]+', '_',
                              cls.__normalize_ascii_chars(source_id.strip()))
        return formatted_id[:64] if len(formatted_id) > 64 else formatted_id

    @classmethod
    def _format_display_name(cls, source_name):
        formatted_name = re.sub(
            r'[^a-zA-Z0-9_\- ]+', '_',
            cls.__normalize_ascii_chars(source_name).strip())
        return formatted_name

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
