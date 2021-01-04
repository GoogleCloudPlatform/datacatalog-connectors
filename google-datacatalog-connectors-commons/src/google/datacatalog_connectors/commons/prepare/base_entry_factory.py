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

from google.datacatalog_connectors.commons import prepare


class BaseEntryFactory:
    __ASCII_CHARACTER_ENCODING = 'ASCII'
    __DEFAULT_ENTRY_ID_HASH_LENGTH = 8
    __DEFAULT_ENTRY_ID_INVALID_CHARS_REGEX_PATTERN = r'[^a-zA-Z0-9]+'
    __ID_MAX_LENGTH = 64

    # Linked_resource must contain only letters, numbers, periods, colons,
    # slashes, underscores, dashes and hashes.
    __LINKED_RESOURCE_INVALID_CHARS_REGEX_PATTERN = r'[^\w\.\/\-#:]+'
    __LINKED_RESOURCE_UTF8_MAX_LENGTH = 200

    @classmethod
    def _format_id(cls, source_id):
        formatted_id = cls.__normalize_string(
            cls.__DEFAULT_ENTRY_ID_INVALID_CHARS_REGEX_PATTERN, source_id)

        return formatted_id[:cls.__ID_MAX_LENGTH] if \
            len(formatted_id) > cls.__ID_MAX_LENGTH else formatted_id

    @classmethod
    def _format_id_with_hashing(
            cls,
            source_id,
            regex_pattern=__DEFAULT_ENTRY_ID_INVALID_CHARS_REGEX_PATTERN,
            hash_length=__DEFAULT_ENTRY_ID_HASH_LENGTH):
        """
        Normalizes the source_id using the regex_pattern, and optionally
        applies a hashing strategy (considering the provided hash_length)
        over the source_id to generate a unique ID on a best-effort basis.

        :param source_id: value to be formatted.
        :param regex_pattern: pattern used to replace invalid characters.
        :param hash_length: length to be used for hashing
         the ending of the ID.

        :return: The formatted ID.
        """
        formatted_id = cls.__normalize_string(regex_pattern, source_id)

        if len(formatted_id) <= cls.__ID_MAX_LENGTH:
            return formatted_id

        hash = hashlib.sha1()
        hash.update(formatted_id.encode(cls.__ASCII_CHARACTER_ENCODING))

        return formatted_id[:cls.__ID_MAX_LENGTH - hash_length] + \
            hash.hexdigest()[:hash_length]

    @classmethod
    def _format_linked_resource(cls, linked_resource, normalize=True):
        """
        Formats the linked_resource to fit the string bytes limit enforced by
        Data Catalog, and optionally normalizes it by applying a regex pattern
        that replaces unsupported characters with underscore.

        Warning: truncating and normalizing linked resources
        may lead to invalid resulting URLs.

        :param linked_resource: the value to be formatted.
        :param normalize: enables the normalize logic.

        :return: The formatted linked resource.
        """
        formatted_linked_resource = linked_resource
        if normalize:
            formatted_linked_resource = cls.__normalize_string(
                cls.__LINKED_RESOURCE_INVALID_CHARS_REGEX_PATTERN,
                linked_resource)

        return prepare.DataCatalogStringsHelper.truncate_string(
            formatted_linked_resource, cls.__LINKED_RESOURCE_UTF8_MAX_LENGTH)

    @classmethod
    def _format_display_name(cls, source_name):
        return cls.__normalize_string(r'[^\w\- ]+', source_name)

    @classmethod
    def __normalize_string(cls, regex_pattern, source_string):
        formatted_str = re.sub(
            regex_pattern, '_',
            cls.__normalize_ascii_chars(source_string.strip()))

        # The __normalize_ascii_chars logic may replace a non ascii
        # char with space at the end of the string, so we need to do
        # an additional strip() to make sure it is removed from the
        # final normalized string.
        return formatted_str.strip()

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
