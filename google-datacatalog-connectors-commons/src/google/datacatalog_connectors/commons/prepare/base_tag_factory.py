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

from google.datacatalog_connectors.commons import prepare


class BaseTagFactory:
    __STRING_VALUE_UTF8_MAX_LENGTH = 2000

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
        length when encoded in UTF-8. Given a string Tag Field and its
        value, this method assigns the value to the field, truncating
        if needed.
        """

        if not (value and isinstance(value, six.string_types)):
            return

        truncated_string = prepare.DataCatalogStringsHelper.truncate_string(
            value, cls.__STRING_VALUE_UTF8_MAX_LENGTH)

        string_field = datacatalog.TagField()
        string_field.string_value = truncated_string

        tag.fields[field_id] = string_field

    @classmethod
    def _set_timestamp_field(cls, tag, field_id, value):
        if value:
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(value)

            timestamp_field = datacatalog.TagField()
            timestamp_field.timestamp_value = timestamp
            tag.fields[field_id] = timestamp_field
