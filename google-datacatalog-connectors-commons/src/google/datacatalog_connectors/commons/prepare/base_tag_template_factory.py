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


class BaseTagTemplateFactory:

    @classmethod
    def _add_enum_type_field(cls, tag_template, field_id, values,
                             display_name):

        field = datacatalog.TagTemplateField()
        for value in values:
            enum_value = datacatalog.FieldType.EnumType.EnumValue()
            enum_value.display_name = value
            field.type.enum_type.allowed_values.append(enum_value)

        field.display_name = display_name
        tag_template.fields[field_id] = field

    @classmethod
    def _add_primitive_type_field(cls, tag_template, field_id, field_type,
                                  display_name):

        field = datacatalog.TagTemplateField()
        field.type.primitive_type = field_type
        field.display_name = display_name
        tag_template.fields[field_id] = field
