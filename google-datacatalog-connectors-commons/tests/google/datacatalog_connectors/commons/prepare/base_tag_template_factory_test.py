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

from google.cloud import datacatalog


class BaseTagFactoryTestCase(unittest.TestCase):

    def test_add_enum_type_field_should_set_elementary_properties(self):
        tag_template = datacatalog.TagTemplate()
        prepare.BaseTagTemplateFactory._add_enum_type_field(
            tag_template, 'enum-field', ['VALUE_1', 'VALUE_2'], 'Enum field')

        self.assertIn('enum-field', tag_template.fields)

        field = tag_template.fields['enum-field']
        self.assertEqual(2, len(field.type.enum_type.allowed_values))
        self.assertEqual('Enum field', field.display_name)

    def test_add_primitive_type_field_should_set_elementary_properties(self):
        tag_template = datacatalog.TagTemplate()
        prepare.BaseTagTemplateFactory._add_primitive_type_field(
            tag_template, 'string-field',
            datacatalog.FieldType.PrimitiveType.STRING, 'String field')

        self.assertIn('string-field', tag_template.fields)

        field = tag_template.fields['string-field']
        self.assertEqual(datacatalog.FieldType.PrimitiveType.STRING,
                         field.type.primitive_type)
        self.assertEqual('String field', field.display_name)
