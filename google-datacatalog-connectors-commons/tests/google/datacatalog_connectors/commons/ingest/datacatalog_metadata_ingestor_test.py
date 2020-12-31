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

import unittest

import mock
from google.api_core import exceptions
from google.cloud import datacatalog
from google.datacatalog_connectors.commons_test import utils

from google.datacatalog_connectors.commons import ingest


class DataCatalogMetadataIngestorTestCase(unittest.TestCase):
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'

    @mock.patch(
        '{}.datacatalog_facade.DataCatalogFacade'.format(__COMMONS_PACKAGE))
    def setUp(self, mock_datacatalog_facade):
        self.__metadata_ingestor = ingest \
            .DataCatalogMetadataIngestor(
                'project-id', 'location-id', 'entry_group_id')
        # Shortcut for the object assigned
        # to self.__metadata_ingestor.__datacatalog_facade
        self.__datacatalog_facade = mock_datacatalog_facade.return_value

    def test_ingest_metadata_should_succeed(self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    def test_ingest_metadata_on_upsert_entry_failed_precondition_should_not_raise(  # noqa:E501
            self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.upsert_entry.side_effect = \
            exceptions.FailedPrecondition('Failed precondition')

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    def test_ingest_metadata_on_upsert_entry_permission_denied_should_not_raise(  # noqa:E501
            self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.upsert_entry.side_effect = \
            exceptions.PermissionDenied('Permission denied')

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    def test_ingest_metadata_with_delete_tags_managed_template_config_should_succeed(  # noqa:E501
            self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None

        expected_tag_template_arg = 'entry_group_id'

        self.__metadata_ingestor.ingest_metadata(entries, {},
                                                 {'delete_tags': {}})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)
        self.assertEqual(2, datacatalog_facade.delete_tags.call_count)

        delete_tag_args = datacatalog_facade.delete_tags.call_args_list
        managed_tag_template_0 = delete_tag_args[0].args[2]
        managed_tag_template_1 = delete_tag_args[1].args[2]

        self.assertEqual(expected_tag_template_arg, managed_tag_template_0)
        self.assertEqual(expected_tag_template_arg, managed_tag_template_1)

    def test_ingest_metadata_with_delete_tags_config_should_succeed(self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None

        expected_tag_template_arg = 'my-template'

        self.__metadata_ingestor.ingest_metadata(entries, {}, {
            'delete_tags': {
                'managed_tag_template': expected_tag_template_arg
            }
        })

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)
        self.assertEqual(2, datacatalog_facade.delete_tags.call_count)

        delete_tag_args = datacatalog_facade.delete_tags.call_args_list
        managed_tag_template_0 = delete_tag_args[0].args[2]
        managed_tag_template_1 = delete_tag_args[1].args[2]

        self.assertEqual(expected_tag_template_arg, managed_tag_template_0)
        self.assertEqual(expected_tag_template_arg, managed_tag_template_1)

    def test_ingest_metadata_nonexistent_tag_template_should_succeed(self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None

        tag_template = self.__create_tag_template()
        tag_templates_dict = {'database': tag_template}

        self.__metadata_ingestor.ingest_metadata(entries, tag_templates_dict)

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)
        self.assertEqual(1, datacatalog_facade.create_tag_template.call_count)

    def test_ingest_metadata_existing_template_should_succeed(self):
        entries = utils \
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None
        datacatalog_facade.create_tag_template.side_effect = \
            exceptions.AlreadyExists('Tag Template already exists')

        tag_template = self.__create_tag_template()
        tag_templates_dict = {'database': tag_template}

        self.__metadata_ingestor.ingest_metadata(entries, tag_templates_dict)

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)
        self.assertEqual(1, datacatalog_facade.create_tag_template.call_count)

    def test_existing_entry_group_should_manually_build_entry_group_path(self):
        entries = utils.Utils \
            .create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.create_entry_group.side_effect = \
            exceptions.AlreadyExists('Entry Group already exists')

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    @classmethod
    def __create_tag_template(cls):
        template = datacatalog.TagTemplate()

        tag_template_id = 'template'

        template.name = 'template name'

        template.display_name = 'template display name'

        bool_field = datacatalog.TagTemplateField()
        bool_field.type.primitive_type = cls.__BOOL_TYPE
        template.fields['bool-field'] = bool_field

        double_field = datacatalog.TagTemplateField()
        double_field.type.primitive_type = cls.__DOUBLE_TYPE
        template.fields['double-field'] = double_field

        string_field = datacatalog.TagTemplateField()
        string_field.type.primitive_type = cls.__STRING_TYPE
        template.fields['string-field'] = string_field

        timestamp_field = datacatalog.TagTemplateField()
        timestamp_field.type.primitive_type = cls.__TIMESTAMP_TYPE
        template.fields['timestamp-field'] = timestamp_field

        enum_value = datacatalog.FieldType.EnumType.EnumValue()
        enum_value.display_name = 'Test ENUM Value'
        enum_field = datacatalog.TagTemplateField()
        enum_field.type.enum_type.allowed_values.append(enum_value)
        template.fields['enum-field'] = enum_field

        return tag_template_id, template
