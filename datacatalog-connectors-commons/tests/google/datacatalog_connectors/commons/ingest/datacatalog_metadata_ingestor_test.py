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

from google.datacatalog_connectors.commons_test import utils
from google.datacatalog_connectors.commons import ingest
import mock

from google.api_core import exceptions
from google.cloud.datacatalog import enums, types


class DataCatalogMetadataIngestorTestCase(unittest.TestCase):
    __BOOL_TYPE = enums.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = enums.FieldType.PrimitiveType.TIMESTAMP

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
        entries = utils\
            .Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.get_entry.return_value = None

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    def test_ingest_metadata_nonexistent_tag_template_should_succeed(self):
        entries = utils\
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
        entries = utils\
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
        entries = utils.Utils\
            .create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.create_entry_group.side_effect = \
            exceptions.AlreadyExists('Entry Group already exists')

        self.__metadata_ingestor.ingest_metadata(entries, {})

        self.assertEqual(1, datacatalog_facade.create_entry_group.call_count)
        self.assertEqual(2, datacatalog_facade.upsert_entry.call_count)

    @classmethod
    def __create_tag_template(cls):
        template = types.TagTemplate()

        tag_template_id = 'template'

        template.name = 'template name'

        template.display_name = 'template display name'

        template.fields['bool-field'].type.primitive_type = cls.__BOOL_TYPE
        template.fields['bool-field'].display_name = 'Bool Field'

        template.fields['double-field'].type.primitive_type = cls.__DOUBLE_TYPE
        template.fields['double-field'].display_name = 'Double Field'

        template.fields['string-field'].type.primitive_type = cls.__STRING_TYPE
        template.fields['string-field'].display_name = 'String Field'

        template.fields['timestamp-field'].type.primitive_type = \
            cls.__TIMESTAMP_TYPE
        template.fields['timestamp-field'].display_name = 'Timestamp Field'

        template.fields['enum-field'].type.enum_type.allowed_values\
            .add().display_name = 'Test ENUM Value'

        return tag_template_id, template
