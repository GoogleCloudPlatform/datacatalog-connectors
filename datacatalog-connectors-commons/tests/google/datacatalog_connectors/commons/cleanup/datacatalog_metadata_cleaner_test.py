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
from google.datacatalog_connectors.commons import cleanup
import mock

from google.api_core import exceptions


class DataCatalogMetadataCleanerTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __SEARCH_QUERY = 'system=my_system'

    @mock.patch(
        '{}.datacatalog_facade.DataCatalogFacade'.format(__COMMONS_PACKAGE))
    def setUp(self, mock_datacatalog_facade):
        self.__metadata_cleaner =\
            cleanup.DataCatalogMetadataCleaner(
                'project-id', 'location-id', 'entry_group_id')
        # Shortcut for the object assigned
        # to self.__metadata_cleaner.__datacatalog_facade
        self.__datacatalog_facade = mock_datacatalog_facade.return_value

    def test_delete_obsolete_no_deleted_entries_should_not_clean_up(self):
        entries = \
            utils.Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.search_catalog_relative_resource_name\
            .return_value = ['name_1']

        self.__metadata_cleaner.delete_obsolete_metadata(
            entries, self.__SEARCH_QUERY)

        self.assertEqual(
            1, datacatalog_facade.search_catalog_relative_resource_name.
            call_count)
        datacatalog_facade.delete_entry.assert_not_called()
        datacatalog_facade.delete_entry_group.assert_not_called()

    def test_delete_obsolete_deleted_entries_should_clean_up(self):
        entries = \
            utils.Utils.create_assembled_entries_user_defined_types()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.search_catalog_relative_resource_name\
            .return_value = ['deleted_entry_name_1', 'deleted_entry_name_2']

        self.__metadata_cleaner.delete_obsolete_metadata(
            entries, self.__SEARCH_QUERY)

        self.assertEqual(
            1, datacatalog_facade.search_catalog_relative_resource_name.
            call_count)
        self.assertEqual(2, datacatalog_facade.delete_entry.call_count)
        datacatalog_facade.delete_entry_group.assert_not_called()

    def test_delete_obsolete_should_trigger_entry_group_clean_up(self):
        entries = []

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.search_catalog_relative_resource_name\
            .return_value = [
                'projects/uat-env-1/locations/us-central1/'
                'entryGroups/system/entries/database',
                'projects/uat-env-1/locations/us-central1/'
                'entryGroups/system/entries/table'
            ]

        self.__metadata_cleaner.delete_obsolete_metadata(
            entries, self.__SEARCH_QUERY)

        self.assertEqual(
            1, datacatalog_facade.search_catalog_relative_resource_name.
            call_count)
        self.assertEqual(2, datacatalog_facade.delete_entry.call_count)
        self.assertEqual(1, datacatalog_facade.delete_entry_group.call_count)

    def test_delete_metadata_should_succeed(self):
        entries = \
            utils.Utils.create_assembled_entries_user_defined_types()

        self.__metadata_cleaner.delete_metadata(entries)

        datacatalog_facade = self.__datacatalog_facade
        self.assertEqual(2, datacatalog_facade.delete_entry.call_count)

    def test_delete_metadata_no_entries_should_succeed(self):
        self.__metadata_cleaner.delete_metadata([])

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.delete_entry.assert_not_called()

    def test_error_when_deleting_entry_group_should_be_ignored(self):
        entries = []

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.search_catalog_relative_resource_name\
            .return_value = [
                'projects/uat-env-1/locations/us-central1/'
                'entryGroups/system/entries/database',
                'projects/uat-env-1/locations/us-central1/'
                'entryGroups/system/entries/table'
            ]

        datacatalog_facade.delete_entry_group.side_effect = \
            exceptions.GoogleAPICallError('Error when deleting entry group')

        self.__metadata_cleaner.delete_obsolete_metadata(
            entries, self.__SEARCH_QUERY)

        self.assertEqual(
            1, datacatalog_facade.search_catalog_relative_resource_name.
            call_count)
        self.assertEqual(2, datacatalog_facade.delete_entry.call_count)
        self.assertEqual(1, datacatalog_facade.delete_entry_group.call_count)
