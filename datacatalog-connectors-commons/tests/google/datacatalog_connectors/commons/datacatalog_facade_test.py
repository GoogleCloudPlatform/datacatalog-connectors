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
from google.datacatalog_connectors import commons
import mock

from google.api_core import exceptions
from google.cloud.datacatalog import types


class DataCatalogFacadeTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __SEARCH_CATALOG_METHOD = '{}.DataCatalogFacade.search_catalog'.format(
        __COMMONS_PACKAGE)

    @mock.patch('{}.datacatalog_facade.datacatalog.DataCatalogClient'.format(
        __COMMONS_PACKAGE))
    def setUp(self, mock_datacatalog_client):
        self.__datacatalog_facade = commons\
            .DataCatalogFacade('test-project')
        # Shortcut for the object assigned
        # to self.__datacatalog_facade.__datacatalog
        self.__datacatalog_client = mock_datacatalog_client.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__datacatalog_facade.__dict__
        self.assertIsNotNone(attrs['_DataCatalogFacade__datacatalog'])
        self.assertEqual('test-project',
                         attrs['_DataCatalogFacade__project_id'])

    def test_create_entry_should_succeed(self):
        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        self.__datacatalog_facade.create_entry('entry_group_name', 'entry_id',
                                               entry)

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_entry.call_count)

    def test_create_entry_should_return_original_on_permission_denied(self):
        datacatalog = self.__datacatalog_client
        datacatalog.create_entry.side_effect = \
            exceptions.PermissionDenied('Permission denied')

        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        result = self.__datacatalog_facade.create_entry(
            'entry_group_name', 'entry_id', entry)

        self.assertEqual(1, datacatalog.create_entry.call_count)
        self.assertEqual(entry, result)

    def test_get_entry_should_succeed(self):
        self.__datacatalog_facade.get_entry('entry_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.get_entry.call_count)

    def test_update_entry_should_succeed(self):
        self.__datacatalog_facade.update_entry({})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.update_entry.call_count)

    def test_upsert_entry_nonexistent_should_create(self):
        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.side_effect = \
            exceptions.PermissionDenied('Entry not found')

        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'entry_id',
                                               entry)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.create_entry.call_count)

    def test_upsert_entry_changed_should_update(self):
        entry_1 = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource_1', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry_1

        entry_2 = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource_2', 11, 22)

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'entry_id',
                                               entry_2)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.update_entry.call_count)
        datacatalog.update_entry.assert_called_with(entry=entry_2,
                                                    update_mask=None)

    def test_upsert_entry_should_return_original_on_failed_precondition(self):
        entry_1 = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource_1', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry_1
        datacatalog.update_entry.side_effect = \
            exceptions.FailedPrecondition('Failed precondition')

        entry_2 = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource_2', 11, 22)

        result = self.__datacatalog_facade.upsert_entry(
            'entry_group_name', 'entry_id', entry_2)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        self.assertEqual(1, datacatalog.update_entry.call_count)
        self.assertEqual(entry_1, result)

    def test_upsert_entry_unchanged_should_not_update(self):
        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        datacatalog = self.__datacatalog_client
        datacatalog.get_entry.return_value = entry

        self.__datacatalog_facade.upsert_entry('entry_group_name', 'entry_id',
                                               entry)

        self.assertEqual(1, datacatalog.get_entry.call_count)
        datacatalog.update_entry.assert_not_called()

    def test_delete_entry_should_succeed(self):
        self.__datacatalog_facade.delete_entry('entry_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.delete_entry.call_count)

    def test_delete_entry_error_should_be_ignored(self):
        datacatalog = self.__datacatalog_client
        datacatalog.delete_entry.side_effect = \
            Exception('Error when deleting entry')

        self.__datacatalog_facade.delete_entry('entry_name')

        self.assertEqual(1, datacatalog.delete_entry.call_count)

    def test_create_entry_group_should_succeed(self):
        self.__datacatalog_facade.create_entry_group('location-id',
                                                     'entry_group_id')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_entry_group.call_count)

    def test_delete_entry_group_should_succeed(self):
        self.__datacatalog_facade.delete_entry_group('entry_group_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.delete_entry_group.call_count)

    def test_create_tag_template_should_succeed(self):
        self.__datacatalog_facade.create_tag_template('location-id',
                                                      'tag_template_id', {})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_tag_template.call_count)

    def test_get_tag_template_should_succeed(self):
        self.__datacatalog_facade.get_tag_template('tag_template_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.get_tag_template.call_count)

    def test_delete_tag_template_should_succeed(self):
        self.__datacatalog_facade.delete_tag_template('tag_template_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.delete_tag_template.call_count)

    def test_create_tag_should_succeed(self):
        self.__datacatalog_facade.create_tag('entry_name', {})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.create_tag.call_count)

    def test_list_tags_should_succeed(self):
        self.__datacatalog_facade.list_tags('entry_name')

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.list_tags.call_count)

    def test_update_tag_should_succeed(self):
        self.__datacatalog_facade.update_tag({})

        datacatalog = self.__datacatalog_client
        self.assertEqual(1, datacatalog.update_tag.call_count)

    def test_upsert_tags_nonexistent_should_succeed(self):
        datacatalog = self.__datacatalog_client
        datacatalog.list_tags.return_value = []

        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)
        self.__datacatalog_facade.upsert_tags(entry, [self.__create_tag()])

        self.assertEqual(1, datacatalog.create_tag.call_count)
        datacatalog.update_tag.assert_not_called()

    def test_upsert_tags_changed_should_succeed(self):
        datacatalog = self.__datacatalog_client
        datacatalog.list_tags.return_value = [self.__create_tag()]

        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)
        changed_tag = self.__create_tag()
        changed_tag.fields['bool-field'].bool_value = False
        self.__datacatalog_facade.upsert_tags(entry, [changed_tag])

        datacatalog.create_tag.assert_not_called()
        self.assertEqual(1, datacatalog.update_tag.call_count)

    def test_upsert_tags_unchanged_should_succeed(self):
        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        tag = self.__create_tag()

        datacatalog = self.__datacatalog_client
        datacatalog.list_tags.return_value = [tag]

        self.__datacatalog_facade.upsert_tags(entry, [tag])

        datacatalog.create_tag.assert_not_called()
        datacatalog.update_tag.assert_not_called()

    def test_upsert_tags_should_handle_empty_list(self):
        entry = utils.Utils.create_entry_user_defined_type(
            'type', 'system', 'display_name', 'name', 'description',
            'linked_resource', 11, 22)

        try:
            self.__datacatalog_facade.upsert_tags(entry, None)
        except exceptions.GoogleAPICallError as e:
            super(DataCatalogFacadeTestCase, self).fail(e)

    def test_search_results_should_return_values(self):
        expected_return_value = [
            self.__create_search_result('localhost//asset_1'),
            self.__create_search_result('localhost//asset_2')
        ]

        datacatalog = self.__datacatalog_client
        datacatalog.search_catalog.return_value = expected_return_value

        return_value = self.__datacatalog_facade.search_catalog('query')

        self.assertEqual(1, datacatalog.search_catalog.call_count)
        self.assertEqual(expected_return_value, return_value)

    @mock.patch(__SEARCH_CATALOG_METHOD)
    def test_search_catalog_relative_resource_name_should_return_names(
            self, mock_search_catalog):  # noqa: E125

        expected_resource_names = ['localhost//asset_1', 'localhost//asset_2']

        search_return_values = [
            self.__create_search_result(resource_name)
            for resource_name in expected_resource_names
        ]

        mock_search_catalog.return_value = search_return_values

        resource_names = self.__datacatalog_facade\
            .search_catalog_relative_resource_name(
                'system=bigquery')

        self.assertEqual(1, mock_search_catalog.call_count)
        self.assertEqual(expected_resource_names, resource_names)

    @classmethod
    def __create_tag(cls):
        tag = types.Tag()
        tag.name = 'tag_template'
        tag.template = 'template'
        tag.fields['bool-field'].bool_value = True
        tag.fields['double-field'].double_value = 1
        tag.fields['string-field'].string_value = 'Test String Value'
        tag.fields['timestamp-field'].timestamp_value.FromJsonString(
            '2019-09-06T11:00:00-03:00')
        tag.fields['enum-field'].enum_value.display_name = \
            'Test ENUM Value'

        return tag

    @classmethod
    def __create_search_result(cls, relative_resource_name):
        search_result = types.SearchCatalogResult()
        search_result.relative_resource_name = relative_resource_name
        return search_result
