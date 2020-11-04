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
from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare


class EntryRelationshipMapperTest(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __PREPARE_PACKAGE = '{}.prepare'.format(__COMMONS_PACKAGE)

    def setUp(self):
        self.__mapper = ChildParentRelationshipMapper()

    def test_fulfill_tag_fields_should_resolve_valid_mapping(self):
        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'parent')
        parent_tag = self.__make_fake_tag(string_fields=(('id', parent_id),))

        child_id = 'test_child'
        child_entry = self.__make_fake_entry(child_id, 'child')
        string_fields = ('id', child_id), ('parent_id', parent_id)
        child_tag = self.__make_fake_tag(string_fields=string_fields)

        parent_assembled_entry = prepare.AssembledEntryData(
            parent_id, parent_entry, [parent_tag])
        child_assembled_entry = prepare.AssembledEntryData(
            child_id, child_entry, [child_tag])

        self.__mapper.fulfill_tag_fields(
            [parent_assembled_entry, child_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/{}'.format(
                parent_entry.name),
            child_tag.fields['parent_entry'].string_value)

    def test_fulfill_multiple_tags_same_field_should_resolve_valid_mapping(
            self):
        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'parent')
        parent_tag = self.__make_fake_tag(string_fields=(('id', parent_id),))

        parent_id_2 = 'test_parent_2'
        parent_entry_2 = self.__make_fake_entry(parent_id_2, 'parent')
        parent_tag_2 = self.__make_fake_tag(string_fields=(('id',
                                                            parent_id_2),))

        child_id = 'test_child'
        child_entry = self.__make_fake_entry(child_id, 'child')
        string_fields = ('id', child_id), ('parent_id', parent_id)
        child_tag = self.__make_fake_tag(string_fields=string_fields)

        child_id_2 = 'test_child_2'
        string_fields_2 = ('id', child_id_2), ('parent_id', 'test_parent_2')
        child_tag_2 = self.__make_fake_tag(string_fields=string_fields_2)

        parent_assembled_entry = prepare.AssembledEntryData(
            parent_id, parent_entry, [parent_tag])

        parent_assembled_entry_2 = prepare.AssembledEntryData(
            parent_id_2, parent_entry_2, [parent_tag_2])

        child_assembled_entry = prepare.AssembledEntryData(
            child_id, child_entry, [child_tag, child_tag_2])

        self.__mapper.fulfill_tag_fields([
            parent_assembled_entry, parent_assembled_entry_2,
            child_assembled_entry
        ])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/{}'.format(
                parent_entry.name),
            child_tag.fields['parent_entry'].string_value)

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/{}'.format(
                parent_entry_2.name),
            child_tag_2.fields['parent_entry'].string_value)

    def test_fulfill_tag_fields_should_not_resolve_invalid_mapping(self):
        child_id = 'test_child'

        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'parent')
        string_fields = ('id', parent_id), ('child_id', child_id)
        parent_tag = self.__make_fake_tag(string_fields=string_fields)

        child_entry = self.__make_fake_entry(child_id, 'child')
        child_tag = self.__make_fake_tag(string_fields=(('id', child_id),))

        parent_assembled_entry = prepare.AssembledEntryData(
            parent_id, parent_entry, [parent_tag])
        child_assembled_entry = prepare.AssembledEntryData(
            child_id, child_entry, [child_tag])

        self.__mapper.fulfill_tag_fields(
            [parent_assembled_entry, child_assembled_entry])

        self.assertNotIn('child_entry', parent_tag.fields)
        self.assertNotIn('parent_entry', child_tag.fields)

    @mock.patch('{}.BaseEntryRelationshipMapper'
                '._BaseEntryRelationshipMapper__build_id_name_pairs'.format(
                    __PREPARE_PACKAGE))
    def test_fulfill_tag_fields_no_entries_should_skip(self, mock_build_pairs):
        ChildParentRelationshipMapper().fulfill_tag_fields([])
        mock_build_pairs.assert_not_called()

    def test_fulfill_tag_fields_should_ignore_non_mappable_entry(self):
        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'parent')
        parent_tag = self.__make_fake_tag()

        child_id = 'test_child'
        child_entry = self.__make_fake_entry(child_id, 'child')
        string_fields = ('id', child_id), ('parent_id', parent_id)
        child_tag = self.__make_fake_tag(string_fields=string_fields)

        parent_assembled_entry = prepare.AssembledEntryData(
            parent_id, parent_entry, [parent_tag])
        child_assembled_entry = prepare.AssembledEntryData(
            child_id, child_entry, [child_tag])

        self.__mapper.fulfill_tag_fields(
            [parent_assembled_entry, child_assembled_entry])

        self.assertNotIn('parent_entry', child_tag.fields)

    def test_fulfill_tag_fields_should_ignore_entry_with_no_tags(self):
        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'parent')

        child_id = 'test_child'
        child_entry = self.__make_fake_entry(child_id, 'child')

        parent_assembled_entry = prepare.AssembledEntryData(
            parent_id, parent_entry, [])
        child_assembled_entry = prepare.AssembledEntryData(
            child_id, child_entry, [])

        self.__mapper.fulfill_tag_fields(
            [parent_assembled_entry, child_assembled_entry])

        self.assertEqual(len(child_assembled_entry.tags), 0)

    @classmethod
    def __make_fake_entry(cls, entry_id, entry_type):
        entry = datacatalog.Entry()
        entry.name = 'fake_entries/{}'.format(entry_id)
        entry.user_specified_type = entry_type
        return entry

    @classmethod
    def __make_fake_tag(cls, string_fields=None, double_fields=None):
        tag = datacatalog.Tag()

        if string_fields:
            for field in string_fields:
                string_field = datacatalog.TagField()
                string_field.string_value = field[1]
                tag.fields[field[0]] = string_field

        if double_fields:
            for field in double_fields:
                double_field = datacatalog.TagField()
                double_field.double_value = field[1]
                tag.fields[field[0]] = double_field

        return tag


class ChildParentRelationshipMapper(prepare.BaseEntryRelationshipMapper):

    def fulfill_tag_fields(self, assembled_entries_data):
        self._fulfill_tag_fields(assembled_entries_data,
                                 [self.__resolve_mappings])

    @classmethod
    def __resolve_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == 'child':
                continue

            cls._map_related_entry(assembled_entry_data, 'parent', 'parent_id',
                                   'parent_entry', id_name_pairs)
