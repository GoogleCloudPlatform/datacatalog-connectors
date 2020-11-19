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

import json
import os
import re

from google.datacatalog_connectors.commons import ingest
from google.datacatalog_connectors.commons import prepare

from google.cloud import datacatalog
from google.protobuf import timestamp_pb2

import pandas as pd


class Utils:
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    @classmethod
    def create_assembled_entries_user_defined_types(cls):
        entry_1 = cls.create_assembled_entry_user_defined_type(
            'entry_1', 'parent_type', 'system', 'display_name', 'name_1',
            'description', 'linked_resource_1', 11, 22)

        col_1 = cls.create_column_schema('column_1', 'int', 'description')
        col_2 = cls.create_column_schema('column_2', 'string', 'description')

        entry_2 = cls.create_assembled_entry_user_defined_type(
            'entry_1', 'child_type', 'system', 'display_name', 'name_2',
            'description', 'linked_resource_2', 11, 22, [col_1, col_2])

        return [entry_1, entry_2]

    @classmethod
    def create_assembled_entry_user_defined_type(cls,
                                                 entry_id,
                                                 entry_type,
                                                 system,
                                                 display_name,
                                                 name,
                                                 description,
                                                 linked_resource,
                                                 create_time_seconds,
                                                 update_time_seconds,
                                                 columns=None):

        entry = cls.create_entry_user_defined_type(
            entry_type, system, display_name, name, description,
            linked_resource, create_time_seconds, update_time_seconds, columns)

        return prepare.AssembledEntryData(entry_id, entry, None)

    @classmethod
    def create_column_schema(cls, name, column_type, description, mode=None):
        return datacatalog.ColumnSchema(column=name,
                                        type=column_type,
                                        description=description,
                                        mode=mode)

    @classmethod
    def convert_json_to_str(cls, json_obj):
        return json.dumps(json_obj, sort_keys=True, default=str)

    @classmethod
    def convert_json_to_object(cls, module_path, name):
        resolved_name = os.path.join(module_path,
                                     '../test_data/{}'.format(name))
        with open(resolved_name, 'r') as f:
            return json.load(f, object_hook=Utils.__timestamp_parser)

    @classmethod
    def create_entry_for_table(cls,
                               entry_id,
                               entry_type,
                               system,
                               display_name,
                               name,
                               description,
                               linked_resource,
                               create_time_seconds,
                               update_time_seconds,
                               column_schemas,
                               tags=None):
        user_defined_entry = Utils.create_user_defined_entry(
            entry_id, entry_type, system, display_name, name, description,
            linked_resource, create_time_seconds, update_time_seconds, tags)
        entry = user_defined_entry.entry
        entry.schema.columns.extend(column_schemas)
        return user_defined_entry

    @classmethod
    def create_entry_user_defined_type(cls,
                                       entry_type,
                                       system,
                                       display_name,
                                       name,
                                       description,
                                       linked_resource,
                                       create_time_seconds,
                                       update_time_seconds,
                                       columns=None):

        entry = datacatalog.Entry()

        entry.user_specified_type = entry_type
        entry.user_specified_system = system

        entry.display_name = display_name
        entry.name = name

        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromSeconds(create_time_seconds)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromSeconds(update_time_seconds)
        entry.source_system_timestamps.create_time = create_timestamp
        entry.source_system_timestamps.update_time = update_timestamp

        entry.description = description
        entry.linked_resource = linked_resource

        if columns:
            entry.schema.columns.extend(columns)

        return entry

    @classmethod
    def create_fake_tag(cls):
        tag = datacatalog.Tag()
        tag.name = 'tag_template'
        tag.template = 'test-template'
        tag.fields['test-bool-field'].bool_value = True
        tag.fields['test-double-field'].double_value = 1
        tag.fields['test-string-field'].string_value = 'Test String Value'
        tag.fields['test-timestamp-field'].timestamp_value.FromJsonString(
            '2019-09-06T11:00:00-03:00')
        tag.fields['test-enum-field'].enum_value.display_name = \
            'Test ENUM Value'

        return tag

    @classmethod
    def create_fake_tag_template(cls):
        tag_template = datacatalog.TagTemplate()

        tag_template_id = 'template'

        tag_template.name = 'template name'

        tag_template.display_name = 'template display name'

        tag_template.fields['test-bool-field']. \
            type.primitive_type = Utils.__BOOL_TYPE
        tag_template.fields['test-bool-field']. \
            display_name = 'Bool Field'

        tag_template.fields['test-double-field']. \
            type.primitive_type = Utils.__DOUBLE_TYPE
        tag_template.fields['test-double-field']. \
            display_name = 'Double Field'

        tag_template.fields['test-string-field']. \
            type.primitive_type = Utils.__STRING_TYPE
        tag_template.fields['test-string-field']. \
            display_name = 'String Field'

        tag_template.fields['test-timestamp-field']. \
            type.primitive_type = Utils.__TIMESTAMP_TYPE
        tag_template.fields['test-timestamp-field']. \
            display_name = 'Timestamp Field'

        tag_template.fields[
            'test-enum-field']. \
            type.enum_type.allowed_values.add(). \
            display_name = 'Test ENUM Value'

        return tag_template_id, tag_template

    @classmethod
    def create_user_defined_entries(cls, source_system):
        database = Utils.create_user_defined_entry('entry_1', 'database',
                                                   source_system,
                                                   'display_name', 'name',
                                                   'description',
                                                   'linked_resource', 11, 22)

        column_1 = Utils.create_column_schema('column_1', 'int', 'description',
                                              None)
        column_2 = Utils.create_column_schema('column_2', 'string',
                                              'description', None)

        table = Utils.create_entry_for_table('entry_1', 'database',
                                             source_system, 'display_name',
                                             'name', 'description',
                                             'linked_resource', 11, 22,
                                             [column_1, column_2])
        entries = [(database, [table])]
        return entries

    @classmethod
    def create_user_defined_entries_with_tags(cls, source_system):
        database = Utils.create_user_defined_entry('entry_1', 'database',
                                                   source_system,
                                                   'display_name', 'name',
                                                   'description',
                                                   'linked_resource', 11, 22,
                                                   [Utils.create_fake_tag()])

        column_1 = Utils.create_column_schema('column_1', 'int', 'description',
                                              None)
        column_2 = Utils.create_column_schema('column_2', 'string',
                                              'description', None)

        table = Utils.create_entry_for_table('entry_1', 'database',
                                             source_system, 'display_name',
                                             'name', 'description',
                                             'linked_resource', 11, 22,
                                             [column_1, column_2],
                                             [Utils.create_fake_tag()])
        entries = [(database, [table])]
        return entries

    @classmethod
    def create_user_defined_entry(cls,
                                  entry_id,
                                  entry_type,
                                  system,
                                  display_name,
                                  name,
                                  description,
                                  linked_resource,
                                  create_time_seconds,
                                  update_time_seconds,
                                  tags=None):
        entry = datacatalog.Entry()

        entry.user_specified_type = entry_type
        entry.user_specified_system = system

        entry.display_name = display_name

        entry.name = name

        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromSeconds(create_time_seconds)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromSeconds(update_time_seconds)
        entry.source_system_timestamps.create_time = create_timestamp
        entry.source_system_timestamps.update_time = update_timestamp

        entry.description = description
        entry.linked_resource = linked_resource
        return ingest.AssembledEntryData(entry_id, entry, tags)

    @classmethod
    def get_metadata_def_obj(
            cls,
            module_path,
            metadata_def_file_name='metadata_definition.json'):
        resolved_name = Utils.get_resolved_file_name(module_path,
                                                     metadata_def_file_name)
        with open(resolved_name, 'r') as f:
            return json.load(f, object_hook=Utils.__timestamp_parser)

    @classmethod
    def get_test_config_path(cls, module_path):
        return os.path.join(module_path, '../test_data')

    @classmethod
    def get_resolved_file_name(cls, module_path, name):
        resolved_name = os.path.join(module_path,
                                     '../test_data/{}'.format(name))
        return resolved_name

    @classmethod
    def retrieve_dataframe_from_file(cls, module_path, name):
        resolved_name = os.path.join(module_path,
                                     '../test_data/{}'.format(name))

        return pd.read_csv(resolved_name)

    @classmethod
    def __timestamp_parser(cls, dct):
        datetime_format_regex = re.compile(
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')

        for k, v in dct.items():
            if isinstance(v, str) and datetime_format_regex.match(v):
                dct[k] = pd.Timestamp(v)
        return dct


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
