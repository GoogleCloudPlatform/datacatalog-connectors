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

import abc

from google.cloud import datacatalog

ABC = abc.ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class BaseEntryRelationshipMapper(ABC):
    __DATA_CATALOG_UI_URL = 'https://console.cloud.google.com/datacatalog'
    __ID_FIELD_KEY = 'id'

    @abc.abstractmethod
    def fulfill_tag_fields(self, assembled_entries_data):
        pass

    @classmethod
    def _fulfill_tag_fields(cls, assembled_entries_data, resolvers):
        if not (assembled_entries_data and resolvers):
            return

        id_name_pairs = cls.__build_id_name_pairs(assembled_entries_data)

        for resolver in resolvers:
            resolver(assembled_entries_data, id_name_pairs)

    @classmethod
    def __build_id_name_pairs(cls, assembled_entries_data):
        id_name_pairs = {}
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            for tag in assembled_entry_data.tags:
                asset_id_field_key = cls._get_asset_identifier_tag_field_key()
                if asset_id_field_key not in tag.fields:
                    continue
                asset_id_field = tag.fields[asset_id_field_key]
                asset_id = asset_id_field.string_value \
                    if asset_id_field.string_value \
                    else int(asset_id_field.double_value)
                dict_key = '{}-{}'.format(entry.user_specified_type, asset_id)
                id_name_pairs[dict_key] = entry.name

        return id_name_pairs

    @classmethod
    def _get_asset_identifier_tag_field_key(cls):
        return cls.__ID_FIELD_KEY

    @classmethod
    def _map_related_entry(cls, assembled_entry_data, related_asset_type,
                           source_field_id, target_field_id, id_name_pairs):

        relationship_tag_dict = {}
        related_asset_ids = []
        tags = assembled_entry_data.tags
        if not tags:
            return

        for tag in tags:
            if source_field_id not in tag.fields:
                continue
            source_field = tag.fields[source_field_id]
            related_asset_id = source_field.string_value \
                if source_field.string_value \
                else int(source_field.double_value)
            related_asset_ids.append(related_asset_id)
            relationship_tag_dict[related_asset_id] = tag

        if related_asset_ids:
            for related_asset_id in related_asset_ids:
                related_asset_key = '{}-{}'.format(related_asset_type,
                                                   related_asset_id)
                if related_asset_key in id_name_pairs:
                    relationship_tag = relationship_tag_dict[related_asset_id]
                    string_field = datacatalog.TagField()
                    string_field.string_value = cls.__format_related_entry_url(
                        id_name_pairs[related_asset_key])
                    relationship_tag.fields[target_field_id] = string_field

    @classmethod
    def __format_related_entry_url(cls, entry_name):
        return '{}/{}'.format(cls.__DATA_CATALOG_UI_URL, entry_name)
