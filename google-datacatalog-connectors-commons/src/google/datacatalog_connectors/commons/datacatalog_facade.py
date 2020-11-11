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

import logging

from google.datacatalog_connectors.commons import utils

from google.api_core import exceptions
from google.cloud import datacatalog


class DataCatalogFacade:
    """Wraps Data Catalog's API calls."""

    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id):
        self.__datacatalog = datacatalog.DataCatalogClient()
        self.__project_id = project_id

    def create_entry(self, entry_group_name, entry_id, entry):
        """Creates a Data Catalog Entry.

        :param entry_group_name: Parent Entry Group name.
        :param entry_id: Entry id.
        :param entry: An Entry object.
        :return: The created Entry.
        """
        try:
            entry = self.__datacatalog.create_entry(parent=entry_group_name,
                                                    entry_id=entry_id,
                                                    entry=entry)
            self.__log_entry_operation('created', entry=entry)
        except exceptions.PermissionDenied as e:
            entry_name = '{}/entries/{}'.format(entry_group_name, entry_id)
            self.__log_entry_operation('was not created',
                                       entry_name=entry_name)
            logging.warning('Error: %s', e)

        return entry

    def get_entry(self, name):
        """Retrieves Data Catalog Entry.

        :param name: The Entry name.
        :return: An Entry object if it exists.
        """
        return self.__datacatalog.get_entry(name=name)

    def update_entry(self, entry):
        """Updates an Entry.

        :param entry: An Entry object.
        :return: The updated Entry.
        """
        entry = self.__datacatalog.update_entry(entry=entry, update_mask=None)
        self.__log_entry_operation('updated', entry=entry)
        return entry

    def upsert_entry(self, entry_group_name, entry_id, entry):
        """
        Update a Data Catalog Entry if it exists and has been changed.
        Creates a new Entry if it does not exist.

        :param entry_group_name: Parent Entry Group name.
        :param entry_id: Entry id.
        :param entry: An Entry object.
        :return: The updated or created Entry.
        """
        persisted_entry = entry
        entry_name = '{}/entries/{}'.format(entry_group_name, entry_id)
        try:
            persisted_entry = self.get_entry(entry_name)
            self.__log_entry_operation('already exists', entry_name=entry_name)
            if self.__entry_was_updated(persisted_entry, entry):
                persisted_entry = self.update_entry(entry)
            else:
                self.__log_entry_operation('is up-to-date',
                                           entry=persisted_entry)
        except exceptions.PermissionDenied:
            self.__log_entry_operation('does not exist', entry_name=entry_name)
            persisted_entry = self.create_entry(
                entry_group_name=entry_group_name,
                entry_id=entry_id,
                entry=entry)
        except exceptions.FailedPrecondition as e:
            logging.warning('Entry was not updated: %s', entry_name)
            logging.warning('Error: %s', e)

        return persisted_entry

    @classmethod
    def __entry_was_updated(cls, current_entry, new_entry):
        # Update time comparison allows to verify whether the entry was
        # updated on the source system.
        current_update_time = 0
        if current_entry.source_system_timestamps.update_time:
            current_update_time = \
                current_entry.source_system_timestamps.update_time.timestamp()

        new_update_time = 0
        if new_entry.source_system_timestamps.update_time:
            new_update_time = \
                new_entry.source_system_timestamps.update_time.timestamp()

        updated_time_changed = \
            new_update_time != 0 and current_update_time != new_update_time

        return updated_time_changed or not cls.__entries_are_equal(
            current_entry, new_entry)

    @classmethod
    def __entries_are_equal(cls, entry_1, entry_2):
        object_1 = utils.ValuesComparableObject()
        object_1.user_specified_system = entry_1.user_specified_system
        object_1.user_specified_type = entry_1.user_specified_type
        object_1.display_name = entry_1.display_name
        object_1.description = entry_1.description
        object_1.linked_resource = entry_1.linked_resource

        object_2 = utils.ValuesComparableObject()
        object_2.user_specified_system = entry_2.user_specified_system
        object_2.user_specified_type = entry_2.user_specified_type
        object_2.display_name = entry_2.display_name
        object_2.description = entry_2.description
        object_2.linked_resource = entry_2.linked_resource

        return object_1 == object_2

    def delete_entry(self, name):
        """Deletes a Data Catalog Entry.

        :param name: The Entry name.
        """
        try:
            self.__datacatalog.delete_entry(name=name)
            self.__log_entry_operation('deleted', entry_name=name)
        except Exception as e:
            logging.info(
                'An exception ocurred while attempting to'
                ' delete Entry: %s', name)
            logging.debug(str(e))

    @classmethod
    def __log_entry_operation(cls, description, entry=None, entry_name=None):

        formatted_description = 'Entry {}: '.format(description)
        logging.info('%s%s', formatted_description,
                     entry.name if entry else entry_name)

        if entry:
            logging.info('%s^ [%s] %s', ' ' * len(formatted_description),
                         entry.user_specified_type, entry.linked_resource)

    def create_entry_group(self, location_id, entry_group_id):
        """Creates a Data Catalog Entry Group.

        :param location_id: Location id.
        :param entry_group_id: Entry Group id.
        :return: The created Entry Group.
        """
        entry_group = self.__datacatalog.create_entry_group(
            parent=f'projects/{self.__project_id}/locations/{location_id}',
            entry_group_id=entry_group_id,
            entry_group=datacatalog.EntryGroup())
        logging.info('Entry Group created: %s', entry_group.name)
        return entry_group

    def delete_entry_group(self, name):
        """
        Deletes a Data Catalog Entry Group.

        :param name: The Entry Group name.
        """
        self.__datacatalog.delete_entry_group(name=name)

    def create_tag_template(self, location_id, tag_template_id, tag_template):
        """Creates a Data Catalog Tag Template.

        :param location_id: Location id.
        :param tag_template_id: Tag Template id.
        :param tag_template: A Tag Template object.
        :return: The created Tag Template.
        """
        created_tag_template = self.__datacatalog.create_tag_template(
            parent=f'projects/{self.__project_id}/locations/{location_id}',
            tag_template_id=tag_template_id,
            tag_template=tag_template)

        logging.info('Tag Template created: %s', created_tag_template.name)
        return created_tag_template

    def get_tag_template(self, name):
        """Retrieves a Data Catalog Tag Template.

        :param name: The Tag Templane name.
        :return: A Tag Template object if it exists.
        """
        return self.__datacatalog.get_tag_template(name=name)

    def get_tag_field_values_for_search_results(self, query, template,
                                                tag_field, tag_field_type):
        """Retrieves Data Catalog Tag field values for search results.

        :param query: Query used on search.
        :param template: The Tag Template name.
        :param tag_field: The Tag Field name.
        :param tag_field_type: The Tag Field type.

        :return: List of tag field values.
        """
        tag_field_values = []
        table_entries_name = \
            self.search_catalog_relative_resource_name(query)
        for table_entry_name in table_entries_name:
            tags = self.list_tags(table_entry_name)
            for tag in tags:
                if template in tag.template:
                    field = tag.fields[tag_field]

                    if self.__STRING_TYPE == tag_field_type:
                        tag_field_value = field.string_value
                    elif self.__BOOL_TYPE == tag_field_type:
                        tag_field_value = field.bool_value
                    elif self.__DOUBLE_TYPE == tag_field_type:
                        tag_field_value = field.double_value
                    elif self.__TIMESTAMP_TYPE == tag_field_type:
                        tag_field_value = field.timestamp_value
                    else:
                        tag_field_value = field.enum_value.display_name
                    tag_field_values.append(tag_field_value)
        return tag_field_values

    def delete_tag_template(self, name):
        """Deletes a Data Catalog Tag Template.

        :param name: The Tag Template name.
        """
        self.__datacatalog.delete_tag_template(name=name, force=True)
        logging.info('Tag Template deleted: %s', name)

    def create_tag(self, entry_name, tag):
        """Creates a Data Catalog Tag.

        :param entry_name: Parent Entry name.
        :param tag: A Tag object.
        :return: The created Tag.
        """
        return self.__datacatalog.create_tag(parent=entry_name, tag=tag)

    def delete_tag(self, tag):
        """Deletes a Data Catalog Tag.

        :param tag: A Tag object.
        :return: The deleted Tag.
        """
        return self.__datacatalog.delete_tag(name=tag.name)

    def list_tags(self, entry_name):
        """List Tags for a given Entry.

        :param entry_name: The parent Entry name.
        :return: A list of Tag objects.
        """
        return self.__datacatalog.list_tags(parent=entry_name)

    def update_tag(self, tag):
        """Updates a Tag.

        :param tag: A Tag object.
        :return: The updated Tag.
        """
        return self.__datacatalog.update_tag(tag=tag, update_mask=None)

    def upsert_tags(self, entry, tags):
        """Updates or creates Tag for a given Entry.

        :param entry: The Entry object.
        :param tags: A list of Tag objects.
        """
        if not tags:
            return

        persisted_tags = self.list_tags(entry.name)

        # Fetch GRPCIterator.
        persisted_tags = [tag for tag in persisted_tags]

        for tag in tags:
            logging.info('Processing Tag from Template: %s ...', tag.template)

            tag_to_create = tag
            tag_to_update = None
            for persisted_tag in persisted_tags:
                # The column field is not case sensitive.
                if tag.template == persisted_tag.template and \
                   tag.column.lower() == persisted_tag.column.lower():

                    tag_to_create = None
                    tag.name = persisted_tag.name
                    if not self.__tag_fields_are_equal(tag, persisted_tag):
                        tag_to_update = tag
                    break

            if tag_to_create:
                created_tag = self.create_tag(entry.name, tag_to_create)
                logging.info('Tag created: %s', created_tag.name)
            elif tag_to_update:
                self.update_tag(tag_to_update)
                logging.info('Tag updated: %s', tag_to_update.name)
            else:
                logging.info('Tag is up-to-date: %s', tag.name)

    def delete_tags(self, entry, tags, tag_template_name):
        """Deletes Tags for a given Entry if they don't exist
        in Data Catalog.

        :param entry: The Entry object.
        :param tags: A list of Tag objects.
        :param tag_template_name: Template name used to filter
        templates out, it can be a part of the template name.
        """
        persisted_tags = self.list_tags(entry.name)

        # Fetch GRPCIterator.
        persisted_tags = [tag for tag in persisted_tags]

        for persisted_tag in persisted_tags:
            logging.info('Processing Tag from Template: %s ...',
                         persisted_tag.template)
            tag_to_delete = None

            if tag_template_name in persisted_tag.template:
                tag_to_delete = persisted_tag
                for tag in tags:
                    if tag.template == persisted_tag.template and \
                       tag.column == persisted_tag.column:
                        tag_to_delete = None
                        break

            if tag_to_delete:
                self.delete_tag(tag_to_delete)
                logging.info('Tag deleted: %s', tag_to_delete.name)
            else:
                logging.info('Tag is up-to-date: %s', persisted_tag.name)

    @classmethod
    def __tag_fields_are_equal(cls, tag_1, tag_2):
        for field_id in tag_1.fields:
            tag_1_field = tag_1.fields[field_id]
            tag_2_field = tag_2.fields.get(field_id)

            if tag_2_field is None:
                return False

            values_are_equal = tag_1_field.bool_value == \
                tag_2_field.bool_value
            values_are_equal = values_are_equal \
                and tag_1_field.double_value == tag_2_field.double_value
            values_are_equal = values_are_equal \
                and tag_1_field.string_value == tag_2_field.string_value
            values_are_equal = values_are_equal \
                and cls.__timestamp_tag_fields_are_equal(
                    tag_1_field, tag_2_field)
            values_are_equal = values_are_equal \
                and tag_1_field.enum_value.display_name == \
                tag_2_field.enum_value.display_name

            if not values_are_equal:
                return False

        return True

    @classmethod
    def __timestamp_tag_fields_are_equal(cls, tag_1_field, tag_2_field):
        if not (tag_1_field.timestamp_value and tag_2_field.timestamp_value):
            return True

        return tag_1_field.timestamp_value.timestamp() == \
            tag_2_field.timestamp_value.timestamp()

    def search_catalog(self, query):
        """Searches Data Catalog for a given query.

        :param query: The query string.
        :return: A Search Result list.
        """
        scope = datacatalog.SearchCatalogRequest.Scope()
        scope.include_project_ids.append(self.__project_id)

        request = datacatalog.SearchCatalogRequest()
        request.scope = scope
        request.query = query
        request.page_size = 1000

        return [
            result for result in self.__datacatalog.search_catalog(request)
        ]

    def search_catalog_relative_resource_name(self, query):
        """Searches Data Catalog for a given query.

        :param query: The query string.
        :return: A string list in which each element represents
        an Entry resource name.
        """
        return [
            result.relative_resource_name
            for result in self.search_catalog(query)
        ]
