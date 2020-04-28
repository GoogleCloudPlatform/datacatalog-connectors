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
import re

from google.datacatalog_connectors.commons import \
    datacatalog_facade

from google.api_core import exceptions
from google.cloud import datacatalog


class DataCatalogMetadataCleaner:

    def __init__(self, project_id, location_id, entry_group_id):
        self.__datacatalog_facade = datacatalog_facade.DataCatalogFacade(
            project_id)
        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id

    def delete_obsolete_metadata(self, new_assembled_entries_data,
                                 existing_entries_search_query):
        """Cleans up obsolete entries and entry_groups.

        Uses the required args to find out which Entries and Entry Groups
        should be deleted.

        :param
        - new_assembled_entries_data
            (:obj:`list` of :obj:`ingest.AssembledEntryData`):
            new entries from the custom system
        - old_entries_search_query (str):
            query used to retrieve the old entries from DataCatalog.
        """
        logging.info('')
        logging.info('Starting to clean up the catalog...')

        # Delete any pre-existing Entries.
        old_entries_name = \
            self.__datacatalog_facade.search_catalog_relative_resource_name(
                existing_entries_search_query)

        logging.info(
            '%s entries that match the search query'
            ' exist in Data Catalog!', len(old_entries_name))
        logging.info('Looking for entries to be deleted...')

        new_entries_name = [
            assembled_entry_data.entry.name
            for assembled_entry_data in new_assembled_entries_data
        ]

        entries_name_pending_deletion = set(old_entries_name).difference(
            set(new_entries_name))
        logging.info('%s entries will be deleted.',
                     len(entries_name_pending_deletion))

        for entry_name in entries_name_pending_deletion:
            self.__datacatalog_facade.delete_entry(entry_name)

        self.__cleanup_entry_groups(old_entries_name)

    def __cleanup_entry_groups(self, obsolete_entries_name):
        entries_group_name = []

        datacatalog_entry_name_pattern =\
            '(?P<entry_group_name>.+?)/entries/(.+?)'
        for name in obsolete_entries_name:
            match = re.match(pattern=datacatalog_entry_name_pattern,
                             string=name)
            if match:
                entry_group_name = match.group('entry_group_name')
                entries_group_name.append(entry_group_name)

        for entry_group_name in set(entries_group_name):
            try:
                self.__datacatalog_facade.delete_entry_group(entry_group_name)
                logging.info('Entry Group deleted: %s', entry_group_name)
            except exceptions.GoogleAPICallError as e:
                logging.info('Exception deleting Entry Group: %s',
                             entry_group_name)
                logging.debug(str(e))

    def delete_metadata(self, assembled_entries_data):
        """Deletes the given assembled_entries_data from Data Catalog.

        :param assembled_entries_data: type
            datacatalog_connectors_commons/ingest/assembled_entry_data.py
        """
        logging.info('')
        logging.info('Starting the deletion flow...')

        self.__delete_entries(assembled_entries_data)

    def __delete_entries(self, assembled_entries_data):
        for assembled_entry_data in assembled_entries_data:
            entry_id = assembled_entry_data.entry_id
            name = datacatalog.DataCatalogClient.entry_path(
                self.__project_id, self.__location_id, self.__entry_group_id,
                entry_id)
            self.__datacatalog_facade.delete_entry(name=name)
