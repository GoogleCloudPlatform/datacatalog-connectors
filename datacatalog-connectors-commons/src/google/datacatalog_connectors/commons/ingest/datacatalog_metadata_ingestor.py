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

from google.datacatalog_connectors.commons import \
    datacatalog_facade

from google.api_core import exceptions
from google.cloud import datacatalog


class DataCatalogMetadataIngestor:
    """Ingests custom metadata into Data Catalog."""

    def __init__(self, project_id, location_id, entry_group_id):
        self.__datacatalog_facade = datacatalog_facade.DataCatalogFacade(
            project_id)
        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id

    def ingest_metadata(self,
                        assembled_entries_data,
                        tag_templates_dict=None,
                        config=None):
        """Ingest metadata into Data Catalog.

         :param
         - assembled_entries_data: type
             datacatalog_connectors_commons/ingest/assembled_entry_data.py
         - tag_templates_dict: type dict
         - config: dict with ingestion config
        """
        logging.info('')
        logging.info('Starting the ingestion flow...')

        self.__create_tag_templates(tag_templates_dict)

        try:
            entry_group = self.__datacatalog_facade.create_entry_group(
                location_id=self.__location_id,
                entry_group_id=self.__entry_group_id)
            entry_group_name = entry_group.name
        except exceptions.AlreadyExists:
            entry_group_name = datacatalog.DataCatalogClient.entry_group_path(
                self.__project_id, self.__location_id, self.__entry_group_id)
            logging.info(
                'Entry Group already exists!'
                ' Name "%s" built as fallback.', entry_group_name)

        self.__ingest_entries(entry_group_name, assembled_entries_data, config)

    def __create_tag_templates(self, tag_templates_dict=None):
        if not tag_templates_dict:
            return

        for tag_template_id, tag_template in tag_templates_dict.items():
            try:
                self.__datacatalog_facade.create_tag_template(
                    location_id=self.__location_id,
                    tag_template_id=tag_template_id,
                    tag_template=tag_template)
            except exceptions.AlreadyExists:
                logging.info('Tag Template "%s" already exists!',
                             tag_template_id)

    def __ingest_entries(self,
                         entry_group_name,
                         assembled_entries_data,
                         config=None):
        progress_indicator = 0
        assembled_entries_count = len(assembled_entries_data)
        for assembled_entry_data in assembled_entries_data:
            progress_indicator += 1
            logging.info('')
            logging.info('%s/%s', progress_indicator, assembled_entries_count)

            entry_id = assembled_entry_data.entry_id
            new_entry = assembled_entry_data.entry
            entry = self.__datacatalog_facade.upsert_entry(
                entry_group_name, entry_id, new_entry)

            logging.info('')
            logging.info('Starting the upsert tags step')
            self.__datacatalog_facade.upsert_tags(entry,
                                                  assembled_entry_data.tags)
            if config and 'delete_tags' in config:
                delete_tags = config['delete_tags']
                logging.info('')
                logging.info('Starting the delete tags step')
                # If not specified uses the entry group id to find
                # what tag templates should have their tags deleted.
                managed_tag_template = delete_tags.get('managed_tag_template')
                if not managed_tag_template:
                    managed_tag_template = self.__entry_group_id

                self.__datacatalog_facade.delete_tags(
                    entry, assembled_entry_data.tags, managed_tag_template)
