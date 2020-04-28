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
import timeit

from google.datacatalog_connectors.commons.monitoring import \
    monitoring_facade


class MetricsProcessor:

    def __init__(self,
                 project_id,
                 location_id,
                 entry_group_id,
                 enable_monitoring,
                 task_id=None):
        self.__enable_monitoring = enable_monitoring
        if enable_monitoring:
            self.__monitoring_facade = monitoring_facade.MonitoringFacade(
                project_id, location_id, entry_group_id, task_id)
            self.__monitoring_facade.create_metrics()
            self.__start_time = timeit.default_timer()

    def reset_start_time(self):
        self.__start_time = timeit.default_timer()

    def process_elapsed_time_metric(self):
        if self.__enable_monitoring:
            stop_time = timeit.default_timer()
            elapsed_time = int((stop_time - self.__start_time) * 1000)
            self.__monitoring_facade.write_elapsed_time_metric(elapsed_time)

    def process_entries_length_metric(self, entries_length):
        if self.__enable_monitoring:
            self.__monitoring_facade.write_entries_length_metric(
                entries_length)

    def process_metadata_payload_bytes_metric(self, metadata):
        if self.__enable_monitoring:
            metadata_as_json = json.dumps(metadata, default=str)
            json_bytes = len(metadata_as_json.encode())
            self.__monitoring_facade.write_metadata_payload_bytes_metric(
                json_bytes)
