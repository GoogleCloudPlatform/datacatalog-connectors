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

from google.datacatalog_connectors.commons import monitoring
import mock


class MetricsProcessorTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'

    @mock.patch('{}.monitoring.monitoring_facade.MonitoringFacade'.format(
        __COMMONS_PACKAGE))
    def setUp(self, mock_monitoring_facade):
        self.__metrics_processor = monitoring \
            .MetricsProcessor(
                'project-id', 'location-id', 'entry_group_id', True)
        self.__metrics_processor_disabled = monitoring \
            .MetricsProcessor(
                'project-id', 'location-id', 'entry_group_id', False)
        # Shortcut for the object assigned
        # to self.__metrics_processor.__monitoring_facade
        self.__monitoring_facade = mock_monitoring_facade.return_value

    def test_process_elapsed_time_metric_should_succeed(self):
        monitoring_facade = self.__monitoring_facade

        self.__metrics_processor.process_elapsed_time_metric()

        self.assertEqual(
            1, monitoring_facade.write_elapsed_time_metric.call_count)

    def test_disabled_process_elapsed_time_metric_should_not_call_facade(self):
        monitoring_facade = self.__monitoring_facade

        self.__metrics_processor_disabled.process_elapsed_time_metric()

        self.assertEqual(
            0, monitoring_facade.write_elapsed_time_metric.call_count)

    def test_process_entries_length_metric_should_succeed(self):
        monitoring_facade = self.__monitoring_facade

        self.__metrics_processor.process_entries_length_metric(1000)

        self.assertEqual(
            1, monitoring_facade.write_entries_length_metric.call_count)

    def test_disabled_process_entries_length_metric_should_not_call_facade(
            self):  # noqa: E125
        monitoring_facade = self.__monitoring_facade

        self.__metrics_processor_disabled.process_entries_length_metric(1000)

        self.assertEqual(
            0, monitoring_facade.write_entries_length_metric.call_count)

    def test_process_metadata_payload_bytes_metric_should_succeed(self):
        monitoring_facade = self.__monitoring_facade

        d = {'col1': [1, 2, 3, 4, [1, 2, 3, 4]], 'col2': [3, 4]}
        self.__metrics_processor.process_metadata_payload_bytes_metric(d)

        self.assertEqual(
            1,
            monitoring_facade.write_metadata_payload_bytes_metric.call_count)

    def test_disabled_payload_bytes_metric_should_not_call_facade(  # noqa: E501
            self):  # noqa: E125
        monitoring_facade = self.__monitoring_facade

        self.__metrics_processor_disabled.\
            process_metadata_payload_bytes_metric({})

        self.assertEqual(
            0,
            monitoring_facade.write_metadata_payload_bytes_metric.call_count)

    def test_reset_start_time_should_change_start_time(self):
        metrics_processor = self.__metrics_processor

        start_time = metrics_processor._MetricsProcessor__start_time

        metrics_processor.reset_start_time()

        new_start_time = metrics_processor._MetricsProcessor__start_time

        self.assertGreater(new_start_time, start_time)
