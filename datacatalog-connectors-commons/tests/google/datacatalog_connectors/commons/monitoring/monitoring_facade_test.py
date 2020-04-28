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

from datetime import datetime
import unittest
import uuid

from google.datacatalog_connectors.commons import monitoring
import mock

from google.api_core import exceptions


class MonitoringFacadeTestCase(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'

    @mock.patch('{}.monitoring.monitoring_facade.'
                'monitoring_v3.MetricServiceClient'.format(__COMMONS_PACKAGE))
    def setUp(self, mock_monitoring_client):
        now = datetime.now()
        task_id = '{}_{}'.format(now.strftime('%Y%m%d%H%M%S'),
                                 uuid.uuid4().hex[:8])
        self.__monitoring_facade = monitoring \
            .MonitoringFacade(
                'uat-env-1', 'us-central1', 'oracle', task_id)
        # Shortcut for the object assigned
        # to self.__monitoring_facade.__monitoring_client
        self.__monitoring_client = mock_monitoring_client.return_value

    def test_create_metrics_should_succeed(self):
        self.__monitoring_facade.create_metrics()

        monitoring_client = self.__monitoring_client
        self.assertEqual(3,
                         monitoring_client.create_metric_descriptor.call_count)

    def test_create_metrics_on_exception_should_not_raise_error(self):
        monitoring_client = self.__monitoring_client
        monitoring_client.create_metric_descriptor.side_effect = \
            exceptions.AlreadyExists('Metric Descriptor already exists')

        self.__monitoring_facade.create_metrics()

        self.assertEqual(3,
                         monitoring_client.create_metric_descriptor.call_count)

    def test_delete_metrics_should_succeed(self):
        self.__monitoring_facade.delete_metrics()

        monitoring_client = self.__monitoring_client
        self.assertEqual(3,
                         monitoring_client.metric_descriptor_path.call_count)
        self.assertEqual(3,
                         monitoring_client.delete_metric_descriptor.call_count)

    def test_delete_metrics_on_exception_should_not_raise_error(self):
        monitoring_client = self.__monitoring_client
        monitoring_client.delete_metric_descriptor.side_effect = \
            exceptions.NotFound('Metric Descriptor does not exist')

        self.__monitoring_facade.delete_metrics()

        self.assertEqual(3,
                         monitoring_client.delete_metric_descriptor.call_count)

    def test_write_metadata_payload_bytes_metric_should_succeed(self):
        self.__monitoring_facade.write_metadata_payload_bytes_metric(5000)

        monitoring_client = self.__monitoring_client
        self.assertEqual(1, monitoring_client.create_time_series.call_count)

    def test_write_entries_length_metric_should_succeed(self):
        self.__monitoring_facade.write_entries_length_metric(5000)

        monitoring_client = self.__monitoring_client
        self.assertEqual(1, monitoring_client.create_time_series.call_count)

    def test_write_elapsed_time_metric_should_succeed(self):
        self.__monitoring_facade.write_elapsed_time_metric(5000)

        monitoring_client = self.__monitoring_client
        self.assertEqual(1, monitoring_client.create_time_series.call_count)

    def test_list_metrics_should_succeed(self):
        start_datetime_str = '12/16/19 16:00:00'
        start_datetime = datetime.strptime(start_datetime_str,
                                           '%m/%d/%y %H:%M:%S')

        end_datetime_str = '12/16/19 17:00:00'
        end_datetime = datetime.strptime(end_datetime_str, '%m/%d/%y %H:%M:%S')

        results_iterator = MockedObject()
        results_iterator.pages = [{}]

        monitoring_client = self.__monitoring_client
        monitoring_client.list_time_series.return_value = results_iterator

        self.__monitoring_facade.list_metrics(
            monitoring.MonitoringFacade.ELAPSED_TIME, start_datetime,
            end_datetime)

        self.assertEqual(1, monitoring_client.list_time_series.call_count)

    def test_list_datacatalog_apis_metric(self):
        start_datetime_str = '12/16/19 16:50:00'
        start_datetime = datetime.strptime(start_datetime_str,
                                           '%m/%d/%y %H:%M:%S')

        end_datetime_str = '12/16/19 17:00:00'
        end_datetime = datetime.strptime(end_datetime_str, '%m/%d/%y %H:%M:%S')

        results_iterator = MockedObject()
        results_iterator.pages = [{}]

        monitoring_client = self.__monitoring_client
        monitoring_client.list_time_series.return_value = results_iterator

        self.__monitoring_facade.list_datacatalog_apis_metric(
            start_datetime, end_datetime)

        self.assertEqual(1, monitoring_client.list_time_series.call_count)


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
