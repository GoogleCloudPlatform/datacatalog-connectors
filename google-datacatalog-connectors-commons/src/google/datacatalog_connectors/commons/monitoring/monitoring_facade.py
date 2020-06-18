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

import calendar
import datetime
import uuid

from google.api_core import exceptions
from google.cloud import monitoring_v3


class MonitoringFacade:
    """Wraps Monitoring API calls."""

    ELAPSED_TIME = 'elapsed_time'
    ENTRIES_LENGTH = 'entries_length'
    METADATA_PAYLOAD_BYTES = 'metadata_payload_bytes'

    def __init__(self,
                 project_id,
                 location,
                 entry_group_id,
                 task_id=uuid.uuid4().hex[:8]):
        self.__monitoring_client = monitoring_v3.MetricServiceClient()
        self.__project_id = project_id
        self.__location = location
        self.__entry_group_id = entry_group_id
        self.__metrics = self.__init_metrics()
        self.__task_id = task_id

    def create_metrics(self):
        for metric in self.__metrics.keys():
            try:
                self.__create_metric_descriptor(metric)
            except exceptions.AlreadyExists:
                pass

    def delete_metrics(self):
        for metric in self.__metrics.keys():
            try:
                self.delete_metric(metric)
            except exceptions.NotFound:
                pass

    def delete_metric(self, metric_name):
        self.__monitoring_client.delete_metric_descriptor(
            self.__monitoring_client.metric_descriptor_path(
                self.__project_id, self.__metrics[metric_name]['type']))

    def list_metrics(self, metric_name, start_datetime, end_datetime):
        project_name = self.__monitoring_client.project_path(self.__project_id)
        interval = monitoring_v3.types.TimeInterval()
        interval.end_time.seconds = int(self.__to_timestamp(end_datetime))
        interval.start_time.seconds = int(self.__to_timestamp(start_datetime))

        aggregation = monitoring_v3.types.Aggregation()
        aggregation.alignment_period.seconds = 600  # 10 minutes
        aggregation.per_series_aligner = (
            monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN)
        aggregation.cross_series_reducer = (
            monitoring_v3.enums.Aggregation.Reducer.REDUCE_NONE)

        list_results = self.__monitoring_client.list_time_series(
            project_name,
            'metric.type = "{}"'.format(self.__metrics[metric_name]['type']),
            interval,
            monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation)

        results = []
        for page in list_results.pages:
            results.extend(page)

        return results

    def list_datacatalog_apis_metric(self, start_datetime, end_datetime):
        project_name = self.__monitoring_client.project_path(self.__project_id)
        interval = monitoring_v3.types.TimeInterval()
        end_seconds = int(self.__to_timestamp(end_datetime))
        start_seconds = int(self.__to_timestamp(start_datetime))
        interval.end_time.seconds = end_seconds
        interval.start_time.seconds = start_seconds

        aggregation = monitoring_v3.types.Aggregation()
        # ADD 150 seconds to mitigate delay on execution
        period_seconds = end_seconds - start_seconds + 150
        aggregation.alignment_period.seconds = period_seconds
        aggregation.per_series_aligner = (
            monitoring_v3.enums.Aggregation.Aligner.ALIGN_SUM)
        aggregation.cross_series_reducer = (
            monitoring_v3.enums.Aggregation.Reducer.REDUCE_NONE)

        query_filter = \
            'metric.type="{}" resource.type="{}" ' \
            'resource.label.service="{}" ' \
            .format('serviceruntime.googleapis.com/api/request_count',
                    'consumed_api',
                    'datacatalog.googleapis.com')

        list_results = self.__monitoring_client.list_time_series(
            project_name, query_filter, interval,
            monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation)

        results = []
        for page in list_results.pages:
            results.extend(page)

        return results

    def write_metadata_payload_bytes_metric(self, value):
        self.__write_metric(self.METADATA_PAYLOAD_BYTES, value)

    def write_entries_length_metric(self, value):
        self.__write_metric(self.ENTRIES_LENGTH, value)

    def write_elapsed_time_metric(self, value):
        self.__write_metric(self.ELAPSED_TIME, value)

    def __create_metric_descriptor(self, metric_name):
        project_name = self.__monitoring_client.project_path(self.__project_id)
        descriptor = monitoring_v3.types.MetricDescriptor()
        descriptor.type = self.__metrics[metric_name]['type']
        descriptor.metric_kind = (
            monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
        descriptor.value_type = (
            monitoring_v3.enums.MetricDescriptor.ValueType.DOUBLE)
        descriptor.description = 'Custom metric for {}.'.format(metric_name)
        self.__monitoring_client.create_metric_descriptor(
            project_name, descriptor)

    def __write_metric(self, metric_name, value):
        project_name = self.__monitoring_client.project_path(self.__project_id)
        series = monitoring_v3.types.TimeSeries()
        series.metric.type = self.__metrics[metric_name]['type']
        self.__add_common_metrics_labels(series)
        point = series.points.add()
        point.value.double_value = value
        now = self.__to_timestamp(datetime.datetime.now())
        point.interval.end_time.seconds = int(now)
        self.__monitoring_client.create_time_series(project_name, [series])

    def __add_common_metrics_labels(self, series):
        series.resource.type = 'generic_task'
        series.resource.labels['project_id'] = self.__project_id
        series.resource.labels['location'] = self.__location
        series.resource.labels['namespace'] = 'datacatalog/connectors'
        series.resource.labels['job'] = self.__entry_group_id
        series.resource.labels['task_id'] = self.__task_id

    def __init_metrics(self):
        metadata_payload_bytes = self.METADATA_PAYLOAD_BYTES
        entries_length = self.ENTRIES_LENGTH
        elapsed_time = self.ELAPSED_TIME
        return {
            metadata_payload_bytes: {
                'type': self.__build_metric_type(metadata_payload_bytes)
            },
            entries_length: {
                'type': self.__build_metric_type(entries_length)
            },
            elapsed_time: {
                'type': self.__build_metric_type(elapsed_time)
            }
        }

    def __build_metric_type(self, metric_name):
        return 'custom.googleapis.com/datacatalog/connectors/{}/{}'.format(
            self.__entry_group_id, metric_name)

    def __to_timestamp(self, datetime_arg):
        return calendar.timegm(datetime_arg.utctimetuple())
