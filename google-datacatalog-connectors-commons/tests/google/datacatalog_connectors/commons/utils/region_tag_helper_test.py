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

from google.datacatalog_connectors.commons import utils


class RegionTagHelperTestCase(unittest.TestCase):

    def test_extract_tag_content_should_return_correct_content(self):
        region_tag_helper = utils.RegionTagHelper()

        expected_tag_content = '''
        metadata_definition:
          - name: 'sp_calculateOrder'
            purpose: 'This stored procedure will calculate orders.'
            inputs:
              - name: 'in1'
                type: 'string'
            outputs:
              - name: 'out1'
                type: 'int'
        '''.strip()

        content_string = \
            '[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_START] \n' + \
            expected_tag_content + \
            '\n[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_END] \n'

        extracted_tag_content = region_tag_helper.extract_content(
            content_string)

        self.assertEqual(extracted_tag_content, expected_tag_content)

    def test_extract_tag_content_no_end_region_tag_should_return_none(self):
        region_tag_helper = utils.RegionTagHelper()

        expected_tag_content = '''
        metadata_definition:
          - name: 'sp_calculateOrder'
            purpose: 'This stored procedure will calculate orders.'
            inputs:
              - name: 'in1'
                type: 'string'
            outputs:
              - name: 'out1'
                type: 'int'
        '''.strip()

        content_string = \
            '[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_START] \n' + \
            expected_tag_content + '\n'

        extracted_tag_content = region_tag_helper.extract_content(
            content_string)

        self.assertIsNone(extracted_tag_content)

    def test_extract_tag_content_no_start_region_tag_should_return_none(self):
        region_tag_helper = utils.RegionTagHelper()

        expected_tag_content = '''
        metadata_definition:
          - name: 'sp_calculateOrder'
            purpose: 'This stored procedure will calculate orders.'
            inputs:
              - name: 'in1'
                type: 'string'
            outputs:
              - name: 'out1'
                type: 'int'
        '''.strip()

        content_string = expected_tag_content + \
            '\n[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_END] \n'

        extracted_tag_content = region_tag_helper.extract_content(
            content_string)

        self.assertIsNone(extracted_tag_content)

    def test_extract_tag_content_no_region_tags_should_return_none(self):
        region_tag_helper = utils.RegionTagHelper()

        expected_tag_content = '''
        metadata_definition:
          - name: 'sp_calculateOrder'
            purpose: 'This stored procedure will calculate orders.'
            inputs:
              - name: 'in1'
                type: 'string'
            outputs:
              - name: 'out1'
                type: 'int'
        '''.strip()

        extracted_tag_content = region_tag_helper.extract_content(
            expected_tag_content)

        self.assertIsNone(extracted_tag_content)

    def test_extract_tag_content_invalid_region_tags_should_return_none(self):
        region_tag_helper = utils.RegionTagHelper()

        expected_tag_content = '''
        metadata_definition:
          - name: 'sp_calculateOrder'
            purpose: 'This stored procedure will calculate orders.'
            inputs:
              - name: 'in1'
                type: 'string'
            outputs:
              - name: 'out1'
                type: 'int'
        '''.strip()

        content_string = \
            '[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_STARX] \n' + \
            expected_tag_content + \
            '\n[GOOGLE_DATA_CATALOG_METADATA_DEFINITION_ENX] \n'

        extracted_tag_content = region_tag_helper.extract_content(
            content_string)

        self.assertIsNone(extracted_tag_content)
