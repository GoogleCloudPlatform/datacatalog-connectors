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

import yaml

from google.datacatalog_connectors.commons import config


class YamlConfigTestCase(unittest.TestCase):

    def test_parse_as_dict_should_succeed(self):
        yaml_config = config.YamlConfig()

        content = '''
        metadata_definition:
          name: 'sp_calculateOrder'
          purpose: 'This stored procedure will calculate orders.'
          inputs:
            - name: 'in1'
              type: 'string'
          outputs:
            - name: 'out1'
              type: 'int'
        '''.strip()

        yaml_content = yaml_config.parse_as_dict(content)

        metadata_definition = yaml_content['metadata_definition']

        self.assertIsNotNone(metadata_definition)
        self.assertEqual(metadata_definition['name'], 'sp_calculateOrder')
        self.assertEqual(metadata_definition['purpose'],
                         'This stored procedure will calculate orders.')
        self.assertEqual(metadata_definition['inputs'][0]['name'], 'in1')
        self.assertEqual(metadata_definition['inputs'][0]['type'], 'string')
        self.assertEqual(metadata_definition['outputs'][0]['name'], 'out1')
        self.assertEqual(metadata_definition['outputs'][0]['type'], 'int')

    def test_parse_as_dict_invalid_yaml_should_raise(self):
        yaml_config = config.YamlConfig()

        content = '''
        metadata_definition:
          name: 'sp_calculateOrder'
          purpose: 'This stored procedure will calculate orders.'
          inputs:
            * name: 'in1'
              type: 'string'
          outputs:
            - name: 'out1'
              type: 'int'
        '''.strip()

        self.assertRaises(yaml.scanner.ScannerError, yaml_config.parse_as_dict,
                          content)
