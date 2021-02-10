#!/usr/bin/python
#
# Copyright 2021 Google LLC
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


class RegionTagHelper:
    """Helper class with common logic to work with region tags such as:
    [GOOGLE_DATA_CATALOG_METADATA_DEFINITION_START]
    ...
    [GOOGLE_DATA_CATALOG_METADATA_DEFINITION_END]

    In the scenario there are multiple tags with the same name, the
    logic will be applied only to the last one.
    """

    # Regex pattern to split a string into 3 groups:
    # Group 1: [{REGION_TAG_NAME}_START]
    # Group 2: Content between the START and END tags
    # Group 3: [{REGION_TAG_NAME}_END]
    __REGION_TAG_GROUP_REGEX_TEMPLATE = \
        r'^(?s:.)*(?P<start_tag>\[{}_START\][^\S\r\n]*)' \
        r'(?P<tag_content>(?s:.)*)' \
        r'(?P<end_tag>\s*\[{}_END\]\s*)$'

    @classmethod
    def extract_content(cls, tag_name, string):
        """
        Extracts the content between START and END tags.
        """

        tag_group_regex = \
            cls.__REGION_TAG_GROUP_REGEX_TEMPLATE.replace(
                '{}', tag_name)

        compiled_regex = re.compile(tag_group_regex, re.MULTILINE)

        re_match = re.match(pattern=compiled_regex, string=string)
        if re_match:
            start_tag, tag_content, end_tag, = \
                re_match.group(
                    'start_tag', 'tag_content', 'end_tag')
            logging.debug('Region tags found! START tag: "%s" END tag: "%s"',
                          start_tag, end_tag)
            # Strip additional whitespaces
            return tag_content.strip()

        logging.debug('No START/END region tags found!')
