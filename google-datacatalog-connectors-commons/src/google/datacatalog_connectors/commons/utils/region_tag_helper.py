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
    """
    Regex pattern to split a string into 3 groups:

    Group 1: [{REGION_TAG_NAME}_START]
    Group 2: Content between the START and END tags
    Group 3: [{REGION_TAG_NAME}_END]
    """
    __REGION_TAG_GROUP_REGEX_TEMPLATE = \
        r'^(?s:.)*(?P<region_tag_start>\[{}_START\][^\S\r\n]*)' \
        r'(?P<tag_content>(?s:.)*)' \
        r'(?P<region_tag_end>\s*\[{}_END\]\s*)$'

    @classmethod
    def extract_content(cls, region_tag_name, string):
        """
        Extracts the content between START and END region_tag_name tags.
        """

        region_tag_group_regex = \
            cls.__REGION_TAG_GROUP_REGEX_TEMPLATE.replace(
                '{}', region_tag_name)

        region_tag_group_regex_compiled = re.compile(region_tag_group_regex,
                                                     re.MULTILINE)

        re_match = re.match(pattern=region_tag_group_regex_compiled,
                            string=string)
        if re_match:
            region_tag_start, tag_content, region_tag_end, = \
                re_match.group(
                    'region_tag_start', 'tag_content', 'region_tag_end')
            logging.debug('Region tags found! START tag: "%s" END tag: "%s"',
                          region_tag_start, region_tag_end)
            # Strip additional whitespaces
            return tag_content.strip()

        logging.debug('No START/END region tags found!')
