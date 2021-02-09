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
    Regex pattern to split string into 3 groups:

    Group 1: [{REGION_TAG_NAME}_START]
    Group 2: Content between START and END region tags
    Group 3: [{REGION_TAG_NAME}_END]
    """
    __REGION_TAG_GROUP_REGEX = re.compile(
        r"^(\s*\[\S*_START\][^\S\r\n]*)((?s:.)*)(\s*\[\S*_END\]\s*)",
        re.MULTILINE)

    @classmethod
    def extract_content(cls, content_with_tags):
        """
        Extract content inside defined START/END region tags.
        """
        re_match = re.match(pattern=cls.__REGION_TAG_GROUP_REGEX,
                            string=content_with_tags)
        if re_match:
            region_tag_start, content_with_tags, region_tag_end, = re_match.groups(
            )
            logging.debug(
                'Region tags defined! START region tag: %s END region tag: %s',
                region_tag_start, region_tag_end)
            # Strip additional whitespaces
            return content_with_tags.strip()
        else:
            logging.debug('No START/END region tags defined!')
