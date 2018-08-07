#!/usr/bin/env python
"""
EODataDown - encode password for entering into config JSON files.
"""
# This file is part of 'EODataDown'
# A tool for automating Earth Observation Data Downloading.
#
# Copyright 2018 Pete Bunting
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Purpose:  Encode password for entering into config JSON files.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import eodatadown.eodatadownutils
import argparse
import logging

logger = logging.getLogger('eoddpassencode.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--password", type=str, required=True, help="Password which is going to be encoded.")
    args = parser.parse_args()

    try:
        eddPassEncoder = eodatadown.eodatadownutils.EDDPasswordTools()
        encodedPass = eddPassEncoder.encodePassword(args.password)
        print(encodedPass)
        logger.info('Successfully created encoded password.')
    except Exception as e:
        logger.error('Failed to open file', exc_info=True)

