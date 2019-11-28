#!/usr/bin/env python
"""
EODataDown - Setup/Update the system.
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
# Purpose:  Command line tool for running the system.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 26/11/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import logging
import os
import os.path
import datetime
import rsgislib

import eodatadown.eodatadownrun

from eodatadown import EODATADOWN_SENSORS_LIST

logger = logging.getLogger('eoddcreatereport.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("--start", type=str, required=True, help="The start date (recent), with format YYYYMMDD.")
    parser.add_argument("--end", type=str, required=True, help="The start date (earliest), with format YYYYMMDD.")
    parser.add_argument("-o", "--output", type=str, required=True, help="The output PDF report file.")
    parser.add_argument("-v", "--vecfile", type=str, required=True, help="A vector file to be overlain, for context."
                                                                         "For example, country boundaries.")
    parser.add_argument("-l", "--veclyr", type=str, required=True, help="The layer within the vector file.")
    parser.add_argument("-t", "--tmpdir", type=str, required=True, help="A temp directory within which to work.")


    args = parser.parse_args()

    config_file = args.config
    main_config_value = os.getenv('EDD_MAIN_CFG', None)
    if (config_file == '') and (main_config_value is not None):
        config_file = main_config_value

    print("'" + config_file + "'")

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '" + config_file + "'")
        raise Exception("Config file does not exist")

    t = rsgislib.RSGISTime()
    t.start(True)

    start_date = datetime.datetime.strptime(args.start, '%Y%m%d').date()
    end_date = datetime.datetime.strptime(args.end, '%Y%m%d').date()
    eodatadown.eodatadownrun.create_date_report(config_file, args.sensor, args.output, start_date, end_date,
                                                args.vecfile, args.veclyr, args.tmpdir)

    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddcreatereport.py.')

