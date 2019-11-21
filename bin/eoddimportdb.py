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
# Purpose:  Command line tool to import the database table for a particular sensor
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 21/11/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import eodatadown.eodatadownrun
import argparse
import logging
import os
import os.path
import rsgislib

from eodatadown import EODATADOWN_SENSORS_LIST

logger = logging.getLogger('eoddimportdb.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor you wish to export''')
    parser.add_argument("-i", "--input", type=str, required=True,
                        help="Specify the JSON file which should be appended to sensors database.")
    parser.add_argument("-p", "--paths", type=str, required=False,
                        help="""Specify a JSON file will key pairs for file paths which should be updated
                                (keys are the path to replace and value is the replacement value).""")
    args = parser.parse_args()

    config_file = args.config
    sys_config_value = os.getenv('EDD_SYS_CFG', None)
    if (config_file == '') and (sys_config_value is not None):
        config_file = sys_config_value
        print("Using system config file: '" + config_file + "'")

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '" + config_file + "'")
        raise Exception("Config file does not exist")

    t = rsgislib.RSGISTime()
    t.start(True)

    eodatadown.eodatadownrun.import_sensor_database(config_file, args.sensor, args.input, args.paths)

    t.end(reportDiff=True, preceedStr='EODataDown import completed ', postStr=' - eoddimportdb.py.')

