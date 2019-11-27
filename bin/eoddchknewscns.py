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
import rsgislib

import eodatadown.eodatadownrun

from eodatadown import EODATADOWN_SENSORS_LIST

logger = logging.getLogger('eoddchknewscns.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensors", type=str, nargs='+', required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("-n", "--ncores", type=int, default=0,
                        help="Specify the number of processing cores to use (or use EDD_NCORES).")
    parser.add_argument("--chkstart", action='store_true', default=False,
                        help="Specify that the system should check from the start date rather than just updates.")

    args = parser.parse_args()

    config_file = args.config
    main_config_value = os.getenv('EDD_MAIN_CFG', None)
    if (config_file == '') and (main_config_value is not None):
        config_file = main_config_value

    print("'" + config_file + "'")

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '" + config_file + "'")
        raise Exception("Config file does not exist")

    ncores_val = 1
    if args.ncores > 0:
        ncores_val = args.ncores
    else:
        ncores_env_val = os.getenv('EDD_NCORES', None)
        if ncores_env_val is not None:
            ncores_val = int(ncores_env_val)

    t = rsgislib.RSGISTime()
    t.start(True)

    eodatadown.eodatadownrun.find_new_downloads(config_file, ncores_val, args.sensors, check_from_start=args.chkstart)

    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddchknewscns.py.')

