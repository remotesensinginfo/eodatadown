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
# Purpose:  Command line function for initialising the system.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import eodatadown.eodatadowninit
import argparse
import logging
import os
import os.path
import rsgislib

logger = logging.getLogger('eoddsetup.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file or use env variable EDD_MAIN_CFG.")
    parser.add_argument("--new", action='store_true', default=False,
                        help="Specify that this is a new system that is being setup.")
    parser.add_argument("--update", action='store_true', default=False,
                        help="Specify that this is an existing system that is being updated.")
    args = parser.parse_args()

    config_file = args.config
    main_config_value = os.getenv('EDD_MAIN_CFG', None)
    if (config_file == '') and (main_config_value is not None):
        config_file = main_config_value

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '"+config_file+"'")

    t = rsgislib.RSGISTime()
    t.start(True)
    if args.new:
        try:
            logger.info('Initialising a new system.')
            eodatadown.eodatadowninit.initialise_new_system(config_file)
            logger.info('Finished initialising the new system.')
        except Exception as e:
            logger.error('Failed to initialise the new system.', exc_info=True)
    elif args.update:
        try:
            logger.info('Updating the configuration of the existing system.')
            eodatadown.eodatadowninit.update_existing_system(config_file)
            logger.info('Finished updating the configuration of the existing system.')
        except Exception as e:
            logger.error('Failed to updated the configuration of the existing system.', exc_info=True)
    else:
        raise Exception("You must specify one of either --new or --update.")
    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddsetup.py.')


