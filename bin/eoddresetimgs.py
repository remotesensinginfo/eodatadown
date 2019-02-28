#!/usr/bin/env python
"""
EODataDown - Reset images so they can be re-downloaded.
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
# Purpose:  Command line tool to reset image so they can be re-downloaded.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 08/02/2019
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

logger = logging.getLogger('eoddresetimgs.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("--noard", action='store_true', default=False,
                        help="Resets (deletes download) an images for which an ARD product hasn't been calculated.")
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

    if args.noard:
        try:
            logger.info('Running process to reset scenes which have not been converted to ARD.')
            sensor_obj = eodatadown.eodatadownrun.get_sensor_obj(config_file, args.sensor)
            scns = sensor_obj.get_scnlist_con2ard()
            for scn in scns:
                sensor_obj.reset_scn(scn)
            logger.info('Finished process to reset scenes which have not been converted to ARD.')
        except Exception as e:
            logger.error('Failed to reset all scenes the available data.', exc_info=True)
    else:
        logger.info('No processing option given (i.e., --noard).')

    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddrestimgs.py.')
