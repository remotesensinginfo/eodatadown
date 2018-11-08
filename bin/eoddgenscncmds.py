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
# Date: 08/11/2018
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

logger = logging.getLogger('eoddgenscncmds.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("-p", "--process", type=str, required=True, choices=['performdownload', 'processard'],
                        help='''Specify the processing to be performed.''')
    parser.add_argument("-o", "--output", type=str, required=True,
                        help='''Specify a path and name for an output text file listing the eoddrun.py commands
                                for individual scenes.''')

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
    cmds_lst = []
    if args.process == 'performdownload':
        try:
            logger.info('Running process to generate commands to perform image downloads.')
            sensor_obj = eodatadown.eodatadownrun.get_sensor_obj(config_file, args.sensor)
            scns = sensor_obj.get_scnlist_download()
            for scn in scns:
                cmd = 'eoddrun.py -c {0} -n 1 -s {1} --performdownload --sceneid {2}'.format(config_file, args.sensor, scn)
                cmds_lst.append(cmd)
            logger.info('Finished process to generate commands to perform image downloads.')
        except Exception as e:
            logger.error('Failed to complete the process of finding new downloads.', exc_info=True)
    elif args.process == 'processard':
        try:
            logger.info('Running process to generate commands to perform ARD conversion.')
            sensor_obj = eodatadown.eodatadownrun.get_sensor_obj(config_file, args.sensor)
            scns = sensor_obj.get_scnlist_con2ard()
            for scn in scns:
                cmd = 'eoddrun.py -c {0} -n 1 -s {1} --processard --sceneid {2}'.format(config_file, args.sensor, scn)
                cmds_lst.append(cmd)
            logger.info('Finished process to generate commands to perform ARD conversion.')
        except Exception as e:
            logger.error('Failed to download the available data.', exc_info=True)
    else:
        logger.error('Did not recognise the process specified.', exc_info=True)
    rsgisUtils = rsgislib.RSGISPyUtils()
    rsgisUtils.writeList2File(cmds_lst, args.output)
    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddgenscncmds.py.')

