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
import eodatadown.eodatadownutils

from eodatadown import EODATADOWN_SENSORS_LIST

logger = logging.getLogger('eoddgenobsdatecmds.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("-o", "--output", required=False, help="Specify the output file listing the commands.")
    parser.add_argument("-p", "--prefix", type=str, default="",
                        help='''Optionally provide a command prefix (e.g., when using singularity or docker).''')

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
    prefix_cmd = ""
    if args.prefix is not None:
        prefix_cmd = args.prefix

    obs_date_lst = eodatadown.eodatadownrun.get_obs_dates_need_processing(config_file, args.sensor)
    cmds = list()
    for obs in obs_date_lst:
        cmd = "{} eoddobsdatetools.py -c {} -s {} -p {} -d {} --createvis ".format(prefix_cmd, config_file, obs[0],
                                                                      obs[1], obs[2].strftime('%Y%m%d'))
        cmds.append(cmd)

    eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
    eoddutils.writeList2File(cmds, args.output)

    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddgenobsdatecmds.py.')

