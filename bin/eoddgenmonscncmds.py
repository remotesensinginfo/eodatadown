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

logger = logging.getLogger('eoddgenmonscncmds.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensors", type=str, nargs='+', required=True, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensor for which this process should be executed''')
    parser.add_argument("-o", "--output", type=str, required=True,
                        help='''Specify a path and name for an output text file listing the eoddrun.py commands
                                for individual scenes.''')
    parser.add_argument("-p", "--prefix", type=str, default="",
                        help='''Optionally provide a command prefix (e.g., when using singularity or docker).''')
    parser.add_argument("--nonewscns", action='store_true', default=False,
                        help="Specify that the system should not check for new scenes but just process existing scenes.")

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

    if not args.nonewscns:
        eodatadown.eodatadownrun.find_new_downloads(config_file, 1, args.sensors, check_from_start=False)

    scn_tasks = eodatadown.eodatadownrun.get_scenes_need_processing(config_file, args.sensors)
    cmds_lst = list()
    for scn_tsk in scn_tasks:
        cmd = "{} eoddrunscnmonitoring.py -c {} -s {} -p {}".format(args.prefix, scn_tsk[0], scn_tsk[1], scn_tsk[2])
        cmds_lst.append(cmd)

    rsgis_utils = rsgislib.RSGISPyUtils()
    rsgis_utils.writeList2File(cmds_lst, args.output)

    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddgenscncmds.py.')

