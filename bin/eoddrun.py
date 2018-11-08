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
# Date: 07/08/2018
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

logger = logging.getLogger('eoddrun.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-n", "--ncores", type=int, default=0,
                        help="Specify the number of processing cores to use (or use EDD_NCORES).")
    parser.add_argument("-s", "--sensors", type=str, nargs='+', default=None, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensors for which this process should be executed, 
                                if not specified then processing is executed for all.''')
    parser.add_argument("--finddownloads", action='store_true', default=False,
                        help="Specify that the system such look for availability of new downloads.")
    parser.add_argument("--performdownload", action='store_true', default=False,
                        help="Specify that the system should downloads files which have not been downloaded.")
    parser.add_argument("--processard", action='store_true', default=False,
                        help="Specify that the system should process downloads to and ARD product.")
    parser.add_argument("--loaddc", action='store_true', default=False,
                        help="Specify that the system should load available scenes into the associated datacube.")
    parser.add_argument("--sceneid", type=str, default=None,
                        help="Specify an ID of a scene to be processed.")
    args = parser.parse_args()

    config_file = args.config
    main_config_value = os.getenv('EDD_MAIN_CFG', None)
    if (config_file == '') and (main_config_value is not None):
        config_file = main_config_value

    print("'" + config_file + "'")

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '" + config_file + "'")
        raise Exception("Config file does not exist")

    ncores = int(os.getenv('EDD_NCORES', 0))
    if args.ncores > ncores:
        ncores = args.ncores

    process_single_scn = False
    single_scn_sensor = ""
    if args.sceneid is not None:
        process_single_scn = True
        if process_single_scn is "":
            raise Exception("The specified scene ID is an empty string.")
        if args.sensors == None:
            raise Exception("If a scene ID has been specified then a sensor must be specified.")
        elif len(args.sensors) != 1:
            raise Exception("If a scene ID has been specified then a single scene is being processed which can only be from a single sensor.")
        else:
            single_scn_sensor = args.sensors[0]

    if ncores == 0:
        logger.info("The number of cores has not been specified. Either use -n or the variable EDD_NCORES.")
        raise Exception("The number of cores to use has not been specified.")

    if (not args.finddownloads) and (not args.performdownload) and (not args.processard) and (not args.loaddc):
        logger.info("At least one of --finddownloads, --performdownload, --processard or --loaddc needs to be specified.")
        raise Exception("At least one of --finddownloads, --performdownload, --processard or --loaddc needs to be specified.")


    t = rsgislib.RSGISTime()
    t.start(True)
    if args.finddownloads:
        try:
            if process_single_scn:
                raise Exception('It is not possible to find new downloads for a given scene ID - this does not make sense.')
            else:
                logger.info('Running process to find new downloads.')
                eodatadown.eodatadownrun.find_new_downloads(config_file, ncores, args.sensors)
                logger.info('Finished process to find new downloads.')
        except Exception as e:
            logger.error('Failed to complete the process of finding new downloads.', exc_info=True)
    if args.performdownload:
        try:
            if process_single_scn:
                logger.info('Running single download for scene "{}".'.format(args.sceneid))
                eodatadown.eodatadownrun.perform_scene_download(config_file, single_scn_sensor, args.sceneid)
                logger.info('Finished single download for scene "{}".'.format(args.sceneid))
            else:
                logger.info('Running process to download the available data.')
                eodatadown.eodatadownrun.perform_downloads(config_file, ncores, args.sensors)
                logger.info('Finished process to download the available data.')
        except Exception as e:
            logger.error('Failed to download the available data.', exc_info=True)
    if args.processard:
        try:
            if process_single_scn:
                raise Exception('Error no implementation - should be processing to ARD "{}"'.format(args.sceneid))
            else:
                logger.info('Running process to data to an ARD product.')
                eodatadown.eodatadownrun.process_data_ard(config_file, ncores, args.sensors)
                logger.info('Finished process to data to an ARD product.')
        except Exception as e:
            logger.error('Failed to process data to ARD products.', exc_info=True)
    if args.loaddc:
        try:
            if process_single_scn:
                raise Exception('Error no implementation - should be loading into ODC "{}"'.format(args.sceneid))
            else:
                logger.info('Running process to load data into a datacube.')
                eodatadown.eodatadownrun.datacube_load_data(config_file, args.sensors)
                logger.info('Finished process to load data into a datacube.')
        except Exception as e:
            logger.error('Failed to load data into a datacube.', exc_info=True)
    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddrun.py.')

