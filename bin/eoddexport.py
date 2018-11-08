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

logger = logging.getLogger('eoddexport.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the JSON config file.")
    parser.add_argument("-s", "--sensor", type=str, default=None, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensors for which this process should be executed, 
                                if not specified then processing is executed for all.''')
    parser.add_argument("-t", "--table", type=str, default=None,
                        help='''Specify the table if GenericDataset sensor if being used.''')
    parser.add_argument("--exportvector", action='store_true', default=False,
                        help="Specify that a vector file should be exported with the image extents.")
    parser.add_argument("--file", type=str, default="",
                        help="Specify the output vector file.")
    parser.add_argument("--layer", type=str, default="",
                        help="Specify the output vector layer (if shapefile then the same name as the file but without extension).")
    parser.add_argument("--driver", type=str, default="SQLite",
                        help="Specify the GDAL driver to use. Default is 'SQLite'.")
    parser.add_argument("--add_layer", action='store_true', default=False,
                        help="Specify the vector layer should be added to an existing file.")
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
    if args.exportvector:
        try:
            logger.info('Running process to export vector footprints.')
            eodatadown.eodatadownrun.export_image_footprints_vector(config_file, args.sensor, args.table, args.file, args.layer, args.driver, args.add_layer)
            logger.info('Finished process to export vector footprints.')
        except Exception as e:
            logger.error('Failed to complete the process to export vector footprints.', exc_info=True)
    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddexport.py.')

