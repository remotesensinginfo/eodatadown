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

import eodatadown.eodatadownutils
import argparse
import logging
import os
import os.path
import rsgislib

logger = logging.getLogger('eoddsenroi.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sensors", nargs='*', default=None, choices=['Landsat', 'Sentinel2',
                                                                             'JAXADegTiles', 'OtherBBOX'],
                        help='''Specify the sensors for which this process should be executed, 
                                if not specified then processing is executed for all.''')
    parser.add_argument("-f", "--roifile", type=str, required=True,
                        help='''Specify the region of interest (ROI) vector file.''')
    parser.add_argument("-l", "--roilyr", type=str, required=True,
                        help='''Specify the region of interest (ROI) vector layer within the vector file.''')
    parser.add_argument("-o", "--output", type=str, required=True,
                        help='''Specify the output JSON file.''')

    args = parser.parse_args()

    t = rsgislib.RSGISTime()
    t.start(True)
    try:
        logger.info('Running process to find image ROIs.')
        install_prefix = os.path.split(os.path.dirname(__file__))[0]
        sensor_luts_file = os.path.join(install_prefix, "share", "eodatadown", "sensor_scn_lut.gpkg")

        sensors_lst = []
        if (args.sensors is None) or (not args.sensors):
            sensors_lst = ['Landsat', 'Sentinel2', 'OtherBBOX']
        else:
            sensors_lst = args.sensors

        define_sensor_roi = eodatadown.eodatadownutils.EODDDefineSensorROI()
        define_sensor_roi.findSensorROI(sensor_luts_file, sensors_lst, args.roifile, args.roilyr, args.output)

        logger.info('Finished process to find image ROIs.')
    except Exception as e:
        logger.error('Failed to complete the process to export vector footprints.', exc_info=True)
    t.end(reportDiff=True, preceedStr='EODataDown processing completed ', postStr=' - eoddsenroi.py.')

