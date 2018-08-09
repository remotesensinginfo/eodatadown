#!/usr/bin/env python
"""
EODataDown - a set of functions to provide a simplified python interface to ARCSI.
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
#
# Purpose:  Provides a set of functions to provide a simplified python interface to ARCSI.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 09/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
from eodatadown.eodatadownutils import EODataDownException

logger = logging.getLogger(__name__)


def run_arcsi_landsat(input_mtl, dem_file, output_dir, tmp_dir, spacecraft_str, sensor_str):
    """
    A function to run ARCSI for a landsat scene using python rather than
    the command line interface.
    :param input_mtl:
    :param dem_file:
    :param output_dir:
    :param tmp_dir:
    :return:
    """
    import arcsilib.arcsirun

    if (spacecraft_str == "LANDSAT_8") and (sensor_str == ""):
        arcsi_sensor_str = "ls8"
    elif (spacecraft_str == "LANDSAT_7") and (sensor_str == "ETM"):
        arcsi_sensor_str = "ls7"
    elif (spacecraft_str == "LANDSAT_5") and (sensor_str == "TM"):
        arcsi_sensor_str = "ls5tm"
    elif (spacecraft_str == "LANDSAT_4") and (sensor_str == "TM"):
        arcsi_sensor_str = "ls4tm"
    else:
        logger.error("Did not recognise the spacecraft and sensor combination. (" + spacecraft_str + ", " + sensor_str + ")")
        raise EODataDownException("Did not recognise the spacecraft and sensor combination.")
    logger.info("Starting to run ARCSI for: "+input_mtl)
    arcsilib.arcsirun.runARCSI(input_mtl, None, None, arcsi_sensor_str, None, "KEA",
                               output_dir, None, None, None, None, None, None,
                               ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA"],
                               True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                               "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                               None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                               20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                               True, False, False, None, None, False, None)
    logger.info("Finished running ARCSI for: " + input_mtl)
