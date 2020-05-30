#!/usr/bin/env python
"""
EODataDown - this file is needed to ensure it can be imported

See other source files for details
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
# Purpose:  Setup variables and imports across the whole module
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.


from distutils.version import LooseVersion
import os
import logging
import logging.config
import json

EODATADOWN_VERSION_MAJOR = 2
EODATADOWN_VERSION_MINOR = 2
EODATADOWN_VERSION_PATCH = 18

# Check is GTIFF Creation Options Flag has been defined and if not then define it.
rsgislib_img_opts_tif_envvar = os.getenv('RSGISLIB_IMG_CRT_OPTS_GTIFF', None)
if rsgislib_img_opts_tif_envvar is None:
    os.environ["RSGISLIB_IMG_CRT_OPTS_GTIFF"] = "TILED=YES:COMPRESS=LZW:BIGTIFF=YES"

eodd_log_level = os.getenv('EDD_LOG_LVL', 'INFO')

# Check if the number of cores for Gamma to use through OMP has been used defined. If not, define it as 1.
omp_num_threads_envvar = os.getenv('OMP_NUM_THREADS', None)
if omp_num_threads_envvar is None:
    os.environ["OMP_NUM_THREADS"] = "1"

EODATADOWN_VERSION = str(EODATADOWN_VERSION_MAJOR) + "."  + str(EODATADOWN_VERSION_MINOR) + "." + str(EODATADOWN_VERSION_PATCH)
EODATADOWN_VERSION_OBJ = LooseVersion(EODATADOWN_VERSION)
EODATADOWN_COPYRIGHT_YEAR = "2018"
EODATADOWN_COPYRIGHT_NAMES = "Pete Bunting"
EODATADOWN_SUPPORT_EMAIL = "rsgislib-support@googlegroups.com"
EODATADOWN_WEBSITE = "https://www.remotesensing.info/eodatadown"
EODATADOWN_SENSORS_LIST = ["LandsatGOOG", "Sentinel2GOOG", "Sentinel1ASF", "GEDI", "ICESAT2"]

log_default_level=logging.INFO
if eodd_log_level.upper() == 'INFO':
    log_default_level = logging.INFO
elif eodd_log_level.upper() == 'DEBUG':
    log_default_level = logging.DEBUG
elif eodd_log_level.upper() == 'WARNING':
    log_default_level = logging.WARNING
elif eodd_log_level.upper() == 'ERROR':
    log_default_level = logging.ERROR
elif eodd_log_level.upper() == 'CRITICAL':
    log_default_level = logging.CRITICAL
else:
    raise Exception("Logging level specified ('{}') is not recognised.".format(eodd_log_level))

log_config_path = os.getenv('EDD_LOG_CFG', None)
if (log_config_path is not None) and os.path.exists(log_config_path):
    with open(log_config_path, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=log_default_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
