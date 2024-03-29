#!/usr/bin/env python
"""
EODataDown - initialise the EODataDown System.
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
# Purpose:  Initialise the EODataDown System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils
import eodatadown.eodatadownsystemmain

logger = logging.getLogger(__name__)


def initialise_new_system(config_file):
    """
    Initialise the a new system based on the provided configuration file.
    :param config_file:
    :return:
    """
    # Create the signature file for the configuration file.
    edd_file_checker = eodatadown.eodatadownutils.EDDCheckFileHash()
    edd_file_checker.createFileSig(config_file)
    logger.debug("Created signature file for config file.")

    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file, True)
    logger.debug("Parsed the system configuration.")

    # Create and initialise the sensor databases
    sys_main_obj.init_dbs()
    logger.debug("Initialised the sensor databases.")
    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Initialising the databases and configure files.", start_block=True)
    # I don't think there is anything which need doing - keeping start and end for consistency.
    edd_usage_db.add_entry("Finished: Initialising the databases and configure files.", end_block=True)


def update_existing_system(config_file):
    """
    A function which will update the config file signatures and check parsing.
    :param config_file:
    :return:
    """
    # Create the signature file for the configuration file.
    edd_file_checker = eodatadown.eodatadownutils.EDDCheckFileHash()
    edd_file_checker.createFileSig(config_file)
    logger.debug("Created signature file for config file.")

    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file, True)
    logger.debug("Parsed the system configuration.")

    # Create and initialise the sensor databases
    sys_main_obj.init_dbs(drop_tables=False)
    logger.debug("Initialised the sensor databases.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Updating initialisation of the configure files.", start_block=True)
    # I don't think there is anything which need doing - keeping start and end for consistency.
    edd_usage_db.add_entry("Finished: Updating initialisation of the configure files.", end_block=True)

