#!/usr/bin/env python
"""
EODataDown - run the EODataDown System.
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
# Purpose:  Run the EODataDown System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import multiprocessing
from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils
import eodatadown.eodatadownsystemmain

logger = logging.getLogger(__name__)

########### Function for Pool ################
def _check_new_data_qfunc(sensorObj):
    sensorObj.check_new_scns()
##############################################

class EODataDownRun(object):

    def __init__(self):
        self.sysMainObj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
        self.parsedConfig = False

    def find_new_downloads(self, config_file, ncores, sensors):
        """
        A function to run the process of finding new data to download.
        :param config_file:
        :param ncores:
        :param sensors:
        :return:
        """
        logger.info("Running process to fund new downloads.")
        # Create the System 'Main' object and parse the configuration file.
        if not self.parsedConfig:
            self.sysMainObj.parse_config(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        edd_usage_db = self.sysMainObj.get_usage_db_obj()
        edd_usage_db.addEntry("Started: Finding Available Downloads.", start_block=True)

        sensor_objs = self.sysMainObj.get_sensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.get_sensor_name() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)
        with multiprocessing.Pool(processes=ncores) as pool:
            pool.map(_check_new_data_qfunc, sensor_objs_to_process)
        edd_usage_db.addEntry("Finished: Finding Available Downloads.", end_block=True)


    def perform_downloads(self, config_file, n_cores, sensors):
        """
        A function which runs the process of performing the downloads of available scenes
        which have not yet been downloaded.
        :param config_file:
        :param n_cores:
        :param sensors:
        :return:
        """
        # Create the System 'Main' object and parse the configuration file.
        if not self.parsedConfig:
            self.sysMainObj.parse_config(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        edd_usage_db = self.sysMainObj.get_usage_db_obj()
        edd_usage_db.addEntry("Started: Downloading Available Scenes.", start_block=True)

        sensor_objs = self.sysMainObj.get_sensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.get_sensor_name() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)

        for sensorObj in sensor_objs_to_process:
            try:
                sensorObj.download_all_avail(n_cores)
            except Exception as e:
                logger.debug("Error occurred while downloading for sensor: "+sensorObj.get_sensor_name())
                logger.debug(e.__str__(), exc_info=True)
        edd_usage_db.addEntry("Finished: Downloading Available Scenes.", end_block=True)


    def process_data_ard(self, config_file, n_cores, sensors):
        """
        A function which runs the process of converting the downloaded scenes to an ARD product.
        :param config_file:
        :param n_cores:
        :param sensors:
        :return:
        """
        if not self.parsedConfig:
            self.sysMainObj.parse_config(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        edd_usage_db = self.sysMainObj.get_usage_db_obj()
        edd_usage_db.addEntry("Starting: Converting Available Scenes to ARD Product.", start_block=True)

        sensor_objs = self.sysMainObj.get_sensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.get_sensor_name() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)

        for sensorObj in sensor_objs_to_process:
            try:
                sensorObj.scns2ard_all_avail(n_cores)
            except Exception as e:
                logger.debug("Error occurred while converting to ARD for sensor: " + sensorObj.get_sensor_name())
                logger.debug(e.__str__(), exc_info=True)
        edd_usage_db.addEntry("Finished: Converting Available Scenes to ARD Product.", end_block=True)
