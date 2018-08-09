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
    sensorObj.check4NewData()
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
            self.sysMainObj.parseConfig(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        sensor_objs = self.sysMainObj.getSensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.getSensorName() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)

        with multiprocessing.Pool(processes=ncores) as pool:
            pool.map(_check_new_data_qfunc, sensor_objs_to_process)

        edd_usage_db = self.sysMainObj.getUsageDBObj()
        edd_usage_db.addEntry("Finished Finding Available Downloads.")


    def perform_downloads(self, config_file, ncores, sensors):
        """
        A function which runs the process of performing the downloads of available scenes
        which have not yet been downloaded.
        :param config_file:
        :param ncores:
        :param sensors:
        :return:
        """
        # Create the System 'Main' object and parse the configuration file.
        if not self.parsedConfig:
            self.sysMainObj.parseConfig(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        sensor_objs = self.sysMainObj.getSensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.getSensorName() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)

        for sensorObj in sensor_objs_to_process:
            try:
                sensorObj.downloadNewData(ncores)
            except Exception as e:
                logger.debug("Error occurred while downloading for sensor: "+sensorObj.getSensorName())
                logger.debug(e.__str__(), exc_info=True)
        edd_usage_db = self.sysMainObj.getUsageDBObj()
        edd_usage_db.addEntry("Finished Downloading Available Scenes.")


    def process_data_ard(self, config_file, ncores, sensors):
        """
        A function which runs the process of converting the downloaded scenes to an ARD product.
        :param config_file:
        :param ncores:
        :param sensors:
        :return:
        """
        if not self.parsedConfig:
            self.sysMainObj.parseConfig(config_file)
            self.parsedConfig = True
            logger.debug("Parsed the system configuration.")

        sensor_objs = self.sysMainObj.getSensors()
        process_sensor = False
        sensor_objs_to_process = []
        for sensor_obj in sensor_objs:
            process_sensor = False
            if sensors is None:
                process_sensor = True
            if sensors is not None:
                if sensor_obj.getSensorName() in sensors:
                    process_sensor = True
            if process_sensor:
                sensor_objs_to_process.append(sensor_obj)

        for sensorObj in sensor_objs_to_process:
            try:
                sensorObj.convertNewData2ARD(ncores)
            except Exception as e:
                logger.debug("Error occurred while converting to ARD for sensor: " + sensorObj.getSensorName())
                logger.debug(e.__str__(), exc_info=True)
        edd_usage_db = self.sysMainObj.getUsageDBObj()
        edd_usage_db.addEntry("Finished Converting Available Scenes to ARD Product.")

