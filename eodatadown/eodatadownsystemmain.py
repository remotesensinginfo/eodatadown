#!/usr/bin/env python
"""
EODataDown - provide the main class where the functionality of EODataDown is accessed.
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
# Purpose:  Provide the main class where the functionality of EODataDown is accessed.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.


from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils
from eodatadown.eodatadownlandsatgoogsensor import EODataDownLandsatGoogSensor
from eodatadown.eodatadownsentinel2googsensor import EODataDownSentinel2GoogSensor

import logging
import json

logger = logging.getLogger(__name__)


class EODataDownSystemMain(object):

    def __init__(self):
        self.name = ''
        self.description = ''

        self.dbInfoObj = None

        self.sensorConfigFiles = dict()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):

        db_info = {'connection':self.dbInfoObj.getDBConnection(),
                   'name':self.dbInfoObj.getDBName(),
                   'pass':self.dbInfoObj.getEncodedDBPass(),
                   'user':self.dbInfoObj.getDBUser()}
        sys_info = {'name:':self.name, 'description':self.description}
        data = {'database':db_info, 'details':sys_info, 'sensors':self.sensorConfigFiles}
        str_data = json.dumps(data, indent=4, sort_keys=True)
        return str_data

    def getSensorObj(self, sensor):
        """
        Get an instance of an object for the sensor specified.
        :param sensor:
        :return:
        """
        sensorObj = None
        if sensor == "LandsatGOOG":
            logger.debug("Found sensor LandsatGOOG")
            sensorObj = EODataDownLandsatGoogSensor(self.dbInfoObj)
        elif sensor == "Sentinel2GOOG":
            logger.debug("Found sensor Sentinel2GOOG")
            sensorObj = EODataDownSentinel2GoogSensor(self.dbInfoObj)
        else:
            raise EODataDownException("Do not know of an object for sensor: '"+sensor+"'")
        return sensorObj


    def parseConfig(self, config_file):
        """
        Parse the inputted JSON configuration file
        :param config_file:
        :return:
        """
        eddFileChecker = eodatadown.eodatadownutils.EDDCheckFileHash()
        if not eddFileChecker.checkFileSig(config_file):
            raise EODataDownException("Input config did not match the file signature.")
        with open(config_file) as f:
            config_data = json.load(f)

            # Get Basic System Info.
            self.name = config_data['details']['name']
            self.description = config_data['details']['description']

            # Get Database Information
            eddPassEncoder = eodatadown.eodatadownutils.EDDPasswordTools()

            self.dbInfoObj = eodatadown.eodatadownutils.EODataDownDatabaseInfo(config_data['database']['connection'],
                                                                               config_data['database']['user'],
                                                                               eddPassEncoder.unencodePassword(config_data['database']['pass']),
                                                                               config_data['database']['name'])

            # Get Sensor Configuration File List
            for sensor in config_data['sensors']:
                self.sensorConfigFiles[sensor] = config_data['sensors'][sensor]

    def initSensorDBs(self):
        """
        A function which will setup the system data base for each of the sensors.
        Note. this function should only be used to initialing the system.
        :return:
        """
        for sensor in self.sensorConfigFiles:
            logger.debug("Getting sensor object: '" + sensor + "'")
            sensorObj = self.getSensorObj(sensor)
            logger.debug("Got sensor object: '"+sensor+"'")
            sensorObj.initSensorDB()
            logger.debug("Finished initialising the sensor database for '"+sensor+"'")
