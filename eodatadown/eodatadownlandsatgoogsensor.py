#!/usr/bin/env python
"""
EODataDown - a sensor class for Landsat data downloaded from the Google Cloud.
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
# Purpose:  Provides a sensor class for Landsat data downloaded from the Google Cloud.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDLandsatGoogle(Base):
    __tablename__ = "EDDLandsatGoogle"

    ID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    Scene_ID = sqlalchemy.Column(sqlalchemy.String)
    Product_ID = sqlalchemy.Column(sqlalchemy.String)
    Spacecraft_ID = sqlalchemy.Column(sqlalchemy.String)
    Sensor_ID = sqlalchemy.Column(sqlalchemy.String)
    Date_Acquired = sqlalchemy.Column(sqlalchemy.Date)
    Collection_Number = sqlalchemy.Column(sqlalchemy.String)
    Collection_Catagory = sqlalchemy.Column(sqlalchemy.String)
    Sensing_Time = sqlalchemy.Column(sqlalchemy.Time)
    Data_Type = sqlalchemy.Column(sqlalchemy.String)
    WRS_Path = sqlalchemy.Column(sqlalchemy.Integer)
    WRS_Row = sqlalchemy.Column(sqlalchemy.Integer)
    Cloud_Cover = sqlalchemy.Column(sqlalchemy.Float)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float)
    West_lon = sqlalchemy.Column(sqlalchemy.Float)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String)
    Query_Search = sqlalchemy.Column(sqlalchemy.DateTime)
    ToDownload = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    Downloaded = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    ARDProductPath = sqlalchemy.Column(sqlalchemy.String)
    Download_Path = sqlalchemy.Column(sqlalchemy.String)


class EODataDownLandsatGoogSensor (EODataDownSensor):
    """
    An class which represents a the Landsat sensor being downloaded from the Google Cloud.
    """

    def __init__(self, dbInfoObj):
        EODataDownSensor.__init__(self, dbInfoObj)
        self.sensorName = "LandsatGOOG"

    def parseSensorConfig(self, config_file, first_parse=False):
        """

        :param config_file:
        :param first_parse:
        :return:
        """
        eddFileChecker = eodatadown.eodatadownutils.EDDCheckFileHash()
        # If it is the first time the config_file is parsed then create the signature file.
        if first_parse:
            eddFileChecker.createFileSig(config_file)
            logger.debug("Created signature file for config file.")

        if not eddFileChecker.checkFileSig(config_file):
            raise EODataDownException("Input config did not match the file signature.")

        with open(config_file) as f:
            config_data = json.load(f)
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            logger.debug("Testing config file is for 'LandsatGOOG'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensorName])
            logger.debug("Have the correct config file for 'LandsatGOOG'")

            logger.debug("Find ARCSI params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "arcsi", "dem"])
            logger.debug("Found ARCSI params from config file")

            logger.debug("Find paths from config file")
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardtmp"])
            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            self.spacecraftLst = json_parse_helper.getStrListValue(config_data,
                                                                   ["eodatadown", "sensor", "download", "spacecraft"],
                                                                   ["LANDSAT_8", "LANDSAT_7", "LANDSAT_5", "LANDSAT_4",
                                                                    "LANDSAT_3", "LANDSAT_2", "LANDSAT_1"])

            self.sensorLst = json_parse_helper.getStrListValue(config_data,
                                                               ["eodatadown", "sensor", "download", "sensor"],
                                                               ["OLI_TIRS", "ETM", "TM", "MSS"])

            self.collectionLst = json_parse_helper.getStrListValue(config_data,
                                                                   ["eodatadown", "sensor", "download", "collection"],
                                                                   ["T1", "T2", "RT", "PRE"])

            self.cloudCoverThres = json_parse_helper.getNumericValue(config_data,
                                                                     ["eodatadown", "sensor", "download", "cloudcover"],
                                                                     0, 100)

            self.startDate = json_parse_helper.getDateValue(config_data,
                                                            ["eodatadown", "sensor", "download", "startdate"],
                                                            "%Y-%m-%d")
            logger.debug("Found search params from config file")

    def initSensorDB(self):
        """
        Initialise the sensor database table.
        :return:
        """
        logger.debug("Creating Database Engine.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(dbEng)

        logger.debug("Creating LandsatGoog Database.")
        Base.metadata.bind = dbEng
        Base.metadata.create_all()


    def check4NewData(self):
        """

        :return:
        """
        print("Checking for new data... 'LandsatGoog'")

    def downloadNewData(self, ncores):
        """

        :return:
        """
        raise EODataDownException("EODataDownLandsatGoogSensor::downloadNewData not implemented")

    def convertNewData2ARD(self, ncores):
        """

        :return:
        """
        raise EODataDownException("EODataDownLandsatGoogSensor::convertNewData2ARD not implemented")
