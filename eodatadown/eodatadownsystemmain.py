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
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
from eodatadown.eodatadowndatereports import EODataDownDateReports
from eodatadown.eodatadownsensor import EODataDownObsDates

import logging
import json

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.orm

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDSysDetails(Base):
    __tablename__ = "EDDSystemDetails"

    ID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Name = sqlalchemy.Column(sqlalchemy.String)
    Description = sqlalchemy.Column(sqlalchemy.String)

class EODataDownSystemMain(object):

    def __init__(self):
        self.name = ''
        self.description = ''
        self.db_info_obj = None
        self.sensorConfigFiles = dict()
        self.sensors = list()
        self.date_report_config_file = None
        self.obsdates_config_file = None
        self.parsed_config = False

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        db_info = {'connection':self.db_info_obj.getDBConnection()}
        sys_info = {'name:':self.name, 'description':self.description}
        data = {'database':db_info, 'details':sys_info, 'sensors':self.sensorConfigFiles}
        str_data = json.dumps(data, indent=4, sort_keys=True)
        return str_data

    def has_parsed_config(self):
        """
        A function which returns a boolean as to whether the config file has been parsed
        or not.
        :return: boolean; True the config file has been parsed.
        """
        return self.parsed_config

    def get_usage_db_obj(self):
        logger.debug("Creating Usage database object.")
        if self.db_info_obj is None:
            raise EODataDownException("Need to parse the configuration file to find database information.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        return edd_usage_db

    def parse_config(self, config_file, first_parse=False):
        """
        Parse the inputted JSON configuration file
        :param first_parse:
        :param config_file:
        :return:
        """
        edd_file_checker = eodatadown.eodatadownutils.EDDCheckFileHash()
        if not edd_file_checker.checkFileSig(config_file):
            raise EODataDownException("Input config did not match the file signature.")
        with open(config_file) as f:
            config_data = json.load(f)
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()

            # Get Basic System Info.
            self.name = json_parse_helper.getStrValue(config_data, ['eodatadown', 'details', 'name'])
            self.description = json_parse_helper.getStrValue(config_data, ['eodatadown', 'details', 'description'])

            # Get Database Information
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()

            db_conn_str = json_parse_helper.getStrValue(config_data, ['eodatadown', 'database', 'connection'])
            self.db_info_obj = eodatadown.eodatadownutils.EODataDownDatabaseInfo(db_conn_str)

            if json_parse_helper.doesPathExist(config_data, ['eodatadown', 'reports', 'date_report_config']):
                self.date_report_config_file = json_parse_helper.getStrValue(config_data, ['eodatadown', 'reports',
                                                                                           'date_report_config'])
                report_obj = EODataDownDateReports(self.db_info_obj)
                report_obj.parse_sensor_config(self.date_report_config_file, first_parse)

            if json_parse_helper.doesPathExist(config_data, ['eodatadown', 'obsdates']):
                self.obsdates_config_file = json_parse_helper.getStrValue(config_data, ['eodatadown', 'obsdates'])
                obsdates_obj = EODataDownObsDates(self.db_info_obj)
                obsdates_obj.parse_sensor_config(self.obsdates_config_file, first_parse)

            # Get Sensor Configuration File List
            for sensor in config_data['eodatadown']['sensors']:
                self.sensorConfigFiles[sensor] = json_parse_helper.getStrValue(config_data, ['eodatadown', 'sensors',
                                                                                             sensor, 'config'])
                logger.debug("Getting sensor object: '" + sensor + "'")
                sensor_obj = self.create_sensor_obj(sensor)
                logger.debug("Parse sensor config file: '" + sensor + "'")
                sensor_obj.parse_sensor_config(self.sensorConfigFiles[sensor], first_parse)
                self.sensors.append(sensor_obj)
                logger.debug("Parsed sensor config file: '" + sensor + "'")

            self.parsed_config = True

    def create_sensor_obj(self, sensor):
        """
        Get an instance of an object for the sensor specified.
        :param sensor:
        :return:
        """
        sensor_obj = None
        if sensor == "LandsatGOOG":
            logger.debug("Found sensor LandsatGOOG")
            from eodatadown.eodatadownlandsatgoogsensor import EODataDownLandsatGoogSensor
            sensor_obj = EODataDownLandsatGoogSensor(self.db_info_obj)
        elif sensor == "Sentinel2GOOG":
            logger.debug("Found sensor Sentinel2GOOG")
            from eodatadown.eodatadownsentinel2googsensor import EODataDownSentinel2GoogSensor
            sensor_obj = EODataDownSentinel2GoogSensor(self.db_info_obj)
        elif sensor == "Sentinel1ASF":
            logger.debug("Found sensor Sentinel1ASF")
            from eodatadown.eodatadownsentinel1asf import EODataDownSentinel1ASFProcessorSensor
            sensor_obj = EODataDownSentinel1ASFProcessorSensor(self.db_info_obj)
        elif sensor == "GEDI":
            logger.debug("Found sensor GEDI")
            from eodatadown.eodatadownGEDIsensor import EODataDownGEDISensor
            sensor_obj = EODataDownGEDISensor(self.db_info_obj)
        else:
            raise EODataDownException("Do not know of an object for sensor: '"+sensor+"'")
        return sensor_obj

    def get_sensors(self):
        """
        Function which returns the list of sensor objects.
        :return:
        """
        return self.sensors

    def get_sensor_obj(self, sensor):
        """
        A function to get a sensor object.
        :param sensor:
        :return:
        """
        sensor_obj_to_process = None
        for sensor_obj in self.sensors:
            if sensor_obj.get_sensor_name() == sensor:
                sensor_obj_to_process = sensor_obj
                break

        if sensor_obj_to_process is None:
            logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
            raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

        return sensor_obj_to_process

    def init_dbs(self):
        """
        A function which will setup the system data base for each of the sensors.
        Note. this function should only be used to initialing the system.
        :return:
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Initialise the data usage database.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.init_usage_log_db()

        logger.debug("Creating System Details Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Adding System Details to Database.")
        ses.add(EDDSysDetails(Name=self.name, Description=self.description))
        ses.commit()
        ses.close()
        logger.debug("Committed and closed db session.")

        for sensor_obj in self.sensors:
            logger.debug("Initialise Sensor Database: '" + sensor_obj.get_sensor_name() + "'")
            sensor_obj.init_sensor_db()
            logger.debug("Finished initialising the sensor database for '" + sensor_obj.get_sensor_name() + "'")

        if self.date_report_config_file is not None:
            report_obj = EODataDownDateReports(self.db_info_obj)
            report_obj.parse_sensor_config(self.date_report_config_file)
            report_obj.init_db()

        if self.obsdates_config_file is not None:
            obsdates_obj = EODataDownObsDates(self.db_info_obj)
            obsdates_obj.parse_sensor_config(self.obsdates_config_file)
            obsdates_obj.init_db()

    def get_date_report_obj(self):
        """
        A function to retrieve an instance of a date report object.
        :return: instance of EODataDownDateReports object or None if configure file has not been specified.
        """
        report_obj = None
        if self.date_report_config_file is not None:
            report_obj = EODataDownDateReports(self.db_info_obj)
            report_obj.parse_sensor_config(self.date_report_config_file)
        return report_obj
    
    def get_obsdates_obj(self):
        """
        A function to retrieve an instance of a observation dates object.
        :return: instance of EODataDownObsDates object or None if configure file has not been specified.
        """
        obsdates_obj = None
        if self.date_report_config_file is not None:
            obsdates_obj = EODataDownObsDates(self.db_info_obj)
            obsdates_obj.parse_sensor_config(self.obsdates_config_file)
        return obsdates_obj



