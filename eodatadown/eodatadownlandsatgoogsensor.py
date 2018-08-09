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
import os
import datetime

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDLandsatGoogle(Base):
    __tablename__ = "EDDLandsatGoogle"

    Scene_ID = sqlalchemy.Column(sqlalchemy.String, primary_key=True, nullable=False)
    Product_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Spacecraft_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Sensor_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Date_Acquired = sqlalchemy.Column(sqlalchemy.Date, nullable=True)
    Collection_Number = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Collection_Catagory = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Sensing_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Data_Type = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    WRS_Path = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    WRS_Row = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    Cloud_Cover = sqlalchemy.Column(sqlalchemy.Float, nullable=False, default=0.0)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Total_Size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Query_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Download_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ToDownload = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Downloaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProductPath = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    Download_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")


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

            self.wrs2RowPaths = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor", "download", "wrs2"])
            for wrs2 in self.wrs2RowPaths:
                if (wrs2['path'] < 1) or (wrs2['path'] > 233):
                    logger.debug("Path error: " +str(wrs2))
                    raise EODataDownException("WRS2 paths must be between (including) 1 and 233.")
                if (wrs2['row'] < 1) or (wrs2['row'] > 248):
                    logger.debug("Row error: " + str(wrs2))
                    raise EODataDownException("WRS2 rows must be between (including) 1 and 248.")
            logger.debug("Found search params from config file")

            logger.debug("Find Google Account params from config file")
            self.googProjName = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "googleinfo", "projectname"])
            self.googKeyJSON = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "googleinfo", "googlejsonkey"])
            logger.debug("Found Google Account params from config file")


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
        logger.info("Checking for new data... 'LandsatGoog'")
        logger.debug("Export Google Environmental Variable.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.googKeyJSON
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.googProjName
        from google.cloud import bigquery
        client = bigquery.Client()

        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)

        logger.debug("Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        ses = Session()
        if ses.query(EDDLandsatGoogle).first() is not None:
            print(ses.query(EDDLandsatGoogle).order_by(EDDLandsatGoogle.Date_Acquired.desc()).first())

        logger.debug("Perform google query...")
        goog_fields = "scene_id,product_id,spacecraft_id,sensor_id,date_acquired,sensing_time,collection_number," \
                      "collection_category,data_type,wrs_path,wrs_row,cloud_cover,north_lat,south_lat,west_lon," \
                      "east_lon,total_size,base_url"
        goog_db_str = "`bigquery-public-data.cloud_storage_geo_index.landsat_index`"

        goog_filter_date = "PARSE_DATE('%Y-%m-%d', date_acquired) > DATE(\""+query_date.strftime("%Y-%m-%d")+"\")"
        goog_filter_cloud = "cloud_cover < "+str(self.cloudCoverThres)
        goog_filter_spacecraft = "spacecraft_id IN ("
        first = True
        for spacecraft_str in self.spacecraftLst:
            if first:
                goog_filter_spacecraft = goog_filter_spacecraft + "\"" + spacecraft_str + "\""
                first = False
            else:
                goog_filter_spacecraft = goog_filter_spacecraft + ",\"" + spacecraft_str + "\""
        goog_filter_spacecraft = goog_filter_spacecraft + ")"

        goog_filter_sensor = "sensor_id IN ("
        first = True
        for sensor_str in self.sensorLst:
            if first:
                goog_filter_sensor = goog_filter_sensor + "\"" + sensor_str + "\""
                first = False
            else:
                goog_filter_sensor = goog_filter_sensor + ",\"" + sensor_str + "\""
        goog_filter_sensor = goog_filter_sensor + ")"

        goog_filter_collection = "collection_category IN ("
        first = True
        for collect_str in self.collectionLst:
            if first:
                goog_filter_collection = goog_filter_collection + "\"" + collect_str + "\""
                first = False
            else:
                goog_filter_collection = goog_filter_collection + ",\"" + collect_str + "\""
        goog_filter_collection = goog_filter_collection + ")"

        goog_filter = goog_filter_date + " AND " + goog_filter_cloud + " AND " + goog_filter_spacecraft + " AND " \
                      + goog_filter_sensor + " AND " + goog_filter_collection

        new_scns_avail = False
        for wrs2 in self.wrs2RowPaths:
            wrs2_filter = "wrs_path = "+str(wrs2['path'])+" AND wrs_row = " +str(wrs2['row'])
            goog_query = "SELECT " + goog_fields + " FROM " + goog_db_str + " WHERE " + goog_filter + " AND " + wrs2_filter
            logger.debug("Query: '"+goog_query+"'")
            query_results = client.query(goog_query)
            logger.debug("Performed google query")

            logger.debug("Process google query result and add to local database (Path: "+str(wrs2['path'])+", Row:"+str(wrs2['row'])+")")
            if query_results.result():
                db_records = []
                for row in query_results.result():
                    query_rtn = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID==row.scene_id).one_or_none()
                    if query_rtn is None:
                        sensing_time_tmp = row.sensing_time.replace('Z', '')[:-1]
                        db_records.append(
                            EDDLandsatGoogle(Scene_ID=row.scene_id, Product_ID=row.product_id, Spacecraft_ID=row.spacecraft_id,
                                             Sensor_ID=row.sensor_id,
                                             Date_Acquired=datetime.datetime.strptime(row.date_acquired, "%Y-%m-%d").date(),
                                             Collection_Number=row.collection_number,
                                             Collection_Catagory=row.collection_category,
                                             Sensing_Time=datetime.datetime.strptime(sensing_time_tmp, "%Y-%m-%dT%H:%M:%S.%f"),
                                             Data_Type=row.data_type, WRS_Path=row.wrs_path, WRS_Row=row.wrs_row,
                                             Cloud_Cover=row.cloud_cover, North_Lat=row.north_lat, South_Lat=row.south_lat,
                                             East_Lon=row.east_lon, West_Lon=row.west_lon, Total_Size=row.total_size,
                                             Remote_URL=row.base_url, Query_Date=datetime.datetime.now(), Download_Date=None,
                                             ToDownload=False, Downloaded=False, ARDProduct=False, ARDProductPath="",
                                             Download_Path=""))
                if len(db_records) > 0:
                    ses.add_all(db_records)
                    ses.commit()
                    new_scns_avail = True
            logger.debug("Processed google query result and added to local database (Path: " + str(wrs2['path']) + ", Row:" + str(wrs2['row'])+")")
        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked for availability of new scenes", sensor_val=self.sensorName, updated_lcl_db=True, scns_avail=new_scns_avail)

    def downloadNewData(self, ncores):
        """
        :param ncores:
        :return:
        """
        raise EODataDownException("EODataDownLandsatGoogSensor::downloadNewData not implemented")

    def convertNewData2ARD(self, ncores):
        """
        :param ncores:
        :return:
        """
        raise EODataDownException("EODataDownLandsatGoogSensor::convertNewData2ARD not implemented")
