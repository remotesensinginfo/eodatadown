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
import os.path
import datetime
import multiprocessing
import shutil
import rsgislib
import uuid
import yaml

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
import eodatadown.eodatadownrunarcsi

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDLandsatGoogle(Base):
    __tablename__ = "EDDLandsatGoogle"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Scene_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
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
    Download_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Download_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Downloaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Download_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    Archived = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    DCLoaded_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    DCLoaded_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    DCLoaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_goog(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    scn_id = params[0]
    db_info_obj = params[1]
    goog_key_json = params[2]
    goog_proj_name = params[3]
    bucket_name = params[4]
    scn_dwnlds_filelst = params[5]
    scn_lcl_dwnld_path = params[6]

    logger.debug("Set up Google storage API.")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = goog_key_json
    os.environ["GOOGLE_CLOUD_PROJECT"] = goog_proj_name
    from google.cloud import storage
    storage_client = storage.Client()
    bucket_obj = storage_client.get_bucket(bucket_name)

    logger.info("Downloading "+scn_id)
    start_date = datetime.datetime.now()
    for dwnld in scn_dwnlds_filelst:
        blob_obj = bucket_obj.blob(dwnld["bucket_path"])
        blob_obj.download_to_filename(dwnld["dwnld_path"])
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading " + scn_id)

    logger.debug("Set up database connection and update record.")
    db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
    session = sqlalchemy.orm.sessionmaker(bind=db_engine)
    ses = session()
    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID == scn_id).one_or_none()
    if query_result is None:
        logger.error("Could not find the scene within local database: " + scn_id)
    query_result.Downloaded = True
    query_result.Download_Start_Date = start_date
    query_result.Download_End_Date = end_date
    query_result.Download_Path = scn_lcl_dwnld_path
    ses.commit()
    ses.close()
    logger.debug("Finished download and updated database.")


def _process_to_ard(params):
    """
    A function which is used with the python multiprocessing pool feature to convert a scene to an ARD product
    using multiple processing cores.
    :param params:
    :return:
    """
    scn_id = params[0]
    db_info_obj = params[1]
    scn_path = params[2]
    dem_file = params[3]
    output_dir = params[4]
    tmp_dir = params[5]
    spacecraft_str = params[6]
    sensor_str = params[7]
    final_ard_path = params[8]
    reproj_outputs = params[9]
    proj_wkt_file = params[10]
    projabbv = params[11]

    edd_utils = eodatadown.eodatadownutils.EODataDownUtils()
    input_mtl = edd_utils.findFile(scn_path, "*MTL.txt")

    start_date = datetime.datetime.now()
    eodatadown.eodatadownrunarcsi.run_arcsi_landsat(input_mtl, dem_file, output_dir, tmp_dir, spacecraft_str,
                                                    sensor_str, reproj_outputs, proj_wkt_file, projabbv)

    logger.debug("Move final ARD files to specified location.")
    # Move ARD files to be kept.
    eodatadown.eodatadownrunarcsi.move_arcsi_stdsref_products(output_dir, final_ard_path)
    # Remove Remaining files.
    shutil.rmtree(output_dir)
    shutil.rmtree(tmp_dir)
    logger.debug("Moved final ARD files to specified location.")
    end_date = datetime.datetime.now()

    logger.debug("Set up database connection and update record.")
    db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
    session = sqlalchemy.orm.sessionmaker(bind=db_engine)
    ses = session()
    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID == scn_id).one_or_none()
    if query_result is None:
        logger.error("Could not find the scene within local database: " + scn_id)
    query_result.ARDProduct = True
    query_result.ARDProduct_Start_Date = start_date
    query_result.ARDProduct_End_Date = end_date
    query_result.ARDProduct_Path = final_ard_path
    ses.commit()
    ses.close()
    logger.debug("Finished download and updated database.")


class EODataDownLandsatGoogSensor (EODataDownSensor):
    """
    An class which represents a the Landsat sensor being downloaded from the Google Cloud.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "LandsatGOOG"
        self.db_tab_name = "EDDLandsatGoogle"

    def parse_sensor_config(self, config_file, first_parse=False):
        """
        Parse the JSON configuration file. If first_parse=True then a signature file will be created
        which will be checked each time the system runs to ensure changes are not back to the
        configuration file. If the signature does not match the input file then an expection will be
        thrown. To update the configuration (e.g., extent date range or spatial area) run with first_parse=True.
        :param config_file: string with the path to the JSON file.
        :param first_parse: boolean as to whether the file has been previously parsed.
        """
        edd_file_checker = eodatadown.eodatadownutils.EDDCheckFileHash()
        # If it is the first time the config_file is parsed then create the signature file.
        if first_parse:
            edd_file_checker.createFileSig(config_file)
            logger.debug("Created signature file for config file.")

        if not edd_file_checker.checkFileSig(config_file):
            raise EODataDownException("Input config did not match the file signature.")

        with open(config_file) as f:
            config_data = json.load(f)
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            logger.debug("Testing config file is for 'LandsatGOOG'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'LandsatGOOG'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.projEPSG = -1
            self.projabbv = ""
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "proj",
                                                                       "epsg"], 0, 1000000000))
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data,
                                                                  ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data,
                                                                 ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data,
                                                                ["eodatadown", "sensor", "paths", "ardtmp"])
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

            self.wrs2RowPaths = json_parse_helper.getListValue(config_data,
                                                               ["eodatadown", "sensor", "download", "wrs2"])
            for wrs2 in self.wrs2RowPaths:
                if (wrs2['path'] < 1) or (wrs2['path'] > 233):
                    logger.debug("Path error: " + str(wrs2))
                    raise EODataDownException("WRS2 paths must be between (including) 1 and 233.")
                if (wrs2['row'] < 1) or (wrs2['row'] > 248):
                    logger.debug("Row error: " + str(wrs2))
                    raise EODataDownException("WRS2 rows must be between (including) 1 and 248.")
            logger.debug("Found search params from config file")

            logger.debug("Find Google Account params from config file")
            self.goog_proj_name = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "googleinfo", "projectname"])
            self.goog_key_json = json_parse_helper.getStrValue(config_data,
                                                             ["eodatadown", "sensor", "googleinfo", "googlejsonkey"])
            logger.debug("Found Google Account params from config file")

    def init_sensor_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Creating LandsatGOOG Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def resolve_duplicated_scene_id(self, scn_id):
        """
        A function to resolve a duplicated scene ID within the database.
        :param scn_id:
        :return:
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()
        logger.debug("Find duplicate records for the scene_id: "+scn_id)
        query_rtn = ses.query(EDDLandsatGoogle.PID, EDDLandsatGoogle.Scene_ID, EDDLandsatGoogle.Product_ID).\
            filter(EDDLandsatGoogle.Scene_ID == scn_id).all()
        process_dates = []
        for record in query_rtn:
            prod_id = record.Product_ID
            logger.debug("Record (Product ID): " + prod_id)
            if (prod_id is None) or (prod_id == ""):
                process_dates.append(None)
            prod_date = datetime.datetime.strptime(prod_id.split("_")[4], "%Y%m%d").date()
            process_dates.append(prod_date)

        curent_date = datetime.datetime.now().date()
        min_timedelta = None
        min_date_idx = 0
        idx = 0
        first = True
        for date_val in process_dates:
            if date_val is not None:
                c_timedelta = curent_date - date_val
                if first:
                    min_timedelta = c_timedelta
                    min_date_idx = idx
                    first = False
                elif c_timedelta < min_timedelta:
                    min_timedelta = c_timedelta
                    min_date_idx = idx
            idx = idx + 1
        logger.debug("Keeping (Product ID): " + query_rtn[min_date_idx].Product_ID)
        logger.debug("Deleting Remaining Products")
        ses.query(EDDLandsatGoogle.PID, EDDLandsatGoogle.Scene_ID, EDDLandsatGoogle.Product_ID, ).\
            filter(EDDLandsatGoogle.Scene_ID == scn_id).\
            filter(EDDLandsatGoogle.Product_ID != query_rtn[min_date_idx].Product_ID).delete()
        ses.commit()
        ses.close()
        logger.debug("Completed processing of removing duplicate scene ids.")

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.info("Checking for new data... 'LandsatGoog'")
        logger.debug("Export Google Environmental Variable.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.goog_key_json
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.goog_proj_name
        from google.cloud import bigquery

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)

        logger.debug("Find the start date for query - if table is empty then using config date "
                     "otherwise date of last acquried image.")
        query_date = self.startDate
        ses = session()
        if (not check_from_start) and (ses.query(EDDLandsatGoogle).first() is not None):
            query_date = ses.query(EDDLandsatGoogle).order_by(
                EDDLandsatGoogle.Date_Acquired.desc()).first().Date_Acquired
        logger.info("Query with start at date: " + str(query_date))

        logger.debug("Perform google query...")
        goog_fields = "scene_id,product_id,spacecraft_id,sensor_id,date_acquired,sensing_time,collection_number," \
                      "collection_category,data_type,wrs_path,wrs_row,cloud_cover,north_lat,south_lat,west_lon," \
                      "east_lon,total_size,base_url"
        goog_db_str = "`bigquery-public-data.cloud_storage_geo_index.landsat_index`"

        goog_filter_date = "PARSE_DATE('%Y-%m-%d', date_acquired) > DATE(\"" + query_date.strftime("%Y-%m-%d") + "\")"
        goog_filter_cloud = "cloud_cover < " + str(self.cloudCoverThres)
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

        goog_filter = goog_filter_date + " AND " + goog_filter_cloud + " AND " + goog_filter_spacecraft + " AND " + \
                      goog_filter_sensor + " AND " + goog_filter_collection

        new_scns_avail = False
        for wrs2 in self.wrs2RowPaths:
            client = bigquery.Client()
            logger.info("Finding scenes for Path: " + str(wrs2['path']) + " Row: " + str(wrs2['row']))
            wrs2_filter = "wrs_path = " + str(wrs2['path']) + " AND wrs_row = " + str(wrs2['row'])
            goog_query = "SELECT " + goog_fields + " FROM " + goog_db_str + " WHERE " + goog_filter + \
                         " AND " + wrs2_filter
            logger.debug("Query: '" + goog_query + "'")
            query_results = client.query(goog_query)
            logger.debug("Performed google query")

            logger.debug("Process google query result and add to local database (Path: " + str(wrs2['path']) +
                          ", Row:" + str(wrs2['row']) + ")")

            if query_results.result():
                db_records = []
                for row in query_results.result():
                    query_rtn = ses.query(EDDLandsatGoogle).filter(
                        EDDLandsatGoogle.Scene_ID == row.scene_id).one_or_none()
                    if query_rtn is None:
                        logger.debug("SceneID: " + row.scene_id + "\tProduct_ID: " + row.product_id)
                        sensing_time_tmp = row.sensing_time.replace('Z', '')[:-1]
                        db_records.append(
                            EDDLandsatGoogle(Scene_ID=row.scene_id, Product_ID=row.product_id,
                                             Spacecraft_ID=row.spacecraft_id,
                                             Sensor_ID=row.sensor_id,
                                             Date_Acquired=datetime.datetime.strptime(row.date_acquired,
                                                                                      "%Y-%m-%d").date(),
                                             Collection_Number=row.collection_number,
                                             Collection_Catagory=row.collection_category,
                                             Sensing_Time=datetime.datetime.strptime(sensing_time_tmp,
                                                                                     "%Y-%m-%dT%H:%M:%S.%f"),
                                             Data_Type=row.data_type, WRS_Path=row.wrs_path, WRS_Row=row.wrs_row,
                                             Cloud_Cover=row.cloud_cover, North_Lat=row.north_lat,
                                             South_Lat=row.south_lat,
                                             East_Lon=row.east_lon, West_Lon=row.west_lon, Total_Size=row.total_size,
                                             Remote_URL=row.base_url, Query_Date=datetime.datetime.now(),
                                             Download_Start_Date=None,
                                             Download_End_Date=None, Downloaded=False, Download_Path="",
                                             Archived=False, ARDProduct_Start_Date=None,
                                             ARDProduct_End_Date=None, ARDProduct=False, ARDProduct_Path="",
                                             DCLoaded_Start_Date=None, DCLoaded_End_Date=None, DCLoaded=False))
                if len(db_records) > 0:
                    ses.add_all(db_records)
                    ses.commit()
                    new_scns_avail = True
            logger.debug("Processed google query result and added to local database (Path: " + str(
                wrs2['path']) + ", Row:" + str(wrs2['row']) + ")")
            client = None

        logger.debug("Check for any duplicate scene ids which have been added to database and "
                     "only keep the one processed more recently")
        query_rtn = ses.query(sqlalchemy.func.count(EDDLandsatGoogle.Scene_ID), EDDLandsatGoogle.Scene_ID).group_by(
            EDDLandsatGoogle.Scene_ID).all()
        for result in query_rtn:
            if result[0] > 1:
                self.resolve_duplicated_scene_id(result[1])
        logger.debug("Completed duplicate check/removal.")

        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked for availability of new scenes", sensor_val=self.sensor_name,
                               updated_lcl_db=True, scns_avail=new_scns_avail)

    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == False).all()

        scns2dwnld = []
        if query_result is not None:
            for record in query_result:
                scns2dwnld.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dwnld

    def download_scn(self, unq_id):
        """
        A function which downloads an individual scene and updates the database if download is successful.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: returns boolean indicating successful or otherwise download.
        """
        if not os.path.exists(self.baseDownloadPath):
            raise EODataDownException("The download path does not exist, please create and run again.")

        logger.debug("Import Google storage module and create storage object.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.goog_key_json
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.goog_proj_name
        from google.cloud import storage
        storage_client = storage.Client()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id,
                                                          EDDLandsatGoogle.Downloaded == False).all()
        ses.close()
        success = False
        if query_result is not None:
            if len(query_result) == 1:
                record = query_result[0]
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                url_path = record.Remote_URL
                url_path = url_path.replace("gs://", "")
                url_path_parts = url_path.split("/")
                bucket_name = url_path_parts[0]
                if bucket_name != "gcp-public-data-landsat":
                    logger.error("Incorrect bucket name '" + bucket_name + "'")
                    raise EODataDownException("The bucket specified in the URL is not the Google Public Landsat Bucket"
                                              " - something has gone wrong.")
                bucket_prefix = url_path.replace(bucket_name + "/", "")
                dwnld_out_dirname = url_path_parts[-1]
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, dwnld_out_dirname)
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)

                logger.debug("Get the storage bucket and blob objects.")
                bucket_obj = storage_client.get_bucket(bucket_name)
                bucket_blobs = bucket_obj.list_blobs(prefix=bucket_prefix)
                scn_dwnlds_filelst = []
                for blob in bucket_blobs:
                    if "$folder$" in blob.name:
                        continue
                    scnfilename = blob.name.replace(bucket_prefix + "/", "")
                    dwnld_file = os.path.join(scn_lcl_dwnld_path, scnfilename)
                    dwnld_dirpath = os.path.split(dwnld_file)[0]
                    if not os.path.exists(dwnld_dirpath):
                        os.makedirs(dwnld_dirpath, exist_ok=True)
                    scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})

                    _download_scn_goog([record.Scene_ID, self.db_info_obj, self.goog_key_json, self.goog_proj_name,
                                        bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path])
                success = True
            elif len(query_result) == 0:
                logger.info("PID {0} is either not available or already been downloaded.".format(unq_id))
            else:
                logger.error("PID {0} has returned more than 1 scene - must be unique something really wrong.".
                             format(unq_id))
                raise EODataDownException("There was more than 1 scene which has been found - "
                                          "something has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return success

    def download_all_avail(self, n_cores):
        """
        Queries the database to find all scenes which have not been downloaded and then downloads them.
        This function uses the python multiprocessing Pool to allow multiple simultaneous downloads to occur.
        Be careful not use more cores than your internet connection and server can handle.
        :param n_cores: The number of scenes to be simultaneously downloaded.
        """
        if not os.path.exists(self.baseDownloadPath):
            raise EODataDownException("The download path does not exist, please create and run again.")

        logger.debug("Import Google storage module and create storage object.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.goog_key_json
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.goog_proj_name
        from google.cloud import storage
        storage_client = storage.Client()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == False).all()

        dwnld_params = []
        if query_result is not None:
            logger.debug("Build download file list.")
            for record in query_result:
                logger.debug("Building download info for '"+record.Remote_URL+"'")
                url_path = record.Remote_URL
                url_path = url_path.replace("gs://", "")
                url_path_parts = url_path.split("/")
                bucket_name = url_path_parts[0]
                if bucket_name != "gcp-public-data-landsat":
                    logger.error("Incorrect bucket name '"+bucket_name+"'")
                    raise EODataDownException("The bucket specified in the URL is not the Google Public "
                                              "Landsat Bucket - something has gone wrong.")
                bucket_prefix = url_path.replace(bucket_name+"/", "")
                dwnld_out_dirname = url_path_parts[-1]
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, dwnld_out_dirname)
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)

                logger.debug("Get the storage bucket and blob objects.")
                bucket_obj = storage_client.get_bucket(bucket_name)
                bucket_blobs = bucket_obj.list_blobs(prefix=bucket_prefix)
                scn_dwnlds_filelst = []
                for blob in bucket_blobs:
                    if "$folder$" in blob.name:
                        continue
                    scnfilename = blob.name.replace(bucket_prefix+"/", "")
                    dwnld_file = os.path.join(scn_lcl_dwnld_path, scnfilename)
                    dwnld_dirpath = os.path.split(dwnld_file)[0]
                    if not os.path.exists(dwnld_dirpath):
                        os.makedirs(dwnld_dirpath, exist_ok=True)
                    scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})

                dwnld_params.append([record.Scene_ID, self.db_info_obj, self.goog_key_json, self.goog_proj_name,
                                     bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path])
        else:
            logger.info("There are no scenes to be downloaded.")
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_scn_goog, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked downloaded new scenes.", sensor_val=self.sensor_name,
                               updated_lcl_db=True, downloaded_new_scns=True)

    def get_scnlist_con2ard(self):
        """
        A function which queries the database to find scenes which have been downloaded but have not yet been
        processed to an analysis ready data (ARD) format.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to process.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False).all()

        scns2ard = []
        if query_result is not None:
            for record in query_result:
                scns2ard.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2ard

    def scn2ard(self, unq_id):
        """
        A function which processes a single scene to an analysis ready data (ARD) format.
        :param unq_id: the unique ID of the scene to be processed.
        :return: returns boolean indicating successful or otherwise processing.
        """
        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdWorkPath):
            raise EODataDownException("The ARD working path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The ARD tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id,
                                                          EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False).all()
        ses.close()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        if query_result is not None:
            if len(query_result) == 1:
                record = query_result[0]
                logger.debug("Create the specific output directories for the ARD processing.")
                dt_obj = datetime.datetime.now()

                work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
                if not os.path.exists(work_ard_path):
                    os.mkdir(work_ard_path)

                tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
                if not os.path.exists(tmp_ard_path):
                    os.mkdir(tmp_ard_path)

                logger.debug("Create info for running ARD analysis for scene: " + record.Scene_ID)
                final_ard_scn_path = os.path.join(self.ardFinalPath, record.Product_ID)
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                work_ard_scn_path = os.path.join(work_ard_path, record.Product_ID)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, record.Product_ID)
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                if self.ardProjDefined:
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Product_ID+"_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                _process_to_ard([record.Scene_ID, self.db_info_obj, record.Download_Path, self.demFile,
                                 work_ard_scn_path, tmp_ard_scn_path, record.Spacecraft_ID, record.Sensor_ID,
                                 final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv])
            elif len(query_result) == 0:
                logger.info("PID {0} is either not available or already been processed.".format(unq_id))
            else:
                logger.error("PID {0} has returned more than 1 scene - must be unique something really wrong.".
                             format(unq_id))
                raise EODataDownException("There was more than 1 scene which has been found - "
                                          "something has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

    def scns2ard_all_avail(self, n_cores):
        """
        Queries the database to find all scenes which have been downloaded but not processed to an
        analysis ready data (ARD) format and then processed them to an ARD format.
        This function uses the python multiprocessing Pool to allow multiple simultaneous processing
        of the scenes using a single core for each scene.
        Be careful not use more cores than your system has or have I/O capacity for. The processing being
        undertaken is I/O heavy in the ARD Work and tmp paths. If you have high speed storage (e.g., SSD)
        available it is recommended the ARD work and tmp paths are located on this volume.
        :param n_cores: The number of scenes to be simultaneously processed.
        """
        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdWorkPath):
            raise EODataDownException("The ARD working path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The ARD tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False).all()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        ard_params = []
        if query_result is not None:
            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(work_ard_path):
                os.mkdir(work_ard_path)

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            for record in query_result:
                logger.debug("Create info for running ARD analysis for scene: " + record.Scene_ID)
                final_ard_scn_path = os.path.join(self.ardFinalPath, record.Product_ID)
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                work_ard_scn_path = os.path.join(work_ard_path, record.Product_ID)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, record.Product_ID)
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                if self.ardProjDefined:
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Product_ID+"_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                ard_params.append([record.Scene_ID, self.db_info_obj, record.Download_Path, self.demFile,
                                   work_ard_scn_path, tmp_ard_scn_path, record.Spacecraft_ID, record.Sensor_ID,
                                   final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv])
        else:
            logger.info("There are no scenes which have been downloaded but not processed to an ARD product.")
        ses.close()
        logger.debug("Closed the database session.")

        if len(ard_params) > 0:
            logger.info("Start processing the scenes.")
            with multiprocessing.Pool(processes=n_cores) as pool:
                pool.map(_process_to_ard, ard_params)
            logger.info("Finished processing the scenes.")

        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Processed scenes to an ARD product.", sensor_val=self.sensor_name,
                               updated_lcl_db=True, convert_scns_ard=True)

    def get_scnlist_add2datacube(self):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.ARDProduct == True,
                                                          EDDLandsatGoogle.DCLoaded == False).all()
        scns2dcload = []
        if query_result is not None:
            for record in query_result:
                scns2dcload.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dcload

    def create_dc_load_yaml(self, ardpath):
        print("Hello World.")

    def scn2datacube(self, unq_id):
        """
        A function which loads a single scene into the datacube system.
        :param unq_id: the unique ID of the scene to be loaded.
        :return: returns boolean indicating successful or otherwise loading into the datacube.
        """
        # TODO ADD DataCube Functionality
        raise EODataDownException("Not implemented.")

    def scns2datacube_all_avail(self):
        """
        Queries the database to find all scenes which have been processed to an ARD format but not loaded
        into the datacube and then loads these scenes into the datacube.
        """
        rsgis_utils = rsgislib.RSGISPyUtils()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.ARDProduct == True,
                                                          EDDLandsatGoogle.DCLoaded == False).all()

        if query_result is not None:
            logger.debug("Create the yaml files for the data cube to enable import.")
            yaml_scn_files = []
            for record in query_result:
                scn_id = str(str(uuid.uuid5(uuid.NAMESPACE_URL, record.ARDProduct_Path)))
                print("{}: {}".format(record.Scene_ID, scn_id))
                img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*vmsk_rad_srefdem_stdsref.kea')
                yaml_file = os.path.splitext(img_file)[0]+"_yaml.yaml"
                epsg_code = rsgis_utils.getEPSGCode(img_file)
                lcl_proj_bbox = rsgis_utils.getImageBBOX(img_file)

                scn_info = {
                    'id': scn_id,
                    'processing_level': 'LEVEL_2',
                    'product_type': 'arcsi_ard',
                    'creation_dt': record.ARDProduct_End_Date.strftime("%Y-%m-%d %H:%M:%S"),
                    'label': record.Scene_ID,
                    'platform': {'code': record.Spacecraft_ID},
                    'instrument': {'name': record.Sensor_ID},
                    'extent': {
                        'from_dt': record.Sensing_Time.strftime("%Y-%m-%d %H:%M:%S"),
                        'to_dt': record.Sensing_Time.strftime("%Y-%m-%d %H:%M:%S"),
                        'center_dt': record.Sensing_Time.strftime("%Y-%m-%d %H:%M:%S"),
                        'coord': {
                            'll': {'lat': record.South_Lat, 'lon': record.West_Lon},
                            'lr': {'lat': record.South_Lat, 'lon': record.East_Lon},
                            'ul': {'lat': record.North_Lat, 'lon': record.West_Lon},
                            'ur': {'lat': record.North_Lat, 'lon': record.East_Lon}
                        }
                    },
                    'format': {'name': 'KEA'},
                    'grid_spatial': {
                        'projection': {
                            'spatial_reference': 'EPSG:{}'.format(epsg_code),
                            'geo_ref_points': {
                                'll': {'x': lcl_proj_bbox[0], 'y': lcl_proj_bbox[2]},
                                'lr': {'x': lcl_proj_bbox[1], 'y': lcl_proj_bbox[2]},
                                'ul': {'x': lcl_proj_bbox[0], 'y': lcl_proj_bbox[3]},
                                'ur': {'x': lcl_proj_bbox[1], 'y': lcl_proj_bbox[3]}
                            }
                        }
                    },
                    'image': {'path': img_file},
                    'lineage': {'source_datasets': {}},
                }
                with open(yaml_file, 'w') as stream:
                    yaml.dump(scn_info, stream)
                yaml_scn_files.append(yaml_file)

    def get_scn_record(self, unq_id):
        """
        A function which queries the database using the unique ID of a scene returning the record
        :param unq_id:
        :return: Returns the database record object
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).all()
        ses.close()
        scn_record = None
        if query_result is not None:
            if len(query_result) == 1:
                scn_record = query_result[0]
            else:
                logger.error(
                    "PID {0} has returned more than 1 scene - must be unique something really wrong.".format(unq_id))
                raise EODataDownException(
                    "There was more than 1 scene which has been found - soomething has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return scn_record

    def query_scn_records_date(self, start_date, end_date):
        """
        A function which queries the database to find scenes within a specified date range.
        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :return: list of database records.
        """
        raise EODataDownException("Not implemented.")

    def query_scn_records_bbox(self, lat_north, lat_south, lon_east, lon_west):
        """
        A function which queries the database to find scenes within a specified bounding box.
        :param lat_north: double with latitude north
        :param lat_south: double with latitude south
        :param lon_east: double with longitude east
        :param lon_west: double with longitude west
        :return: list of database records.
        """
        raise EODataDownException("Not implemented.")

    def update_dwnld_path(self, replace_path, new_path):
        """
        If the path to the downloaded files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not implemented.")

    def update_ard_path(self, replace_path, new_path):
        """
        If the path to the ARD files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not implemented.")

    def dwnlds_archived(self, replace_path=None, new_path=None):
        """
        This function identifies scenes which have been downloaded but the download is no longer available
        in the download path. It will set the archived option on the database for these files. It is expected
        that these files will have been move to an archive location (e.g., AWS glacier or tape etc.) but they
        could have just be deleted. There is an option to update the path to the downloads if inputs are not
        None but a check will not be performed as to whether the data is present at the new path.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files are located.
        """
        raise EODataDownException("Not implemented.")

    def export2db(self, db_info_obj):
        """
        This function exports the existing database to the database specified by the
        input database info object.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        # TODO function to export data from DB.
        raise EODataDownException("Not implemented.")

    def import_append_db(self, db_info_obj):
        """
        This function imports from the database specified by the input database info object
        and appends the data to the exisitng database. This might be used if data was processed
        on another system (e.g., HPC cluster).
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        # TODO function to import data from a DB
        raise EODataDownException("Not implemented.")

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        raise EODataDownException("Not Implemented")

    def reset_scn(self, unq_id):
        """
        A function which resets an image. This means any downloads and products are deleted
        and the database fields are reset to defaults. This allows the scene to be re-downloaded
        and processed.
        :param unq_id: unique id for the scene to be reset.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()

        if scn_record is None:
            ses.close()
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

        if scn_record.DCLoaded:
            # How to remove from datacube?
            scn_record.DCLoaded_Start_Date = None
            scn_record.DCLoaded_End_Date = None
            scn_record.DCLoaded = False

        if scn_record.ARDProduct:
            ard_path = scn_record.ARDProduct_Path
            if os.path.exists(ard_path):
                shutil.rmtree(ard_path)
            scn_record.ARDProduct_Start_Date = None
            scn_record.ARDProduct_End_Date = None
            scn_record.ARDProduct_Path = ""
            scn_record.ARDProduct = False

        if scn_record.Downloaded:
            dwn_path = scn_record.Download_Path
            if os.path.exists(dwn_path):
                shutil.rmtree(dwn_path)
            scn_record.Download_Start_Date = None
            scn_record.Download_End_Date = None
            scn_record.Download_Path = ""
            scn_record.Downloaded = False

        ses.commit()
        ses.close()
