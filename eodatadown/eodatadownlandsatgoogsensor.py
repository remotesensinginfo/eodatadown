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
import sys
import datetime
import multiprocessing
import shutil
import rsgislib
import uuid
import yaml
import subprocess
import importlib

from osgeo import osr
from osgeo import ogr
from osgeo import gdal

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
import eodatadown.eodatadownrunarcsi

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.types
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import func

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
    Collection_Category = sqlalchemy.Column(sqlalchemy.String, nullable=True)
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
    Invalid = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)
    RegCheck = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_goog(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    pid = params[0]
    scn_id = params[1]
    db_info_obj = params[2]
    goog_key_json = params[3]
    goog_proj_name = params[4]
    bucket_name = params[5]
    scn_dwnlds_filelst = params[6]
    scn_lcl_dwnld_path = params[7]
    scn_remote_url = params[8]
    goog_down_meth = params[9]

    download_completed = False
    logger.info("Downloading " + scn_id)
    start_date = datetime.datetime.now()
    if goog_down_meth == 'PYAPI':
        logger.debug("Set up Google storage API.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = goog_key_json
        os.environ["GOOGLE_CLOUD_PROJECT"] = goog_proj_name
        from google.cloud import storage
        storage_client = storage.Client()
        bucket_obj = storage_client.get_bucket(bucket_name)
        for dwnld in scn_dwnlds_filelst:
            blob_obj = bucket_obj.blob(dwnld["bucket_path"])
            blob_obj.download_to_filename(dwnld["dwnld_path"])
        download_completed = True
    elif goog_down_meth == 'GSUTIL':
        logger.debug("Using Google GSUTIL utility to download.")
        auth_cmd = "gcloud auth activate-service-account --key-file={}".format(goog_key_json)
        cmd = "gsutil cp -r {} {}".format(scn_remote_url, scn_lcl_dwnld_path)
        try:
            logger.debug("Running command: '{}'".format(auth_cmd))
            subprocess.call(auth_cmd, shell=True)
            logger.debug("Running command: '{}'".format(cmd))
            subprocess.call(cmd, shell=True)
            download_completed = True
        except OSError as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
        except Exception as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
    elif goog_down_meth == 'GSUTIL_MULTI':
        logger.debug("Using Google GSUTIL (multi threaded) utility to download.")
        auth_cmd = "gcloud auth activate-service-account --key-file={}".format(goog_key_json)
        cmd = "gsutil -m cp -r {} {}".format(scn_remote_url, scn_lcl_dwnld_path)
        try:
            logger.debug("Running command: '{}'".format(auth_cmd))
            subprocess.call(auth_cmd, shell=True)
            logger.debug("Running command: '{}'".format(cmd))
            subprocess.call(cmd, shell=True)
            download_completed = True
        except OSError as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
        except Exception as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
    else:
        raise EODataDownException("Do not recognise the google download method provided.")
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading " + scn_id)

    if download_completed:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == pid).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: PID = {}".format(pid))
            ses.commit()
            ses.close()
            raise EODataDownException("Could not find the scene within local database: PID = {}".format(pid))

        query_result.Downloaded = True
        query_result.Download_Start_Date = start_date
        query_result.Download_End_Date = end_date
        query_result.Download_Path = scn_lcl_dwnld_path
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")
    else:
        logger.error("Download did not complete, re-run and it should continue from where it left off: {}".format(scn_lcl_dwnld_path))



def _process_to_ard(params):
    """
    A function which is used with the python multiprocessing pool feature to convert a scene to an ARD product
    using multiple processing cores.
    :param params:
    :return:
    """
    pid = params[0]
    scn_id = params[1]
    db_info_obj = params[2]
    scn_path = params[3]
    dem_file = params[4]
    output_dir = params[5]
    tmp_dir = params[6]
    spacecraft_str = params[7]
    sensor_str = params[8]
    final_ard_path = params[9]
    reproj_outputs = params[10]
    proj_wkt_file = params[11]
    projabbv = params[12]
    use_roi = params[13]
    intersect_vec_file = params[14]
    intersect_vec_lyr = params[15]
    subset_vec_file = params[16]
    subset_vec_lyr = params[17]
    mask_outputs = params[18]
    mask_vec_file = params[19]
    mask_vec_lyr = params[20]

    edd_utils = eodatadown.eodatadownutils.EODataDownUtils()
    input_mtl = edd_utils.findFirstFile(scn_path, "*MTL.txt")

    start_date = datetime.datetime.now()
    eodatadown.eodatadownrunarcsi.run_arcsi_landsat(input_mtl, dem_file, output_dir, tmp_dir, spacecraft_str,
                                                    sensor_str, reproj_outputs, proj_wkt_file, projabbv)

    logger.debug("Move final ARD files to specified location.")
    # Move ARD files to be kept.
    valid_output = eodatadown.eodatadownrunarcsi.move_arcsi_stdsref_products(output_dir, final_ard_path, use_roi,
                                                                             intersect_vec_file, intersect_vec_lyr,
                                                                             subset_vec_file, subset_vec_lyr,
                                                                             mask_outputs, mask_vec_file, mask_vec_lyr,
                                                                             tmp_dir)
    # Remove Remaining files.
    shutil.rmtree(output_dir)
    shutil.rmtree(tmp_dir)
    logger.debug("Moved final ARD files to specified location.")
    end_date = datetime.datetime.now()

    if valid_output:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID == scn_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + scn_id)
        query_result.ARDProduct = True
        query_result.ARDProduct_Start_Date = start_date
        query_result.ARDProduct_End_Date = end_date
        query_result.ARDProduct_Path = final_ard_path
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database - scene valid.")
    else:
        logger.debug("Scene is not valid (e.g., too much cloud cover).")
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID == scn_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + scn_id)
        query_result.Invalid = True
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database - scene not valid.")


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

        self.use_roi = False
        self.intersect_vec_file = ''
        self.intersect_vec_lyr = ''
        self.subset_vec_file = ''
        self.subset_vec_lyr = ''
        self.mask_outputs = False
        self.mask_vec_file = ''
        self.mask_vec_lyr = ''
        self.std_vis_img_stch = None
        self.monthsOfInterest = None

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
            self.use_roi = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "roi"]):
                self.use_roi = True
                self.intersect_vec_file = json_parse_helper.getStrValue(config_data,
                                                                        ["eodatadown", "sensor", "ardparams", "roi",
                                                                         "intersect", "vec_file"])
                self.intersect_vec_lyr = json_parse_helper.getStrValue(config_data,
                                                                       ["eodatadown", "sensor", "ardparams", "roi",
                                                                        "intersect", "vec_layer"])
                self.subset_vec_file = json_parse_helper.getStrValue(config_data,
                                                                     ["eodatadown", "sensor", "ardparams", "roi",
                                                                      "subset", "vec_file"])
                self.subset_vec_lyr = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "ardparams", "roi",
                                                                     "subset", "vec_layer"])
                self.mask_outputs = False
                if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "roi", "mask"]):
                    self.mask_vec_file = json_parse_helper.getStrValue(config_data,
                                                                       ["eodatadown", "sensor", "ardparams", "roi",
                                                                        "mask", "vec_file"])
                    self.mask_vec_lyr = json_parse_helper.getStrValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "roi",
                                                                       "mask", "vec_layer"])

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "visual"]):
                if json_parse_helper.doesPathExist(config_data,
                                                   ["eodatadown", "sensor", "ardparams", "visual", "stretch_file"]):
                    self.std_vis_img_stch = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor",
                                                                                        "ardparams", "visual",
                                                                                        "stretch_file"])

            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths"]):
                self.parse_output_paths_config(config_data["eodatadown"]["sensor"]["paths"])
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

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "months"]):
                self.monthsOfInterest = json_parse_helper.getListValue(config_data,
                                                                       ["eodatadown", "sensor", "download", "months"])

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

            self.scn_intersect = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "validity"]):
                logger.debug("Find scene validity params from config file")
                if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "validity", "scn_intersect"]):
                    self.scn_intersect_vec_file = json_parse_helper.getStrValue(config_data,
                                                                                ["eodatadown", "sensor", "validity",
                                                                                 "scn_intersect", "vec_file"])
                    self.scn_intersect_vec_lyr = json_parse_helper.getStrValue(config_data,
                                                                               ["eodatadown", "sensor", "validity",
                                                                                "scn_intersect", "vec_lyr"])
                    self.scn_intersect = True
                logger.debug("Found scene validity params from config file")

            logger.debug("Find Google Account params from config file")
            self.goog_proj_name = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "googleinfo", "projectname"])
            self.goog_key_json = json_parse_helper.getStrValue(config_data,
                                                             ["eodatadown", "sensor", "googleinfo", "googlejsonkey"])

            self.goog_down_meth = "PYAPI"
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "googleinfo", "downloadtool"]):
                self.goog_down_meth = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "googleinfo", "downloadtool"],
                                                                    ["PYAPI", "GSUTIL", "GSUTIL_MULTI"])
            logger.debug("Found Google Account params from config file")

            logger.debug("Find the plugins params")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "plugins"]):
                self.parse_plugins_config(config_data["eodatadown"]["sensor"]["plugins"])
            logger.debug("Found the plugins params")

    def init_sensor_db(self, drop_tables=True):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        if drop_tables:
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Find duplicate records for the scene_id: "+scn_id)
        query_rtn = ses.query(EDDLandsatGoogle.PID, EDDLandsatGoogle.Scene_ID, EDDLandsatGoogle.Product_ID).\
            filter(EDDLandsatGoogle.Scene_ID == scn_id).all()
        process_dates = list()
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Find the start date for query - if table is empty then using config date "
                     "otherwise date of last acquired image.")
        query_date = self.startDate

        if (not check_from_start) and (ses.query(EDDLandsatGoogle).first() is not None):
            query_date = ses.query(EDDLandsatGoogle).order_by(
                EDDLandsatGoogle.Date_Acquired.desc()).first().Date_Acquired
        logger.info("Query with start at date: " + str(query_date))

        # Get the next PID value to ensure increment
        c_max_pid = ses.query(func.max(EDDLandsatGoogle.PID).label("max_pid")).one().max_pid
        if c_max_pid is None:
            n_max_pid = 0
        else:
            n_max_pid = c_max_pid + 1

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

        wrs2_filter = ''
        first = True
        for wrs2 in self.wrs2RowPaths:
            sgn_wrs2_filter = "(wrs_path = {} AND wrs_row = {})".format(str(wrs2['path']),  str(wrs2['row']))
            if first:
                wrs2_filter = sgn_wrs2_filter
                first = False
            else:
                wrs2_filter = "{} OR {}".format(wrs2_filter, sgn_wrs2_filter)
        wrs2_filter = "({})".format(wrs2_filter)

        logger.info("Finding scenes for {}".format(wrs2_filter))

        month_filter = ''
        first = True
        if self.monthsOfInterest is not None:
            for curr_month in self.monthsOfInterest:
                sgn_month_filter = "(EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', date_acquired)) = {})".format(curr_month)
                if first:
                    month_filter = sgn_month_filter
                    first = False
                else:
                    month_filter = "{} OR {}".format(month_filter, sgn_month_filter)
            if month_filter != '':
                logger.info("Finding scenes for with month filter {}".format(month_filter))
                month_filter = "({})".format(month_filter)

        goog_filter = goog_filter_date + " AND " + goog_filter_cloud + " AND " + \
                      goog_filter_spacecraft + " AND " + goog_filter_sensor + " AND " + \
                      goog_filter_collection

        # If using month filter
        if self.monthsOfInterest is not None:
            goog_filter = "{} AND {}".format(goog_filter, month_filter)

        # Create final query
        goog_query = "SELECT " + goog_fields + " FROM " + goog_db_str + " WHERE " + goog_filter + \
                     " AND " + wrs2_filter
        logger.debug("Query: '{}'".format(goog_query))
        client = bigquery.Client()
        query_results = client.query(goog_query)
        logger.debug("Performed google query")

        logger.debug("Process google query result and add to local database")
        new_scns_avail = False
        if query_results.result():
            db_records = list()
            for row in query_results.result():
                query_rtn = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Scene_ID == row.scene_id).all()
                if len(query_rtn) == 0:
                    logger.debug("SceneID: " + row.scene_id + "\tProduct_ID: " + row.product_id)
                    sensing_time_tmp = row.sensing_time.replace('Z', '')[:-1]
                    db_records.append(
                        EDDLandsatGoogle(PID=n_max_pid, Scene_ID=row.scene_id, Product_ID=row.product_id,
                                         Spacecraft_ID=row.spacecraft_id,
                                         Sensor_ID=row.sensor_id,
                                         Date_Acquired=datetime.datetime.strptime(row.date_acquired,
                                                                                  "%Y-%m-%d").date(),
                                         Collection_Number=row.collection_number,
                                         Collection_Category=row.collection_category,
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
                    n_max_pid = n_max_pid + 1
            if len(db_records) > 0:
                ses.add_all(db_records)
                ses.commit()
                new_scns_avail = True
        logger.debug("Processed google query result and added to local database")
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

    def rm_scns_intersect(self, all_scns=False):
        """
        A function which checks whether the bounding box for the scene intersects with a specified
        vector layer. If the scene does not intersect then it is deleted from the database. By default
        this is only testing the scenes which have not been downloaded.

        :param all_scns: If True all the scenes in the database will be tested otherwise only the
                         scenes which have not been downloaded will be tested.

        """
        if self.scn_intersect:
            import rsgislib.vectorutils
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scenes which need downloading.")

            if all_scns:
                scns = ses.query(EDDLandsatGoogle).order_by(EDDLandsatGoogle.Date_Acquired.asc()).all()
            else:
                scns = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == False).order_by(
                                                                  EDDLandsatGoogle.Date_Acquired.asc()).all()

            if scns is not None:
                eodd_vec_utils = eodatadown.eodatadownutils.EODDVectorUtils()
                vec_idx, geom_lst = eodd_vec_utils.create_rtree_index(self.scn_intersect_vec_file, self.scn_intersect_vec_lyr)

                for scn in scns:
                    logger.debug("Check Scene '{}' to check for intersection".format(scn.PID))
                    rsgis_utils = rsgislib.RSGISPyUtils()
                    north_lat = scn.North_Lat
                    south_lat = scn.South_Lat
                    east_lon = scn.East_Lon
                    west_lon = scn.West_Lon
                    # (xMin, xMax, yMin, yMax)
                    scn_bbox = [west_lon, east_lon, south_lat, north_lat]

                    intersect_vec_epsg = rsgis_utils.getProjEPSGFromVec(self.scn_intersect_vec_file, self.scn_intersect_vec_lyr)
                    if intersect_vec_epsg != 4326:
                        scn_bbox = rsgis_utils.reprojBBOX_epsg(scn_bbox, 4326, intersect_vec_epsg)

                    has_scn_intersect = eodd_vec_utils.bboxIntersectsIndex(vec_idx, geom_lst, scn_bbox)
                    if not has_scn_intersect:
                        logger.info("Removing scene {} from Landsat as it does not intersect.".format(scn.PID))
                        ses.query(EDDLandsatGoogle.PID).filter(EDDLandsatGoogle.PID == scn.PID).delete()
                        ses.commit()
            ses.close()

    def get_scnlist_all(self):
        """
        A function which returns a list of the unique IDs for all the scenes within the database.

        :return: list of integers
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).order_by(EDDLandsatGoogle.Date_Acquired.asc()).all()
        scns = list()
        if query_result is not None:
            for record in query_result:
                scns.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns

    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == False).order_by(
                                                          EDDLandsatGoogle.Date_Acquired.asc()).all()

        scns2dwnld = list()
        if query_result is not None:
            for record in query_result:
                scns2dwnld.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dwnld

    def has_scn_download(self, unq_id):
        """
        A function which checks whether an individual scene has been downloaded.
        :param unq_id: the unique ID of the scene.
        :return: boolean (True for downloaded; False for not downloaded)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.Downloaded

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

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
                dwnld_out_dirname = "{}_{}".format(url_path_parts[-1], unq_id)
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, dwnld_out_dirname)
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)

                logger.debug("Get the storage bucket and blob objects.")
                bucket_obj = storage_client.get_bucket(bucket_name)
                bucket_blobs = bucket_obj.list_blobs(prefix=bucket_prefix)
                scn_dwnlds_filelst = list()
                for blob in bucket_blobs:
                    if "$folder$" in blob.name:
                        continue
                    scnfilename = blob.name.replace(bucket_prefix + "/", "")
                    dwnld_file = os.path.join(scn_lcl_dwnld_path, scnfilename)
                    dwnld_dirpath = os.path.split(dwnld_file)[0]
                    if (not os.path.exists(dwnld_dirpath)) and (self.goog_down_meth == "PYAPI"):
                        os.makedirs(dwnld_dirpath, exist_ok=True)
                    scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})

                _download_scn_goog([record.PID, record.Scene_ID, self.db_info_obj, self.goog_key_json,
                                    self.goog_proj_name, bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path,
                                    record.Remote_URL, self.goog_down_meth])
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == False).all()

        dwnld_params = list()
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
                dwnld_out_dirname = "{}_{}".format(url_path_parts[-1], record.PID)
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, dwnld_out_dirname)
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)

                logger.debug("Get the storage bucket and blob objects.")
                bucket_obj = storage_client.get_bucket(bucket_name)
                bucket_blobs = bucket_obj.list_blobs(prefix=bucket_prefix)
                scn_dwnlds_filelst = list()
                for blob in bucket_blobs:
                    if "$folder$" in blob.name:
                        continue
                    scnfilename = blob.name.replace(bucket_prefix+"/", "")
                    dwnld_file = os.path.join(scn_lcl_dwnld_path, scnfilename)
                    dwnld_dirpath = os.path.split(dwnld_file)[0]
                    if (not os.path.exists(dwnld_dirpath)) and (self.goog_down_meth == "PYAPI"):
                        os.makedirs(dwnld_dirpath, exist_ok=True)
                    scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})

                dwnld_params.append([record.PID, record.Scene_ID, self.db_info_obj, self.goog_key_json,
                                     self.goog_proj_name, bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path,
                                     record.Remote_URL, self.goog_down_meth])
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False,
                                                          EDDLandsatGoogle.Invalid == False).order_by(
                                                          EDDLandsatGoogle.Date_Acquired.asc()).all()

        scns2ard = list()
        if query_result is not None:
            for record in query_result:
                scns2ard.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2ard

    def has_scn_con2ard(self, unq_id):
        """
        A function which checks whether a scene has been converted to an ARD product.
        :param unq_id: the unique ID of the scene of interest.
        :return: boolean (True: has been converted. False: Has not been converted)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return (query_result.ARDProduct == True) and (query_result.Invalid == False)

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id,
                                                          EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False).one_or_none()
        ses.close()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        if query_result is not None:
            record = query_result
            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(work_ard_path):
                os.mkdir(work_ard_path)

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            logger.debug("Create info for running ARD analysis for scene: " + record.Scene_ID)
            final_ard_scn_path = os.path.join(self.ardFinalPath, "{}_{}".format(record.Product_ID, record.PID))
            if not os.path.exists(final_ard_scn_path):
                os.mkdir(final_ard_scn_path)

            work_ard_scn_path = os.path.join(work_ard_path, "{}_{}".format(record.Product_ID, record.PID))
            if not os.path.exists(work_ard_scn_path):
                os.mkdir(work_ard_scn_path)

            tmp_ard_scn_path = os.path.join(tmp_ard_path, "{}_{}".format(record.Product_ID, record.PID))
            if not os.path.exists(tmp_ard_scn_path):
                os.mkdir(tmp_ard_scn_path)

            if self.ardProjDefined:
                proj_wkt_file = os.path.join(work_ard_scn_path, record.Product_ID+"_wkt.wkt")
                rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

            _process_to_ard([record.PID, record.Scene_ID, self.db_info_obj, record.Download_Path, self.demFile,
                             work_ard_scn_path, tmp_ard_scn_path, record.Spacecraft_ID, record.Sensor_ID,
                             final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv, self.use_roi,
                             self.intersect_vec_file, self.intersect_vec_lyr, self.subset_vec_file, self.subset_vec_lyr,
                             self.mask_outputs, self.mask_vec_file, self.mask_vec_lyr])
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True,
                                                          EDDLandsatGoogle.ARDProduct == False,
                                                          EDDLandsatGoogle.Invalid == False).all()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        ard_params = list()
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
                logger.debug("Create info for running ARD analysis for scene: {}".format(record.Product_ID))
                final_ard_scn_path = os.path.join(self.ardFinalPath, "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                work_ard_scn_path = os.path.join(work_ard_path, "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                if self.ardProjDefined:
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Product_ID+"_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                ard_params.append([record.PID, record.Scene_ID, self.db_info_obj, record.Download_Path, self.demFile,
                                   work_ard_scn_path, tmp_ard_scn_path, record.Spacecraft_ID, record.Sensor_ID,
                                   final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv, self.use_roi,
                                   self.intersect_vec_file, self.intersect_vec_lyr, self.subset_vec_file,
                                   self.subset_vec_lyr, self.mask_outputs, self.mask_vec_file, self.mask_vec_lyr])
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

    def get_scnlist_datacube(self, loaded=False):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.ARDProduct == True,
                                                          EDDLandsatGoogle.DCLoaded == loaded).order_by(
                                                          EDDLandsatGoogle.Date_Acquired.asc()).all()
        scns2dcload = list()
        if query_result is not None:
            for record in query_result:
                scns2dcload.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dcload

    def has_scn_datacube(self, unq_id):
        """
        A function to find whether a scene has been loaded in the DataCube.
        :param unq_id: the unique ID of the scene.
        :return: boolean (True: Loaded in DataCube. False: Not loaded in DataCube)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.DCLoaded

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

        datacube_cmd_path = 'datacube'
        datacube_cmd_path_env_value = os.getenv('DATACUBE_CMD_PATH', None)
        if datacube_cmd_path_env_value is not None:
            datacube_cmd_path = datacube_cmd_path_env_value

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.ARDProduct == True,
                                                          EDDLandsatGoogle.DCLoaded == False).all()

        if query_result is not None:
            logger.debug("Create the yaml files and load data into the datacube.")
            for record in query_result:
                start_date = datetime.datetime.now()
                scn_id = str(str(uuid.uuid5(uuid.NAMESPACE_URL, record.ARDProduct_Path)))
                print("{}: {}".format(record.Scene_ID, scn_id))
                img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*vmsk_rad_srefdem_stdsref.tif')
                vmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_valid.tif')
                cmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_clouds.tif')
                yaml_file = os.path.splitext(img_file)[0]+"_yaml.yaml"
                epsg_code = rsgis_utils.getEPSGCode(img_file)
                lcl_proj_bbox = rsgis_utils.getImageBBOX(img_file)

                image_lyrs = dict()
                if record.Spacecraft_ID.upper() == "LANDSAT_8":
                    image_lyrs['coastal'] = {'layer': 1, 'path': img_file}
                    image_lyrs['blue'] = {'layer': 2, 'path': img_file}
                    image_lyrs['green'] = {'layer': 3, 'path': img_file}
                    image_lyrs['red'] = {'layer': 4, 'path': img_file}
                    image_lyrs['nir'] = {'layer': 5, 'path': img_file}
                    image_lyrs['swir1'] = {'layer': 6, 'path': img_file}
                    image_lyrs['swir2'] = {'layer': 7, 'path': img_file}
                    image_lyrs['fmask'] = {'layer': 1, 'path': cmsk_img_file}
                    image_lyrs['vmask'] = {'layer': 1, 'path': vmsk_img_file}
                else:
                    image_lyrs['blue'] = {'layer': 1, 'path': img_file}
                    image_lyrs['green'] = {'layer': 2, 'path': img_file}
                    image_lyrs['red'] = {'layer': 3, 'path': img_file}
                    image_lyrs['nir'] = {'layer': 4, 'path': img_file}
                    image_lyrs['swir1'] = {'layer': 5, 'path': img_file}
                    image_lyrs['swir2'] = {'layer': 6, 'path': img_file}
                    image_lyrs['fmask'] = {'layer': 1, 'path': cmsk_img_file}
                    image_lyrs['vmask'] = {'layer': 1, 'path': vmsk_img_file}

                scn_info = {
                    'id': scn_id,
                    'processing_level': 'LEVEL_2',
                    'product_type': 'ARCSI_SREF',
                    'creation_dt': record.ARDProduct_End_Date.strftime("%Y-%m-%d %H:%M:%S"),
                    'label': record.Scene_ID,
                    'platform': {'code': record.Spacecraft_ID.upper()},
                    'instrument': {'name': record.Sensor_ID.upper()},
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
                    'format': {'name': 'GTIFF'},
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
                    'image': {'bands': image_lyrs},
                    'lineage': {'source_datasets': {}},
                }
                with open(yaml_file, 'w') as stream:
                    yaml.dump(scn_info, stream)

                cmd = "{0} dataset add {1}".format(datacube_cmd_path, yaml_file)
                try:
                    subprocess.call(cmd, shell=True)
                    # TODO Check that the dataset is really loaded - i.e., query datacube database
                    end_date = datetime.datetime.now()
                    record.DCLoaded_Start_Date = start_date
                    record.DCLoaded_End_Date = end_date
                    record.DCLoaded = True
                except Exception as e:
                    logger.debug("Failed to load scene: '{}'".format(cmd), exc_info=True)

        ses.commit()
        ses.close()
        logger.debug("Finished loading data into the datacube.")

    def get_scnlist_quicklook(self):
        """
        Get a list of all scenes which have not had a quicklook generated.

        :return: list of unique IDs
        """
        scns2quicklook = list()
        if self.calc_scn_quicklook():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scene.")
            query_result = ses.query(EDDLandsatGoogle).filter(
                sqlalchemy.or_(
                    EDDLandsatGoogle.ExtendedInfo.is_(None),
                    sqlalchemy.not_(EDDLandsatGoogle.ExtendedInfo.has_key('quicklook'))),
                EDDLandsatGoogle.Invalid == False,
                EDDLandsatGoogle.ARDProduct == True).order_by(
                            EDDLandsatGoogle.Date_Acquired.asc()).all()
            if query_result is not None:
                for record in query_result:
                    scns2quicklook.append(record.PID)
            ses.close()
            logger.debug("Closed the database session.")
        return scns2quicklook

    def has_scn_quicklook(self, unq_id):
        """
        Check whether the quicklook has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has quicklook. False = has not got a quicklook)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one()
        scn_json = query_result.ExtendedInfo
        ses.close()
        logger.debug("Closed the database session.")

        quicklook_calcd = False
        if scn_json is not None:
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            quicklook_calcd = json_parse_helper.doesPathExist(scn_json, ["quicklook"])
        return quicklook_calcd

    def scn2quicklook(self, unq_id):
        """
        Generate the quicklook image for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        if (self.quicklookPath is None) or (not os.path.exists(self.quicklookPath)):
            raise EODataDownException("The quicklook path does not exist or not provided, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
        if query_result is not None:
            if not query_result.ARDProduct:
                raise EODataDownException("Cannot create a quicklook as an ARD product has not been created.")
            if query_result.Invalid:
                raise EODataDownException("Cannot create a quicklook as image has been assigned as 'invalid'.")

            scn_json = query_result.ExtendedInfo
            if (scn_json is None) or (scn_json == ""):
                scn_json = dict()

            ard_img_path = query_result.ARDProduct_Path
            eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
            ard_img_file = eodd_utils.findFile(ard_img_path, '*vmsk_rad_srefdem_stdsref.tif')

            out_quicklook_path = os.path.join(self.quicklookPath,
                                              "{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(out_quicklook_path):
                os.mkdir(out_quicklook_path)

            tmp_quicklook_path = os.path.join(self.ardProdTmpPath,
                                              "quicklook_{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(tmp_quicklook_path):
                os.mkdir(tmp_quicklook_path)

            # NIR, SWIR, RED
            bands = '4,5,3'
            if query_result.Spacecraft_ID.upper() == 'LANDSAT_8'.upper():
                bands = '5,6,4'

            ard_img_basename = os.path.splitext(os.path.basename(ard_img_file))[0]
            
            quicklook_imgs = list()
            quicklook_imgs.append(os.path.join(out_quicklook_path, "{}_250px.jpg".format(ard_img_basename)))
            quicklook_imgs.append(os.path.join(out_quicklook_path, "{}_1000px.jpg".format(ard_img_basename)))

            import rsgislib.tools.visualisation
            rsgislib.tools.visualisation.createQuicklookImgs(ard_img_file, bands, outputImgs=quicklook_imgs,
                                                             output_img_sizes=[250, 1000],  scale_axis='auto',
                                                             img_stats_msk=None, img_msk_vals=1,
                                                             stretch_file=self.std_vis_img_stch,
                                                             tmp_dir=tmp_quicklook_path)

            if not ("quicklook" in scn_json):
                scn_json["quicklook"] = dict()

            scn_json["quicklook"]["quicklookpath"] = out_quicklook_path
            scn_json["quicklook"]["quicklookimgs"] = quicklook_imgs
            query_result.ExtendedInfo = scn_json
            flag_modified(query_result, "ExtendedInfo")
            ses.add(query_result)
            ses.commit()
        else:
            raise EODataDownException("Could not find input image with PID {}".format(unq_id))
        ses.close()
        logger.debug("Closed the database session.")

    def scns2quicklook_all_avail(self):
        """
        Generate the quicklook images for the scenes for which a quicklook image do not exist.

        """
        scn_lst = self.get_scnlist_quicklook()
        for scn in scn_lst:
            self.scn2quicklook(scn)

    def get_scnlist_tilecache(self):
        """
        Get a list of all scenes for which a tile cache has not been generated.

        :return: list of unique IDs
        """
        scns2tilecache = list()
        if self.calc_scn_tilecache():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scene.")
            query_result = ses.query(EDDLandsatGoogle).filter(
                sqlalchemy.or_(
                    EDDLandsatGoogle.ExtendedInfo.is_(None),
                    sqlalchemy.not_(EDDLandsatGoogle.ExtendedInfo.has_key('tilecache'))),
                EDDLandsatGoogle.Invalid == False,
                EDDLandsatGoogle.ARDProduct == True).order_by(
                            EDDLandsatGoogle.Date_Acquired.asc()).all()
            if query_result is not None:
                for record in query_result:
                    scns2tilecache.append(record.PID)
            ses.close()
            logger.debug("Closed the database session.")
        return scns2tilecache

    def has_scn_tilecache(self, unq_id):
        """
        Check whether a tile cache has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has tile cache. False = has not got a tile cache)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one()
        scn_json = query_result.ExtendedInfo
        ses.close()
        logger.debug("Closed the database session.")

        tile_cache_calcd = False
        if scn_json is not None:
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            tile_cache_calcd = json_parse_helper.doesPathExist(scn_json, ["tilecache"])
        return tile_cache_calcd

    def scn2tilecache(self, unq_id):
        """
        Generate the tile cache for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        if (self.tilecachePath is None) or (not os.path.exists(self.tilecachePath)):
            raise EODataDownException("The tilecache path does not exist or not provided, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
        if query_result is not None:
            if not query_result.ARDProduct:
                raise EODataDownException("Cannot create a tilecache as an ARD product has not been created.")
            if query_result.Invalid:
                raise EODataDownException("Cannot create a tilecache as image has been assigned as 'invalid'.")

            scn_json = query_result.ExtendedInfo
            if (scn_json is None) or (scn_json == ""):
                scn_json = dict()

            ard_img_path = query_result.ARDProduct_Path
            eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
            ard_img_file = eodd_utils.findFile(ard_img_path, '*vmsk_rad_srefdem_stdsref.tif')

            out_tilecache_dir = os.path.join(self.tilecachePath,
                                             "{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(out_tilecache_dir):
                os.mkdir(out_tilecache_dir)

            out_visual_gtiff = os.path.join(out_tilecache_dir,
                                            "{}_{}_vis.tif".format(query_result.Product_ID, query_result.PID))

            tmp_tilecache_path = os.path.join(self.ardProdTmpPath,
                                            "tilecache_{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(tmp_tilecache_path):
                os.mkdir(tmp_tilecache_path)

            # NIR, SWIR, RED
            bands = '4,5,3'
            if query_result.Spacecraft_ID.upper() == 'LANDSAT_8'.upper():
                bands = '5,6,4'

            import rsgislib.tools.visualisation
            rsgislib.tools.visualisation.createWebTilesVisGTIFFImg(ard_img_file, bands, out_tilecache_dir,
                                                                   out_visual_gtiff, zoomLevels='2-12',
                                                                   img_stats_msk=None, img_msk_vals=1,
                                                                   stretch_file=self.std_vis_img_stch,
                                                                   tmp_dir=tmp_tilecache_path, webview=True)

            if not ("tilecache" in scn_json):
                scn_json["tilecache"] = dict()
            scn_json["tilecache"]["tilecachepath"] = out_tilecache_dir
            scn_json["tilecache"]["visgtiff"] = out_visual_gtiff
            query_result.ExtendedInfo = scn_json
            flag_modified(query_result, "ExtendedInfo")
            ses.add(query_result)
            ses.commit()
        else:
            raise EODataDownException("Could not find input image with PID {}".format(unq_id))
        ses.close()
        logger.debug("Closed the database session.")
        shutil.rmtree(tmp_tilecache_path)

    def scns2tilecache_all_avail(self):
        """
        Generate the tile cache for the scenes for which a tile cache does not exist.
        """
        scn_lst = self.get_scnlist_tilecache()
        for scn in scn_lst:
            self.scn2tilecache(scn)

    def get_scn_record(self, unq_id):
        """
        A function which queries the database using the unique ID of a scene returning the record
        :param unq_id:
        :return: Returns the database record object
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

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
                    "There was more than 1 scene which has been found - something has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return scn_record

    def get_scn_obs_date(self, unq_id):
        """
        A function which returns a datetime object for the observation date/time of a scene.

        :param unq_id: the unique id (PID) of the scene of interest.
        :return: a datetime object.

        """
        import copy
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
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
                        "There was more than 1 scene which has been found - something has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return copy.copy(scn_record.Sensing_Time)

    def get_scnlist_usr_analysis(self):
        """
        Get a list of all scenes for which user analysis needs to be undertaken.

        :return: list of unique IDs
        """
        scns2runusranalysis = list()
        if self.calc_scn_usr_analysis():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()

            for plugin_info in self.analysis_plugins:
                plugin_path = os.path.abspath(plugin_info["path"])
                plugin_module_name = plugin_info["module"]
                plugin_cls_name = plugin_info["class"]
                # Check if plugin path input is already in system path.
                already_in_path = False
                for c_path in sys.path:
                    c_path = os.path.abspath(c_path)
                    if c_path == plugin_path:
                        already_in_path = True
                        break
                # Add plugin path to system path
                if not already_in_path:
                    sys.path.insert(0, plugin_path)
                    logger.debug("Add plugin path ('{}') to the system path.".format(plugin_path))
                # Try to import the module.
                logger.debug("Try to import the plugin module: '{}'".format(plugin_module_name))
                plugin_mod_inst = importlib.import_module(plugin_module_name)
                logger.debug("Imported the plugin module: '{}'".format(plugin_module_name))
                if plugin_mod_inst is None:
                    raise Exception("Could not load the module: '{}'".format(plugin_module_name))
                # Try to make instance of class.
                logger.debug("Try to create instance of class: '{}'".format(plugin_cls_name))
                plugin_cls_inst = getattr(plugin_mod_inst, plugin_cls_name)()
                logger.debug("Created instance of class: '{}'".format(plugin_cls_name))
                if plugin_cls_inst is None:
                    raise Exception("Could not create instance of '{}'".format(plugin_cls_name))

                plugin_key = plugin_cls_inst.get_ext_info_key()
                query_result = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.or_(
                                EDDLandsatGoogle.ExtendedInfo.is_(None),
                                sqlalchemy.not_(EDDLandsatGoogle.ExtendedInfo.has_key(plugin_key))),
                        EDDLandsatGoogle.Invalid == False,
                        EDDLandsatGoogle.ARDProduct == True).order_by(
                        EDDLandsatGoogle.Date_Acquired.asc()).all()

                if query_result is not None:
                    for record in query_result:
                        if record.PID not in scns2runusranalysis:
                            scns2runusranalysis.append(record.PID)

            ses.close()
            logger.debug("Closed the database session.")
        return scns2runusranalysis

    def has_scn_usr_analysis(self, unq_id):
        usr_plugins_calcd = False
        logger.debug("Going to test whether there are plugins.")
        if self.calc_scn_usr_analysis():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scene.")
            query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
            if query_result is None:
                raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
            scn_json = query_result.ExtendedInfo
            ses.close()
            logger.debug("Closed the database session.")

            if scn_json is not None:
                json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
                usr_plugins_calcd = True
                for plugin_info in self.analysis_plugins:
                    plugin_path = os.path.abspath(plugin_info["path"])
                    plugin_module_name = plugin_info["module"]
                    plugin_cls_name = plugin_info["class"]
                    logger.debug("Using plugin '{}' from '{}'.".format(plugin_cls_name, plugin_module_name))
                    # Check if plugin path input is already in system path.
                    already_in_path = False
                    for c_path in sys.path:
                        c_path = os.path.abspath(c_path)
                        if c_path == plugin_path:
                            already_in_path = True
                            break
                    # Add plugin path to system path
                    if not already_in_path:
                        sys.path.insert(0, plugin_path)
                        logger.debug("Add plugin path ('{}') to the system path.".format(plugin_path))
                    # Try to import the module.
                    logger.debug("Try to import the plugin module: '{}'".format(plugin_module_name))
                    plugin_mod_inst = importlib.import_module(plugin_module_name)
                    logger.debug("Imported the plugin module: '{}'".format(plugin_module_name))
                    if plugin_mod_inst is None:
                        raise Exception("Could not load the module: '{}'".format(plugin_module_name))
                    # Try to make instance of class.
                    logger.debug("Try to create instance of class: '{}'".format(plugin_cls_name))
                    plugin_cls_inst = getattr(plugin_mod_inst, plugin_cls_name)()
                    logger.debug("Created instance of class: '{}'".format(plugin_cls_name))
                    if plugin_cls_inst is None:
                        raise Exception("Could not create instance of '{}'".format(plugin_cls_name))

                    plugin_key = plugin_cls_inst.get_ext_info_key()
                    plugin_completed = json_parse_helper.doesPathExist(scn_json, [plugin_key])
                    if not plugin_completed:
                        usr_plugins_calcd = False
                        break

        return usr_plugins_calcd

    def run_usr_analysis(self, unq_id):
        if self.calc_scn_usr_analysis():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scene.")
            scn_db_obj = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
            if scn_db_obj is None:
                raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
            ses.close()
            logger.debug("Closed the database session.")

            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            for plugin_info in self.analysis_plugins:
                plugin_path = os.path.abspath(plugin_info["path"])
                plugin_module_name = plugin_info["module"]
                plugin_cls_name = plugin_info["class"]
                logger.debug("Using plugin '{}' from '{}'.".format(plugin_cls_name, plugin_module_name))

                # Check if plugin path input is already in system path.
                already_in_path = False
                for c_path in sys.path:
                    c_path = os.path.abspath(c_path)
                    if c_path == plugin_path:
                        already_in_path = True
                        break

                # Add plugin path to system path
                if not already_in_path:
                    sys.path.insert(0, plugin_path)
                    logger.debug("Add plugin path ('{}') to the system path.".format(plugin_path))

                # Try to import the module.
                logger.debug("Try to import the plugin module: '{}'".format(plugin_module_name))
                plugin_mod_inst = importlib.import_module(plugin_module_name)
                logger.debug("Imported the plugin module: '{}'".format(plugin_module_name))
                if plugin_mod_inst is None:
                    raise Exception("Could not load the module: '{}'".format(plugin_module_name))

                # Try to make instance of class.
                logger.debug("Try to create instance of class: '{}'".format(plugin_cls_name))
                plugin_cls_inst = getattr(plugin_mod_inst, plugin_cls_name)()
                logger.debug("Created instance of class: '{}'".format(plugin_cls_name))
                if plugin_cls_inst is None:
                    raise Exception("Could not create instance of '{}'".format(plugin_cls_name))

                # Try to read any plugin parameters to be passed to the plugin when instantiated.
                if "params" in plugin_info:
                    plugin_cls_inst.set_users_param(plugin_info["params"])
                    logger.debug("Read plugin params and passed to plugin.")

                plugin_key = plugin_cls_inst.get_ext_info_key()
                scn_json = scn_db_obj.ExtendedInfo
                if scn_json is not None:
                    plugin_completed = json_parse_helper.doesPathExist(scn_json, [plugin_key])
                else:
                    plugin_completed = False

                if not plugin_completed:
                    plg_success, out_dict = plugin_cls_inst.perform_analysis(scn_db_obj, self)
                    if plg_success:
                        logger.debug("The plugin analysis has been completed - SUCCESSFULLY.")
                        if scn_json is None:
                            logger.debug("No existing extended info so creating the dict.")
                            scn_json = dict()

                        if out_dict is None:
                            logger.debug("No output dict from the plugin so just setting as True to indicate "
                                         "the plugin has successfully executed.")
                            scn_json[plugin_key] = True
                        else:
                            logger.debug("An output dict from the plugin was provided so adding to extended info.")
                            scn_json[plugin_key] = out_dict

                        logger.debug("Creating Database Engine and Session.")
                        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                        ses = session_sqlalc()
                        logger.debug("Updating the extended info field in the database.")
                        scn_db_obj.ExtendedInfo = scn_json
                        flag_modified(scn_db_obj, "ExtendedInfo")
                        ses.add(scn_db_obj)
                        ses.commit()
                        logger.debug("Updated the extended info field in the database.")
                        ses.close()
                        logger.debug("Closed the database session.")
                    else:
                        logger.debug("The plugin analysis has not been completed - UNSUCCESSFUL.")
                else:
                    logger.debug("The plugin '{}' from '{}' has already been run so will not be run again".format(plugin_cls_name, plugin_module_name))

    def run_usr_analysis_all_avail(self, n_cores):
        scn_lst = self.get_scnlist_usr_analysis()
        for scn in scn_lst:
            self.run_usr_analysis(scn)

    def reset_usr_analysis(self, plgin_lst=None, scn_pid=None):
        """
        Reset the user analysis plugins within the database.

        :param plgin_lst: A list of plugins to be reset. If None (default) then all reset.
        :param scn_pid: Optionally specify the a scene PID, if provided then only that scene will be reset.
                        If None then all the scenes will be reset.

        """
        if self.calc_scn_usr_analysis():
            if plgin_lst is None:
                logger.debug(
                    "A list of plugins to reset has not been provided so populating that list with all plugins.")
                plgin_lst = self.get_usr_analysis_keys()
            logger.debug("There are {} plugins to reset".format(len(plgin_lst)))

            if len(plgin_lst) > 0:
                logger.debug("Creating Database Engine and Session.")
                db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                ses = session_sqlalc()

                if scn_pid is None:
                    logger.debug("No scene PID has been provided so resetting all the scenes.")
                    query_result = ses.query(EDDLandsatGoogle).all()
                    if query_result is not None:
                        for record in query_result:
                            out_ext_info = dict()
                            in_ext_info = record.ExtendedInfo
                            if in_ext_info is not None:
                                for key in in_ext_info:
                                    if key not in plgin_lst:
                                        out_ext_info[key] = in_ext_info[key]
                                # If out dict is empty then set to None.
                                if not out_ext_info:
                                    out_ext_info = sqlalchemy.sql.null()
                                record.ExtendedInfo = out_ext_info
                                flag_modified(record, "ExtendedInfo")
                                ses.add(record)
                                ses.commit()
                else:
                    logger.debug("Scene PID {} has been provided so resetting.".format(scn_pid))
                    scn_db_obj = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == scn_pid).one_or_none()
                    if scn_db_obj is None:
                        raise EODataDownException("Scene ('{}') could not be found in database".format(scn_pid))
                    out_ext_info = dict()
                    in_ext_info = scn_db_obj.ExtendedInfo
                    if in_ext_info is not None:
                        for key in in_ext_info:
                            if key not in plgin_lst:
                                out_ext_info[key] = in_ext_info[key]
                        # If out dict is empty then set to None.
                        if not out_ext_info:
                            out_ext_info = sqlalchemy.sql.null()
                        scn_db_obj.ExtendedInfo = out_ext_info
                        flag_modified(scn_db_obj, "ExtendedInfo")
                        ses.add(scn_db_obj)
                        ses.commit()
                ses.close()

    def is_scn_invalid(self, unq_id):
        """
        A function which tests whether a scene has been defined as invalid.

        :param unq_id: the unique PID for the scene to test.
        :return: True: The scene is invalid. False: the Scene is valid.

        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
        if query_result is None:
            raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
        invalid = query_result.Invalid
        ses.close()
        logger.debug("Closed the database session.")
        return invalid

    def get_scn_unq_name(self, unq_id):
        """
        A function which returns a name which will be unique for the specified scene.

        :param unq_id: the unique PID for the scene.
        :return: string with a unique name.

        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.PID == unq_id).one_or_none()
        if query_result is None:
            raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
        unq_name = "{}_{}".format(query_result.Product_ID, query_result.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return unq_name

    def get_scn_unq_name_record(self, scn_record):
        """
        A function which returns a name which will be unique using the scene record object passed to the function.

        :param scn_record: the database dict like object representing the scene.
        :return: string with a unique name.

        """
        unq_name = "{}_{}".format(scn_record.Product_ID, scn_record.PID)
        return unq_name

    def find_unique_platforms(self):
        """
        A function which returns a list of unique platforms within the database (e.g., Landsat 5, Landsat 8).
        :return: list of strings.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        platforms = ses.query(EDDLandsatGoogle.Spacecraft_ID).group_by(EDDLandsatGoogle.Spacecraft_ID)
        ses.close()
        return platforms

    def query_scn_records_date_count(self, start_date, end_date, valid=True, cloud_thres=None):
        """
        A function which queries the database to find scenes within a specified date range
        and returns the number of records available.

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :param cloud_thres: threshold for cloud cover. If None, then ignored.
        :return: count of records available
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if cloud_thres is not None:
            if valid:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Invalid == False,
                                                            EDDLandsatGoogle.ARDProduct == True,
                                                            EDDLandsatGoogle.Cloud_Cover <= cloud_thres).count()
            else:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Cloud_Cover <= cloud_thres).count()
        else:
            if valid:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Invalid == False,
                                                            EDDLandsatGoogle.ARDProduct == True).count()
            else:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date).count()
        ses.close()
        return n_rows

    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True, cloud_thres=None):
        """
        A function which queries the database to find scenes within a specified date range.
        The order of the records is descending (i.e., from current to historical)

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param start_rec: A parameter specifying the start record, for example for pagination.
        :param n_recs: A parameter specifying the number of records to be returned.
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :param cloud_thres: threshold for cloud cover. If None, then ignored.
        :return: list of database records
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if cloud_thres is not None:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
        else:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
        ses.close()
        scn_records = list()
        if (query_result is not None) and (len(query_result) > 0):
            for rec in query_result:
                scn_records.append(rec)
        else:
            logger.error("No scenes were found within this date range.")
            raise EODataDownException("No scenes were found within this date range.")
        return scn_records

    def query_scn_records_date_bbox_count(self, start_date, end_date, bbox, valid=True, cloud_thres=None):
        """
        A function which queries the database to find scenes within a specified date range
        and returns the number of records available.

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param bbox: Bounding box, with which scenes will intersect [West_Lon, East_Lon, South_Lat, North_Lat]
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :param cloud_thres: threshold for cloud cover. If None, then ignored.
        :return: count of records available
        """
        west_lon_idx = 0
        east_lon_idx = 1
        south_lat_idx = 2
        north_lat_idx = 3

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if cloud_thres is not None:
            if valid:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Invalid == False,
                                                            EDDLandsatGoogle.ARDProduct == True,
                                                            EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                            (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                            (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                            (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                            (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).count()
            else:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                            (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                            (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                            (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                            (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).count()
        else:
            if valid:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date,
                                                            EDDLandsatGoogle.Invalid == False,
                                                            EDDLandsatGoogle.ARDProduct == True).filter(
                                                            (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                            (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                            (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                            (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).count()
            else:
                n_rows = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                            EDDLandsatGoogle.Date_Acquired >= end_date).filter(
                                                            (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                            (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                            (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                            (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).count()
        ses.close()
        return n_rows

    def query_scn_records_date_bbox(self, start_date, end_date, bbox, start_rec=0, n_recs=0, valid=True, cloud_thres=None):
        """
        A function which queries the database to find scenes within a specified date range.
        The order of the records is descending (i.e., from current to historical)

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param bbox: Bounding box, with which scenes will intersect [West_Lon, East_Lon, South_Lat, North_Lat]
        :param start_rec: A parameter specifying the start record, for example for pagination.
        :param n_recs: A parameter specifying the number of records to be returned.
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :param cloud_thres: threshold for cloud cover. If None, then ignored.
        :return: list of database records
        """
        west_lon_idx = 0
        east_lon_idx = 1
        south_lat_idx = 2
        north_lat_idx = 3

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if cloud_thres is not None:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Cloud_Cover <= cloud_thres).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
        else:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date,
                                                                      EDDLandsatGoogle.Invalid == False,
                                                                      EDDLandsatGoogle.ARDProduct == True).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Date_Acquired <= start_date,
                                                                      EDDLandsatGoogle.Date_Acquired >= end_date).filter(
                                                                      (bbox[east_lon_idx] > EDDLandsatGoogle.West_Lon),
                                                                      (EDDLandsatGoogle.East_Lon > bbox[west_lon_idx]),
                                                                      (bbox[north_lat_idx] > EDDLandsatGoogle.South_Lat),
                                                                      (EDDLandsatGoogle.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDLandsatGoogle.Date_Acquired.desc()).all()
        ses.close()
        scn_records = list()
        if (query_result is not None) and (len(query_result) > 0):
            for rec in query_result:
                scn_records.append(rec)
        else:
            logger.error("No scenes were found within this date range.")
            raise EODataDownException("No scenes were found within this date range.")
        return scn_records


    def find_unique_scn_dates(self, start_date, end_date, valid=True, order_desc=True, platform=None):
        """
        A function which returns a list of unique dates on which acquisitions have occurred.
        :param start_date: A python datetime object specifying the start date (most recent date)
        :param end_date: A python datetime object specifying the end date (earliest date)
        :param valid: If True only valid observations are considered.
        :return: List of datetime.date objects.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        if platform is None:
            if valid:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                        EDDLandsatGoogle.Date_Acquired <= start_date,
                        EDDLandsatGoogle.Date_Acquired >= end_date,
                        EDDLandsatGoogle.Invalid == False).group_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                            EDDLandsatGoogle.Date_Acquired <= start_date,
                            EDDLandsatGoogle.Date_Acquired >= end_date,
                            EDDLandsatGoogle.Invalid == False).group_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                        EDDLandsatGoogle.Date_Acquired <= start_date,
                        EDDLandsatGoogle.Date_Acquired >= end_date).group_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                            EDDLandsatGoogle.Date_Acquired <= start_date,
                            EDDLandsatGoogle.Date_Acquired >= end_date).group_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).asc())
        else:
            if valid:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                        EDDLandsatGoogle.Date_Acquired <= start_date,
                        EDDLandsatGoogle.Date_Acquired >= end_date,
                        EDDLandsatGoogle.Invalid == False,
                        EDDLandsatGoogle.Spacecraft_ID == platform).group_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                            EDDLandsatGoogle.Date_Acquired <= start_date,
                            EDDLandsatGoogle.Date_Acquired >= end_date,
                            EDDLandsatGoogle.Invalid == False,
                            EDDLandsatGoogle.Spacecraft_ID == platform).group_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                        EDDLandsatGoogle.Date_Acquired <= start_date,
                        EDDLandsatGoogle.Date_Acquired >= end_date,
                        EDDLandsatGoogle.Spacecraft_ID == platform).group_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).filter(
                            EDDLandsatGoogle.Date_Acquired <= start_date,
                            EDDLandsatGoogle.Date_Acquired >= end_date,
                            EDDLandsatGoogle.Spacecraft_ID == platform).group_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date).asc())
        ses.close()
        return scn_dates

    def get_scns_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None):
        """
        A function to retrieve a list of scenes which have been acquired on a particular date.

        :param date_of_interest: a datetime.date object specifying the date of interest.
        :param valid: If True only valid observations are considered.
        :param ard_prod: If True only observations which have been converted to an ARD product are considered.
        :param platform: If None then all scenes, if value provided then it just be for that platform.
        :return: a list of sensor objects
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        if platform is None:
            if valid and ard_prod:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.Invalid == False, EDDLandsatGoogle.ARDProduct == True).all()
            elif valid:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.Invalid == False).all()
            elif ard_prod:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.ARDProduct == True).all()
            else:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest).all()
        else:
            if valid and ard_prod:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.Invalid == False, EDDLandsatGoogle.ARDProduct == True,
                        EDDLandsatGoogle.Spacecraft_ID == platform).all()
            elif valid:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.Invalid == False, EDDLandsatGoogle.Spacecraft_ID == platform).all()
            elif ard_prod:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.ARDProduct == True, EDDLandsatGoogle.Spacecraft_ID == platform).all()
            else:
                scns = ses.query(EDDLandsatGoogle).filter(
                        sqlalchemy.cast(EDDLandsatGoogle.Date_Acquired, sqlalchemy.Date) == date_of_interest,
                        EDDLandsatGoogle.Spacecraft_ID == platform).all()
        return scns

    def get_scn_pids_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None):
        """
        A function to retrieve a list of scene PIDs which have been acquired on a particular date.

        :param date_of_interest: a datetime.date object specifying the date of interest.
        :param valid: If True only valid observations are considered.
        :param ard_prod: If True only observations which have been converted to an ARD product are considered.
        :param platform: If None then all scenes, if value provided then it just be for that platform.
        :return: a list of PIDs (ints)
        """
        scns = self.get_scns_for_date(date_of_interest, valid, ard_prod, platform)
        scn_pids = list()
        for scn in scns:
            scn_pids.append(scn.PID)
        return scn_pids

    def create_scn_date_imgs(self, start_date, end_date, img_size, out_img_dir, img_format, vec_file, vec_lyr,
                             tmp_dir, order_desc=True):
        """
        A function which created stretched and formatted visualisation images by combining all the scenes
        for a particular date. It does that for each of the unique dates within the date range specified.

        :param start_date: A python datetime object specifying the start date (most recent date)
        :param end_date: A python datetime object specifying the end date (earliest date)
        :param img_size: The output image size in pixels
        :param out_img_dir: The output image directory
        :param img_format: the output image format (JPEG, PNG or GTIFF)
        :param vec_file: A vector file (polyline) which can be overlaid for context.
        :param vec_lyr: The layer in the vector file.
        :param tmp_dir: A temp directory for intermediate files.
        :return: dict with date (YYYYMMDD) as key with a dict of image info, including
                 an qkimage field for the generated image
        """
        out_img_ext = 'png'
        if img_format.upper() == 'PNG':
            out_img_ext = 'png'
        elif img_format.upper() == 'JPEG':
            out_img_ext = 'jpg'
        elif img_format.upper() == 'GTIFF':
            out_img_ext = 'tif'
        else:
            raise EODataDownException("The input image format ({}) was recognised".format(img_format))
        eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
        scn_dates = self.find_unique_scn_dates(start_date, end_date, valid=True, order_desc=order_desc)
        scn_qklks = dict()
        for scn_date in scn_dates:
            print("Processing {}:".format(scn_date[0].strftime('%Y-%m-%d')))
            scns = self.get_scns_for_date(scn_date[0])
            scn_files = []
            first = True
            spacecraft = ''
            for scn in scns:
                ard_file = eoddutils.findFile(scn.ARDProduct_Path, "*vmsk_rad_srefdem_stdsref.tif")
                print("\t{}: {} {} - {}".format(scn.PID, scn.Spacecraft_ID, scn.Scene_ID, ard_file))
                scn_files.append(ard_file)
                if first:
                    spacecraft = scn.Spacecraft_ID
                    first = False
                elif spacecraft.upper() != scn.Spacecraft_ID.upper():
                    raise Exception("The input images are from different sensors which cannot be mixed.")

            bands = '4,5,3'
            if spacecraft.upper() == 'LANDSAT_8'.upper():
                bands = '5,6,4'

            scn_date_str = scn_date[0].strftime('%Y%m%d')
            quicklook_img = os.path.join(out_img_dir, "ls_qklk_{}.{}".format(scn_date_str, out_img_ext))
            import rsgislib.tools.visualisation
            rsgislib.tools.visualisation.createQuicklookOverviewImgsVecOverlay(scn_files, bands, tmp_dir,
                                                                               vec_file, vec_lyr,
                                                                               outputImgs=quicklook_img,
                                                                               output_img_sizes=img_size,
                                                                               gdalformat=img_format,
                                                                               scale_axis='auto',
                                                                               stretch_file=self.std_vis_img_stch,
                                                                               overlay_clr=[255, 255, 255])
            scn_qklks[scn_date_str] = dict()
            scn_qklks[scn_date_str]['qkimage'] = quicklook_img
            scn_qklks[scn_date_str]['scn_date'] = scn_date[0]
        return scn_qklks

    def create_multi_scn_visual(self, scn_pids, out_imgs, out_img_sizes, out_extent_vec, out_extent_lyr,
                                gdal_format, tmp_dir):
        """

        :param scn_pids: A list of scene PIDs (scenes without an ARD image are ignored silently)
        :param out_imgs: A list of output image files
        :param out_img_sizes: A list of output image sizes.
        :param out_extent_vec: A vector file defining the output image extent (can be None)
        :param out_extent_lyr: A vector layer name for the layer defining the output image extent (can be None)
        :param gdal_format: The GDAL file format of the output images (e.g., GTIFF)
        :param tmp_dir: A directory for temporary files to be written to.
        :return: boolean. True: Completed. False: Failed to complete - invalid.
        """
        eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
        # Get the ARD images.
        ard_images = []
        first = True
        spacecraft = ''
        for pid in scn_pids:
            scn = self.get_scn_record(pid)
            ard_file = eoddutils.findFileNone(scn.ARDProduct_Path, "*vmsk_rad_srefdem_stdsref.tif")
            if ard_file is not None:
                ard_images.append(ard_file)
            if first:
                spacecraft = scn.Spacecraft_ID
                first = False
            elif spacecraft.upper() != scn.Spacecraft_ID.upper():
                raise Exception("The input images are from different sensors which cannot be mixed.")

        if len(ard_images) > 0:
            bands = '4,5,3'
            if spacecraft.upper() == 'LANDSAT_8'.upper():
                bands = '5,6,4'

            export_stretch_file = False
            strch_file = self.std_vis_img_stch
            if self.std_vis_img_stch is None:
                export_stretch_file = True
                strch_file_basename = eoddutils.get_file_basename(out_imgs[0], n_comps=3)
                strch_file_path = os.path.split(out_imgs[0])[0]
                strch_file = os.path.join(strch_file_path, "{}_srtch_stats.txt".format(strch_file_basename))

            import rsgislib.tools.visualisation
            rsgislib.tools.visualisation.createVisualOverviewImgsVecExtent(ard_images, bands, tmp_dir,
                                                                           out_extent_vec, out_extent_lyr,
                                                                           out_imgs, out_img_sizes, gdal_format,
                                                                           'auto', strch_file, export_stretch_file)
            return True
        # else there weren't any ard_images...
        return False


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

    def export_db_to_json(self, out_json_file):
        """
        This function exports the database table to a JSON file.
        :param out_json_file: output JSON file path.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()

        query_result = ses.query(EDDLandsatGoogle).all()
        db_scn_dict = dict()
        for scn in query_result:
            db_scn_dict[scn.PID] = dict()
            db_scn_dict[scn.PID]['PID'] = scn.PID
            db_scn_dict[scn.PID]['Scene_ID'] = scn.Scene_ID
            db_scn_dict[scn.PID]['Product_ID'] = scn.Product_ID
            db_scn_dict[scn.PID]['Spacecraft_ID'] = scn.Spacecraft_ID
            db_scn_dict[scn.PID]['Sensor_ID'] = scn.Sensor_ID
            db_scn_dict[scn.PID]['Date_Acquired'] = eodd_utils.getDateTimeAsString(scn.Date_Acquired)
            db_scn_dict[scn.PID]['Collection_Number'] = scn.Collection_Number
            db_scn_dict[scn.PID]['Collection_Category'] = scn.Collection_Category
            db_scn_dict[scn.PID]['Sensing_Time'] = eodd_utils.getDateTimeAsString(scn.Sensing_Time)
            db_scn_dict[scn.PID]['Data_Type'] = scn.Data_Type
            db_scn_dict[scn.PID]['WRS_Path'] = scn.WRS_Path
            db_scn_dict[scn.PID]['WRS_Row'] = scn.WRS_Row
            db_scn_dict[scn.PID]['Cloud_Cover'] = scn.Cloud_Cover
            db_scn_dict[scn.PID]['North_Lat'] = scn.North_Lat
            db_scn_dict[scn.PID]['South_Lat'] = scn.South_Lat
            db_scn_dict[scn.PID]['East_Lon'] = scn.East_Lon
            db_scn_dict[scn.PID]['West_Lon'] = scn.West_Lon
            db_scn_dict[scn.PID]['Total_Size'] = scn.Total_Size
            db_scn_dict[scn.PID]['Remote_URL'] = scn.Remote_URL
            db_scn_dict[scn.PID]['Query_Date'] = eodd_utils.getDateTimeAsString(scn.Query_Date)
            db_scn_dict[scn.PID]['Download_Start_Date'] = eodd_utils.getDateTimeAsString(scn.Download_Start_Date)
            db_scn_dict[scn.PID]['Download_End_Date'] = eodd_utils.getDateTimeAsString(scn.Download_End_Date)
            db_scn_dict[scn.PID]['Downloaded'] = scn.Downloaded
            db_scn_dict[scn.PID]['Download_Path'] = scn.Download_Path
            db_scn_dict[scn.PID]['Archived'] = scn.Archived
            db_scn_dict[scn.PID]['ARDProduct_Start_Date'] = eodd_utils.getDateTimeAsString(scn.ARDProduct_Start_Date)
            db_scn_dict[scn.PID]['ARDProduct_End_Date'] = eodd_utils.getDateTimeAsString(scn.ARDProduct_End_Date)
            db_scn_dict[scn.PID]['ARDProduct'] = scn.ARDProduct
            db_scn_dict[scn.PID]['ARDProduct_Path'] = scn.ARDProduct_Path
            db_scn_dict[scn.PID]['DCLoaded_Start_Date'] = eodd_utils.getDateTimeAsString(scn.DCLoaded_Start_Date)
            db_scn_dict[scn.PID]['DCLoaded_End_Date'] = eodd_utils.getDateTimeAsString(scn.DCLoaded_End_Date)
            db_scn_dict[scn.PID]['DCLoaded'] = scn.DCLoaded
            db_scn_dict[scn.PID]['Invalid'] = scn.Invalid
            db_scn_dict[scn.PID]['ExtendedInfo'] = scn.ExtendedInfo
            db_scn_dict[scn.PID]['RegCheck'] = scn.RegCheck
        ses.close()

        with open(out_json_file, 'w') as outfile:
            json.dump(db_scn_dict, outfile, indent=4, separators=(',', ': '), ensure_ascii=False)

    def import_sensor_db(self, input_json_file, replace_path_dict=None):
        """
        This function imports from the database records from the specified input JSON file.

        :param input_json_file: input JSON file with the records to be imported.
        :param replace_path_dict: a dictionary of file paths to be updated, if None then ignored.
        """
        db_records = list()
        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        with open(input_json_file) as json_file_obj:
            sensor_rows = json.load(json_file_obj)
            for pid in sensor_rows:
                # This is due to typo - in original table def so will keep this for a while to allow export and import
                if 'Collection_Category' in sensor_rows[pid]:
                    collect_cat = sensor_rows[pid]['Collection_Category']
                else:
                    collect_cat = sensor_rows[pid]['Collection_Catagory']
                db_records.append(EDDLandsatGoogle(PID=sensor_rows[pid]['PID'],
                                                   Scene_ID=sensor_rows[pid]['Scene_ID'],
                                                   Product_ID=sensor_rows[pid]['Product_ID'],
                                                   Spacecraft_ID=sensor_rows[pid]['Spacecraft_ID'],
                                                   Sensor_ID=sensor_rows[pid]['Sensor_ID'],
                                                   Date_Acquired=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Date_Acquired']),
                                                   Collection_Number=sensor_rows[pid]['Collection_Number'],
                                                   Collection_Category=collect_cat,
                                                   Sensing_Time=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Sensing_Time']),
                                                   Data_Type=sensor_rows[pid]['Data_Type'],
                                                   WRS_Path=sensor_rows[pid]['WRS_Path'],
                                                   WRS_Row=sensor_rows[pid]['WRS_Row'],
                                                   Cloud_Cover=sensor_rows[pid]['Cloud_Cover'],
                                                   North_Lat=sensor_rows[pid]['North_Lat'],
                                                   South_Lat=sensor_rows[pid]['South_Lat'],
                                                   East_Lon=sensor_rows[pid]['East_Lon'],
                                                   West_Lon=sensor_rows[pid]['West_Lon'],
                                                   Total_Size=sensor_rows[pid]['Total_Size'],
                                                   Remote_URL=sensor_rows[pid]['Remote_URL'],
                                                   Query_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Query_Date']),
                                                   Download_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Download_Start_Date']),
                                                   Download_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Download_End_Date']),
                                                   Downloaded=sensor_rows[pid]['Downloaded'],
                                                   Download_Path=eodd_utils.update_file_path(sensor_rows[pid]['Download_Path'], replace_path_dict),
                                                   Archived=sensor_rows[pid]['Archived'],
                                                   ARDProduct_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_Start_Date']),
                                                   ARDProduct_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_End_Date']),
                                                   ARDProduct=sensor_rows[pid]['ARDProduct'],
                                                   ARDProduct_Path=eodd_utils.update_file_path(sensor_rows[pid]['ARDProduct_Path'], replace_path_dict),
                                                   DCLoaded_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['DCLoaded_Start_Date']),
                                                   DCLoaded_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['DCLoaded_End_Date']),
                                                   DCLoaded=sensor_rows[pid]['DCLoaded'],
                                                   Invalid=sensor_rows[pid]['Invalid'],
                                                   ExtendedInfo=self.update_extended_info_qklook_tilecache_paths(sensor_rows[pid]['ExtendedInfo'], replace_path_dict),
                                                   RegCheck=sensor_rows[pid]['RegCheck']))
        if len(db_records) > 0:
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            ses.add_all(db_records)
            ses.commit()
            ses.close()

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        try:
            gdal.UseExceptions()

            vec_osr = osr.SpatialReference()
            vec_osr.ImportFromEPSG(4326)

            driver = ogr.GetDriverByName(driver_name)
            if os.path.exists(file_path) and add_lyr:
                out_data_source = gdal.OpenEx(file_path, gdal.OF_UPDATE)
            elif os.path.exists(file_path):
                driver.DeleteDataSource(file_path)
                out_data_source = driver.CreateDataSource(file_path)
            else:
                out_data_source = driver.CreateDataSource(file_path)

            out_vec_lyr = out_data_source.GetLayerByName(lyr_name)
            if out_vec_lyr is None:
                out_vec_lyr = out_data_source.CreateLayer(lyr_name, srs=vec_osr, geom_type=ogr.wkbPolygon)

            pid_field_defn = ogr.FieldDefn("PID", ogr.OFTInteger)
            if out_vec_lyr.CreateField(pid_field_defn) != 0:
                raise EODataDownException("Could not create 'PID' field in output vector lyr.")

            scene_id_field_defn = ogr.FieldDefn("Scene_ID", ogr.OFTString)
            scene_id_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(scene_id_field_defn) != 0:
                raise EODataDownException("Could not create 'Scene_ID' field in output vector lyr.")

            product_id_field_defn = ogr.FieldDefn("Product_ID", ogr.OFTString)
            product_id_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(product_id_field_defn) != 0:
                raise EODataDownException("Could not create 'Product_ID' field in output vector lyr.")

            spacecraft_id_field_defn = ogr.FieldDefn("Spacecraft_ID", ogr.OFTString)
            spacecraft_id_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(spacecraft_id_field_defn) != 0:
                raise EODataDownException("Could not create 'Spacecraft_ID' field in output vector lyr.")

            sensor_id_field_defn = ogr.FieldDefn("Sensor_ID", ogr.OFTString)
            sensor_id_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(sensor_id_field_defn) != 0:
                raise EODataDownException("Could not create 'Sensor_ID' field in output vector lyr.")

            date_acq_field_defn = ogr.FieldDefn("Date_Acquired", ogr.OFTString)
            date_acq_field_defn.SetWidth(32)
            if out_vec_lyr.CreateField(date_acq_field_defn) != 0:
                raise EODataDownException("Could not create 'Date_Acquired' field in output vector lyr.")

            collect_num_field_defn = ogr.FieldDefn("Collection_Number", ogr.OFTString)
            collect_num_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(collect_num_field_defn) != 0:
                raise EODataDownException("Could not create 'Collection_Number' field in output vector lyr.")

            collect_cat_field_defn = ogr.FieldDefn("Collection_Category", ogr.OFTString)
            collect_cat_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(collect_cat_field_defn) != 0:
                raise EODataDownException("Could not create 'Collection_Category' field in output vector lyr.")

            sense_time_field_defn = ogr.FieldDefn("Sensing_Time", ogr.OFTString)
            sense_time_field_defn.SetWidth(32)
            if out_vec_lyr.CreateField(sense_time_field_defn) != 0:
                raise EODataDownException("Could not create 'Sensing_Time' field in output vector lyr.")

            wrs_path_field_defn = ogr.FieldDefn("WRS_Path", ogr.OFTInteger)
            if out_vec_lyr.CreateField(wrs_path_field_defn) != 0:
                raise EODataDownException("Could not create 'WRS_Path' field in output vector lyr.")

            wrs_row_field_defn = ogr.FieldDefn("WRS_Row", ogr.OFTInteger)
            if out_vec_lyr.CreateField(wrs_row_field_defn) != 0:
                raise EODataDownException("Could not create 'WRS_Row' field in output vector lyr.")

            cloud_cover_field_defn = ogr.FieldDefn("Cloud_Cover", ogr.OFTReal)
            if out_vec_lyr.CreateField(cloud_cover_field_defn) != 0:
                raise EODataDownException("Could not create 'Cloud_Cover' field in output vector lyr.")

            down_path_field_defn = ogr.FieldDefn("Download_Path", ogr.OFTString)
            down_path_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(down_path_field_defn) != 0:
                raise EODataDownException("Could not create 'Download_Path' field in output vector lyr.")

            ard_path_field_defn = ogr.FieldDefn("ARD_Path", ogr.OFTString)
            ard_path_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(ard_path_field_defn) != 0:
                raise EODataDownException("Could not create 'ARD_Path' field in output vector lyr.")

            north_field_defn = ogr.FieldDefn("North_Lat", ogr.OFTReal)
            if out_vec_lyr.CreateField(north_field_defn) != 0:
                raise EODataDownException("Could not create 'North_Lat' field in output vector lyr.")

            south_field_defn = ogr.FieldDefn("South_Lat", ogr.OFTReal)
            if out_vec_lyr.CreateField(south_field_defn) != 0:
                raise EODataDownException("Could not create 'South_Lat' field in output vector lyr.")

            east_field_defn = ogr.FieldDefn("East_Lon", ogr.OFTReal)
            if out_vec_lyr.CreateField(east_field_defn) != 0:
                raise EODataDownException("Could not create 'East_Lon' field in output vector lyr.")

            west_field_defn = ogr.FieldDefn("West_Lon", ogr.OFTReal)
            if out_vec_lyr.CreateField(west_field_defn) != 0:
                raise EODataDownException("Could not create 'West_Lon' field in output vector lyr.")

            # Get the output Layer's Feature Definition
            feature_defn = out_vec_lyr.GetLayerDefn()

            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            db_session = sqlalchemy.orm.sessionmaker(bind=db_engine)
            db_ses = db_session()

            query_rtn = db_ses.query(EDDLandsatGoogle).all()

            if len(query_rtn) > 0:
                for record in query_rtn:
                    geo_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                    geo_bbox.setBBOX(record.North_Lat, record.South_Lat, record.West_Lon, record.East_Lon)
                    bboxs = geo_bbox.getGeoBBoxsCut4LatLonBounds()

                    for bbox in bboxs:
                        poly = bbox.getOGRPolygon()
                        # Add to output shapefile.
                        out_feat = ogr.Feature(feature_defn)
                        out_feat.SetField("PID", record.PID)
                        out_feat.SetField("Scene_ID", record.Scene_ID)
                        out_feat.SetField("Product_ID", record.Product_ID)
                        out_feat.SetField("Spacecraft_ID", record.Spacecraft_ID)
                        out_feat.SetField("Sensor_ID", record.Sensor_ID)
                        out_feat.SetField("Date_Acquired", record.Date_Acquired.strftime('%Y-%m-%d'))
                        out_feat.SetField("Collection_Number", record.Collection_Number)
                        out_feat.SetField("Collection_Category", record.Collection_Category)
                        out_feat.SetField("Sensing_Time", record.Sensing_Time.strftime('%Y-%m-%d %H:%M:%S'))
                        out_feat.SetField("WRS_Path", record.WRS_Path)
                        out_feat.SetField("WRS_Row", record.WRS_Row)
                        out_feat.SetField("Cloud_Cover", record.Cloud_Cover)
                        out_feat.SetField("Download_Path", record.Download_Path)
                        if record.ARDProduct:
                            out_feat.SetField("ARD_Path", record.ARDProduct_Path)
                        else:
                            out_feat.SetField("ARD_Path", "")
                        out_feat.SetField("North_Lat", record.North_Lat)
                        out_feat.SetField("South_Lat", record.South_Lat)
                        out_feat.SetField("East_Lon", record.East_Lon)
                        out_feat.SetField("West_Lon", record.West_Lon)
                        out_feat.SetGeometry(poly)
                        out_vec_lyr.CreateFeature(out_feat)
                        out_feat = None
            out_vec_lyr = None
            out_data_source = None
            db_ses.close()
        except Exception as e:
            raise e

    def reset_scn(self, unq_id, reset_download=False, reset_invalid=False):
        """
        A function which resets an image. This means any downloads and products are deleted
        and the database fields are reset to defaults. This allows the scene to be re-downloaded
        and processed.
        :param unq_id: unique id for the scene to be reset.
        :param reset_download: if True the download is deleted and reset in the database.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

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

        if scn_record.Downloaded and reset_download:
            dwn_path = scn_record.Download_Path
            if os.path.exists(dwn_path):
                shutil.rmtree(dwn_path)
            scn_record.Download_Start_Date = None
            scn_record.Download_End_Date = None
            scn_record.Download_Path = ""
            scn_record.Downloaded = False

        if reset_invalid:
            scn_record.Invalid = False

        scn_record.ExtendedInfo = None
        flag_modified(scn_record, "ExtendedInfo")
        ses.add(scn_record)

        ses.commit()
        ses.close()

    def reset_dc_load(self, unq_id):
        """
        A function which resets whether an image has been loaded into a datacube
        (i.e., sets the flag to False).

        :param unq_id: unique id for the scene to be reset.
        
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

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

        ses.commit()
        ses.close()

    def get_sensor_summary_info(self):
        """
        A function which returns a dict of summary information for the sensor.
        For example, summary statistics for the download time, summary statistics
        for the file size, summary statistics for the ARD processing time.

        :return: dict of information.

        """
        import statistics
        info_dict = dict()
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Find the scene count.")
        vld_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Invalid == False).count()
        invld_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Invalid == True).count()
        dwn_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True).count()
        ard_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.ARDProduct == True).count()
        dcload_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.DCLoaded == True).count()
        arch_scn_count = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Archived == True).count()
        info_dict['n_scenes'] = dict()
        info_dict['n_scenes']['n_valid_scenes'] = vld_scn_count
        info_dict['n_scenes']['n_invalid_scenes'] = invld_scn_count
        info_dict['n_scenes']['n_downloaded_scenes'] = dwn_scn_count
        info_dict['n_scenes']['n_ard_processed_scenes'] = ard_scn_count
        info_dict['n_scenes']['n_dc_loaded_scenes'] = dcload_scn_count
        info_dict['n_scenes']['n_archived_scenes'] = arch_scn_count
        logger.debug("Calculated the scene count.")

        logger.debug("Find the scene file sizes.")
        file_sizes = ses.query(EDDLandsatGoogle.Total_Size).filter(EDDLandsatGoogle.Invalid == False).all()
        if file_sizes is not None:
            if len(file_sizes) > 0:
                file_sizes_nums = list()
                for file_size in file_sizes:
                    if file_size[0] is not None:
                        file_sizes_nums.append(file_size[0])
                if len(file_sizes_nums) > 0:
                    total_file_size = sum(file_sizes_nums)
                    info_dict['file_size'] = dict()
                    info_dict['file_size']['file_size_total'] = total_file_size
                    if total_file_size > 0:
                        info_dict['file_size']['file_size_mean'] = statistics.mean(file_sizes_nums)
                        info_dict['file_size']['file_size_min'] = min(file_sizes_nums)
                        info_dict['file_size']['file_size_max'] = max(file_sizes_nums)
                        info_dict['file_size']['file_size_stdev'] = statistics.stdev(file_sizes_nums)
                        info_dict['file_size']['file_size_median'] = statistics.median(file_sizes_nums)
                        if eodatadown.py_sys_version_flt >= 3.8:
                            info_dict['file_size']['file_size_quartiles'] = statistics.quantiles(file_sizes_nums)
        logger.debug("Calculated the scene file sizes.")

        logger.debug("Find download and processing time stats.")
        download_times = []
        ard_process_times = []
        scns = ses.query(EDDLandsatGoogle).filter(EDDLandsatGoogle.Downloaded == True)
        for scn in scns:
            download_times.append((scn.Download_End_Date - scn.Download_Start_Date).total_seconds())
            if scn.ARDProduct:
                ard_process_times.append((scn.ARDProduct_End_Date - scn.ARDProduct_Start_Date).total_seconds())

        if len(download_times) > 0:
            info_dict['download_time'] = dict()
            info_dict['download_time']['download_time_mean_secs'] = statistics.mean(download_times)
            info_dict['download_time']['download_time_min_secs'] = min(download_times)
            info_dict['download_time']['download_time_max_secs'] = max(download_times)
            info_dict['download_time']['download_time_stdev_secs'] = statistics.stdev(download_times)
            info_dict['download_time']['download_time_median_secs'] = statistics.median(download_times)
            if eodatadown.py_sys_version_flt >= 3.8:
                info_dict['download_time']['download_time_quartiles_secs'] = statistics.quantiles(download_times)

        if len(ard_process_times) > 0:
            info_dict['ard_process_time'] = dict()
            info_dict['ard_process_time']['ard_process_time_mean_secs'] = statistics.mean(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_min_secs'] = min(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_max_secs'] = max(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_stdev_secs'] = statistics.stdev(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_median_secs'] = statistics.median(ard_process_times)
            if eodatadown.py_sys_version_flt >= 3.8:
                info_dict['ard_process_time']['ard_process_time_quartiles_secs'] = statistics.quantiles(ard_process_times)
        logger.debug("Calculated the download and processing time stats.")
        return info_dict


