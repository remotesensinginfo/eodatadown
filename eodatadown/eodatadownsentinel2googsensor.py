#!/usr/bin/env python
"""
EODataDown - a sensor class for Sentinel-2 data downloaded from the Google Cloud.
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
# Purpose:  Provides a sensor class for Sentinel-2 data downloaded from the Google Cloud.
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
import subprocess

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
import eodatadown.eodatadownrunarcsi

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDSentinel2Google(Base):
    __tablename__ = "EDDSentinel2Google"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Granule_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Product_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Datatake_Identifier = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Mgrs_Tile = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Sensing_Time = sqlalchemy.Column(sqlalchemy.Date, nullable=True)
    Geometric_Quality_Flag = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Generation_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
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
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.JSON, nullable=True)
    RegCheck = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_goog(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    granule_id = params[0]
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

    logger.info("Downloading ".format(granule_id))
    start_date = datetime.datetime.now()
    for dwnld in scn_dwnlds_filelst:
        blob_obj = bucket_obj.blob(dwnld["bucket_path"])
        blob_obj.download_to_filename(dwnld["dwnld_path"])
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading ".format(granule_id))

    logger.debug("Set up database connection and update record.")
    db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
    session =sqlalchemy.orm.sessionmaker(bind=db_engine)
    ses= session()
    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Granule_ID == granule_id).one_or_none()
    if query_result is None:
        logger.error("Could not find the scene within local database: " + granule_id)
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
    granule_id = params[0]
    db_info_obj = params[1]
    scn_path = params[2]
    dem_file = params[3]
    output_dir = params[4]
    tmp_dir = params[5]
    final_ard_path = params[6]
    reproj_outputs = params[7]
    proj_wkt_file = params[8]
    projabbv = params[9]

    edd_utils = eodatadown.eodatadownutils.EODataDownUtils()
    input_hdr = edd_utils.findFile(scn_path, "*MTD*.xml")

    start_date = datetime.datetime.now()
    eodatadown.eodatadownrunarcsi.run_arcsi_sentinel2(input_hdr, dem_file, output_dir, tmp_dir, reproj_outputs,
                                                      proj_wkt_file, projabbv)

    logger.debug("Move final ARD files to specified location.")
    # Move ARD files to be kept.
    valid_output = eodatadown.eodatadownrunarcsi.move_arcsi_stdsref_products(output_dir, final_ard_path)
    # Remove Remaining files.
    shutil.rmtree(output_dir)
    shutil.rmtree(tmp_dir)
    end_date = datetime.datetime.now()
    logger.debug("Moved final ARD files to specified location.")

    if valid_output:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Granule_ID == granule_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + granule_id)
        query_result.ARDProduct = True
        query_result.ARDProduct_Start_Date = start_date
        query_result.ARDProduct_End_Date = end_date
        query_result.ARDProduct_Path = final_ard_path
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")
    else:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Granule_ID == granule_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + granule_id)
        query_result.Invalid = True
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database - not valid")


class EODataDownSentinel2GoogSensor (EODataDownSensor):
    """
    An class which represents a the Sentinel-2 sensor being downloaded from the Google Cloud.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "Sentinel2GOOG"
        self.db_tab_name = "EDDSentinel2Google"

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
            logger.debug("Testing config file is for 'Sentinel2GOOG'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'Sentinel2GOOG'")

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
            self.s2Granules = json_parse_helper.getStrListValue(config_data,
                                                                ["eodatadown", "sensor", "download", "granules"])
            self.cloudCoverThres = json_parse_helper.getNumericValue(config_data,
                                                                     ["eodatadown", "sensor", "download", "cloudcover"],
                                                                     0, 100)
            self.startDate = json_parse_helper.getDateValue(config_data,
                                                            ["eodatadown", "sensor", "download", "startdate"],
                                                            "%Y-%m-%d")
            self.monthsOfInterest = [None]
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "months"]):
                self.monthsOfInterest = json_parse_helper.getListValue(config_data,
                                                                       ["eodatadown", "sensor", "download", "months"])
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

        logger.debug("Creating Sentinel2GOOG Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.info("Checking for new data... 'Sentinel2GOOG'")
        logger.debug("Export Google Environmental Variable.")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.goog_key_json
        os.environ["GOOGLE_CLOUD_PROJECT"] = self.goog_proj_name
        from google.cloud import bigquery

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Find the start date for query - if table is empty then using config date "
                     "otherwise date of last acquried image.")
        query_date = self.startDate
        if (not check_from_start) and (ses.query(EDDSentinel2Google).first() is not None):
            query_date = ses.query(EDDSentinel2Google).order_by(
                EDDSentinel2Google.Sensing_Time.desc()).first().Sensing_Time
        logger.info("Query with start at date: " + str(query_date))

        logger.debug("Perform google query...")
        goog_fields = "granule_id,product_id,datatake_identifier,mgrs_tile,sensing_time,geometric_quality_flag," \
                      "generation_time,north_lat,south_lat,west_lon,east_lon,base_url,total_size,cloud_cover"
        goog_db_str = "`bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "

        goog_filter_date = "PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', sensing_time) > PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '" + query_date.strftime("%Y-%m-%d %H:%M:%S") + "')"
        goog_filter_cloud = "CAST(cloud_cover AS NUMERIC) < " + str(self.cloudCoverThres)

        new_scns_avail = False
        for granule_str in self.s2Granules:
            logger.info("Finding scenes for granule: " + granule_str)
            for curr_month in self.monthsOfInterest:
                if curr_month is not None:
                    logger.info("Finding scenes for granule: {} in month {}".format(granule_str, curr_month))
                    goog_filter_month = "EXTRACT(MONTH FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', sensing_time)) = {}".format(curr_month)
                    goog_filter = goog_filter_date + " AND " + goog_filter_cloud + " AND " + goog_filter_month

                client = bigquery.Client()
                granule_filter = "mgrs_tile = \"" + granule_str + "\""
                goog_query = "SELECT " + goog_fields + " FROM " + goog_db_str + " WHERE " \
                             + goog_filter + " AND " + granule_filter
                logger.debug("Query: '" + goog_query + "'")
                query_results = client.query(goog_query)
                logger.debug("Performed google query")

                logger.debug("Process google query result and add to local database (Granule: " + granule_str + ")")
                if query_results.result():
                    db_records = []
                    for row in query_results.result():
                        query_rtn = ses.query(EDDSentinel2Google).filter(
                            EDDSentinel2Google.Granule_ID == row.granule_id).one_or_none()
                        if query_rtn is None:
                            logger.debug("Granule_ID: " + row.granule_id + "\tProduct_ID: " + row.product_id)
                            sensing_time_tmp = row.sensing_time.replace('Z', '')[:-1]
                            generation_time_tmp = row.generation_time.replace('Z', '')[:-1]
                            db_records.append(
                                EDDSentinel2Google(Granule_ID=row.granule_id, Product_ID=row.product_id,
                                                   Datatake_Identifier=row.datatake_identifier, Mgrs_Tile=row.mgrs_tile,
                                                   Sensing_Time=datetime.datetime.strptime(sensing_time_tmp,
                                                                                           "%Y-%m-%dT%H:%M:%S.%f"),
                                                   Geometric_Quality_Flag=row.geometric_quality_flag,
                                                   Generation_Time=datetime.datetime.strptime(generation_time_tmp,
                                                                                              "%Y-%m-%dT%H:%M:%S.%f"),
                                                   Cloud_Cover=float(row.cloud_cover), North_Lat=row.north_lat,
                                                   South_Lat=row.south_lat,
                                                   East_Lon=row.east_lon, West_Lon=row.west_lon, Total_Size=row.total_size,
                                                   Remote_URL=row.base_url, Query_Date=datetime.datetime.now(),
                                                   Download_Start_Date=None, Download_End_Date=None, Downloaded=False,
                                                   Download_Path="", ARDProduct_Start_Date=None, ARDProduct_End_Date=None,
                                                   ARDProduct=False, ARDProduct_Path=""))
                    if len(db_records) > 0:
                        ses.add_all(db_records)
                        ses.commit()
                        new_scns_avail = True
                logger.debug("Processed google query result and added to local database (Granule: " + granule_str + ")")
                client = None

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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == False).all()

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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id,
                                                            EDDSentinel2Google.Downloaded == False).all()
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
                if bucket_name != "gcp-public-data-sentinel-2":
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

                    _download_scn_goog([record.Granule_ID, self.db_info_obj, self.goog_key_json, self.goog_proj_name,
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
        session =sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == False).all()

        if query_result is not None:
            logger.debug("Create the output directory for this download.")
            dt_obj = datetime.datetime.now()

            logger.debug("Build download file list.")
            dwnld_params = []
            for record in query_result:
                logger.debug("Building download info for '"+record.Remote_URL+"'")
                url_path = record.Remote_URL
                url_path = url_path.replace("gs://", "")
                url_path_parts = url_path.split("/")
                bucket_name = url_path_parts[0]
                if bucket_name != "gcp-public-data-sentinel-2":
                    logger.error("Incorrect bucket name '"+bucket_name+"'")
                    raise EODataDownException("The bucket specified in the URL is not the Google Public "
                                              "Sentinel-2 Bucket - something has gone wrong.")
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

                dwnld_params.append([record.Granule_ID, self.db_info_obj, self.goog_key_json, self.goog_proj_name,
                                     bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path])
            downloaded_new_scns = True
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_scn_goog, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked downloaded new scenes.", sensor_val=self.sensor_name,
                               updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)

    def get_scnlist_con2ard(self):
        """
        A function which queries the database to find scenes which have been downloaded but have not yet been
        processed to an analysis ready data (ARD) format.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to process.
        """
        raise EODataDownException("Not implemented.")

    def scn2ard(self, unq_id):
        """
        A function which processes a single scene to an analysis ready data (ARD) format.
        :param unq_id: the unique ID of the scene to be processed.
        :return: returns boolean indicating successful or otherwise processing.
        """
        raise EODataDownException("Not implemented.")

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
        session =sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == True,
                                                            EDDSentinel2Google.ARDProduct == False).all()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        if query_result is not None:
            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(work_ard_path):
                os.mkdir(work_ard_path)

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            ard_params = []
            for record in query_result:
                logger.debug("Create info for running ARD analysis for scene: " + record.Granule_ID)
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

                ard_params.append([record.Granule_ID, self.db_info_obj, record.Download_Path, self.demFile,
                                   work_ard_scn_path, tmp_ard_scn_path, final_ard_scn_path, self.ardProjDefined,
                                   proj_wkt_file, self.projabbv])
        else:
            logger.info("There are no scenes which have been downloaded but not processed to an ARD product.")
        ses.close()
        logger.debug("Closed the database session.")

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
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.ARDProduct == True,
                                                            EDDSentinel2Google.DCLoaded == loaded).all()
        scns2dcload = []
        if query_result is not None:
            for record in query_result:
                scns2dcload.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dcload

    def scn2datacube(self, unq_id):
        """
        A function which loads a single scene into the datacube system.
        :param unq_id: the unique ID of the scene to be loaded.
        :return: returns boolean indicating successful or otherwise loading into the datacube.
        """
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
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.ARDProduct == True,
                                                            EDDSentinel2Google.DCLoaded == False).all()

        if query_result is not None:
            logger.debug("Create the yaml files for the data cube to enable import.")
            for record in query_result:
                start_date = datetime.datetime.now()
                scn_id = str(str(uuid.uuid5(uuid.NAMESPACE_URL, record.ARDProduct_Path)))
                print("{}: {}".format(record.Product_ID, scn_id))
                img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*vmsk_rad_srefdem_stdsref.kea')
                vmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_valid.kea')
                cmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_clouds.kea')
                yaml_file = os.path.splitext(img_file)[0] + "_yaml.yaml"
                epsg_code = rsgis_utils.getEPSGCode(img_file)
                lcl_proj_bbox = rsgis_utils.getImageBBOX(img_file)

                image_lyrs = dict()
                image_lyrs['coastal'] = {'layer': 1, 'path': img_file}
                image_lyrs['blue'] = {'layer': 2, 'path': img_file}
                image_lyrs['green'] = {'layer': 3, 'path': img_file}
                image_lyrs['red'] = {'layer': 4, 'path': img_file}
                image_lyrs['nir'] = {'layer': 5, 'path': img_file}
                image_lyrs['swir1'] = {'layer': 6, 'path': img_file}
                image_lyrs['swir2'] = {'layer': 7, 'path': img_file}
                image_lyrs['fmask'] = {'layer': 1, 'path': cmsk_img_file}
                image_lyrs['vmask'] = {'layer': 1, 'path': vmsk_img_file}

                scn_info = {
                    'id': scn_id,
                    'processing_level': 'LEVEL_2',
                    'product_type': 'ARCSI_SREF',
                    'creation_dt': record.ARDProduct_End_Date.strftime("%Y-%m-%d %H:%M:%S"),
                    'label': record.Product_ID,
                    'platform': {'code': 'SENTINEL-2'},
                    'instrument': {'name': 'MSI'},
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
                    'image': {'bands': image_lyrs},
                    'lineage': {'source_datasets': {}},
                }
                with open(yaml_file, 'w') as stream:
                    yaml.dump(scn_info, stream)

                cmd = "{0} dataset add {1}".format(datacube_cmd_path, yaml_file)
                try:
                    subprocess.call(cmd, shell=True)
                    end_date = datetime.datetime.now()
                    record.DCLoaded_Start_Date = start_date
                    record.DCLoaded_End_Date = end_date
                    record.DCLoaded = True
                except Exception as e:
                    logger.debug("Failed to load scene: '{}'".format(cmd), exc_info=True)


    def get_scn_record(self, unq_id):
        """
        A function which queries the database using the unique ID of a scene returning the record
        :param unq_id:
        :return: Returns the database record object
        """
        raise EODataDownException("Not implemented.")

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
        raise EODataDownException("Not implemented.")

    def import_append_db(self, db_info_obj):
        """
        This function imports from the database specified by the input database info object
        and appends the data to the exisitng database. This might be used if data was processed
        on another system (e.g., HPC cluster).
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
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
        scn_record = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one_or_none()

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

    def reset_dc_load(self, unq_id):
        """
        A function which resets whether an image has been loaded into a datacube
        (i.e., sets the flag to False).
        :param unq_id: unique id for the scene to be reset.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one_or_none()

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
