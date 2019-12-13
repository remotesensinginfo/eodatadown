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
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import func

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDSentinel2Google(Base):
    __tablename__ = "EDDSentinel2Google"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Granule_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Product_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Platform_ID = sqlalchemy.Column(sqlalchemy.String, nullable=True)
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
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)
    RegCheck = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_goog(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    pid = params[0]
    granule_id = params[1]
    db_info_obj = params[2]
    goog_key_json = params[3]
    goog_proj_name = params[4]
    bucket_name = params[5]
    scn_dwnlds_filelst = params[6]
    scn_lcl_dwnld_path = params[7]
    scn_remote_url = params[8]
    goog_down_meth = params[9]

    download_completed = False
    logger.info("Downloading ".format(granule_id))
    start_date = datetime.datetime.now()
    if goog_down_meth == 'PYAPI':
        logger.debug("Using Google storage API to download.")
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
        cmd = "gsutil cp -r {} {}".format(scn_remote_url, scn_lcl_dwnld_path)
        try:
            logger.debug("Running command: '{}'".format(cmd))
            subprocess.call(cmd, shell=True)
            download_completed = True
        except OSError as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
        except Exception as e:
            logger.error("Download Failed for {} with error {}".format(scn_remote_url, e))
    elif goog_down_meth == 'GSUTIL_MULTI':
        logger.debug("Using Google GSUTIL (multi threaded) utility to download.")
        cmd = "gsutil -m cp -r {} {}".format(scn_remote_url, scn_lcl_dwnld_path)
        try:
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
    logger.info("Finished Downloading ".format(granule_id))

    if download_completed:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == pid).one_or_none()
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
    granule_id = params[1]
    db_info_obj = params[2]
    scn_path = params[3]
    dem_file = params[4]
    output_dir = params[5]
    tmp_dir = params[6]
    final_ard_path = params[7]
    reproj_outputs = params[8]
    proj_wkt_file = params[9]
    projabbv = params[10]
    use_roi = params[11]
    intersect_vec_file = params[12]
    intersect_vec_lyr = params[13]
    subset_vec_file = params[14]
    subset_vec_lyr = params[15]
    mask_outputs = params[16]
    mask_vec_file = params[17]
    mask_vec_lyr = params[18]

    edd_utils = eodatadown.eodatadownutils.EODataDownUtils()
    input_hdr = edd_utils.findFirstFile(scn_path, "*MTD*.xml")

    start_date = datetime.datetime.now()
    eodatadown.eodatadownrunarcsi.run_arcsi_sentinel2(input_hdr, dem_file, output_dir, tmp_dir, reproj_outputs,
                                                      proj_wkt_file, projabbv)

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
    end_date = datetime.datetime.now()
    logger.debug("Moved final ARD files to specified location.")

    if valid_output:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == pid).one_or_none()
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == pid).one_or_none()
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
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data,
                                                                  ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data,
                                                                 ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data,
                                                                ["eodatadown", "sensor", "paths", "ardtmp"])

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths", "quicklooks"]):
                self.quicklookPath = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "paths", "quicklooks"])
            else:
                self.quicklookPath = None

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths", "tilecache"]):
                self.tilecachePath = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "paths", "tilecache"])
            else:
                self.tilecachePath = None
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

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "months"]):
                self.monthsOfInterest = json_parse_helper.getListValue(config_data,
                                                                       ["eodatadown", "sensor", "download", "months"])
            logger.debug("Found search params from config file")

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Find the start date for query - if table is empty then using config date "
                     "otherwise date of last acquried image.")
        query_date = self.startDate
        if (not check_from_start) and (ses.query(EDDSentinel2Google).first() is not None):
            query_date = ses.query(EDDSentinel2Google).order_by(
                EDDSentinel2Google.Sensing_Time.desc()).first().Sensing_Time
        logger.info("Query with start at date: " + str(query_date))

        # Get the next PID value to ensure increment
        c_max_pid = ses.query(func.max(EDDSentinel2Google.PID).label("max_pid")).one().max_pid
        n_max_pid = c_max_pid + 1

        logger.debug("Perform google query...")
        goog_fields = "granule_id,product_id,datatake_identifier,mgrs_tile,sensing_time,geometric_quality_flag," \
                      "generation_time,north_lat,south_lat,west_lon,east_lon,base_url,total_size,cloud_cover"
        goog_db_str = "`bigquery-public-data.cloud_storage_geo_index.sentinel_2_index` "

        goog_filter_date = "PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', sensing_time) > PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '" + query_date.strftime("%Y-%m-%d %H:%M:%S") + "')"
        goog_filter_cloud = "CAST(cloud_cover AS NUMERIC) < " + str(self.cloudCoverThres)

        month_filter = ''
        first = True
        if self.monthsOfInterest is not None:
            for curr_month in self.monthsOfInterest:
                sgn_month_filter = "(EXTRACT(MONTH FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', sensing_time)) = {})".format(curr_month)
                if first:
                    month_filter = sgn_month_filter
                    first = False
                else:
                    month_filter = "{} OR {}".format(month_filter, sgn_month_filter)
            if month_filter != '':
                logger.info("Finding scenes for with month filter {}".format(month_filter))
                month_filter = "({})".format(month_filter)

        granule_filter = ''
        first = True
        for granule_str in self.s2Granules:
            sgn_granule_filter = '(mgrs_tile = "{}")'.format(granule_str)
            if first:
                granule_filter = sgn_granule_filter
                first = False
            else:
                granule_filter = "{} OR {}".format(granule_filter, sgn_granule_filter)
        granule_filter = "({})".format(granule_filter)

        goog_filter = "{} AND {}".format(goog_filter_date, goog_filter_cloud)
        if self.monthsOfInterest is not None:
            goog_filter = "{} AND {}".format(goog_filter, month_filter)

        goog_query = "SELECT " + goog_fields + " FROM " + goog_db_str + " WHERE " \
                     + goog_filter + " AND " + granule_filter
        logger.debug("Query: '" + goog_query + "'")
        client = bigquery.Client()
        query_results = client.query(goog_query)
        logger.debug("Performed google query")

        new_scns_avail = False
        db_records = list()
        logger.debug("Process google query result and add to local database.")
        if query_results.result():
            for row in query_results.result():
                generation_time_tmp = row.generation_time.replace('Z', '')[:-1]
                query_rtn = ses.query(EDDSentinel2Google).filter(
                    EDDSentinel2Google.Granule_ID == row.granule_id,
                    EDDSentinel2Google.Generation_Time == datetime.datetime.strptime(generation_time_tmp,
                                                                                     "%Y-%m-%dT%H:%M:%S.%f")).all()
                if len(query_rtn) == 0:
                    logger.debug("Granule_ID: " + row.granule_id + "\tProduct_ID: " + row.product_id)
                    sensing_time_tmp = row.sensing_time.replace('Z', '')[:-1]
                    platform = 'Sentinel2'
                    if 'GS2A' in row.datatake_identifier:
                        platform = 'Sentinel2A'
                    elif 'GS2B' in row.datatake_identifier:
                        platform = 'Sentinel2B'
                    db_records.append(
                        EDDSentinel2Google(PID=n_max_pid, Granule_ID=row.granule_id, Product_ID=row.product_id,
                                           Platform_ID=platform, Datatake_Identifier=row.datatake_identifier,
                                           Mgrs_Tile=row.mgrs_tile,
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
                    n_max_pid = n_max_pid + 1
        if len(db_records) > 0:
            ses.add_all(db_records)
            ses.commit()
            new_scns_avail = True
        logger.debug("Processed google query result and added to local database.")
        client = None
        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked for availability of new scenes", sensor_val=self.sensor_name,
                               updated_lcl_db=True, scns_avail=new_scns_avail)

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
        query_result = ses.query(EDDSentinel2Google).all()
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == False).all()

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
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: boolean (True for downloaded; False for not downloaded)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one()
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

                _download_scn_goog([record.PID, record.Granule_ID, self.db_info_obj, self.goog_key_json,
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
                dwnld_out_dirname = "{}_{}".format(url_path_parts[-1], record.PID)
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
                    if (not os.path.exists(dwnld_dirpath)) and (self.goog_down_meth == "PYAPI"):
                        os.makedirs(dwnld_dirpath, exist_ok=True)
                    scn_dwnlds_filelst.append({"bucket_path": blob.name, "dwnld_path": dwnld_file})

                dwnld_params.append([record.PID, record.Granule_ID, self.db_info_obj, self.goog_key_json,
                                     self.goog_proj_name, bucket_name, scn_dwnlds_filelst, scn_lcl_dwnld_path,
                                     record.Remote_URL, self.goog_down_meth])
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
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == True,
                                                            EDDSentinel2Google.ARDProduct == False,
                                                            EDDSentinel2Google.Invalid == False).all()

        scns2ard = []
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one()
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id,
                                                            EDDSentinel2Google.Downloaded == True,
                                                            EDDSentinel2Google.ARDProduct == False).one_or_none()
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

            logger.debug("Create info for running ARD analysis for scene: " + record.Product_ID)
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
                proj_wkt_file = os.path.join(work_ard_scn_path, record.Product_ID + "_wkt.wkt")
                rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

            _process_to_ard([record.PID, record.Granule_ID, self.db_info_obj, record.Download_Path, self.demFile,
                             work_ard_scn_path, tmp_ard_scn_path, final_ard_scn_path, self.ardProjDefined,
                             proj_wkt_file, self.projabbv, self.use_roi, self.intersect_vec_file,
                             self.intersect_vec_lyr, self.subset_vec_file, self.subset_vec_lyr, self.mask_outputs,
                             self.mask_vec_file, self.mask_vec_lyr])
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Downloaded == True,
                                                            EDDSentinel2Google.ARDProduct == False,
                                                            EDDSentinel2Google.Invalid == False).all()

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
                    proj_wkt_file = os.path.join(work_ard_scn_path, "{}_wkt.wkt".format(record.Product_ID))
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                ard_params.append([record.PID, record.Granule_ID, self.db_info_obj, record.Download_Path, self.demFile,
                                   work_ard_scn_path, tmp_ard_scn_path, final_ard_scn_path, self.ardProjDefined,
                                   proj_wkt_file, self.projabbv, self.use_roi, self.intersect_vec_file,
                                   self.intersect_vec_lyr, self.subset_vec_file, self.subset_vec_lyr, self.mask_outputs,
                                   self.mask_vec_file, self.mask_vec_lyr])
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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.DCLoaded

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.ARDProduct == True,
                                                            EDDSentinel2Google.DCLoaded == False).all()

        if query_result is not None:
            logger.debug("Create the yaml files for the data cube to enable import.")
            for record in query_result:
                start_date = datetime.datetime.now()
                scn_id = str(str(uuid.uuid5(uuid.NAMESPACE_URL, record.ARDProduct_Path)))
                print("{}: {}".format(record.Product_ID, scn_id))
                img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*vmsk_rad_srefdem_stdsref.tif')
                vmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_valid.tif')
                cmsk_img_file = rsgis_utils.findFile(record.ARDProduct_Path, '*_clouds.tif')
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
                    end_date = datetime.datetime.now()
                    record.DCLoaded_Start_Date = start_date
                    record.DCLoaded_End_Date = end_date
                    record.DCLoaded = True
                except Exception as e:
                    logger.debug("Failed to load scene: '{}'".format(cmd), exc_info=True)

    def get_scnlist_quicklook(self):
        """
        Get a list of all scenes which have not had a quicklook generated.

        :return: list of unique IDs
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDSentinel2Google).filter(
            sqlalchemy.or_(
                EDDSentinel2Google.ExtendedInfo.is_(None),
                sqlalchemy.not_(EDDSentinel2Google.ExtendedInfo.has_key('quicklook'))),
            EDDSentinel2Google.Invalid == False,
            EDDSentinel2Google.ARDProduct == True).all()
        scns2quicklook = []
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one()
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one_or_none()
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
            ard_img_file = eodd_utils.findFile(ard_img_path, '*vmsk_sharp_rad_srefdem_stdsref.tif')

            out_quicklook_path = os.path.join(self.quicklookPath,
                                              "{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(out_quicklook_path):
                os.mkdir(out_quicklook_path)

            tmp_quicklook_path = os.path.join(self.ardProdTmpPath,
                                              "quicklook_{}_{}".format(query_result.Product_ID, query_result.PID))
            if not os.path.exists(tmp_quicklook_path):
                os.mkdir(tmp_quicklook_path)

            # NIR, SWIR, RED
            bands = '7,10,3'

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
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDSentinel2Google).filter(
            sqlalchemy.or_(
                EDDSentinel2Google.ExtendedInfo.is_(None),
                sqlalchemy.not_(EDDSentinel2Google.ExtendedInfo.has_key('tilecache'))),
            EDDSentinel2Google.Invalid == False,
            EDDSentinel2Google.ARDProduct == True).all()
        scns2tilecache = []
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one()
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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one_or_none()
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
            ard_img_file = eodd_utils.findFile(ard_img_path, '*vmsk_sharp_rad_srefdem_stdsref.tif')

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
            bands = '7,10,3'

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
        query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).all()
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

    def find_unique_platforms(self):
        """
        A function which returns a list of unique platforms within the database (e.g., Sentinel2A or Sentinel2B).
        :return: list of strings.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        platforms = ses.query(EDDSentinel2Google.Platform_ID).group_by(EDDSentinel2Google.Platform_ID)
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
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Invalid == False,
                                                              EDDSentinel2Google.ARDProduct == True,
                                                              EDDSentinel2Google.Cloud_Cover <= cloud_thres).count()
            else:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Cloud_Cover <= cloud_thres).count()
        else:
            if valid:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Invalid == False,
                                                              EDDSentinel2Google.ARDProduct == True).count()
            else:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date).count()
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
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec+n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
        else:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec+n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
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
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Invalid == False,
                                                              EDDSentinel2Google.ARDProduct == True,
                                                              EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                              (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                              (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                              (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                              (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).count()
            else:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                              (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                              (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                              (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                              (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).count()
        else:
            if valid:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date,
                                                              EDDSentinel2Google.Invalid == False,
                                                              EDDSentinel2Google.ARDProduct == True).filter(
                                                              (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                              (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                              (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                              (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).count()
            else:
                n_rows = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                              EDDSentinel2Google.Sensing_Time >= end_date).filter(
                                                              (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                              (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                              (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                              (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).count()
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
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec+n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Cloud_Cover <= cloud_thres).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
        else:
            if valid:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec+n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date,
                                                                        EDDSentinel2Google.Invalid == False,
                                                                        EDDSentinel2Google.ARDProduct == True).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
            else:
                if n_recs > 0:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc())[start_rec:(start_rec + n_recs)]
                else:
                    query_result = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.Sensing_Time <= start_date,
                                                                        EDDSentinel2Google.Sensing_Time >= end_date).filter(
                                                                        (bbox[east_lon_idx] > EDDSentinel2Google.West_Lon),
                                                                        (EDDSentinel2Google.East_Lon > bbox[west_lon_idx]),
                                                                        (bbox[north_lat_idx] > EDDSentinel2Google.South_Lat),
                                                                        (EDDSentinel2Google.North_Lat > bbox[south_lat_idx])).order_by(
                        EDDSentinel2Google.Sensing_Time.desc()).all()
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
        :param order_desc: If True then results are in descending order otherwise ascending.
        :param platform: If None then all scenes, if value provided then it just be for that platform.
        :return: List of datetime.date objects.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        if platform is None:
            if valid:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                        EDDSentinel2Google.Sensing_Time <= start_date,
                        EDDSentinel2Google.Sensing_Time >= end_date,
                        EDDSentinel2Google.Invalid == False).group_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                            EDDSentinel2Google.Sensing_Time <= start_date,
                            EDDSentinel2Google.Sensing_Time >= end_date,
                            EDDSentinel2Google.Invalid == False).group_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                        EDDSentinel2Google.Sensing_Time <= start_date,
                        EDDSentinel2Google.Sensing_Time >= end_date).group_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                            EDDSentinel2Google.Sensing_Time <= start_date,
                            EDDSentinel2Google.Sensing_Time >= end_date).group_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).asc())
        else:
            if valid:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                        EDDSentinel2Google.Sensing_Time <= start_date,
                        EDDSentinel2Google.Sensing_Time >= end_date,
                        EDDSentinel2Google.Invalid == False,
                        EDDSentinel2Google.Platform_ID == platform).group_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                            EDDSentinel2Google.Sensing_Time <= start_date,
                            EDDSentinel2Google.Sensing_Time >= end_date,
                            EDDSentinel2Google.Invalid == False,
                            EDDSentinel2Google.Platform_ID == platform).group_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                        EDDSentinel2Google.Sensing_Time <= start_date,
                        EDDSentinel2Google.Sensing_Time >= end_date,
                        EDDSentinel2Google.Platform_ID == platform).group_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).filter(
                            EDDSentinel2Google.Sensing_Time <= start_date,
                            EDDSentinel2Google.Sensing_Time >= end_date,
                            EDDSentinel2Google.Platform_ID == platform).group_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date).asc())
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
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.Invalid == False, EDDSentinel2Google.ARDProduct == True).all()
            elif valid:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.Invalid == False).all()
            elif ard_prod:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.ARDProduct == True).all()
            else:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest).all()
        else:
            if valid and ard_prod:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.Invalid == False, EDDSentinel2Google.ARDProduct == True,
                        EDDSentinel2Google.Platform_ID == platform).all()
            elif valid:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.Invalid == False, EDDSentinel2Google.Platform_ID == platform).all()
            elif ard_prod:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.ARDProduct == True, EDDSentinel2Google.Platform_ID == platform).all()
            else:
                scns = ses.query(EDDSentinel2Google).filter(
                        sqlalchemy.cast(EDDSentinel2Google.Sensing_Time, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel2Google.Platform_ID == platform).all()
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

    def create_scn_date_imgs(self, start_date, end_date, img_size, out_img_dir, img_format, vec_file, vec_lyr, tmp_dir, order_desc=True):
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
            for scn in scns:
                ard_file = eoddutils.findFile(scn.ARDProduct_Path, "*vmsk_sharp_rad_srefdem_stdsref.tif")
                print("\t{}: {} - {}".format(scn.PID, scn.Product_ID, ard_file))
                scn_files.append(ard_file)

            # NIR, SWIR, RED
            bands = '7,10,3'

            scn_date_str = scn_date[0].strftime('%Y%m%d')
            quicklook_img = os.path.join(out_img_dir, "sen2_qklk_{}.{}".format(scn_date_str, out_img_ext))
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

        """
        eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
        # Get the ARD images.
        ard_images = []
        for pid in scn_pids:
            scn = self.get_scn_record(pid)
            ard_file = eoddutils.findFileNone(scn.ARDProduct_Path, "*vmsk_sharp_rad_srefdem_stdsref.tif")
            if ard_file is not None:
                ard_images.append(ard_file)

        if len(ard_images) > 0:
            # NIR, SWIR, RED
            bands = '7,10,3'

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

        query_result = ses.query(EDDSentinel2Google).all()
        db_scn_dict = dict()
        for scn in query_result:
            db_scn_dict[scn.PID] = dict()
            db_scn_dict[scn.PID]['PID'] = scn.PID
            db_scn_dict[scn.PID]['Granule_ID'] = scn.Granule_ID
            db_scn_dict[scn.PID]['Product_ID'] = scn.Product_ID
            db_scn_dict[scn.PID]['Platform_ID'] = scn.Platform_ID
            db_scn_dict[scn.PID]['Datatake_Identifier'] = scn.Datatake_Identifier
            db_scn_dict[scn.PID]['Mgrs_Tile'] = scn.Mgrs_Tile
            db_scn_dict[scn.PID]['Sensing_Time'] = eodd_utils.getDateTimeAsString(scn.Sensing_Time)
            db_scn_dict[scn.PID]['Geometric_Quality_Flag'] = scn.Geometric_Quality_Flag
            db_scn_dict[scn.PID]['Generation_Time'] = eodd_utils.getDateTimeAsString(scn.Generation_Time)
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
        The database table checks are not made for duplicated as records are just appended 
        to the table with a new PID.
        :param input_json_file: input JSON file with the records to be imported.
        :param replace_path_dict: a dictionary of file paths to be updated, if None then ignored.
        """
        db_records = list()
        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        with open(input_json_file) as json_file_obj:
            sensor_rows = json.load(json_file_obj)
            for pid in sensor_rows:
                if 'Platform_ID' not in sensor_rows[pid]:
                    platform = 'Sentinel2'
                    if 'GS2A' in sensor_rows[pid]['Datatake_Identifier']:
                        platform = 'Sentinel2A'
                    elif 'GS2B' in sensor_rows[pid]['Datatake_Identifier']:
                        platform = 'Sentinel2B'
                else:
                    platform=sensor_rows[pid]['Platform_ID']

                db_records.append(EDDSentinel2Google(PID=sensor_rows[pid]['PID'],
                                                     Granule_ID=sensor_rows[pid]['Granule_ID'],
                                                     Product_ID=sensor_rows[pid]['Product_ID'],
                                                     Platform_ID=platform,
                                                     Datatake_Identifier=sensor_rows[pid]['Datatake_Identifier'],
                                                     Mgrs_Tile=sensor_rows[pid]['Mgrs_Tile'],
                                                     Sensing_Time=eodd_utils.getDateFromISOString(sensor_rows[pid]['Sensing_Time']),
                                                     Geometric_Quality_Flag=sensor_rows[pid]['Geometric_Quality_Flag'],
                                                     Generation_Time=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Generation_Time']),
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
                                                     Download_Path=eodd_utils.update_file_path(sensor_rows[pid]['Download_Path'],replace_path_dict),
                                                     Archived=sensor_rows[pid]['Archived'],
                                                     ARDProduct_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_Start_Date']),
                                                     ARDProduct_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_End_Date']),
                                                     ARDProduct=sensor_rows[pid]['ARDProduct'],
                                                     ARDProduct_Path=eodd_utils.update_file_path(sensor_rows[pid]['ARDProduct_Path'], replace_path_dict),
                                                     DCLoaded_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['DCLoaded_Start_Date']),
                                                     DCLoaded_End_Date=eodd_utils.getDateTimeFromISOString( sensor_rows[pid]['DCLoaded_End_Date']),
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

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        raise EODataDownException("Not Implemented")

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
        scn_record = ses.query(EDDSentinel2Google).filter(EDDSentinel2Google.PID == unq_id).one_or_none()

        if scn_record is None:
            ses.close()
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

        if scn_record.DCLoaded:
            # TODO: How to remove from datacube?
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
