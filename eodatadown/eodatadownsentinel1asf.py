#!/usr/bin/env python
"""
EODataDown - a sensor class for Sentinel-1 data downloaded from Alaska Satellite Facility (AFS).
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
# Purpose:  Provides a sensor class for Sentinel-1 data downloaded from ESA.
#
# Credits: Reference was made to the sentinelsat utility during the implementation
#          of this code. https://github.com/sentinelsat/sentinelsat
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 23/09/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import requests
import os
import sys
import datetime
import multiprocessing
import shutil
import importlib
import traceback

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
from eodatadown.eodatadownsentinel1_snap import EODataDownSentinel1ProcessorSensor

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import func

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDSentinel1ASF(Base):
    __tablename__ = "EDDSentinel1ASF"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Scene_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Product_Name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Product_File_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False, unique=True)
    ABS_Orbit = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Rel_Orbit = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Doppler = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Flight_Direction = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Granule_Name = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Granule_Type = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Incidence_Angle = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Look_Direction = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Platform = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Polarization = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Process_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Process_Description = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Process_Level = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Process_Type = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Process_Type_Disp = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Acquisition_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Sensor = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    BeginPosition = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    EndPosition = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Remote_FileName = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Remote_URL_MD5 = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Total_Size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
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


class EDDSentinel1ASFPlugins(Base):
    __tablename__ = "EDDSentinel1ASFPlugins"
    Scene_PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    PlugInName = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Completed = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Success = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Outputs = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)


def _download_scn_asf(params):
    """
    Function which is used with multiprocessing pool object for downloading Sentinel-1 data from ASF.
    :param params:
    :return:
    """
    pid = params[0]
    product_file_id = params[1]
    remote_url = params[2]
    db_info_obj = params[3]
    scn_lcl_dwnld_path = params[4]
    asf_user = params[5]
    asf_pass = params[6]
    success = False

    eodd_wget_downloader = eodatadown.eodatadownutils.EODDWGetDownload()
    start_date = datetime.datetime.now()
    try:
        success = eodd_wget_downloader.downloadFile(remote_url, scn_lcl_dwnld_path, username=asf_user,
                                                    password=asf_pass, try_number="10", time_out="60")
    except Exception as e:
        logger.error("An error has occurred while downloading from ASF: '{}'".format(e))
    end_date = datetime.datetime.now()

    if success and os.path.exists(scn_lcl_dwnld_path):
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == pid).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + product_file_id)
        else:
            query_result.Downloaded = True
            query_result.Download_Start_Date = start_date
            query_result.Download_End_Date = end_date
            query_result.Download_Path = scn_lcl_dwnld_path
            ses.commit()
        ses.close()
        logger.info("Finished download and updated database: {}".format(scn_lcl_dwnld_path))
    else:
        logger.error("Download did not complete, re-run and it should try again: {}".format(scn_lcl_dwnld_path))


class EODataDownSentinel1ASFProcessorSensor (EODataDownSentinel1ProcessorSensor):
    """
    An class which represents a the Sentinel-1 sensor being downloaded from ESA Copernicus Open Access Hub.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "Sentinel1ASF"
        self.db_tab_name = "EDDSentinel1ASF"
        self.base_api_url = "https://api.daac.asf.alaska.edu/services/search/param"

        self.use_roi = False
        self.intersect_vec_file = ''
        self.intersect_vec_lyr = ''
        self.subset_vec_file = ''
        self.subset_vec_lyr = ''
        self.mask_outputs = False
        self.mask_vec_file = ''
        self.mask_vec_lyr = ''
        self.std_vis_img_stch = None

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
            eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
            logger.debug("Testing config file is for 'Sentinel1ASF'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'Sentinel1ASF'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.outImgRes = json_parse_helper.getNumericValue(config_data,
                                                               ["eodatadown", "sensor", "ardparams", "imgres"],
                                                               valid_lower=10.0)
            self.projEPSG = -1
            self.projabbv = ""
            self.out_proj_img_res = -1
            self.out_proj_interp = None
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "proj",
                                                                       "epsg"], 0, 1000000000))
                self.out_proj_img_res = int(json_parse_helper.getNumericValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "proj",
                                                                       "projimgres"], 0.0))
                self.out_proj_interp = json_parse_helper.getStrValue(config_data,
                                                                     ["eodatadown", "sensor", "ardparams",
                                                                      "proj", "interp"],
                                                                     valid_values=["NEAR", "BILINEAR", "CUBIC"])
            self.ardMethod = 'GAMMA'
            if json_parse_helper.doesPathExist(config_data,["eodatadown", "sensor", "ardparams", "software"]):
                self.ardMethod = json_parse_helper.getStrValue(config_data,
                                                               ["eodatadown", "sensor", "ardparams", "software"],
                                                               valid_values=["GAMMA", "SNAP"])
            # Import the class for processing SAR data depending on method requested.
            # Redo import here to overwrite default of SNAP
            if self.ardMethod == 'GAMMA':
                from eodatadown.eodatadownsentinel1_gamma import EODataDownSentinel1ProcessorSensor
            elif self.ardMethod == 'SNAP':
                from eodatadown.eodatadownsentinel1_snap import EODataDownSentinel1ProcessorSensor

            self.use_roi = False
            if json_parse_helper.doesPathExist(config_data,["eodatadown", "sensor", "ardparams", "roi"]):
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
                    self.std_vis_img_stch = json_parse_helper.getStrValue(config_data, ["eodatadown","sensor",
                                                                                        "ardparams","visual",
                                                                                        "stretch_file"])
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths"]):
                self.parse_output_paths_config(config_data["eodatadown"]["sensor"]["paths"])
            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            geo_bounds_lst = json_parse_helper.getListValue(config_data,
                                                            ["eodatadown", "sensor", "download", "geobounds"])
            if not len(geo_bounds_lst) > 0:
                raise EODataDownException("There must be at least 1 geographic boundary given.")

            self.geoBounds = list()
            for geo_bound_json in geo_bounds_lst:
                edd_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                edd_bbox.setNorthLat(json_parse_helper.getNumericValue(geo_bound_json, ["north_lat"], -90, 90))
                edd_bbox.setSouthLat(json_parse_helper.getNumericValue(geo_bound_json, ["south_lat"], -90, 90))
                edd_bbox.setWestLon(json_parse_helper.getNumericValue(geo_bound_json, ["west_lon"], -180, 180))
                edd_bbox.setEastLon(json_parse_helper.getNumericValue(geo_bound_json, ["east_lon"], -180, 180))
                self.geoBounds.append(edd_bbox)

            self.boundsRelation = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "download",
                                                                              "geoboundsrelation"],
                                                                ["intersects", "contains", "iswithin"])
            self.startDate = json_parse_helper.getDateTimeValue(config_data,
                                                                ["eodatadown", "sensor", "download", "startdate"],
                                                                "%Y-%m-%d")
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

            logger.debug("Find ASF Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "asfaccount", "usrpassfile"]):
                usr_pass_file = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "asfaccount", "usrpassfile"])
                if os.path.exists(usr_pass_file):
                    usr_pass_info = eodd_utils.readTextFile2List(usr_pass_file)
                    self.asfUser = usr_pass_info[0]
                    self.asfPass = edd_pass_encoder.unencodePassword(usr_pass_info[1])
                else:
                    raise EODataDownException("The username/password file specified does not exist on the system.")
            else:
                self.asfUser = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "asfaccount", "user"])
                self.asfPass = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "asfaccount", "pass"]))
            logger.debug("Found ASF Account params from config file")

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

        logger.debug("Creating Sentinel1ASF Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def check_http_response(self, response, url):
        """
        Check the HTTP response and raise an exception with appropriate error message
        if request was not successful.
        :param response: the http response object.
        :param url: the URL called.
        :return: boolean as to whether status is successful or otherwise.
        """
        try:
            response.raise_for_status()
            success = True
        except (requests.HTTPError, ValueError):
            success = False
            excpt_msg = "Invalid API response."
            try:
                excpt_msg = response.headers["cause-message"]
            except:
                try:
                    excpt_msg = response.json()["error"]["message"]["value"]
                except:
                    excpt_msg = "Unknown error ('{0}'), check url in a web browser: '{1}'".format(response.reason, url)
            api_error = EODataDownResponseException(excpt_msg, response)
            api_error.__cause__ = None
            raise api_error
        return success

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.debug("Creating HTTP Session Object.")
        session_req = requests.Session()
        session_req.auth = (self.asfUser, self.asfPass)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_req.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug(
            "Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if (not check_from_start) and (ses.query(EDDSentinel1ASF).first() is not None):
            query_date = ses.query(EDDSentinel1ASF).order_by(EDDSentinel1ASF.BeginPosition.desc()).first().BeginPosition
        logger.info("Query with start at date: " + str(query_date))

        # Get the next PID value to ensure increment
        c_max_pid = ses.query(func.max(EDDSentinel1ASF.PID).label("max_pid")).one().max_pid
        if c_max_pid is None:
            n_max_pid = 0
        else:
            n_max_pid = c_max_pid + 1

        str_start_datetime = query_date.isoformat()+"UTC"
        str_now_datetime = datetime.datetime.utcnow().isoformat()+"UTC"
        query_str_date = "start="+str_start_datetime+"\&end="+str_now_datetime
        query_str_product = "processingLevel=GRD_HD"
        query_str_platform = "platform=SA,SB"
        query_datetime = datetime.datetime.now()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        eoed_utils = eodatadown.eodatadownutils.EODataDownUtils()

        new_scns_avail = False
        db_records = list()
        for geo_bound in self.geoBounds:
            csv_poly = geo_bound.getCSVPolygon()
            logger.info("Checking for available scenes for \"" + csv_poly + "\"")
            query_str_geobound = "polygon="+ csv_poly
            query_str = query_str_geobound + "&" + query_str_platform + "&" + query_str_product + "&" + query_str_date + "&output=json"
            query_url = self.base_api_url + "?" + query_str
            logger.debug("Going to use the following URL: " + query_url)
            response = session_req.get(query_url, auth=session_req.auth)
            if self.check_http_response(response, query_url):
                rsp_json = response.json()[0]
                product_file_ids = dict()
                for scn_json in rsp_json:
                    product_file_id_val = json_parse_helper.getStrValue(scn_json, ["product_file_id"])
                    query_rtn = ses.query(EDDSentinel1ASF).filter(
                            EDDSentinel1ASF.Product_File_ID == product_file_id_val).all()
                    if (len(query_rtn) == 0) and (not(product_file_id_val in product_file_ids)):
                        product_file_ids[product_file_id_val] = True
                        scene_id_val = json_parse_helper.getStrValue(scn_json, ["sceneId"])
                        product_name_val = json_parse_helper.getStrValue(scn_json, ["productName"])
                        absolute_orbit_val = int(json_parse_helper.getNumericValue(scn_json, ["absoluteOrbit"]))
                        relative_orbit_val = int(json_parse_helper.getNumericValue(scn_json, ["relativeOrbit"]))
                        doppler_val = int(json_parse_helper.getNumericValue(scn_json, ["doppler"]))
                        flight_direction_val = json_parse_helper.getStrValue(scn_json, ["flightDirection"])
                        granule_name_val = json_parse_helper.getStrValue(scn_json, ["granuleName"])
                        granule_type_val = json_parse_helper.getStrValue(scn_json, ["granuleType"])
                        incidence_angle_strval = json_parse_helper.getStrValue(scn_json, ["incidenceAngle"])
                        if (incidence_angle_strval == "") or (incidence_angle_strval == "NA") or (incidence_angle_strval is None):
                            incidence_angle_val = None
                        elif eoed_utils.isNumber(incidence_angle_strval):
                            incidence_angle_val = float(incidence_angle_strval)
                        else:
                            incidence_angle_val = None
                        look_direction_val = json_parse_helper.getStrValue(scn_json, ["lookDirection"])
                        platform_val = json_parse_helper.getStrValue(scn_json, ["platform"])
                        polarization_val = json_parse_helper.getStrValue(scn_json, ["polarization"])
                        processing_date_val = json_parse_helper.getDateTimeValue(scn_json, ["processingDate"], ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"])
                        processing_description_val = json_parse_helper.getStrValue(scn_json, ["processingDescription"])
                        processing_level_val = json_parse_helper.getStrValue(scn_json, ["processingLevel"])
                        processing_type_val = json_parse_helper.getStrValue(scn_json, ["processingType"])
                        processing_type_disp_val = json_parse_helper.getStrValue(scn_json, ["processingTypeDisplay"])
                        scene_date_val = json_parse_helper.getDateTimeValue(scn_json, ["sceneDate"], ["%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S"])
                        sensor_val = json_parse_helper.getStrValue(scn_json, ["sensor"])
                        start_time_val = json_parse_helper.getDateTimeValue(scn_json, ["startTime"], ["%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S"])
                        stop_time_val = json_parse_helper.getDateTimeValue(scn_json, ["stopTime"], ["%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S"])
                        footprint_wkt_val = json_parse_helper.getStrValue(scn_json, ["stringFootprint"])
                        edd_footprint_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                        edd_footprint_bbox.parseWKTPolygon(footprint_wkt_val)
                        download_url_val = json_parse_helper.getStrValue(scn_json, ["downloadUrl"])
                        file_name_val = json_parse_helper.getStrValue(scn_json, ["fileName"])
                        download_file_size_mb_strval = json_parse_helper.getStrValue(scn_json, ["sizeMB"])
                        if eoed_utils.isNumber(download_file_size_mb_strval):
                            download_file_size_mb_val = float(download_file_size_mb_strval)
                        else:
                            logger.debug("sizeMB is not numeric with value '{}'".format(download_file_size_mb_strval))
                            download_file_size_mb_val = None


                        db_records.append(EDDSentinel1ASF(PID=n_max_pid, Scene_ID=scene_id_val,
                                                          Product_Name=product_name_val, Product_File_ID=product_file_id_val,
                                                          ABS_Orbit=absolute_orbit_val, Rel_Orbit=relative_orbit_val,
                                                          Doppler=doppler_val, Flight_Direction=flight_direction_val,
                                                          Granule_Name=granule_name_val, Granule_Type=granule_type_val,
                                                          Incidence_Angle=incidence_angle_val, Look_Direction=look_direction_val,
                                                          Platform=platform_val, Polarization=polarization_val,
                                                          Process_Date=processing_date_val, Process_Description=processing_description_val,
                                                          Process_Level=processing_level_val, Process_Type=processing_type_val,
                                                          Process_Type_Disp=processing_type_disp_val, Acquisition_Date=scene_date_val,
                                                          Sensor=sensor_val, BeginPosition=start_time_val, EndPosition=stop_time_val,
                                                          North_Lat=edd_footprint_bbox.getNorthLat(), South_Lat=edd_footprint_bbox.getSouthLat(),
                                                          East_Lon=edd_footprint_bbox.getEastLon(), West_Lon=edd_footprint_bbox.getWestLon(),
                                                          Remote_URL=download_url_val, Remote_FileName=file_name_val,
                                                          Total_Size=download_file_size_mb_val, Query_Date=query_datetime))
                        n_max_pid = n_max_pid + 1

                if len(db_records) > 0:
                    logger.debug("Writing records to the database.")
                    ses.add_all(db_records)
                    ses.commit()
                    logger.debug("Written and committed records to the database.")
                    new_scns_avail = True

        ses.commit()
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
            import rsgislib
            import rsgislib.vectorutils
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scenes which need downloading.")

            if all_scns:
                scns = ses.query(EDDSentinel1ASF).order_by(EDDSentinel1ASF.Acquisition_Date.asc()).all()
            else:
                scns = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == False).order_by(
                        EDDSentinel1ASF.Acquisition_Date.asc()).all()

            if scns is not None:
                eodd_vec_utils = eodatadown.eodatadownutils.EODDVectorUtils()
                vec_idx, geom_lst = eodd_vec_utils.create_rtree_index(self.scn_intersect_vec_file,
                                                                      self.scn_intersect_vec_lyr)

                for scn in scns:
                    logger.debug("Check Scene '{}' to check for intersection".format(scn.PID))
                    rsgis_utils = rsgislib.RSGISPyUtils()
                    north_lat = scn.North_Lat
                    south_lat = scn.South_Lat
                    east_lon = scn.East_Lon
                    west_lon = scn.West_Lon
                    # (xMin, xMax, yMin, yMax)
                    scn_bbox = [west_lon, east_lon, south_lat, north_lat]

                    intersect_vec_epsg = rsgis_utils.getProjEPSGFromVec(self.scn_intersect_vec_file,
                                                                        self.scn_intersect_vec_lyr)
                    if intersect_vec_epsg != 4326:
                        scn_bbox = rsgis_utils.reprojBBOX_epsg(scn_bbox, 4326, intersect_vec_epsg)

                    has_scn_intersect = eodd_vec_utils.bboxIntersectsIndex(vec_idx, geom_lst, scn_bbox)
                    if not has_scn_intersect:
                        logger.info("Removing scene {} from Sentinel-1 as it does not intersect.".format(scn.PID))
                        ses.query(EDDSentinel1ASF.PID).filter(EDDSentinel1ASF.PID == scn.PID).delete()
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
        query_result = ses.query(EDDSentinel1ASF).order_by(EDDSentinel1ASF.Acquisition_Date.asc()).all()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == False).filter(
            EDDSentinel1ASF.Remote_URL is not None).order_by(EDDSentinel1ASF.Acquisition_Date.asc()).all()

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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one()
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

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id,
                                                         EDDSentinel1ASF.Downloaded == False).filter(
                                                         EDDSentinel1ASF.Remote_URL is not None).all()
        ses.close()
        success = False
        if query_result is not None:
            if len(query_result) == 1:
                record = query_result[0]
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}".format(record.Product_File_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.Remote_FileName
                _download_scn_asf([record.PID, record.Product_File_ID, record.Remote_URL, self.db_info_obj,
                                     os.path.join(scn_lcl_dwnld_path, out_filename), self.asfUser, self.asfPass])
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

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == False).filter(
                                                         EDDSentinel1ASF.Remote_URL is not None).all()
        dwnld_params = list()
        downloaded_new_scns = False
        if query_result is not None:
            for record in query_result:
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, "{}_{}".format(record.Product_File_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.Remote_FileName
                downloaded_new_scns = True
                dwnld_params.append([record.PID, record.Product_File_ID, record.Remote_URL, self.db_info_obj,
                                     os.path.join(scn_lcl_dwnld_path, out_filename), self.asfUser, self.asfPass])
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_scn_asf, dwnld_params)
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == True,
                                                         EDDSentinel1ASF.ARDProduct == False,
                                                         EDDSentinel1ASF.Invalid == False).order_by(
                                                         EDDSentinel1ASF.Acquisition_Date.asc()).all()

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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one()
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

        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id,
                                                         EDDSentinel1ASF.Downloaded == True,
                                                         EDDSentinel1ASF.ARDProduct == False).one_or_none()

        proj_epsg = None
        if self.ardProjDefined:
            proj_epsg = self.projEPSG

        if query_result is not None:
            start_date = datetime.datetime.now()
            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            wrk_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(wrk_ard_path):
                os.mkdir(wrk_ard_path)

            logger.debug("Create info for running ARD analysis for scene: {}".format(query_result.Product_File_ID))
            final_ard_scn_path = os.path.join(self.ardFinalPath,
                                              "{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(final_ard_scn_path):
                os.mkdir(final_ard_scn_path)

            tmp_ard_scn_path = os.path.join(tmp_ard_path,
                                            "{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(tmp_ard_scn_path):
                os.mkdir(tmp_ard_scn_path)

            wrk_ard_scn_path = os.path.join(wrk_ard_path,
                                            "{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(wrk_ard_scn_path):
                os.mkdir(wrk_ard_scn_path)

            pols = list()
            if 'VV' in query_result.Polarization:
                pols.append('VV')
            if 'VH' in query_result.Polarization:
                pols.append('VH')
            zip_file = eodd_utils.findFilesRecurse(query_result.Download_Path, '.zip')
            if len(zip_file) == 1:
                zip_file = zip_file[0]
            else:
                logger.error("Could not find unique zip file for Sentinel-1 zip: PID = {}".format(query_result.PID))
                raise EODataDownException(
                    "Could not find unique zip file for Sentinel-1 zip: PID = {}".format(query_result.PID))
            success_process_ard = self.convertSen1ARD(zip_file, final_ard_scn_path, wrk_ard_scn_path, tmp_ard_scn_path,
                                                      self.demFile, self.outImgRes, pols, proj_epsg, self.projabbv,
                                                      self.out_proj_img_res, self.out_proj_interp, self.use_roi,
                                                      self.intersect_vec_file, self.intersect_vec_lyr,
                                                      self.subset_vec_file, self.subset_vec_lyr, self.mask_outputs,
                                                      self.mask_vec_file, self.mask_vec_lyr)
            end_date = datetime.datetime.now()
            if success_process_ard:
                query_result.ARDProduct = True
                query_result.ARDProduct_Start_Date = start_date
                query_result.ARDProduct_End_Date = end_date
                query_result.ARDProduct_Path = final_ard_scn_path
                ses.commit()
            else:
                query_result.ARDProduct = False
                query_result.ARDProduct_Start_Date = start_date
                query_result.ARDProduct_End_Date = end_date
                query_result.Invalid = True
                ses.commit()

            if os.path.exists(tmp_ard_scn_path):
                shutil.rmtree(tmp_ard_scn_path)
            if os.path.exists(wrk_ard_scn_path):
                shutil.rmtree(wrk_ard_scn_path)

        ses.close()


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
            raise EODataDownException("The ARD final path '{}' does not exist, please create and run again.",format(self.ardFinalPath))

        if not os.path.exists(self.ardProdWorkPath):
            raise EODataDownException("The ARD working path '{}' does not exist, please create and run again.".format(self.ardProdWorkPath))

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The ARD tmp path '{}' does not exist, please create and run again.".format(self.ardProdTmpPath))

        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == True,
                                                         EDDSentinel1ASF.ARDProduct == False,
                                                         EDDSentinel1ASF.Invalid == False).all()

        proj_epsg = None
        if self.ardProjDefined:
            proj_epsg = self.projEPSG

        if query_result is not None:
            logger.debug("Create the specific output directories for the ARD processing.")

            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            wrk_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(wrk_ard_path):
                os.mkdir(wrk_ard_path)

            for record in query_result:
                start_date = datetime.datetime.now()
                # Check if have already processed but not with EODD
                

                final_ard_scn_path = os.path.join(self.ardFinalPath, "{}_{}".format(record.Product_File_ID, record.PID))
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, "{}_{}".format(record.Product_File_ID, record.PID))
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                wrk_ard_scn_path = os.path.join(wrk_ard_path, "{}_{}".format(record.Product_File_ID, record.PID))
                if not os.path.exists(wrk_ard_scn_path):
                    os.mkdir(wrk_ard_scn_path)

                pols = list()
                if 'VV' in record.Polarization:
                    pols.append('VV')
                if 'VH' in record.Polarization:
                    pols.append('VH')
                zip_file = eodd_utils.findFilesRecurse(record.Download_Path, '.zip')
                if len(zip_file) == 1:
                    zip_file = zip_file[0]
                else:
                    logger.error("Could not find unique zip file for Sentinel-1 zip: PID = {} in {}".format(record.PID, record.Download_Path))
                    raise EODataDownException(
                        "Could not find unique zip file for Sentinel-1 zip: PID = {} in {}".format(record.PID, record.Download_Path))
                success_process_ard = self.convertSen1ARD(zip_file, final_ard_scn_path, wrk_ard_scn_path,
                                                          tmp_ard_scn_path, self.demFile, self.outImgRes, pols,
                                                          proj_epsg, self.projabbv, self.out_proj_img_res,
                                                          self.out_proj_interp, self.use_roi,
                                                          self.intersect_vec_file, self.intersect_vec_lyr,
                                                          self.subset_vec_file, self.subset_vec_lyr, self.mask_outputs,
                                                          self.mask_vec_file, self.mask_vec_lyr)
                end_date = datetime.datetime.now()
                if success_process_ard:
                    record.ARDProduct = True
                    record.ARDProduct_Start_Date = start_date
                    record.ARDProduct_End_Date = end_date
                    record.ARDProduct_Path = final_ard_scn_path
                    ses.commit()
                else:
                    record.ARDProduct = False
                    record.ARDProduct_Start_Date = start_date
                    record.ARDProduct_End_Date = end_date
                    record.Invalid = True
                    ses.commit()

                if os.path.exists(tmp_ard_scn_path):
                    shutil.rmtree(tmp_ard_scn_path)
                if os.path.exists(wrk_ard_scn_path):
                    shutil.rmtree(wrk_ard_scn_path)
        ses.close()

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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.ARDProduct == True,
                                                         EDDSentinel1ASF.DCLoaded == loaded).order_by(
                                                         EDDSentinel1ASF.Acquisition_Date.asc()).all()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one()
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
        raise EODataDownException("Not implemented.")

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
            query_result = ses.query(EDDSentinel1ASF).filter(
                sqlalchemy.or_(
                    EDDSentinel1ASF.ExtendedInfo.is_(None),
                    sqlalchemy.not_(EDDSentinel1ASF.ExtendedInfo.has_key('quicklook'))),
                EDDSentinel1ASF.Invalid == False,
                EDDSentinel1ASF.ARDProduct == True).order_by(EDDSentinel1ASF.Acquisition_Date.asc()).all()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one()
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
            raise EODataDownException("The quicklook path '{}' does not exist or not provided, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
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
            ard_img_file = eodd_utils.findFile(ard_img_path, '*dB*.tif')

            out_quicklook_path = os.path.join(self.quicklookPath,
                                              "{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(out_quicklook_path):
                os.mkdir(out_quicklook_path)

            tmp_quicklook_path = os.path.join(self.ardProdTmpPath,
                                              "quicklook_{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(tmp_quicklook_path):
                os.mkdir(tmp_quicklook_path)

            # VV, VH, VV/VH
            bands = '1,2,3'

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
            query_result = ses.query(EDDSentinel1ASF).filter(
                sqlalchemy.or_(
                    EDDSentinel1ASF.ExtendedInfo.is_(None),
                    sqlalchemy.not_(EDDSentinel1ASF.ExtendedInfo.has_key('tilecache'))),
                EDDSentinel1ASF.Invalid == False,
                EDDSentinel1ASF.ARDProduct == True).order_by(EDDSentinel1ASF.Acquisition_Date.asc()).all()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
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
            ard_img_file = eodd_utils.findFile(ard_img_path, '*dB*.tif')

            out_tilecache_dir = os.path.join(self.tilecachePath,
                                            "{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(out_tilecache_dir):
                os.mkdir(out_tilecache_dir)

            out_visual_gtiff = os.path.join(out_tilecache_dir,
                                            "{}_{}_vis.tif".format(query_result.Product_File_ID, query_result.PID))

            tmp_tilecache_path = os.path.join(self.ardProdTmpPath,
                                            "tilecache_{}_{}".format(query_result.Product_File_ID, query_result.PID))
            if not os.path.exists(tmp_tilecache_path):
                os.mkdir(tmp_tilecache_path)

            # VV, VH, VV/VH
            bands = '1,2,3'

            import rsgislib.tools.visualisation
            rsgislib.tools.visualisation.createWebTilesVisGTIFFImg(ard_img_file, bands, out_tilecache_dir,
                                                                   out_visual_gtiff, zoomLevels='2-12',
                                                                   img_stats_msk=None, img_msk_vals=1,
                                                                   stretch_file=self.std_vis_img_stch,
                                                                   tmp_dir=tmp_tilecache_path, webview=True,
                                                                   scale=50)

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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).all()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).all()
        ses.close()
        scn_record = None
        if query_result is not None:
            if len(query_result) == 1:
                scn_record = query_result[0]
            else:
                logger.error(
                        "PID {0} has returned more than 1 scene - must be unique something really wrong.".format(
                            unq_id))
                raise EODataDownException(
                        "There was more than 1 scene which has been found - something has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return copy.copy(scn_record.BeginPosition)

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

            usr_analysis_keys = self.get_usr_analysis_keys()

            query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Invalid == False,
                                                             EDDSentinel1ASF.ARDProduct == True).order_by(
                                                             EDDSentinel1ASF.Acquisition_Date.asc()).all()

            for scn in query_result:
                scn_plgin_db_objs = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == scn.PID).all()
                if (scn_plgin_db_objs is None) or (not scn_plgin_db_objs):
                    scns2runusranalysis.append(scn.PID)
                else:
                    for plugin_key in usr_analysis_keys:
                        plugin_completed = False
                        for plgin_db_obj in scn_plgin_db_objs:
                            if (plgin_db_obj.PlugInName == plugin_key) and plgin_db_obj.Completed:
                                plugin_completed = True
                                break
                        if not plugin_completed:
                            scns2runusranalysis.append(scn.PID)
                            break
            ses.close()
            logger.debug("Closed the database session.")
        return scns2runusranalysis

    def has_scn_usr_analysis(self, unq_id):
        usr_plugins_calcd = True
        logger.debug("Going to test whether there are plugins to execute.")
        if self.calc_scn_usr_analysis():
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scene.")
            query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
            if query_result is None:
                raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))

            scn_plgin_db_objs = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == unq_id).all()
            ses.close()
            logger.debug("Closed the database session.")
            if (scn_plgin_db_objs is None) or (not scn_plgin_db_objs):
                usr_plugins_calcd = False
            else:
                usr_analysis_keys = self.get_usr_analysis_keys()
                for plugin_key in usr_analysis_keys:
                    plugin_completed = False
                    for plgin_db_obj in scn_plgin_db_objs:
                        if (plgin_db_obj.PlugInName == plugin_key) and plgin_db_obj.Completed:
                            plugin_completed = True
                            break
                    if not plugin_completed:
                        usr_plugins_calcd = False
                        break
        return usr_plugins_calcd

    def run_usr_analysis(self, unq_id):
        if self.calc_scn_usr_analysis():
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
                logger.debug("Using plugin '{}' from '{}' with key '{}'.".format(plugin_cls_name,
                                                                                 plugin_module_name,
                                                                                 plugin_key))

                # Try to read any plugin parameters to be passed to the plugin when instantiated.
                if "params" in plugin_info:
                    plugin_cls_inst.set_users_param(plugin_info["params"])
                    logger.debug("Read plugin params and passed to plugin.")

                logger.debug("Creating Database Engine and Session.")
                db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                ses = session_sqlalc()
                logger.debug("Perform query to find scene.")
                scn_db_obj = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
                if scn_db_obj is None:
                    raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
                logger.debug("Perform query to find scene in plugin DB.")
                plgin_db_obj = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == unq_id,
                                                                        EDDSentinel1ASFPlugins.PlugInName == plugin_key).one_or_none()
                plgin_db_objs = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == unq_id).all()
                ses.close()
                logger.debug("Closed the database session.")

                plgins_dict = dict()
                for plgin_obj in plgin_db_objs:
                    plgins_dict[plgin_obj.PlugInName] = plgin_obj

                plugin_completed = True
                exists_in_db = True
                if plgin_db_obj is None:
                    plugin_completed = False
                    exists_in_db = False
                elif not plgin_db_obj.Completed:
                    plugin_completed = False

                if not plugin_completed:
                    start_time = datetime.datetime.now()
                    try:
                        completed = True
                        error_occurred = False
                        plg_success, out_dict, plg_outputs = plugin_cls_inst.perform_analysis(scn_db_obj, self, plgins_dict)
                    except Exception as e:
                        plg_success = False
                        plg_outputs = False
                        error_occurred = True
                        out_dict = dict()
                        out_dict['error'] = str(e)
                        out_dict['traceback'] = traceback.format_exc()
                        completed = False
                    end_time = datetime.datetime.now()
                    if plg_success:
                        logger.debug("The plugin analysis has been completed - SUCCESSFULLY.")
                    else:
                        logger.debug("The plugin analysis has been completed - UNSUCCESSFULLY.")

                    if exists_in_db:
                        logger.debug("Creating Database Engine and Session.")
                        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                        ses = session_sqlalc()
                        logger.debug("Perform query to find scene in plugin DB.")
                        plgin_db_obj = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == unq_id,
                                                                                EDDSentinel1ASFPlugins.PlugInName == plugin_key).one_or_none()

                        if plgin_db_obj is None:
                            raise EODataDownException(
                                    "Do not know what has happened, scene plugin instance not found but was earlier.")

                        plgin_db_obj.Success = plg_success
                        plgin_db_obj.Completed = completed
                        plgin_db_obj.Outputs = plg_outputs
                        plgin_db_obj.Error = error_occurred
                        plgin_db_obj.Start_Date = start_time
                        plgin_db_obj.End_Date = end_time
                        if out_dict is not None:
                            plgin_db_obj.ExtendedInfo = out_dict
                            flag_modified(plgin_db_obj, "ExtendedInfo")
                        ses.add(plgin_db_obj)
                        ses.commit()
                        logger.debug("Committed updated record to database - PID {}.".format(unq_id))
                        ses.close()
                        logger.debug("Closed the database session.")
                    else:
                        plgin_db_obj = EDDSentinel1ASFPlugins(Scene_PID=scn_db_obj.PID, PlugInName=plugin_key,
                                                              Start_Date=start_time, End_Date=end_time,
                                                              Completed=completed, Success=plg_success,
                                                              Error=error_occurred, Outputs=plg_outputs)
                        if out_dict is not None:
                            plgin_db_obj.ExtendedInfo = out_dict

                        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                        ses = session_sqlalc()
                        ses.add(plgin_db_obj)
                        ses.commit()
                        logger.debug("Committed new record to database - PID {}.".format(unq_id))
                        ses.close()
                        logger.debug("Closed the database session.")
                else:
                    logger.debug("The plugin '{}' from '{}' has already been run so will not be run again".format(
                            plugin_cls_name, plugin_module_name))

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
            reset_all_plgins = False
            if plgin_lst is None:
                logger.debug("A list of plugins to reset has not been provided so populating that list with all plugins.")
                plgin_lst = self.get_usr_analysis_keys()
                reset_all_plgins = True
            logger.debug("There are {} plugins to reset".format(len(plgin_lst)))

            if len(plgin_lst) > 0:
                logger.debug("Creating Database Engine and Session.")
                db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
                session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
                ses = session_sqlalc()

                if scn_pid is None:
                    logger.debug("No scene PID has been provided so resetting all the scenes.")
                    for plgin_key in plgin_lst:
                        ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.PlugInName == plgin_key).delete(synchronize_session=False)
                        ses.commit()
                else:
                    logger.debug("Scene PID {} has been provided so resetting.".format(scn_pid))
                    if reset_all_plgins:
                        ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == scn_pid).delete(synchronize_session=False)
                    else:
                        scn_plgin_db_objs = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.Scene_PID == scn_pid).all()
                        if (scn_plgin_db_objs is None) and (not scn_plgin_db_objs):
                            raise EODataDownException("Scene ('{}') could not be found in database".format(scn_pid))
                        for plgin_db_obj in scn_plgin_db_objs:
                            if plgin_db_obj.PlugInName in plgin_lst:
                                ses.delete(plgin_db_obj)
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()
        if query_result is None:
            raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
        unq_name = "{}_{}".format(query_result.Product_File_ID, query_result.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return unq_name

    def get_scn_unq_name_record(self, scn_record):
        """
        A function which returns a name which will be unique using the scene record object passed to the function.

        :param scn_record: the database dict like object representing the scene.
        :return: string with a unique name.

        """
        unq_name = "{}_{}".format(scn_record.Product_File_ID, scn_record.PID)
        return unq_name

    def find_unique_platforms(self):
        """
        A function which returns a list of unique platforms within the database (e.g., Sentinel1A or Sentinel1B).
        :return: list of strings.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        platforms = ses.query(EDDSentinel1ASF.Platform).group_by(EDDSentinel1ASF.Platform)
        ses.close()
        return platforms

    def query_scn_records_date_count(self, start_date, end_date, valid=True, cloud_thres=None):
        """
        A function which queries the database to find scenes within a specified date range
        and returns the number of records available.

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :param cloud_thres: Sentinel-1 isn't effected by cloud so this parameter is ignored.
        :return: count of records available
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if valid:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                       EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                       EDDSentinel1ASF.Invalid == False,
                                                       EDDSentinel1ASF.ARDProduct == True).count()
        else:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                       EDDSentinel1ASF.Acquisition_Date >= end_date).count()
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
        :param cloud_thres: Sentinel-1 isn't effected by cloud so this parameter is ignored.
        :return: list of database records
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if valid:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                                    EDDSentinel1ASF.Invalid == False,
                                                                    EDDSentinel1ASF.ARDProduct == True).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                                    EDDSentinel1ASF.Invalid == False,
                                                                    EDDSentinel1ASF.ARDProduct == True).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc()).all()
        else:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date >= end_date).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date >= end_date).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc()).all()
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
        :param cloud_thres: Sentinel-1 isn't effected by cloud so this parameter is ignored.
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
        if valid:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                       EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                       EDDSentinel1ASF.Invalid == False,
                                                       EDDSentinel1ASF.ARDProduct == True).filter(
                                                       (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                       (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                       (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                       (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).count()
        else:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                       EDDSentinel1ASF.Acquisition_Date >= end_date).filter(
                                                       (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                       (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                       (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                       (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).count()
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
        :param cloud_thres: Sentinel-1 isn't effected by cloud so this parameter is ignored.
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
        if valid:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                 EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                                 EDDSentinel1ASF.Invalid == False,
                                                                 EDDSentinel1ASF.ARDProduct == True).filter(
                                                                 (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                                 (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                                 (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                                 (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                 EDDSentinel1ASF.Acquisition_Date >= end_date,
                                                                 EDDSentinel1ASF.Invalid == False,
                                                                 EDDSentinel1ASF.ARDProduct == True).filter(
                                                                 (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                                 (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                                 (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                                 (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc()).all()
        else:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                 EDDSentinel1ASF.Acquisition_Date >= end_date).filter(
                                                                 (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                                 (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                                 (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                                 (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date <= start_date,
                                                                 EDDSentinel1ASF.Acquisition_Date >= end_date).filter(
                                                                 (bbox[east_lon_idx] > EDDSentinel1ASF.West_Lon),
                                                                 (EDDSentinel1ASF.East_Lon > bbox[west_lon_idx]),
                                                                 (bbox[north_lat_idx] > EDDSentinel1ASF.South_Lat),
                                                                 (EDDSentinel1ASF.North_Lat > bbox[south_lat_idx])).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc()).all()
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
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                        EDDSentinel1ASF.Acquisition_Date <= start_date,
                        EDDSentinel1ASF.Acquisition_Date >= end_date,
                        EDDSentinel1ASF.Invalid == False).group_by(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date,
                            EDDSentinel1ASF.Invalid == False).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                        EDDSentinel1ASF.Acquisition_Date <= start_date,
                        EDDSentinel1ASF.Acquisition_Date >= end_date).group_by(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).asc())
        else:
            if valid:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date,
                            EDDSentinel1ASF.Invalid == False,
                            EDDSentinel1ASF.Platform == platform).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date,
                            EDDSentinel1ASF.Invalid == False,
                            EDDSentinel1ASF.Platform == platform).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).asc())
            else:
                if order_desc:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date,
                            EDDSentinel1ASF.Platform == platform).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).desc())
                else:
                    scn_dates = ses.query(sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).filter(
                            EDDSentinel1ASF.Acquisition_Date <= start_date,
                            EDDSentinel1ASF.Acquisition_Date >= end_date,
                            EDDSentinel1ASF.Platform == platform).group_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date)).order_by(
                            sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date).asc())
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
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.Invalid == False, EDDSentinel1ASF.ARDProduct == True).all()
            elif valid:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.Invalid == False).all()
            elif ard_prod:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.ARDProduct == True).all()
            else:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest).all()
        else:
            if valid and ard_prod:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.Invalid == False, EDDSentinel1ASF.ARDProduct == True,
                        EDDSentinel1ASF.Platform == platform).all()
            elif valid:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.Invalid == False, EDDSentinel1ASF.Platform == platform).all()
            elif ard_prod:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.ARDProduct == True, EDDSentinel1ASF.Platform == platform).all()
            else:
                scns = ses.query(EDDSentinel1ASF).filter(
                        sqlalchemy.cast(EDDSentinel1ASF.Acquisition_Date, sqlalchemy.Date) == date_of_interest,
                        EDDSentinel1ASF.Platform == platform).all()
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
                ard_file = eoddutils.findFile(scn.ARDProduct_Path, "*dB*.tif")
                print("\t{}: {} - {}".format(scn.PID, scn.Scene_ID, ard_file))
                scn_files.append(ard_file)

            # VV, VH, VV/VH
            bands = '1,2,3'

            scn_date_str = scn_date[0].strftime('%Y%m%d')
            quicklook_img = os.path.join(out_img_dir, "sen1_qklk_{}.{}".format(scn_date_str, out_img_ext))
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
            ard_file = eoddutils.findFileNone(scn.ARDProduct_Path, "*dB*.tif")
            if ard_file is not None:
                ard_images.append(ard_file)

        if len(ard_images) > 0:
            # VV, VH, VV/VH
            bands = '1,2,3'

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

        query_result = ses.query(EDDSentinel1ASF).all()
        db_scn_dict = dict()
        for scn in query_result:
            db_scn_dict[scn.PID] = dict()
            db_scn_dict[scn.PID]['PID'] = scn.PID
            db_scn_dict[scn.PID]['Scene_ID'] = scn.Scene_ID
            db_scn_dict[scn.PID]['Product_Name'] = scn.Product_Name
            db_scn_dict[scn.PID]['Product_File_ID'] = scn.Product_File_ID
            db_scn_dict[scn.PID]['ABS_Orbit'] = scn.ABS_Orbit
            db_scn_dict[scn.PID]['Rel_Orbit'] = scn.Rel_Orbit
            db_scn_dict[scn.PID]['Doppler'] = scn.Doppler
            db_scn_dict[scn.PID]['Flight_Direction'] = scn.Flight_Direction
            db_scn_dict[scn.PID]['Granule_Name'] = scn.Granule_Name
            db_scn_dict[scn.PID]['Granule_Type'] = scn.Granule_Type
            db_scn_dict[scn.PID]['Incidence_Angle'] = scn.Incidence_Angle
            db_scn_dict[scn.PID]['Look_Direction'] = scn.Look_Direction
            db_scn_dict[scn.PID]['Platform'] = scn.Platform
            db_scn_dict[scn.PID]['Polarization'] = scn.Polarization
            db_scn_dict[scn.PID]['Process_Date'] = eodd_utils.getDateTimeAsString(scn.Process_Date)
            db_scn_dict[scn.PID]['Process_Description'] = scn.Process_Description
            db_scn_dict[scn.PID]['Process_Level'] = scn.Process_Level
            db_scn_dict[scn.PID]['Process_Type'] = scn.Process_Type
            db_scn_dict[scn.PID]['Process_Type_Disp'] = scn.Process_Type_Disp
            db_scn_dict[scn.PID]['Acquisition_Date'] = eodd_utils.getDateTimeAsString(scn.Acquisition_Date)
            db_scn_dict[scn.PID]['Sensor'] = scn.Sensor
            db_scn_dict[scn.PID]['BeginPosition'] = eodd_utils.getDateTimeAsString(scn.BeginPosition)
            db_scn_dict[scn.PID]['EndPosition'] = eodd_utils.getDateTimeAsString(scn.EndPosition)
            db_scn_dict[scn.PID]['North_Lat'] = scn.North_Lat
            db_scn_dict[scn.PID]['South_Lat'] = scn.South_Lat
            db_scn_dict[scn.PID]['East_Lon'] = scn.East_Lon
            db_scn_dict[scn.PID]['West_Lon'] = scn.West_Lon
            db_scn_dict[scn.PID]['Remote_URL'] = scn.Remote_URL
            db_scn_dict[scn.PID]['Remote_FileName'] = scn.Remote_FileName
            db_scn_dict[scn.PID]['Remote_URL_MD5'] = scn.Remote_URL_MD5
            db_scn_dict[scn.PID]['Total_Size'] = scn.Total_Size
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

        db_plgin_dict = dict()
        if self.calc_scn_usr_analysis():
            plugin_keys = self.get_usr_analysis_keys()
            for plgin_key in plugin_keys:
                query_result = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.PlugInName == plgin_key).all()
                db_plgin_dict[plgin_key] = dict()
                for scn in query_result:
                    db_plgin_dict[plgin_key][scn.Scene_PID] = dict()
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Scene_PID'] = scn.Scene_PID
                    db_plgin_dict[plgin_key][scn.Scene_PID]['PlugInName'] = scn.PlugInName
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Start_Date'] = eodd_utils.getDateTimeAsString(
                            scn.Start_Date)
                    db_plgin_dict[plgin_key][scn.Scene_PID]['End_Date'] = eodd_utils.getDateTimeAsString(scn.End_Date)
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Completed'] = scn.Completed
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Success'] = scn.Success
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Outputs'] = scn.Outputs
                    db_plgin_dict[plgin_key][scn.Scene_PID]['Error'] = scn.Error
                    db_plgin_dict[plgin_key][scn.Scene_PID]['ExtendedInfo'] = scn.ExtendedInfo
        ses.close()

        fnl_out_dict = dict()
        fnl_out_dict['scn_db'] = db_scn_dict
        if db_plgin_dict:
            fnl_out_dict['plgin_db'] = db_plgin_dict

        with open(out_json_file, 'w') as outfile:
            json.dump(fnl_out_dict, outfile, indent=4, separators=(',', ': '), ensure_ascii=False)

    def import_sensor_db(self, input_json_file, replace_path_dict=None):
        """
        This function imports from the database records from the specified input JSON file.
        The database table checks are not made for duplicated as records are just appended 
        to the table with a new PID.
        :param input_json_file: input JSON file with the records to be imported.
        :param replace_path_dict: a dictionary of file paths to be updated, if None then ignored.
        """
        db_records = list()
        db_plgin_records = list()
        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        with open(input_json_file) as json_file_obj:
            db_data = json.load(json_file_obj)
            if 'scn_db' in db_data:
                sensor_rows = db_data['scn_db']
            else:
                sensor_rows = db_data

            for pid in sensor_rows:
                db_records.append(EDDSentinel1ASF(PID=sensor_rows[pid]['PID'],
                                                  Scene_ID=sensor_rows[pid]['Scene_ID'],
                                                  Product_Name=sensor_rows[pid]['Product_Name'],
                                                  Product_File_ID=sensor_rows[pid]['Product_File_ID'],
                                                  ABS_Orbit=sensor_rows[pid]['ABS_Orbit'],
                                                  Rel_Orbit=sensor_rows[pid]['Rel_Orbit'],
                                                  Doppler=sensor_rows[pid]['Doppler'],
                                                  Flight_Direction=sensor_rows[pid]['Flight_Direction'],
                                                  Granule_Name=sensor_rows[pid]['Granule_Name'],
                                                  Granule_Type=sensor_rows[pid]['Granule_Type'],
                                                  Incidence_Angle=sensor_rows[pid]['Incidence_Angle'],
                                                  Look_Direction=sensor_rows[pid]['Look_Direction'],
                                                  Platform=sensor_rows[pid]['Platform'],
                                                  Polarization=sensor_rows[pid]['Polarization'],
                                                  Process_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Process_Date']),
                                                  Process_Description=sensor_rows[pid]['Process_Description'],
                                                  Process_Level=sensor_rows[pid]['Process_Level'],
                                                  Process_Type=sensor_rows[pid]['Process_Type'],
                                                  Process_Type_Disp=sensor_rows[pid]['Process_Type_Disp'],
                                                  Acquisition_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Acquisition_Date']),
                                                  Sensor=sensor_rows[pid]['Sensor'],
                                                  BeginPosition=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['BeginPosition']),
                                                  EndPosition=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['EndPosition']),
                                                  North_Lat=sensor_rows[pid]['North_Lat'],
                                                  South_Lat=sensor_rows[pid]['South_Lat'],
                                                  East_Lon=sensor_rows[pid]['East_Lon'],
                                                  West_Lon=sensor_rows[pid]['West_Lon'],
                                                  Remote_URL=sensor_rows[pid]['Remote_URL'],
                                                  Remote_FileName=sensor_rows[pid]['Remote_FileName'],
                                                  Remote_URL_MD5=sensor_rows[pid]['Remote_URL_MD5'],
                                                  Total_Size=sensor_rows[pid]['Total_Size'],
                                                  Query_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Query_Date']),
                                                  Download_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Download_Start_Date']),
                                                  Download_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['Download_End_Date']),
                                                  Downloaded=sensor_rows[pid]['Downloaded'],
                                                  Download_Path=eodd_utils.update_file_path(sensor_rows[pid]['Download_Path'],replace_path_dict),
                                                  Archived=sensor_rows[pid]['Archived'],
                                                  ARDProduct_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_Start_Date']),
                                                  ARDProduct_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['ARDProduct_End_Date']),
                                                  ARDProduct=sensor_rows[pid]['ARDProduct'],
                                                  ARDProduct_Path=eodd_utils.update_file_path(sensor_rows[pid]['ARDProduct_Path'],replace_path_dict),
                                                  DCLoaded_Start_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['DCLoaded_Start_Date']),
                                                  DCLoaded_End_Date=eodd_utils.getDateTimeFromISOString(sensor_rows[pid]['DCLoaded_End_Date']),
                                                  DCLoaded=sensor_rows[pid]['DCLoaded'],
                                                  Invalid=sensor_rows[pid]['Invalid'],
                                                  ExtendedInfo=self.update_extended_info_qklook_tilecache_paths(sensor_rows[pid]['ExtendedInfo'], replace_path_dict),
                                                  RegCheck=sensor_rows[pid]['RegCheck']))

                if 'plgin_db' in db_data:
                    plgin_rows = db_data['plgin_db']
                    for plgin_key in plgin_rows:
                        for scn_pid in plgin_rows[plgin_key]:
                            db_plgin_records.append(EDDSentinel1ASFPlugins(Scene_PID=plgin_rows[plgin_key][scn_pid]['Scene_PID'],
                                                                           PlugInName=plgin_rows[plgin_key][scn_pid]['PlugInName'],
                                                                           Start_Date=eodd_utils.getDateTimeFromISOString(plgin_rows[plgin_key][scn_pid]['Start_Date']),
                                                                           End_Date=eodd_utils.getDateTimeFromISOString(plgin_rows[plgin_key][scn_pid]['End_Date']),
                                                                           Completed=plgin_rows[plgin_key][scn_pid]['Completed'],
                                                                           Success=plgin_rows[plgin_key][scn_pid]['Success'],
                                                                           Outputs=plgin_rows[plgin_key][scn_pid]['Outputs'],
                                                                           Error=plgin_rows[plgin_key][scn_pid]['Error'],
                                                                           ExtendedInfo=plgin_rows[plgin_key][scn_pid]['ExtendedInfo']))
        if len(db_records) > 0:
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            ses.add_all(db_records)
            ses.commit()
            if len(db_plgin_records) > 0:
                ses.add_all(db_plgin_records)
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
        scn_record = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()

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
        scn_record = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.PID == unq_id).one_or_none()

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
        vld_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Invalid == False).count()
        invld_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Invalid == True).count()
        dwn_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == True).count()
        ard_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.ARDProduct == True).count()
        dcload_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.DCLoaded == True).count()
        arch_scn_count = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Archived == True).count()
        info_dict['n_scenes'] = dict()
        info_dict['n_scenes']['n_valid_scenes'] = vld_scn_count
        info_dict['n_scenes']['n_invalid_scenes'] = invld_scn_count
        info_dict['n_scenes']['n_downloaded_scenes'] = dwn_scn_count
        info_dict['n_scenes']['n_ard_processed_scenes'] = ard_scn_count
        info_dict['n_scenes']['n_dc_loaded_scenes'] = dcload_scn_count
        info_dict['n_scenes']['n_archived_scenes'] = arch_scn_count
        logger.debug("Calculated the scene count.")

        logger.debug("Find the scene file sizes.")
        file_sizes = ses.query(EDDSentinel1ASF.Total_Size).filter(EDDSentinel1ASF.Invalid == False).all()
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
                        if len(file_sizes_nums) > 1:
                            info_dict['file_size']['file_size_stdev'] = statistics.stdev(file_sizes_nums)
                        info_dict['file_size']['file_size_median'] = statistics.median(file_sizes_nums)
                        if (len(file_sizes_nums) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                            info_dict['file_size']['file_size_quartiles'] = statistics.quantiles(file_sizes_nums)
        logger.debug("Calculated the scene file sizes.")

        logger.debug("Find download and processing time stats.")
        download_times = []
        ard_process_times = []
        scns = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == True)
        for scn in scns:
            download_times.append((scn.Download_End_Date - scn.Download_Start_Date).total_seconds())
            if scn.ARDProduct:
                ard_process_times.append((scn.ARDProduct_End_Date - scn.ARDProduct_Start_Date).total_seconds())

        if len(download_times) > 0:
            info_dict['download_time'] = dict()
            info_dict['download_time']['download_time_mean_secs'] = statistics.mean(download_times)
            info_dict['download_time']['download_time_min_secs'] = min(download_times)
            info_dict['download_time']['download_time_max_secs'] = max(download_times)
            if len(download_times) > 1:
                info_dict['download_time']['download_time_stdev_secs'] = statistics.stdev(download_times)
            info_dict['download_time']['download_time_median_secs'] = statistics.median(download_times)
            if (len(download_times) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                info_dict['download_time']['download_time_quartiles_secs'] = statistics.quantiles(download_times)

        if len(ard_process_times) > 0:
            info_dict['ard_process_time'] = dict()
            info_dict['ard_process_time']['ard_process_time_mean_secs'] = statistics.mean(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_min_secs'] = min(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_max_secs'] = max(ard_process_times)
            if len(ard_process_times) > 1:
                info_dict['ard_process_time']['ard_process_time_stdev_secs'] = statistics.stdev(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_median_secs'] = statistics.median(ard_process_times)
            if (len(ard_process_times) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                info_dict['ard_process_time']['ard_process_time_quartiles_secs'] = statistics.quantiles(
                        ard_process_times)
        logger.debug("Calculated the download and processing time stats.")

        if self.calc_scn_usr_analysis():
            plgin_lst = self.get_usr_analysis_keys()
            info_dict['usr_plugins'] = dict()
            for plgin_key in plgin_lst:
                info_dict['usr_plugins'][plgin_key] = dict()
                scns = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.PlugInName == plgin_key).all()
                n_err_scns = 0
                n_complete_scns = 0
                n_success_scns = 0
                plugin_times = []
                for scn in scns:
                    if scn.Completed:
                        plugin_times.append((scn.End_Date - scn.Start_Date).total_seconds())
                        n_complete_scns += 1
                    if scn.Success:
                        n_success_scns += 1
                    if scn.Error:
                        n_err_scns += 1
                info_dict['usr_plugins'][plgin_key]['n_success'] = n_success_scns
                info_dict['usr_plugins'][plgin_key]['n_completed'] = n_complete_scns
                info_dict['usr_plugins'][plgin_key]['n_error'] = n_err_scns
                if len(plugin_times) > 0:
                    info_dict['usr_plugins'][plgin_key]['processing'] = dict()
                    info_dict['usr_plugins'][plgin_key]['processing']['time_mean_secs'] = statistics.mean(plugin_times)
                    info_dict['usr_plugins'][plgin_key]['processing']['time_min_secs'] = min(plugin_times)
                    info_dict['usr_plugins'][plgin_key]['processing']['time_max_secs'] = max(plugin_times)
                    if len(plugin_times) > 1:
                        info_dict['usr_plugins'][plgin_key]['processing']['time_stdev_secs'] = statistics.stdev(plugin_times)
                    info_dict['usr_plugins'][plgin_key]['processing']['time_median_secs'] = statistics.median(plugin_times)
                    if (len(plugin_times) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                        info_dict['usr_plugins'][plgin_key]['processing']['time_quartiles_secs'] = statistics.quantiles(plugin_times)
        ses.close()
        return info_dict

    def get_sensor_plugin_info(self, plgin_key):
        """
        A function which generates a dictionary of information (e.g., errors) for a plugin.

        :param plgin_key: The name of the plugin for which the information will be produced.
        :return: a dict with the information.

        """
        info_dict = dict()
        if self.calc_scn_usr_analysis():
            plugin_keys = self.get_usr_analysis_keys()
            if plgin_key not in plugin_keys:
                raise EODataDownException("The specified plugin ('{}') does not exist.".format(plgin_key))

            import statistics
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            scns = ses.query(EDDSentinel1ASFPlugins).filter(EDDSentinel1ASFPlugins.PlugInName == plgin_key).all()
            n_err_scns = 0
            n_complete_scns = 0
            n_success_scns = 0
            plugin_times = []
            errors_dict = dict()
            for scn in scns:
                if scn.Completed:
                    plugin_times.append((scn.End_Date - scn.Start_Date).total_seconds())
                    n_complete_scns += 1
                if scn.Success:
                    n_success_scns += 1
                if scn.Error:
                    n_err_scns += 1
                    errors_dict[scn.Scene_PID] = scn.ExtendedInfo
            ses.close()
            info_dict[plgin_key] = dict()
            info_dict[plgin_key]['n_success'] = n_success_scns
            info_dict[plgin_key]['n_completed'] = n_complete_scns
            info_dict[plgin_key]['n_error'] = n_err_scns
            if len(plugin_times) > 0:
                info_dict[plgin_key]['processing'] = dict()
                info_dict[plgin_key]['processing']['time_mean_secs'] = statistics.mean(plugin_times)
                info_dict[plgin_key]['processing']['time_min_secs'] = min(plugin_times)
                info_dict[plgin_key]['processing']['time_max_secs'] = max(plugin_times)
                if len(plugin_times) > 1:
                    info_dict[plgin_key]['processing']['time_stdev_secs'] = statistics.stdev(plugin_times)
                info_dict[plgin_key]['processing']['time_median_secs'] = statistics.median(plugin_times)
                if (len(plugin_times) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                    info_dict[plgin_key]['processing']['time_quartiles_secs'] = statistics.quantiles(plugin_times)
            if n_err_scns > 0:
                info_dict[plgin_key]['errors'] = errors_dict
        else:
            raise EODataDownException("There are no plugins for a summary to be produced for.")

        return info_dict
