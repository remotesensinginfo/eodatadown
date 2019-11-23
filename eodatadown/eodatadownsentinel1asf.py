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
import os.path
import datetime
import multiprocessing
import rsgislib
import shutil

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownsentinel1 import EODataDownSentinel1ProcessorSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified

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
        success = eodd_wget_downloader.downloadFile(remote_url, scn_lcl_dwnld_path, username=asf_user, password=asf_pass, try_number="10", time_out="60")
    except Exception as e:
        logger.error("An error has occured while downloading from ASF: '{}'".format(e))
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
        logger.error("Download did not complete, re-run and it should continue from where it left off: {}".format(scn_lcl_dwnld_path))


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

            logger.debug("Find ASF Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.asfUser = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "asfaccount", "user"])
            self.asfPass = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "asfaccount", "pass"]))
            logger.debug("Found ASF Account params from config file")

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
                    query_rtn = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Product_File_ID == product_file_id_val).one_or_none()
                    if (query_rtn is None) and (not(product_file_id_val in product_file_ids)):
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
                        if (incidence_angle_strval == "") or (incidence_angle_strval == "NA"):
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


                        db_records.append(EDDSentinel1ASF(Scene_ID=scene_id_val,
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
        query_result = ses.query(EDDSentinel1ASF).all()
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
            EDDSentinel1ASF.Remote_URL is not None).all()

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
                                                         EDDSentinel1ASF.Invalid == False).all()

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
                    logger.error("Could not find unique zip file for Sentinel-1 zip: PID = {}".format(record.PID))
                    raise EODataDownException(
                        "Could not find unique zip file for Sentinel-1 zip: PID = {}".format(record.PID))
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
                                                         EDDSentinel1ASF.DCLoaded == loaded).all()
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
            EDDSentinel1ASF.ARDProduct == True).all()
        scns2quicklook = list()
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
            raise EODataDownException("The quicklook path does not exist or not provided, please create and run again.")

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
            if scn_json is None:
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
                                                             output_img_sizes=[250, 1000], img_stats_msk=None,
                                                             img_msk_vals=1, tmp_dir=tmp_quicklook_path)

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
        query_result = ses.query(EDDSentinel1ASF).filter(
            sqlalchemy.or_(
                EDDSentinel1ASF.ExtendedInfo.is_(None),
                sqlalchemy.not_(EDDSentinel1ASF.ExtendedInfo.has_key('tilecache'))),
            EDDSentinel1ASF.Invalid == False,
            EDDSentinel1ASF.ARDProduct == True).all()
        scns2tilecache = list()
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
            if scn_json is None:
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
                                                                   tmp_dir=tmp_tilecache_path, webview=True,
                                                                   scale=50)

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
                    "There was more than 1 scene which has been found - soomething has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))
        return scn_record

    def query_scn_records_date_count(self, start_date, end_date, valid=True):
        """
        A function which queries the database to find scenes within a specified date range
        and returns the number of records available.

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :return: count of records available
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if valid:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                       EDDSentinel1ASF.Acquisition_Date > end_date,
                                                       EDDSentinel1ASF.Invalid == False,
                                                       EDDSentinel1ASF.ARDProduct == True).count()
        else:
            n_rows = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                       EDDSentinel1ASF.Acquisition_Date > end_date).count()
        ses.close()
        return n_rows

    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True):
        """
        A function which queries the database to find scenes within a specified date range.
        The order of the records is descending (i.e., from current to historical)

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param start_rec: A parameter specifying the start record, for example for pagination.
        :param n_recs: A parameter specifying the number of records to be returned.
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :return: list of database records
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        if valid:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date > end_date,
                                                                    EDDSentinel1ASF.Invalid == False,
                                                                    EDDSentinel1ASF.ARDProduct == True).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date > end_date,
                                                                    EDDSentinel1ASF.Invalid == False,
                                                                    EDDSentinel1ASF.ARDProduct == True).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc()).all()
        else:
            if n_recs > 0:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date > end_date).order_by(
                    EDDSentinel1ASF.Acquisition_Date.desc())[start_rec:(start_rec + n_recs)]
            else:
                query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Acquisition_Date < start_date,
                                                                    EDDSentinel1ASF.Acquisition_Date > end_date).order_by(
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
                db_records.append(EDDSentinel1ASF(Scene_ID=sensor_rows[pid]['Scene_ID'],
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
                                                  ExtendedInfo=self.update_extended_info_qklook_tilecache_paths(sensor_rows[pid]['ExtendedInfo']),
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

    def reset_scn(self, unq_id, reset_download=False):
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

        scn_record.ExtendedInfo = ""
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
