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

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

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


def _download_scn_asf(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    product_file_id = params[0]
    remote_url = params[1]
    db_info_obj = params[2]
    scn_lcl_dwnld_path = params[3]
    asf_user = params[4]
    asf_pass = params[5]
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
        session =sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Product_File_ID == product_file_id).one_or_none()
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


class EODataDownSentinel1ASFProcessorSensor (EODataDownSensor):
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
            geo_bounds_lst = json_parse_helper.getListValue(config_data,
                                                            ["eodatadown", "sensor", "download", "geobounds"])
            if not len(geo_bounds_lst) > 0:
                raise EODataDownException("There must be at least 1 geographic boundary given.")

            self.geoBounds = []
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

    def check_new_scns(self):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.debug("Creating HTTP Session Object.")
        session = requests.Session()
        session.auth = (self.asfUser, self.asfPass)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session =sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()

        logger.debug(
            "Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if ses.query(EDDSentinel1ASF).first() is not None:
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
        db_records = []
        for geo_bound in self.geoBounds:
            csv_poly = geo_bound.getCSVPolygon()
            logger.info("Checking for available scenes for \"" + csv_poly + "\"")
            query_str_geobound = "polygon="+ csv_poly
            query_str = query_str_geobound + "&" + query_str_platform + "&" + query_str_product + "&" + query_str_date + "&output=json"
            query_url = self.base_api_url + "?" + query_str
            logger.debug("Going to use the following URL: " + query_url)
            response = session.get(query_url, auth=session.auth)
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
                        processing_date_val = json_parse_helper.getDateTimeValue(scn_json, ["processingDate"], "%Y-%m-%dT%H:%M:%S.%f")
                        processing_description_val = json_parse_helper.getStrValue(scn_json, ["processingDescription"])
                        processing_level_val = json_parse_helper.getStrValue(scn_json, ["processingLevel"])
                        processing_type_val = json_parse_helper.getStrValue(scn_json, ["processingType"])
                        processing_type_disp_val = json_parse_helper.getStrValue(scn_json, ["processingTypeDisplay"])
                        scene_date_val = json_parse_helper.getDateTimeValue(scn_json, ["sceneDate"], "%Y-%m-%dT%H:%M:%S.%f")
                        sensor_val = json_parse_helper.getStrValue(scn_json, ["sensor"])
                        start_time_val = json_parse_helper.getDateTimeValue(scn_json, ["startTime"], "%Y-%m-%dT%H:%M:%S.%f")
                        stop_time_val = json_parse_helper.getDateTimeValue(scn_json, ["stopTime"], "%Y-%m-%dT%H:%M:%S.%f")
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


    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        raise EODataDownException("Not implemented.")

    def download_scn(self, unq_id):
        """
        A function which downloads an individual scene and updates the database if download is successful.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: returns boolean indicating successful or otherwise download.
        """
        raise EODataDownException("Not implemented.")

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
        session =sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses= session()

        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == False).filter(
            EDDSentinel1ASF.Remote_URL is not None).all()
        dwnld_params = []
        if query_result is not None:
            for record in query_result:
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, record.Product_File_ID)
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.Remote_FileName
                downloaded_new_scns = True
                dwnld_params.append([record.Product_File_ID, record.Remote_URL, self.db_info_obj, os.path.join(scn_lcl_dwnld_path, out_filename), self.asfUser, self.asfPass])
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
        edd_usage_db.add_entry(description_val="Checked downloaded new scenes.", sensor_val=self.sensor_name, updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)


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
        query_result = ses.query(EDDSentinel1ASF).filter(EDDSentinel1ASF.Downloaded == True, EDDSentinel1ASF.ARDProduct == False).all()

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

            # TODO IMPLEMENT ESA Sentinel-1 Processing.

        ses.close()
        raise EODataDownException("Not implemented.")

    def get_scnlist_add2datacube(self):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        raise EODataDownException("Not implemented.")

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

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='SQLite', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        raise EODataDownException("Not Implemented")
