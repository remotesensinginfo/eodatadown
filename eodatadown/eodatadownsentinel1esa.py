#!/usr/bin/env python
"""
EODataDown - a sensor class for Sentinel-1 data downloaded from ESA.
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
# Date: 11/08/2018
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
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDSentinel1ESA(Base):
    __tablename__ = "EDDSentinel1ESA"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    UUID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Identifier = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    FileName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    FileFormat = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    InstrumentShortName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    InstrumentName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    MissionDataTakeID = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    OrbitNumber = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    LastOrbitNumber = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    RelativeOrbitNumber = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    LastRelativeOrbitNumber = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    SliceNumber = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    SensorOperationalMode = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    SwathIdentifier = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    PlatformIdentifier = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    OrbitDirection = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    PolarisationMode = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    ProductClass = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    ProductType = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    PlatformName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    IngestionDate = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    BeginPosition = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    EndPosition = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=True)
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
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.JSON, nullable=True)
    RegCheck = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_esa(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    pid = params[0]
    uuid = params[1]
    remote_url = params[2]
    remote_url_md5 = params[3]
    remote_filesize = params[4]
    db_info_obj = params[5]
    scn_lcl_dwnld_path = params[6]
    esa_user = params[7]
    esa_pass = params[8]
    continue_downloads = params[9]

    if remote_url is not None:
        eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()

        start_date = datetime.datetime.now()
        success = eodd_http_downloader.downloadFileContinue(remote_url, remote_url_md5, scn_lcl_dwnld_path, esa_user, esa_pass, remote_filesize, continue_downloads)
        end_date = datetime.datetime.now()

        if success and os.path.exists(scn_lcl_dwnld_path):
            logger.debug("Set up database connection and update record.")
            db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == pid).one_or_none()
            if query_result is None:
                logger.error("Could not find the scene within local database: " + uuid)
            query_result.Downloaded = True
            query_result.Download_Start_Date = start_date
            query_result.Download_End_Date = end_date
            query_result.Download_Path = scn_lcl_dwnld_path
            ses.commit()
            ses.close()
            logger.info("Finished download and updated database: {}".format(scn_lcl_dwnld_path))
        else:
            logger.error("Download did not complete, re-run and it should continue from where it left off: {}".format(scn_lcl_dwnld_path))
    else:
        logger.error("Download did not complete, the URL is NULL: {}".format(scn_lcl_dwnld_path))


class EODataDownSentinel1ESAProcessorSensor (EODataDownSentinel1ProcessorSensor):
    """
    An class which represents a the Sentinel-1 sensor being downloaded from ESA Copernicus Open Access Hub.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "Sentinel1ESA"
        self.db_tab_name = "EDDSentinel1ESA"
        self.base_api_url = "https://scihub.copernicus.eu/apihub/"

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
            logger.debug("Testing config file is for 'Sentinel1ESA'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'Sentinel1ESA'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.outImgRes = json_parse_helper.getNumericValue(config_data,
                                                               ["eodatadown", "sensor", "ardparams", "imgres"],
                                                               valid_lower=10.0)
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
            self.ardMethod = 'GAMMA'
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "software"]):
                self.ardMethod = json_parse_helper.getStrValue(config_data,
                                                               ["eodatadown", "sensor", "ardparams", "software"],
                                                               valid_values=["GAMMA", "SNAP"])
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

            logger.debug("Find ESA Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.esaUser = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "esaaccount", "user"])
            self.esaPass = edd_pass_encoder.unencodePassword(
                json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "esaaccount", "pass"]))
            logger.debug("Found ESA Account params from config file")

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

        logger.debug("Creating Sentinel1ESA Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def create_query_url(self, base_url, start_offset=0, n_rows=100):
        """
        Create the URL being used to undertake the query on the ESA server.
        :param base_url: the base server url (i.e., "https://scihub.copernicus.eu/apihub/")
        :param start_offset: the offset in the number of scenes to step through 'pages'
        :param n_rows: the maximum number of scenes (rows) to be returned as a 'page'.
        :return: a string with the URL.
        """
        urloptions = "search?format=json&rows={0}&start={1}".format(n_rows, start_offset)
        return base_url+urloptions

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

    def parse_json_response(self, rsp_json, sesobj, n_records):
        """
        Parse the JSON response and populate the local database with the scenes found in the response.
        :param n_records:
        :param rsp_json: the JSON from the http response.
        :param sesobj: A database session object.
        """

        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        db_records = []
        entry_lst = []
        if n_records == 1:
            entry_lst.append(rsp_json['entry'])
        else:
            entry_lst = rsp_json['entry']

        for entry in entry_lst:
            found, uuid_val = json_parse_helper.findStringValueESALst(entry["str"], "uuid")
            if not found:
                raise EODataDownException("Could not find uuid for {}".format(entry['title']))
            found, identifier_val = json_parse_helper.findStringValueESALst(entry["str"], "identifier")
            if not found:
                raise EODataDownException("Could not find identifier for {}".format(entry['title']))
            found, filename_val = json_parse_helper.findStringValueESALst(entry["str"], "filename")
            if not found:
                raise EODataDownException("Could not find filename for {}".format(entry['title']))
            found, fileformat_val = json_parse_helper.findStringValueESALst(entry["str"], "fileformat")
            found, instrumentshortname_val = json_parse_helper.findStringValueESALst(entry["str"], "instrumentshortname")
            found, instrumentname_val = json_parse_helper.findStringValueESALst(entry["str"], "instrumentname")
            found, missiondatatakeid_val = json_parse_helper.findIntegerValueESALst(entry["int"], "missiondatatakeid")
            found, orbitnumber_val = json_parse_helper.findIntegerValueESALst(entry["int"], "orbitnumber")
            found, lastorbitnumber_val = json_parse_helper.findIntegerValueESALst(entry["int"], "lastorbitnumber")
            found, relativeorbitnumber_val = json_parse_helper.findIntegerValueESALst(entry["int"], "relativeorbitnumber")
            found, lastrelativeorbitnumber_val = json_parse_helper.findIntegerValueESALst(entry["int"], "lastrelativeorbitnumber")
            found, slicenumber_val = json_parse_helper.findIntegerValueESALst(entry["int"], "slicenumber")

            found, sensoroperationalmode_val = json_parse_helper.findStringValueESALst(entry["str"], "sensoroperationalmode")
            found, swathidentifier_val = json_parse_helper.findStringValueESALst(entry["str"], "swathidentifier")
            found, platformidentifier_val = json_parse_helper.findStringValueESALst(entry["str"], "platformidentifier")
            found, orbitdirection_val = json_parse_helper.findStringValueESALst(entry["str"], "orbitdirection")
            found, polarisationmode_val = json_parse_helper.findStringValueESALst(entry["str"], "polarisationmode")
            found, productclass_val = json_parse_helper.findStringValueESALst(entry["str"], "productclass")
            found, producttype_val = json_parse_helper.findStringValueESALst(entry["str"], "producttype")
            found, platformname_val = json_parse_helper.findStringValueESALst(entry["str"], "platformname")
            found, ingestiondate_str_val = json_parse_helper.findStringValueESALst(entry["date"], "ingestiondate")
            if found:
                ingestiondate_str_val = ingestiondate_str_val.replace('Z', '')[:-1]
                if ingestiondate_str_val.count(".") == 1:
                    if ingestiondate_str_val.split(".")[1] == "":
                        ingestiondate_str_val = ingestiondate_str_val.split(".")[0]
                        ingestiondate_date_val = datetime.datetime.strptime(ingestiondate_str_val, "%Y-%m-%dT%H:%M:%S")
                    else:
                        ingestiondate_date_val = datetime.datetime.strptime(ingestiondate_str_val, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    ingestiondate_date_val = datetime.datetime.strptime(ingestiondate_str_val, "%Y-%m-%dT%H:%M:%S")
            else:
                raise EODataDownException("Could not find ingestiondate for {}".format(entry['title']))
            found, beginposition_str_val = json_parse_helper.findStringValueESALst(entry["date"], "beginposition")
            if found:
                beginposition_str_val = beginposition_str_val.replace('Z', '')[:-1]
                if beginposition_str_val.count(".") == 1:
                    if beginposition_str_val.split(".")[1] == "":
                        beginposition_str_val = beginposition_str_val.split(".")[0]
                        beginposition_date_val = datetime.datetime.strptime(beginposition_str_val, "%Y-%m-%dT%H:%M:%S")
                    else:
                        beginposition_date_val = datetime.datetime.strptime(beginposition_str_val, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    beginposition_date_val = datetime.datetime.strptime(beginposition_str_val, "%Y-%m-%dT%H:%M:%S")
            else:
                raise EODataDownException("Could not find beginposition for {}".format(entry['title']))
            found, endposition_str_val = json_parse_helper.findStringValueESALst(entry["date"], "endposition")
            if found:
                endposition_str_val = endposition_str_val.replace('Z', '')[:-1]
                if endposition_str_val.count(".") == 1:
                    if endposition_str_val.split(".")[1] == "":
                        endposition_str_val = endposition_str_val.split(".")[0]
                        endposition_date_val = datetime.datetime.strptime(endposition_str_val, "%Y-%m-%dT%H:%M:%S")
                    else:
                        endposition_date_val = datetime.datetime.strptime(endposition_str_val, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    endposition_date_val = datetime.datetime.strptime(endposition_str_val, "%Y-%m-%dT%H:%M:%S")
            else:
                raise EODataDownException("Could not find endposition for {}".format(entry['title']))

            found, footprint_val = json_parse_helper.findStringValueESALst(entry["str"], "footprint")
            if found:
                edd_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                edd_bbox.parseWKTPolygon(footprint_val)
                ent_north_Lat = edd_bbox.getNorthLat()
                ent_south_Lat = edd_bbox.getSouthLat()
                ent_west_lon = edd_bbox.getWestLon()
                ent_east_lon = edd_bbox.getEastLon()
            else:
                raise EODataDownException("Could not find footprint for {}".format(entry['title']))
            query_rtn = sesobj.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.UUID == uuid_val).one_or_none()
            if query_rtn is None:
                db_records.append(
                    EDDSentinel1ESA(UUID=uuid_val, Identifier=identifier_val, FileName=filename_val, FileFormat=fileformat_val,
                                    InstrumentShortName=instrumentshortname_val, InstrumentName=instrumentname_val,
                                    MissionDataTakeID=missiondatatakeid_val, OrbitNumber=orbitnumber_val,
                                    LastOrbitNumber=lastorbitnumber_val, RelativeOrbitNumber = relativeorbitnumber_val,
                                    LastRelativeOrbitNumber=lastrelativeorbitnumber_val, SliceNumber=slicenumber_val,
                                    SensorOperationalMode=sensoroperationalmode_val, SwathIdentifier=swathidentifier_val,
                                    PlatformIdentifier=platformidentifier_val, OrbitDirection=orbitdirection_val,
                                    PolarisationMode=polarisationmode_val, ProductClass=productclass_val, ProductType=producttype_val,
                                    PlatformName=platformname_val, IngestionDate=ingestiondate_date_val,
                                    BeginPosition=beginposition_date_val, EndPosition=endposition_date_val,
                                    North_Lat=ent_north_Lat, South_Lat=ent_south_Lat, East_Lon=ent_east_lon, West_Lon=ent_west_lon,
                                    Query_Date=datetime.datetime.now(), Download_Start_Date=None, Download_End_Date=None, Downloaded=False,
                                    Download_Path="", ARDProduct_Start_Date=None, ARDProduct_End_Date=None, ARDProduct=False, ARDProduct_Path=""))
        if len(db_records) > 0:
            logger.debug("Writing records to the database.")
            sesobj.add_all(db_records)
            sesobj.commit()
            logger.debug("Written and committed records to the database.")

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.debug("Creating HTTP Session Object.")
        session_http = requests.Session()
        session_http.auth = (self.esaUser, self.esaPass)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_http.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("""Find the start date for query - if table is empty then using config 
                        date otherwise date of last acquried image.""")
        query_date = self.startDate
        if (not check_from_start) and (ses.query(EDDSentinel1ESA).first() is not None):
            query_date = ses.query(EDDSentinel1ESA).order_by(EDDSentinel1ESA.BeginPosition.desc()).first().BeginPosition
        logger.info("Query with start at date: " + str(query_date))

        str_start_datetime = query_date.isoformat() + "Z"
        str_now_datetime = datetime.datetime.now().isoformat() + "Z"

        query_str_date = "beginPosition:[" + str_start_datetime + " TO " + str_now_datetime + "]"
        query_str_platform = "platformname:Sentinel-1"
        query_str_product = "producttype:GRD"

        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()

        new_scns_avail = False
        n_step = 100
        for geo_bound in self.geoBounds:
            wkt_poly = geo_bound.getWKTPolygon()
            logger.info("Checking for available scenes for \"" + wkt_poly + "\"")
            query_str_geobound = "footprint:\"Intersects(" + wkt_poly + ")\""
            query_str = query_str_date + ", " + query_str_platform + ", " + query_str_product + ", " + query_str_geobound
            url = self.create_query_url(self.base_api_url, 0, n_step)
            logger.debug("Going to use the following URL: " + url)
            logger.debug("Using the following query: " + query_str)
            response = session_http.post(url, {"q": query_str}, auth=session_http.auth)
            if self.check_http_response(response, url):
                rsp_json = response.json()["feed"]
                if rsp_json["opensearch:totalResults"] is not None:
                    n_results = int(json_parse_helper.getNumericValue(rsp_json, ["opensearch:totalResults"]))
                    logger.info("There are {0} scenes for the query.".format(n_results))
                else:
                    raise EODataDownResponseException("JSON returned was not in expected format.", response)

                if n_results > 0:
                    new_scns_avail = True
                    n_records_expt = n_step
                    if n_results < n_step:
                        n_records_expt = n_results
                    self.parse_json_response(rsp_json, ses, n_records_expt)
                if n_results > 100:
                    n_remain_scns = int((n_results - n_step) % n_step)
                    n_full_pages = int(((n_results - n_step) - n_remain_scns) / n_step)
                    logger.debug("n_pages = {0} \t n_remain_scns = {1}".format(n_full_pages, n_remain_scns))
                    if n_full_pages > 0:
                        start_off = n_step
                        for i in range(n_full_pages):
                            url = self.create_query_url(self.base_api_url, start_off, n_step)
                            logger.debug("Going to use the following URL: " + url)
                            logger.debug("Using the following query: " + query_str)
                            response = session_http.post(url, {"q": query_str}, auth=session_http.auth)
                            if self.check_http_response(response, url):
                                rsp_json = response.json()["feed"]
                                self.parse_json_response(rsp_json, ses, n_step)
                            start_off = start_off + n_step
                    if n_remain_scns > 0:
                        start_off = n_results - n_remain_scns
                        n_scns = n_remain_scns
                        url = self.create_query_url(self.base_api_url, start_off, n_scns)
                        logger.debug("Going to use the following URL: " + url)
                        logger.debug("Using the following query: " + query_str)
                        response = session_http.post(url, {"q": query_str}, auth=session_http.auth)
                        if self.check_http_response(response, url):
                            rsp_json = response.json()["feed"]
                            self.parse_json_response(rsp_json, ses, n_remain_scns)
            logger.debug("Processed query result and added to local database for \"" + wkt_poly + "\"")

        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Remote_URL == None).all()
        if query_result is not None:
            for record in query_result:
                url = self.base_api_url + "odata/v1/Products('{}')?$format=json".format(record.UUID)
                response = session_http.get(url, auth=session_http.auth)
                if not self.check_http_response(response, url):
                    logger.error("Could not get the URL for scene: '{}'".format(record.UUID))
                json_url_info = response.json()['d']
                record.Remote_URL_MD5 = json_parse_helper.getStrValue(json_url_info, ["Checksum", "Value"])
                record.Remote_URL = json_parse_helper.getStrValue(json_url_info, ["__metadata", "media_src"])
                record.Total_Size = int(json_parse_helper.getNumericValue(json_url_info, ["ContentLength"]))
            ses.commit()
        ses.close()
        logger.debug("Closed Database Session")
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
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.Downloaded

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

        continue_downloads = True

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Downloaded == False).filter(
            EDDSentinel1ESA.Remote_URL is not None).all()
        dwnld_params = []
        if query_result is not None:
            logger.debug("Create the output directory for this download.")
            dt_obj = datetime.datetime.now()

            for record in query_result:
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath, "{}_{}".format(record.Identifier, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.Identifier+".zip"
                downloaded_new_scns = True
                dwnld_params.append(
                    [record.PID, record.UUID, record.Remote_URL, record.Remote_URL_MD5, record.Total_Size,
                     self.db_info_obj, os.path.join(scn_lcl_dwnld_path, out_filename), self.esaUser, self.esaPass,
                     continue_downloads])
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_scn_esa, dwnld_params)
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
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Downloaded == True,
                                                         EDDSentinel1ESA.ARDProduct == False).all()

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
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.ARDProduct

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Downloaded == True,
                                                         EDDSentinel1ESA.ARDProduct == False).all()

        proj_epsg = 4326
        if self.ardProjDefined:
            proj_epsg = self.projEPSG

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
                logger.debug("Create info for running ARD analysis for scene: {}".format(record.Identifier))
                final_ard_scn_path = os.path.join(self.ardFinalPath, record.Identifier)
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                work_ard_scn_path = os.path.join(work_ard_path, record.Identifier)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, record.Identifier)
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                # TODO IMPLEMENT ESA Sentinel-1 Processing.
                #self.convertSen1ARD(input_safe_file, output_dir, tmp_ard_path, self.demFile, self.outImgRes, proj_epsg, polarisations)

        ses.close()
        raise EODataDownException("Not implemented.")

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
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.ARDProduct == True,
                                                         EDDSentinel1ESA.DCLoaded == loaded).all()
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
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == unq_id).one()
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
        raise EODataDownException('get_scnlist_quicklook not implemented')

    def has_scn_quicklook(self, unq_id):
        """
        Check whether the quicklook has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has quicklook. False = has not got a quicklook)
        """
        raise EODataDownException('has_scn_quicklook not implemented')

    def scn2quicklook(self, unq_id):
        """
        Generate the quicklook image for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        raise EODataDownException('scn2quicklook not implemented')

    def scns2quicklook_all_avail(self):
        """
        Generate the quicklook images for the scenes for which a quicklook image do not exist.

        """
        raise EODataDownException('scns2quicklook_all_avail not implemented')

    def get_scnlist_tilecache(self):
        """
        Get a list of all scenes which a tile cache has not been generated.

        :return: list of unique IDs
        """
        raise EODataDownException('get_scnlist_tilecache not implemented')

    def has_scn_tilecache(self, unq_id):
        """
        Check whether a tile cache has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has quicklook. False = has not got a quicklook)
        """
        raise EODataDownException('has_scn_tilecache not implemented')

    def scn2tilecache(self, unq_id):
        """
        Generate the tile cache for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        raise EODataDownException('scn2tilecache not implemented')

    def scns2tilecache_all_avail(self):
        """
        Generate the tile cache for the scenes for which a tile cache does not exist.

        """
        raise EODataDownException('scns2tilecache_all_avail not implemented')

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == unq_id).one_or_none()

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
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.PID == unq_id).one_or_none()

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
