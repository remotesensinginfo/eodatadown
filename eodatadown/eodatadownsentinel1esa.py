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
import shutil
import rsgislib

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
import eodatadown.eodatadownrunarcsi

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
    ARDProduct_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")


def _download_scn_esa(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    uuid = params[0]
    remote_url = params[1]
    remote_url_md5 = params[2]
    remote_filesize = params[3]
    dbInfoObj = params[4]
    scn_lcl_dwnld_path = params[5]
    esa_user = params[6]
    esa_pass = params[7]
    continue_downloads = params[8]

    eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()

    start_date = datetime.datetime.now()
    success = eodd_http_downloader.downloadFile(remote_url, remote_url_md5, scn_lcl_dwnld_path, esa_user, esa_pass, remote_filesize, continue_downloads)
    end_date = datetime.datetime.now()

    if success and os.path.exists(scn_lcl_dwnld_path):
        logger.debug("Set up database connection and update record.")
        dbEng = sqlalchemy.create_engine(dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()
        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.UUID == uuid).one_or_none()
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


class EODataDownSentinel1ESASensor (EODataDownSensor):
    """
    An class which represents a the Sentinel-1 sensor being downloaded from ESA Copernicus Open Access Hub.
    """

    def __init__(self, dbInfoObj):
        EODataDownSensor.__init__(self, dbInfoObj)
        self.sensorName = "Sentinel1ESA"
        self.base_api_url = "https://scihub.copernicus.eu/apihub/"

    def getSensorName(self):
        return self.sensorName

    def parseSensorConfig(self, config_file, first_parse=False):
        """
        A function to parse the Sentinel1ESA JSON config file.
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
            logger.debug("Testing config file is for 'Sentinel1ESA'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensorName])
            logger.debug("Have the correct config file for 'Sentinel1ESA'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.projEPSG = -1
            self.projabbv = ""
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data, ["eodatadown", "sensor", "ardparams", "proj", "epsg"], 0, 1000000000))
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardtmp"])
            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            geo_bounds_lst = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor", "download", "geobounds"])
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

            self.boundsRelation = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "download", "geoboundsrelation"], ["intersects", "contains", "iswithin"])
            self.startDate = json_parse_helper.getDateTimeValue(config_data, ["eodatadown", "sensor", "download", "startdate"], "%Y-%m-%d")
            logger.debug("Found search params from config file")

            logger.debug("Find ESA Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.esaUser = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "esaaccount", "user"])
            self.esaPass = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "esaaccount", "pass"]))
            logger.debug("Found ESA Account params from config file")

    def initSensorDB(self):
        """
        Initialise the sensor database table.
        :return:
        """
        logger.debug("Creating Database Engine.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(dbEng)

        logger.debug("Creating Sentinel1ESA Database.")
        Base.metadata.bind = dbEng
        Base.metadata.create_all()

    def createQueryURL(self, base_url, start_offset=0, n_rows=100):
        """

        :param baseurl:
        :param start_offset:
        :param n_rows:
        :return:
        """
        urloptions = "search?format=json&rows={0}&start={1}".format(n_rows, start_offset)
        return base_url+urloptions

    def checkResponse(self, response, url):
        """
        Check the HTTP response and raise an exception with appropriate error message
        if request was not successful.
        :param response:
        :param url:
        :return:
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

    def parseResponseJSON(self, rsp_json, sesobj, n_records):
        """
        Parse the JSON response and populate the local database.
        :param rsp_json:
        :param sesobj:
        :return:
        """

        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        db_records = []
        entryLst = []
        if n_records == 1:
            entryLst.append(rsp_json['entry'])
        else:
            entryLst = rsp_json['entry']

        for entry in entryLst:
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
                                    PolarisationMode=polarisationmode_val, ProductClass=productclass_val, ProductType=productclass_val,
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

    def check4NewData(self):
        """
        A function to check the EU Copernicus Open Access Hub for Sentinel-1 GRD data.
        :return:
        """
        logger.debug("Creating HTTP Session Object.")
        session = requests.Session()
        session.auth = (self.esaUser, self.esaPass)
        user_agent = "eoedatadown/"+str(eodatadown.EODATADOWN_VERSION)
        session.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()

        logger.debug("Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if ses.query(EDDSentinel1ESA).first() is not None:
            query_date = ses.query(EDDSentinel1ESA).order_by(EDDSentinel1ESA.BeginPosition.desc()).first().BeginPosition
        logger.info("Query with start at date: " + str(query_date))

        str_start_datetime = query_date.isoformat()+"Z"
        str_now_datetime = datetime.datetime.now().isoformat()+"Z"

        query_str_date = "beginPosition:["+str_start_datetime+" TO "+str_now_datetime+"]"
        query_str_platform = "platformname:Sentinel-1"
        query_str_product = "producttype:GRD"

        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()

        new_scns_avail = False
        n_step = 100
        for geo_bound in self.geoBounds:
            wkt_poly = geo_bound.getWKTPolygon()
            logger.info("Checking for available scenes for \""+wkt_poly+"\"")
            query_str_geobound = "footprint:\"Intersects("+wkt_poly+")\""
            query_str = query_str_date + ", " + query_str_platform + ", " + query_str_product + ", " + query_str_geobound
            url = self.createQueryURL(self.base_api_url, 0, n_step)
            logger.debug("Going to use the following URL: " + url)
            logger.debug("Using the following query: " + query_str)
            response = session.post(url, {"q": query_str}, auth=session.auth)
            if self.checkResponse(response, url):
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
                    self.parseResponseJSON(rsp_json, ses, n_records_expt)
                if n_results > 100:
                    n_remain_scns = int((n_results-n_step)%n_step)
                    n_full_pages = int(((n_results-n_step)-n_remain_scns)/n_step)
                    logger.debug("n_pages = {0} \t n_remain_scns = {1}".format(n_full_pages, n_remain_scns))
                    if n_full_pages > 0:
                        start_off = n_step
                        for i in range(n_full_pages):
                            url = self.createQueryURL(self.base_api_url, start_off, n_step)
                            logger.debug("Going to use the following URL: " + url)
                            logger.debug("Using the following query: " + query_str)
                            response = session.post(url, {"q": query_str}, auth=session.auth)
                            if self.checkResponse(response, url):
                                rsp_json = response.json()["feed"]
                                self.parseResponseJSON(rsp_json, ses, n_step)
                            start_off = start_off + n_step
                    if n_remain_scns > 0:
                        start_off = n_results - n_remain_scns
                        n_scns = n_remain_scns
                        url = self.createQueryURL(self.base_api_url, start_off, n_scns)
                        logger.debug("Going to use the following URL: " + url)
                        logger.debug("Using the following query: " + query_str)
                        response = session.post(url, {"q": query_str}, auth=session.auth)
                        if self.checkResponse(response, url):
                            rsp_json = response.json()["feed"]
                            self.parseResponseJSON(rsp_json, ses, n_remain_scns)
            logger.debug("Processed query result and added to local database for \""+wkt_poly+"\"")

        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Remote_URL == None).all()
        if query_result is not None:
            for record in query_result:
                url = self.base_api_url + "odata/v1/Products('{}')?$format=json".format(record.UUID)
                response = session.get(url, auth=session.auth)
                if not self.checkResponse(response, url):
                    logger.error("Could not get the URL for scene: '{}'".format(record.UUID))
                json_url_info = response.json()['d']
                record.Remote_URL_MD5 = json_parse_helper.getStrValue(json_url_info, ["Checksum", "Value"])
                record.Remote_URL = json_parse_helper.getStrValue(json_url_info, ["__metadata", "media_src"])
                record.Total_Size = int(json_parse_helper.getNumericValue(json_url_info, ["ContentLength"]))
            ses.commit()
        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked for availability of new scenes", sensor_val=self.sensorName, updated_lcl_db=True, scns_avail=new_scns_avail)

    def downloadNewData(self, ncores):
        """

        :param ncores:
        :return:
        """
        continue_downloads = True

        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()

        query_result = ses.query(EDDSentinel1ESA).filter(EDDSentinel1ESA.Downloaded == False).filter(EDDSentinel1ESA.Remote_URL != None).all()
        dwnld_params = []
        if query_result is not None:
            logger.debug("Create the output directory for this download.")
            dt_obj = datetime.datetime.now()
            lcl_dwnld_path = os.path.join(self.baseDownloadPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(lcl_dwnld_path):
                os.mkdir(lcl_dwnld_path)

            for record in query_result:
                out_filename = record.Identifier+".zip"
                downloaded_new_scns = True
                dwnld_params.append([record.UUID, record.Remote_URL, record.Remote_URL_MD5, record.Total_Size, self.dbInfoObj, os.path.join(lcl_dwnld_path, out_filename), self.esaUser, self.esaPass, continue_downloads])
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=ncores) as pool:
            pool.map(_download_scn_esa, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked downloaded new scenes.", sensor_val=self.sensorName, updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)


    def convertNewData2ARD(self, ncores):
        raise EODataDownException("Not implemented.")



