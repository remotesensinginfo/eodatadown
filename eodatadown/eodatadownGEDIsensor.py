#!/usr/bin/env python
"""
EODataDownGEDIsensor.
"""
# This file is part of 'EODataDown'
# A tool for automating Earth Observation Data Downloading.
#
# Copyright 2020 Pete Bunting
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
# Purpose:  Provides an implementation for the GEDI sensor.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 15/04/2020
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import datetime
import os
import shutil
import multiprocessing

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import func

import requests

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDGEDI(Base):
    __tablename__ = "EDDGEDI"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Product_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    FileName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Date_Acquired = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    Time_Acquired = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Product = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Version = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    # MORE ATTRIBUTES
    # MORE ATTRIBUTES
    # MORE ATTRIBUTES
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Total_Size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    File_MD5 = sqlalchemy.Column(sqlalchemy.String, nullable=True)
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

def _download_gedi_file(params):
    """
    Function which is used with multiprocessing pool object for downloading GEDI data.

    :param params: List of parameters [PID, Product_ID, Remote_URL, DB_Info_Obj, download_path, username, password]

    """
    pid = params[0]
    product_id = params[1]
    remote_url = params[2]
    db_info_obj = params[3]
    scn_lcl_dwnld_path = params[4]
    exp_out_file = params[5]
    earth_data_user = params[6]
    earth_data_pass = params[7]
    dir_lcl_data_cache = params[8]
    success = False

    found_lcl_file = False
    if dir_lcl_data_cache is not None:
        file_name = os.path.basename(exp_out_file)
        for lcl_dir in dir_lcl_data_cache:
            if os.path.exists(lcl_dir) and os.path.isdir(lcl_dir):
                lcl_file = os.path.join(lcl_dir, file_name)
                if os.path.exists(lcl_file):
                    found_lcl_file = True
                    break
    start_date = datetime.datetime.now()
    if found_lcl_file:
        shutil.copy(lcl_file, scn_lcl_dwnld_path)
        success = True
    else:
        eodd_wget_downloader = eodatadown.eodatadownutils.EODDWGetDownload()
        try:
            success = eodd_wget_downloader.downloadFile(remote_url, scn_lcl_dwnld_path, username=earth_data_user,
                                                        password=earth_data_pass, try_number="10", time_out="60")
        except Exception as e:
            logger.error("An error has occurred while downloading from GEDI: '{}'".format(e))
    end_date = datetime.datetime.now()

    if success and os.path.exists(exp_out_file) and os.path.isfile(exp_out_file):
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == pid).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + product_id)
        else:
            fileHashUtils = eodatadown.eodatadownutils.EDDCheckFileHash()
            file_md5 = fileHashUtils.calcMD5Checksum(exp_out_file)
            query_result.Downloaded = True
            query_result.Download_Start_Date = start_date
            query_result.Download_End_Date = end_date
            query_result.Download_Path = scn_lcl_dwnld_path
            query_result.File_MD5 = file_md5
            ses.commit()
        ses.close()
        logger.info("Finished download and updated database: {}".format(scn_lcl_dwnld_path))
    else:
        logger.error("Download did not complete, re-run and it should try again: {}".format(scn_lcl_dwnld_path))


class EODataDownGEDISensor (EODataDownSensor):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """

    def __init__(self, db_info_obj):
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "GEDI"
        self.db_tab_name = "EDDGEDI"

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
            logger.debug("Testing config file is for 'GEDI'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'GEDI'")

            logger.debug("Find ARD processing params from config file")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams"]):
                if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "vecformat"]):
                    self.ard_vec_format = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor",
                                                                                      "ardparams", "vecformat"])

                self.ardProjDefined = False
                self.projabbv = ""
                self.projEPSG = None
                if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                    self.ardProjDefined = True
                    self.projabbv = json_parse_helper.getStrValue(config_data,
                                                                  ["eodatadown", "sensor", "ardparams", "proj",
                                                                   "projabbv"])
                    self.projEPSG = int(json_parse_helper.getNumericValue(config_data,
                                                                          ["eodatadown", "sensor", "ardparams",
                                                                           "proj",
                                                                           "epsg"], 0, 1000000000))
                else:
                    self.ard_vec_format = "GEOJSON"
            else:
                self.ard_vec_format = "GEOJSON"
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths"]):
                self.parse_output_paths_config(config_data["eodatadown"]["sensor"]["paths"])
            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            self.startDate = json_parse_helper.getDateValue(config_data,
                                                            ["eodatadown", "sensor", "download", "startdate"],
                                                            "%Y-%m-%d")

            if not json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "products"]):
                raise EODataDownException("You must provide at least one product you want to be downloaded.")

            products_lst = json_parse_helper.getListValue(config_data,
                                                            ["eodatadown", "sensor", "download", "products"])
            self.productsLst = []
            for product in products_lst:
                prod_id = json_parse_helper.getStrValue(product, ["product"], ["GEDI01_B", "GEDI02_A", "GEDI02_B"])

                prod_version = json_parse_helper.getStrValue(product, ["version"],
                                                             ["001", "002", "003", "004", "005", "006", "007",
                                                              "008", "009", "010"])
                self.productsLst.append({"product": prod_id, "version": prod_version})

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
            logger.debug("Found search params from config file")

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "lcl_data_cache"]):
                self.dir_lcl_data_cache = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor",
                                                                                       "download", "lcl_data_cache"])
            else:
                self.dir_lcl_data_cache = None

            logger.debug("Find EarthData Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.earthDataUser = json_parse_helper.getStrValue(config_data,
                                                               ["eodatadown", "sensor", "earthdata", "user"])
            self.earthDataPass = edd_pass_encoder.unencodePassword(
                json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "earthdata", "pass"]))
            logger.debug("Found EarthData Account params from config file")

            logger.debug("Find the plugins params")
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "plugins"]):
                self.parse_plugins_config(config_data["eodatadown"]["sensor"]["plugins"])
            logger.debug("Found the plugins params")

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

        logger.debug("Creating EDDGEDI Database.")
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
                    excpt_msg = response.json()
                    if excpt_msg is None:
                        raise Exception()
                    elif excpt_msg == "":
                        raise Exception()
                except:
                    excpt_msg = "Unknown error ('{0}'), check url in a web browser: '{1}'".format(response.reason, url)
            api_error = EODataDownResponseException(excpt_msg, response)
            api_error.__cause__ = None
            raise api_error
        return success

    def check_new_scns(self, check_from_start=False):
        logger.debug("Creating HTTP Session Object.")
        session_req = requests.Session()
        user_agent = "eoedatadown/{}".format(eodatadown.EODATADOWN_VERSION)
        session_req.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug(
                "Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if (not check_from_start) and (ses.query(EDDGEDI).first() is not None):
            query_date = ses.query(EDDGEDI).order_by(EDDGEDI.Date_Acquired.desc()).first().Date_Acquired
        logger.info("Query with start at date: " + str(query_date))

        # Get the next PID value to ensure increment
        c_max_pid = ses.query(func.max(EDDGEDI.PID).label("max_pid")).one().max_pid
        if c_max_pid is None:
            n_max_pid = 0
        else:
            n_max_pid = c_max_pid + 1
        query_date_int = int(query_date.strftime("%Y%j"))

        new_scns_avail = False
        query_datetime = datetime.datetime.now()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        url_base = 'https://lpdaacsvc.cr.usgs.gov/services/gedifinder?output=json'
        db_records = list()
        for prod in self.productsLst:
            query_base_url = "{}&product={}&version={}".format(url_base, prod['product'], prod['version'])
            for geo_bound in self.geoBounds:
                query_url = "{}&bbox={}".format(query_base_url, geo_bound.getSimpleBBOXStr())
                logger.debug("Going to use the following URL: " + query_url)
                logger.info("Query URL: {}".format(query_url))
                response = session_req.get(query_url, auth=session_req.auth)
                if self.check_http_response(response, query_url):
                    rsp_json = response.json()
                    if json_parse_helper.doesPathExist(rsp_json, ["data"]):
                        data_lst = rsp_json['data']
                        n_new_scns = 0
                        for data_url in data_lst:
                            basename = os.path.basename(data_url)
                            scn_date_comps = basename.split('_')
                            if len(scn_date_comps) >= 2:
                                scn_date_str = scn_date_comps[2]
                                if len(scn_date_str) != 13:
                                    raise EODataDownException("The date format is not as expected, "
                                                              "expected 13 characters. '{}'".format(scn_date_str))
                                scn_date_str = scn_date_str[:7]
                                if not scn_date_str.isnumeric():
                                    raise EODataDownException("Something unexpected about the file name format "
                                                              "did not find a date: '{}'".format(basename))
                                scn_date_int = int(scn_date_str)
                                if scn_date_int > query_date_int:
                                    acq_date = datetime.datetime.strptime(scn_date_str, '%Y%j').date()
                                    product_id = os.path.splitext(basename)[0]
                                    db_records.append(EDDGEDI(PID=n_max_pid, Product_ID=product_id, FileName=basename,
                                                              Date_Acquired=acq_date, Product=prod['product'],
                                                              Version=prod['version'], Remote_URL=data_url,
                                                              Query_Date=query_datetime))
                                    n_max_pid += 1
                                    n_new_scns += 1
                        logger.info("Number Scenes Found: {}".format(n_new_scns))
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
        query_result = ses.query(EDDGEDI).all()
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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == False).filter(
                EDDGEDI.Remote_URL is not None).all()

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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one()
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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id,
                                                 EDDGEDI.Downloaded == False).filter(
                                                 EDDGEDI.Remote_URL is not None).all()
        ses.close()
        success = False
        if query_result is not None:
            if len(query_result) == 1:
                record = query_result[0]
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.FileName
                _download_gedi_file([record.PID, record.Product_ID, record.Remote_URL, self.db_info_obj,
                                     scn_lcl_dwnld_path, os.path.join(scn_lcl_dwnld_path, out_filename),
                                     self.earthDataUser,  self.earthDataPass, self.dir_lcl_data_cache])
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

        query_result = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == False).filter(
                                                 EDDGEDI.Remote_URL is not None).all()
        dwnld_params = list()
        downloaded_new_scns = False
        if query_result is not None:
            for record in query_result:
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.FileName
                downloaded_new_scns = True
                dwnld_params.append([record.PID, record.Product_ID, record.Remote_URL, self.db_info_obj,
                                     scn_lcl_dwnld_path, os.path.join(scn_lcl_dwnld_path, out_filename),
                                     self.earthDataUser,  self.earthDataPass, self.dir_lcl_data_cache])
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_gedi_file, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked downloaded new scenes.", sensor_val=self.sensor_name,
                               updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)

    def get_scnlist_con2ard(self):
        raise Exception("Not Implement...")

    def has_scn_con2ard(self, unq_id):
        raise Exception("Not Implement...")

    def scn2ard(self, unq_id):
        raise Exception("Not Implement...")

    def scns2ard_all_avail(self, n_cores):
        raise Exception("Not Implement...")

    def get_scnlist_datacube(self, loaded=False):
        raise Exception("Not Implement...")

    def has_scn_datacube(self, unq_id):
        raise Exception("Not Implement...")

    def scn2datacube(self, unq_id):
        raise Exception("Not Implement...")

    def scns2datacube_all_avail(self):
        raise Exception("Not Implement...")

    def get_scnlist_quicklook(self):
        raise Exception("Not Implement...")

    def has_scn_quicklook(self, unq_id):
        raise Exception("Not Implement...")

    def scn2quicklook(self, unq_id):
        raise Exception("Not Implement...")

    def scns2quicklook_all_avail(self):
        raise Exception("Not Implement...")

    def get_scnlist_tilecache(self):
        raise Exception("Not Implement...")

    def has_scn_tilecache(self, unq_id):
        raise Exception("Not Implement...")

    def scn2tilecache(self, unq_id):
        raise Exception("Not Implement...")

    def scns2tilecache_all_avail(self):
        raise Exception("Not Implement...")

    def get_scn_record(self, unq_id):
        raise Exception("Not Implement...")

    def get_scnlist_usr_analysis(self):
        raise EODataDownException("Not Implemented")

    def has_scn_usr_analysis(self, unq_id):
        return False

    def run_usr_analysis(self, unq_id):
        raise EODataDownException("Not Implemented")

    def run_usr_analysis_all_avail(self, n_cores):
        raise EODataDownException("Not Implemented")

    def find_unique_platforms(self):
        raise Exception("Not Implement...")

    def query_scn_records_date_count(self, start_date, end_date, valid=True, cloud_thres=None):
        raise Exception("Not Implement...")

    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True, cloud_thres=None):
        raise Exception("Not Implement...")

    def query_scn_records_date_bbox_count(self, start_date, end_date, bbox, valid=True, cloud_thres=None):
        raise Exception("Not Implement...")

    def query_scn_records_date_bbox(self, start_date, end_date, bbox, start_rec=0, n_recs=0,
                                    valid=True, cloud_thres=None):
        raise Exception("Not Implement...")

    def find_unique_scn_dates(self, start_date, end_date, valid=True, order_desc=True, platform=None):
        raise Exception("Not Implement...")

    def get_scns_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None):
        raise Exception("Not Implement...")

    def get_scn_pids_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None):
        raise Exception("Not Implement...")

    def create_scn_date_imgs(self, start_date, end_date, img_size, out_img_dir, img_format, vec_file, vec_lyr,
                             tmp_dir, order_desc=True):
        raise Exception("Not Implement...")

    def create_multi_scn_visual(self, scn_pids, out_imgs, out_img_sizes, out_extent_vec, out_extent_lyr,
                               gdal_format, tmp_dir):
        raise Exception("Not Implement...")

    def query_scn_records_bbox(self, lat_north, lat_south, lon_east, lon_west):
        raise Exception("Not Implement...")

    def update_dwnld_path(self, replace_path, new_path):
        raise Exception("Not Implement...")

    def update_ard_path(self, replace_path, new_path):
        raise Exception("Not Implement...")

    def dwnlds_archived(self, new_path=None):
        raise Exception("Not Implement...")

    def export_db_to_json(self, out_json_file):
        raise Exception("Not Implement...")

    def import_sensor_db(self, input_json_file, replace_path_dict=None):
        raise Exception("Not Implement...")

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        raise Exception("Not Implement...")

    def reset_scn(self, unq_id, reset_download=False, reset_invalid=False):
        raise Exception("Not Implement...")

    def reset_dc_load(self, unq_id):
        raise Exception("Not Implement...")

