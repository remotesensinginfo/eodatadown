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
import sys
import importlib

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

class EDDICESAT2(Base):
    __tablename__ = "EDDICESAT2"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Producer_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Granule_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Title = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Start_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    End_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Updated_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Product = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Version = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Online = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Original_Format = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Orb_Ascending_Crossing = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Orb_Start_Direct = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Orb_Start_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Orb_End_Direct = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Orb_End_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Eq_Cross_Time = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Eq_Cross_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Orbit_Number = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Total_Size = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
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


def _download_icesat2_file(params):
    """
    Function which is used with multiprocessing pool object for downloading ICESAT2 data.

    :param params: List of parameters [PID, Product_ID, Remote_URL, DB_Info_Obj, download_path, username, password]

    """
    pid = params[0]
    producer_id = params[1]
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
            logger.error("An error has occurred while downloading from ICESAT2: '{}'".format(e))
    end_date = datetime.datetime.now()

    if success and os.path.exists(exp_out_file) and os.path.isfile(exp_out_file):
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == pid).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: {}".format(producer_id))
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


class EODataDownICESAT2Sensor (EODataDownSensor):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """

    def __init__(self, db_info_obj):
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "ICESAT2"
        self.db_tab_name = "EDDICESAT2"
        self.ard_vec_format = "GPKG"

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
            logger.debug("Testing config file is for 'ICESAT2'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'ICESAT2'")

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
                prod_id = json_parse_helper.getStrValue(product, ["product"],
                                                        ["ATL02", "ATL03", "ATL04", "ATL06", "ATL07", "ATL08",
                                                         "ATL09", "ATL10", "ATL12", "ATL13"])

                prod_version = json_parse_helper.getStrValue(product,  ["version"],
                                                            ["001", "002", "003", "004", "005", "006", "007",
                                                             "008", "009", "010"])
                self.productsLst.append({"product":prod_id, "version":prod_version})

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

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "lcl_data_cache"]):
                self.dir_lcl_data_cache = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor",
                                                                                       "download", "lcl_data_cache"])
            else:
                self.dir_lcl_data_cache = None
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

            logger.debug("Find EarthData Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "earthdata", "usrpassfile"]):
                usr_pass_file = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "earthdata", "usrpassfile"])
                if os.path.exists(usr_pass_file):
                    usr_pass_info = eodd_utils.readTextFile2List(usr_pass_file)
                    self.earthDataUser = usr_pass_info[0]
                    self.earthDataPass = edd_pass_encoder.unencodePassword(usr_pass_info[1])
                else:
                    raise EODataDownException("The username/password file specified does not exist on the system.")
            else:
                self.earthDataUser = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "earthdata", "user"])
                self.earthDataPass = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "earthdata", "pass"]))
            logger.debug("Found EarthData Account params from config file")

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

        logger.debug("Creating EDDICESAT2 Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def check_http_response_auth(self, response, url):
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
                excpt_msgs = response.json()["errors"]
                excpt_msg = ""
                n = 1
                for msg in excpt_msgs:
                    if n == 1:
                        excpt_msg = "{}, {}".format(n, msg)
                    else:
                        excpt_msg = "{}; {}, {}".format(excpt_msg, n, msg)
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

    def check_http_response_prod(self, response, url):
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
                excpt_msgs = response.json()["errors"]
                excpt_msg = ""
                n = 1
                for msg in excpt_msgs:
                    if n == 1:
                        excpt_msg = "{}, {}".format(n, msg)
                    else:
                        excpt_msg = "{}; {}, {}".format(excpt_msg, n, msg)
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

    def check_http_response_granules(self, response, url):
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
                excpt_msgs = response.json()["errors"]
                excpt_msg = ""
                n = 1
                for msg in excpt_msgs:
                    if n == 1:
                        excpt_msg = "{}, {}".format(n, msg)
                    else:
                        excpt_msg = "{}; {}, {}".format(excpt_msg, n, msg)
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
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        logger.debug("Creating HTTP Session Object.")
        session_req = requests.Session()
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_req.headers["User-Agent"] = user_agent
        headers = {'Accept': 'application/json'}

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        # Get the next PID value to ensure increment
        c_max_pid = ses.query(func.max(EDDICESAT2.PID).label("max_pid")).one().max_pid
        if c_max_pid is None:
            n_max_pid = 0
        else:
            n_max_pid = c_max_pid + 1


        """
        ###########################
        # Authenticate the User:
        token_api_url = 'https://cmr.earthdata.nasa.gov/legacy-services/rest/tokens'
        hostname = socket.gethostname()
        lcl_ip = socket.gethostbyname(hostname)

        data = {
            'token': {
                'username'       : self.earthDataUser,
                'password'       : self.earthDataPass,
                'client_id'      : 'EODataDown_client_id',
                'user_ip_address': lcl_ip
            }
        }
        auth_resp = session_req.post(token_api_url, json=data, headers=headers)
        if not self.check_http_response_auth(auth_resp, token_api_url):
            raise EODataDownException("Failed to authenticate.")
        json_rspn = json.loads(auth_resp.content)
        api_token = json_parse_helper.getStrValue(json_rspn, ['token', 'id'])
        print(api_token)
        # Authenticated
        ###########################
        """
        ###########################
        # Check Product and Versions
        cmr_collcts_url = 'https://cmr.earthdata.nasa.gov/search/collections.json'
        for prod in self.productsLst:
            prod_srch_params = {'short_name': prod["product"]}
            prod_resp = session_req.get(cmr_collcts_url, params=prod_srch_params, headers=headers)
            if not self.check_http_response_auth(prod_resp, cmr_collcts_url):
                raise EODataDownException("Failed to find product information.")
            prod_info = json.loads(prod_resp.content)
            prod_versions = [i['version_id'] for i in prod_info['feed']['entry']]
            if prod['version'] not in prod_versions:
                vers_str = ""
                for ver in prod_versions:
                    if vers_str == "":
                        vers_str = ver
                    else:
                        vers_str = "{}, {}".format(vers_str, ver)
                raise EODataDownException("The specified version ({}) for {} product is not available."
                                          "Available options are: '{}'".format(prod['version'],
                                                                               prod['product'], vers_str))
            max_version = max(prod_versions)
            num_max_version = int(max_version)
            num_prod_version = int(prod['version'])
            if num_prod_version < num_max_version:
                logger.warning("You are not using the most recent version ({}) of "
                               "the product ({}: {})".format(max_version, prod['product'], prod['version']))
        # Checked product versions
        ###########################

        ###########################
        # Find Available Downloads
        new_scns_avail = False
        query_datetime = datetime.datetime.now()
        granule_search_url = 'https://cmr.earthdata.nasa.gov/search/granules'
        end_date_str = datetime.datetime.now().strftime("%Y-%m-%dT23:59:59Z")
        for prod in self.productsLst:
            logger.info("Finding Granules for Product '{}'".format(prod['product']))
            logger.debug("Find the start date for query for product '{}' - if table is empty then using "
                         "config date otherwise date of last acquried image.".format(prod["product"]))
            query_date = self.startDate
            if (not check_from_start) and \
                    (ses.query(EDDICESAT2).filter(EDDICESAT2.Product == prod["product"]).first() is not None):
                query_date = ses.query(EDDICESAT2).filter(EDDICESAT2.Product == prod["product"]).\
                    order_by(EDDICESAT2.Start_Time.desc()).first().Start_Time
            logger.info("Query for product '{}' with start at date: {}".format(prod["product"], query_date))

            start_date_str = query_date.strftime("%Y-%m-%dT00:00:00Z")
            temporal_str = "{},{}".format(start_date_str, end_date_str)

            for geo_bound in self.geoBounds:
                logger.info("Finding Granules for Product '{}' in BBOX: [{}]".format(prod['product'], geo_bound.getSimpleBBOXStr()))
                search_params = {
                    'short_name'  : prod["product"],
                    'version'     : prod["version"],
                    'temporal'    : temporal_str,
                    'page_size'   : 100,
                    'page_num'    : 1,
                    'bounding_box': geo_bound.getBBOXLLURStr()
                }
                db_records = list()
                while True:
                    granules_resp = session_req.get(granule_search_url, params=search_params, headers=headers)
                    if not self.check_http_response_granules(granules_resp, granule_search_url):
                        raise EODataDownException("Failed to expected response to granules search")
                    granules_rslt = json.loads(granules_resp.content)

                    if not json_parse_helper.doesPathExist(granules_rslt, ['feed', 'entry']):
                        break

                    granule_entries = json_parse_helper.getListValue(granules_rslt, ['feed', 'entry'])

                    if len(granule_entries) == 0:
                        # Out of results, so break out of loop
                        break
                    for granule_meta in granule_entries:
                        invalid_granule = False
                        gran_id = json_parse_helper.getStrValue(granule_meta, ['id'])
                        gran_size = json_parse_helper.getNumericValue(granule_meta, ['granule_size'])
                        gran_online = json_parse_helper.getBooleanValue(granule_meta, ['online_access_flag'])
                        gran_orig_format = json_parse_helper.getStrValue(granule_meta, ['original_format'])
                        gran_producer_id = json_parse_helper.getStrValue(granule_meta, ['producer_granule_id'])
                        gran_title = json_parse_helper.getStrValue(granule_meta, ['title'])
                        gran_start_time = json_parse_helper.getDateTimeValue(granule_meta, ['time_start'],
                                                                             ["%Y-%m-%dT%H:%M:%S.%f"])
                        gran_end_time = json_parse_helper.getDateTimeValue(granule_meta, ['time_end'],
                                                                           ["%Y-%m-%dT%H:%M:%S.%f"])
                        gran_updated = json_parse_helper.getDateTimeValue(granule_meta, ['updated'],
                                                                          ["%Y-%m-%dT%H:%M:%S.%f"])

                        if json_parse_helper.doesPathExist(granule_meta, ['orbit']):
                            gran_orb_ascending_crossing = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'ascending_crossing'])
                            gran_orb_start_direction = json_parse_helper.getStrValue(granule_meta, ['orbit', 'start_direction'])
                            gran_orb_start_lat = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'start_lat'])
                            gran_orb_end_direction = json_parse_helper.getStrValue(granule_meta, ['orbit', 'end_direction'])
                            gran_orb_end_lat = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'end_lat'])
                        else:
                            gran_orb_ascending_crossing = None
                            gran_orb_start_direction = None
                            gran_orb_start_lat = None
                            gran_orb_end_direction = None
                            gran_orb_end_lat = None

                        north_lat = 0.0
                        south_lat = 0.0
                        east_lon = 0.0
                        west_lon = 0.0
                        if json_parse_helper.doesPathExist(granule_meta, ['boxes']):
                            gran_bbox = json_parse_helper.getListValue(granule_meta, ['boxes'])
                            first_bbox = True
                            for bbox in gran_bbox:
                                bbox_comps = bbox.split(' ')
                                south_lat_tmp = float(bbox_comps[0])
                                west_lon_tmp = float(bbox_comps[1])
                                north_lat_tmp = float(bbox_comps[2])
                                east_lon_tmp = float(bbox_comps[3])

                                if first_bbox:
                                    west_lon = west_lon_tmp
                                    south_lat = south_lat_tmp
                                    east_lon = east_lon_tmp
                                    north_lat = north_lat_tmp
                                    first_bbox = False
                                else:
                                    if west_lon_tmp < west_lon:
                                        west_lon = west_lon_tmp
                                    if south_lat_tmp < south_lat:
                                        south_lat = south_lat_tmp
                                    if east_lon_tmp > east_lon:
                                        east_lon = east_lon_tmp
                                    if north_lat_tmp > north_lat:
                                        north_lat = north_lat_tmp
                        elif json_parse_helper.doesPathExist(granule_meta, ['polygons']):
                            gran_polys = json_parse_helper.getListValue(granule_meta, ['polygons'])
                            first_coord = True
                            for poly in gran_polys:
                                for coord_str in poly:
                                    coord_str_comps = coord_str.split(' ')
                                    n_pts = int(len(coord_str_comps)/2)
                                    for pt in range(n_pts):
                                        lat_val = float(coord_str_comps[(pt*2)])
                                        lon_val = float(coord_str_comps[(pt*2)+1])
                                        if first_coord:
                                            west_lon = lon_val
                                            south_lat = lat_val
                                            east_lon = lon_val
                                            north_lat = lat_val
                                            first_coord = False
                                        else:
                                            if lon_val < west_lon:
                                                west_lon = lon_val
                                            if lat_val < south_lat:
                                                south_lat = lat_val
                                            if lon_val > east_lon:
                                                east_lon = lon_val
                                            if lat_val > north_lat:
                                                north_lat = lat_val
                        else:
                            if gran_size > 4.0:
                                import pprint
                                pprint.pprint(granule_meta)
                                raise EODataDownException("No BBOX defined for {}".format(gran_producer_id))
                            else:
                                invalid_granule = True
                                logger.debug("The granule '{}' has been defined as invalid as no BBOX "
                                             "or polygon was defined.".format(gran_producer_id))

                        if not invalid_granule:
                            granule_links = json_parse_helper.getListValue(granule_meta, ['links'])
                            gran_url = None
                            for link in granule_links:
                                if link['type'] == 'application/x-hdfeos':
                                    gran_url = link['href']
                                    break
                            if gran_url is None:
                                raise EODataDownException("Could not find a dataset URL for '{}'".format(gran_producer_id))

                            granule_extra_info = dict()
                            granule_extra_info['granule'] = dict()
                            use_extra_info = False
                            gran_equator_crossing_date_time = None
                            gran_equator_crossing_longitude = None
                            gran_orbit_number = None
                            if json_parse_helper.doesPathExist(granule_meta, ['orbit_calculated_spatial_domains']):
                                if len(granule_meta['orbit_calculated_spatial_domains']) == 1:
                                    orb_calcd_spat_domain = granule_meta['orbit_calculated_spatial_domains'][0]
                                    gran_equator_crossing_date_time = json_parse_helper.getDateTimeValue(orb_calcd_spat_domain, ['equator_crossing_date_time'], ["%Y-%m-%dT%H:%M:%S.%f"])
                                    gran_equator_crossing_longitude = json_parse_helper.getNumericValue(orb_calcd_spat_domain, ['equator_crossing_longitude'], -180, 180)
                                    gran_orbit_number = int(json_parse_helper.getNumericValue(orb_calcd_spat_domain, ['orbit_number']))
                                else:
                                    use_extra_info = True
                                    granule_extra_info['granule']['orbit_calculated_spatial_domains'] = granule_meta['orbit_calculated_spatial_domains']
                            if json_parse_helper.doesPathExist(granule_meta, ['polygons']):
                                use_extra_info = True
                                granule_extra_info['granule']['polygons'] = granule_meta['polygons']
                            if json_parse_helper.doesPathExist(granule_meta, ['boxes']):
                                use_extra_info = True
                                granule_extra_info['granule']['boxes'] = granule_meta['boxes']

                            if not use_extra_info:
                                granule_extra_info = None

                        if not invalid_granule:
                            db_records.append(EDDICESAT2(PID=n_max_pid, Producer_ID=gran_producer_id, Granule_ID=gran_id,
                                                         Title=gran_title, Start_Time=gran_start_time,
                                                         End_Time=gran_end_time, Updated_Time=gran_updated,
                                                         Product=prod["product"], Version=prod["version"],
                                                         Online=gran_online, Original_Format=gran_orig_format,
                                                         Orb_Ascending_Crossing=gran_orb_ascending_crossing,
                                                         Orb_Start_Direct=gran_orb_start_direction, Orb_Start_Lat=gran_orb_start_lat,
                                                         Orb_End_Direct=gran_orb_end_direction, Orb_End_Lat=gran_orb_end_lat,
                                                         Eq_Cross_Time=gran_equator_crossing_date_time,
                                                         Eq_Cross_Lon=gran_equator_crossing_longitude,
                                                         Orbit_Number=gran_orbit_number,
                                                         North_Lat=north_lat, South_Lat=south_lat, East_Lon=east_lon,
                                                         West_Lon=west_lon, Total_Size=gran_size, Remote_URL=gran_url,
                                                         Query_Date=query_datetime, ExtendedInfo=granule_extra_info))
                        n_max_pid += 1

                    # Increment page_num
                    search_params['page_num'] += 1
                logger.info("Adding {} records to the database.".format(len(db_records)))
                if len(db_records) > 0:
                    logger.debug("Writing records to the database.")
                    ses.add_all(db_records)
                    ses.commit()
                    logger.debug("Written and committed records to the database.")
                    new_scns_avail = True
        # Found Available Downloads
        ###########################

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
            import rsgislib.vectorutils
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            logger.debug("Perform query to find scenes which need downloading.")

            if all_scns:
                scns = ses.query(EDDICESAT2).order_by(EDDICESAT2.Start_Time.asc()).all()
            else:
                scns = ses.query(EDDICESAT2).filter(EDDICESAT2.Downloaded == False).order_by(
                        EDDICESAT2.Start_Time.asc()).all()

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
                        logger.info("Removing scene {} from ICESAT-2 as it does not intersect.".format(scn.PID))
                        ses.query(EDDICESAT2.PID).filter(EDDICESAT2.PID == scn.PID).delete()
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
        query_result = ses.query(EDDICESAT2).order_by(EDDICESAT2.Start_Time.asc()).all()
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
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.Downloaded == False).filter(
                EDDICESAT2.Remote_URL is not None).order_by(EDDICESAT2.Start_Time.asc()).all()

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
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id).one()
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
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id,
                                                    EDDICESAT2.Downloaded == False).filter(
                                                    EDDICESAT2.Remote_URL is not None).all()
        ses.close()
        success = False
        if query_result is not None:
            if len(query_result) == 1:
                record = query_result[0]
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                producer_id = record.Producer_ID
                basename = os.path.splitext(producer_id)[0]

                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}_{}".format(basename, record.Granule_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                _download_icesat2_file([record.PID, producer_id, record.Remote_URL, self.db_info_obj,
                                        scn_lcl_dwnld_path, os.path.join(scn_lcl_dwnld_path, producer_id),
                                        self.earthDataUser, self.earthDataPass, self.dir_lcl_data_cache])
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

        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.Downloaded == False).filter(
                                                    EDDICESAT2.Remote_URL is not None).order_by(
                                                    EDDICESAT2.Start_Time.asc()).all()
        dwnld_params = list()
        downloaded_new_scns = False
        if query_result is not None:
            for record in query_result:
                logger.debug("Building download info for '" + record.Remote_URL + "'")
                producer_id = record.Producer_ID
                basename = os.path.splitext(producer_id)[0]

                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}_{}".format(basename, record.Granule_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                downloaded_new_scns = True
                dwnld_params.append([record.PID, producer_id, record.Remote_URL, self.db_info_obj,
                                        scn_lcl_dwnld_path, os.path.join(scn_lcl_dwnld_path, producer_id),
                                        self.earthDataUser, self.earthDataPass, self.dir_lcl_data_cache])
        else:
            downloaded_new_scns = False
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_icesat2_file, dwnld_params)
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
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id).all()
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
        query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id).all()
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
        return copy.copy(scn_record.Start_Time)

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
                query_result = ses.query(EDDICESAT2).filter(
                        sqlalchemy.or_(
                                EDDICESAT2.ExtendedInfo.is_(None),
                                sqlalchemy.not_(EDDICESAT2.ExtendedInfo.has_key(plugin_key))),
                        EDDICESAT2.Invalid == False,
                        EDDICESAT2.ARDProduct == True).order_by(EDDICESAT2.Start_Time.asc()).all()

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
            query_result = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id).one_or_none()
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
            scn_db_obj = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == unq_id).one_or_none()
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
                        ses.commit()
                        logger.debug("Updated the extended info field in the database.")
                        ses.close()
                        logger.debug("Closed the database session.")
                    else:
                        logger.debug("The plugin analysis has not been completed - UNSUCCESSFUL.")
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
                    query_result = ses.query(EDDICESAT2).all()
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
                                ses.commit()
                else:
                    logger.debug("Scene PID {} has been provided so resetting.".format(scn_pid))
                    scn_db_obj = ses.query(EDDICESAT2).filter(EDDICESAT2.PID == scn_pid).one_or_none()
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
                        ses.commit()
                ses.close()

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
        vld_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.Invalid == False).count()
        invld_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.Invalid == True).count()
        dwn_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.Downloaded == True).count()
        ard_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.ARDProduct == True).count()
        dcload_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.DCLoaded == True).count()
        arch_scn_count = ses.query(EDDICESAT2).filter(EDDICESAT2.Archived == True).count()
        info_dict['n_scenes'] = dict()
        info_dict['n_scenes']['n_valid_scenes'] = vld_scn_count
        info_dict['n_scenes']['n_invalid_scenes'] = invld_scn_count
        info_dict['n_scenes']['n_downloaded_scenes'] = dwn_scn_count
        info_dict['n_scenes']['n_ard_processed_scenes'] = ard_scn_count
        info_dict['n_scenes']['n_dc_loaded_scenes'] = dcload_scn_count
        info_dict['n_scenes']['n_archived_scenes'] = arch_scn_count
        logger.debug("Calculated the scene count.")

        logger.debug("Find the scene file sizes.")
        file_sizes = ses.query(EDDICESAT2.Total_Size).filter(EDDICESAT2.Invalid == False).all()
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
                        info_dict['file_size']['file_size_quartiles'] = statistics.quantiles(file_sizes_nums)
        logger.debug("Calculated the scene file sizes.")

        logger.debug("Find download and processing time stats.")
        download_times = []
        ard_process_times = []
        scns = ses.query(EDDICESAT2).filter(EDDICESAT2.Downloaded == True)
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
            info_dict['download_time']['download_time_quartiles_secs'] = statistics.quantiles(download_times)

        if len(ard_process_times) > 0:
            info_dict['ard_process_time'] = dict()
            info_dict['ard_process_time']['ard_process_time_mean_secs'] = statistics.mean(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_min_secs'] = min(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_max_secs'] = max(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_stdev_secs'] = statistics.stdev(ard_process_times)
            info_dict['ard_process_time']['ard_process_time_quartiles_secs'] = statistics.quantiles(ard_process_times)
        logger.debug("Calculated the download and processing time stats.")
        return info_dict

