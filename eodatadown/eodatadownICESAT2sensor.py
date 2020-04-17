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
import os.path

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
import socket

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




class EODataDownICESAT2Sensor (EODataDownSensor):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """

    def __init__(self, db_info_obj):
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "ICESAT2"
        self.db_tab_name = "EDDICESAT2"

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
                else:
                    self.ard_vec_format = "GEOJSON"
            else:
                self.ard_vec_format = "GEOJSON"
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
            logger.debug("Found search params from config file")

            logger.debug("Find EarthData Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.earthDataUser = json_parse_helper.getStrValue(config_data,
                                                               ["eodatadown", "sensor", "earthdata", "user"])
            self.earthDataPass = edd_pass_encoder.unencodePassword(
                json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "earthdata", "pass"]))
            logger.debug("Found EarthData Account params from config file")

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
                    order_by(EDDICESAT2.Date_Acquired.desc()).first().Date_Acquired
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

                        have_orbit_info = False
                        if json_parse_helper.doesPathExist(granule_meta, ['orbit']):
                            gran_orb_ascending_crossing = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'ascending_crossing'])
                            gran_orb_start_direction = json_parse_helper.getStrValue(granule_meta, ['orbit', 'start_direction'])
                            gran_orb_start_lat = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'start_lat'])
                            gran_orb_end_direction = json_parse_helper.getStrValue(granule_meta, ['orbit', 'end_direction'])
                            gran_orb_end_lat = json_parse_helper.getNumericValue(granule_meta, ['orbit', 'end_lat'])
                            have_orbit_info = True
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
                            if gran_size > 1.0:
                                raise EODataDownException("No BBOX defined for {}".format(gran_producer_id))
                            else:
                                invalid_granule = True

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
        raise Exception("Not Implement...")

    def download_all_avail(self, n_cores):
        raise Exception("Not Implement...")

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

