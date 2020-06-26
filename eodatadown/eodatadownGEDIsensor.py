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
import traceback

import rsgislib

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


class EDDGEDIPlugins(Base):
    __tablename__ = "EDDGEDIPlugins"
    Scene_PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    PlugInName = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Completed = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Success = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Outputs = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Error = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)


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
    cache_file_dict = params[8]
    use_symlnk_lcl_data_cache = params[9]
    success = False

    found_lcl_file = False
    if cache_file_dict is not None:
        file_name = os.path.basename(exp_out_file)
        if file_name in cache_file_dict:
            lcl_file = cache_file_dict[file_name]
            found_lcl_file = True

    start_date = datetime.datetime.now()
    if found_lcl_file:
        if use_symlnk_lcl_data_cache:
            scn_file_name = os.path.basename(lcl_file)
            scn_symlnk = os.path.join(scn_lcl_dwnld_path, scn_file_name)
            os.symlink(lcl_file, scn_symlnk)
        else:
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
            file_size = os.stat(exp_out_file).st_size
            query_result.Downloaded = True
            query_result.Download_Start_Date = start_date
            query_result.Download_End_Date = end_date
            query_result.Download_Path = scn_lcl_dwnld_path
            query_result.File_MD5 = file_md5
            query_result.Total_Size = file_size
            ses.commit()
        ses.close()
        logger.info("Finished download and updated database: {}".format(scn_lcl_dwnld_path))
    else:
        logger.error("Download did not complete, re-run and it should try again: {}".format(scn_lcl_dwnld_path))


def find_bbox_union(bboxes):
    """
    A function which finds the union of all the bboxes inputted.

    :param bboxes: a list of bboxes [(xMin, xMax, yMin, yMax), (xMin, xMax, yMin, yMax)]
    :return: bbox (xMin, xMax, yMin, yMax)

    """
    if len(bboxes) == 1:
        out_bbox = list(bboxes[0])
    elif len(bboxes) > 1:
        out_bbox = list(bboxes[0])
        for bbox in bboxes:
            if bbox[0] < out_bbox[0]:
                out_bbox[0] = bbox[0]
            if bbox[1] > out_bbox[1]:
                out_bbox[1] = bbox[1]
            if bbox[2] < out_bbox[2]:
                out_bbox[2] = bbox[2]
            if bbox[3] > out_bbox[3]:
                out_bbox[3] = bbox[3]
    else:
        out_bbox = None
    return out_bbox


def _process_to_ard(params):
    import pysl4land.pysl4land_gedi
    scn_pid = params[0]
    gedi_h5_file = params[1]
    out_vec_file = params[2]
    db_info_obj = params[3]
    ard_path = params[4]

    start_date = datetime.datetime.now()
    logger.info("Processing scene: {}".format(scn_pid))
    pysl4land.pysl4land_gedi.gedi02_b_beams_gpkg(gedi_h5_file, out_vec_file, True, 4326)
    logger.info("Finished processing scene: {}".format(scn_pid))
    end_date = datetime.datetime.now()

    import rsgislib.vectorutils
    vec_lyrs = rsgislib.vectorutils.getVecLyrsLst(out_vec_file)
    bboxes = list()
    rsgis_utils = rsgislib.RSGISPyUtils()
    for vec_lyr in vec_lyrs:
        bbox = rsgis_utils.getVecLayerExtent(out_vec_file, vec_lyr, computeIfExp=True)
        bboxes.append(bbox)
    bbox = find_bbox_union(bboxes)

    logger.debug("Set up database connection and update record.")
    db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
    session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
    ses = session_sqlalc()
    query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == scn_pid).one_or_none()
    if query_result is None:
        logger.error("Could not find the scene within local database: {}".format(scn_pid))
    query_result.ARDProduct = True
    query_result.ARDProduct_Start_Date = start_date
    query_result.ARDProduct_End_Date = end_date
    query_result.ARDProduct_Path = ard_path
    query_result.North_Lat = bbox[3]
    query_result.South_Lat = bbox[2]
    query_result.East_Lon = bbox[0]
    query_result.West_Lon = bbox[1]
    ses.commit()
    ses.close()
    logger.debug("Finished download and updated database - scene valid.")


class EODataDownGEDISensor (EODataDownSensor):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """

    def __init__(self, db_info_obj):
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "GEDI"
        self.db_tab_name = "EDDGEDI"
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

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "lcl_data_cache"]):
                self.dir_lcl_data_cache = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor",
                                                                                       "download", "lcl_data_cache"])
            else:
                self.dir_lcl_data_cache = None

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "download", "use_symlnk_data_cache"]):
                self.use_symlnk_lcl_data_cache = json_parse_helper.getBooleanValue(config_data, ["eodatadown", "sensor",
                                                                                                 "download",
                                                                                                 "use_symlnk_data_cache"])
            else:
                self.use_symlnk_lcl_data_cache = False
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

    def rm_scns_intersect(self, all_scns=False):
        """
        A function which checks whether the bounding box for the scene intersects with a specified
        vector layer. If the scene does not intersect then it is deleted from the database. By default
        this is only testing the scenes which have not been downloaded.

        :param all_scns: If True all the scenes in the database will be tested otherwise only the
                         scenes which have not been downloaded will be tested.

        """
        if self.scn_intersect:
            raise EODataDownException("Cannot provide implementation of rm_scns_intersect "
                                      "as database does not have BBOX until after the download "
                                      "has been completed.")

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
        query_result = ses.query(EDDGEDI).order_by(EDDGEDI.Date_Acquired.asc()).all()
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
                EDDGEDI.Remote_URL is not None).order_by(EDDGEDI.Date_Acquired.asc()).all()

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
                cache_file_dict = None
                if self.dir_lcl_data_cache is not None:
                    eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
                    cache_file_dict = eodd_utils.find_files_mpaths_ext(self.dir_lcl_data_cache, 'h5')

                logger.debug("Building download info for '" + record.Remote_URL + "'")
                scn_lcl_dwnld_path = os.path.join(self.baseDownloadPath,
                                                  "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(scn_lcl_dwnld_path):
                    os.mkdir(scn_lcl_dwnld_path)
                out_filename = record.FileName
                _download_gedi_file([record.PID, record.Product_ID, record.Remote_URL, self.db_info_obj,
                                     scn_lcl_dwnld_path, os.path.join(scn_lcl_dwnld_path, out_filename),
                                     self.earthDataUser,  self.earthDataPass, cache_file_dict,
                                     self.use_symlnk_lcl_data_cache])
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
                                                 EDDGEDI.Remote_URL is not None).order_by(
                                                 EDDGEDI.Date_Acquired.asc()).all()
        dwnld_params = list()
        downloaded_new_scns = False
        if query_result is not None:
            cache_file_dict = None
            if self.dir_lcl_data_cache is not None:
                eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
                cache_file_dict = eodd_utils.find_files_mpaths_ext(self.dir_lcl_data_cache, 'h5')

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
                                     self.earthDataUser,  self.earthDataPass, cache_file_dict,
                                     self.use_symlnk_lcl_data_cache])
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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == True,
                                                 EDDGEDI.ARDProduct == False,
                                                 EDDGEDI.Invalid == False).order_by(EDDGEDI.Date_Acquired.asc()).all()

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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return (query_result.ARDProduct == True) and (query_result.Invalid == False)

    def scn2ard(self, unq_id):
        """
        A function which processes a single scene to an analysis ready data (ARD) format.
        :param unq_id: the unique ID of the scene to be processed.
        :return: returns boolean indicating successful or otherwise processing.
        """
        rsgis_utils = rsgislib.RSGISPyUtils()

        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id,
                                                 EDDGEDI.Downloaded == True,
                                                 EDDGEDI.ARDProduct == False).one_or_none()
        ses.close()

        if query_result is not None:
            record = query_result
            logger.debug("Create the specific output directories for the ARD processing.")

            gedi_h5_file = rsgis_utils.findFileNone(record.Download_Path, "GEDI*.h5")
            if gedi_h5_file is None:
                raise EODataDownException("The GEDI data file could not be found in the "
                                          "download path: {}".format(record.Download_Path))

            logger.debug("Create info for running ARD analysis for scene: {}".format(record.PID))
            ard_scn_path = os.path.join(self.ardFinalPath, "{}_{}".format(record.Product_ID, record.PID))
            if not os.path.exists(ard_scn_path):
                os.mkdir(ard_scn_path)

            out_vec_file = os.path.join(ard_scn_path, "{}_{}.gpkg".format(record.Product_ID, record.PID))

            scn_ard_params = list()
            scn_ard_params.append(record.PID)
            scn_ard_params.append(gedi_h5_file)
            scn_ard_params.append(out_vec_file)
            scn_ard_params.append(self.db_info_obj)
            scn_ard_params.append(ard_scn_path)

            _process_to_ard(scn_ard_params)
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

    def scns2ard_all_avail(self, n_cores):
        rsgis_utils = rsgislib.RSGISPyUtils()

        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == True,
                                                 EDDGEDI.ARDProduct == False,
                                                 EDDGEDI.Invalid == False).order_by(EDDGEDI.Date_Acquired.asc()).all()
        ses.close()

        if query_result is not None:
            ard_scns = list()
            for record in query_result:
                logger.debug("Create the specific output directories for the ARD processing.")

                gedi_h5_file = rsgis_utils.findFileNone(record.Download_Path, "GEDI*.h5")
                if gedi_h5_file is None:
                    raise EODataDownException("The GEDI data file could not be found in the "
                                              "download path: {}".format(record.Download_Path))

                logger.debug("Create info for running ARD analysis for scene: {}".format(record.PID))
                ard_scn_path = os.path.join(self.ardFinalPath, "{}_{}".format(record.Product_ID, record.PID))
                if not os.path.exists(ard_scn_path):
                    os.mkdir(ard_scn_path)

                out_vec_file = os.path.join(ard_scn_path, "{}_{}.gpkg".format(record.Product_ID, record.PID))

                scn_ard_params = list()
                scn_ard_params.append(record.PID)
                scn_ard_params.append(gedi_h5_file)
                scn_ard_params.append(out_vec_file)
                scn_ard_params.append(self.db_info_obj)
                scn_ard_params.append(ard_scn_path)
                ard_scns.append(scn_ard_params)

            if len(ard_scns) > 0:
                logger.info("Start processing the scenes.")
                with multiprocessing.Pool(processes=n_cores) as pool:
                    pool.map(_process_to_ard, ard_scns)
                logger.info("Finished processing the scenes.")

            edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
            edd_usage_db.add_entry(description_val="Processed scenes to an ARD product.", sensor_val=self.sensor_name,
                                   updated_lcl_db=True, convert_scns_ard=True)

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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).all()
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
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).all()
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
        return datetime.combine(scn_record.Date_Acquired, datetime.min.time())

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

            query_result = ses.query(EDDGEDI).filter(EDDGEDI.Invalid == False,
                                                     EDDGEDI.ARDProduct == True).order_by(
                                                     EDDGEDI.Date_Acquired.asc()).all()

            for scn in query_result:
                scn_plgin_db_objs = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == scn.PID).all()
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
            query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one_or_none()
            if query_result is None:
                raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))

            scn_plgin_db_objs = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == unq_id).all()
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
                scn_db_obj = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one_or_none()
                if scn_db_obj is None:
                    raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
                logger.debug("Perform query to find scene in plugin DB.")
                plgin_db_obj = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == unq_id,
                                                                EDDGEDIPlugins.PlugInName == plugin_key).one_or_none()
                plgin_db_objs = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == unq_id).all()
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
                        plgin_db_obj = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == unq_id,
                                                                        EDDGEDIPlugins.PlugInName == plugin_key).one_or_none()

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
                        plgin_db_obj = EDDGEDIPlugins(Scene_PID=scn_db_obj.PID, PlugInName=plugin_key,
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
                logger.debug(
                        "A list of plugins to reset has not been provided so populating that list with all plugins.")
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
                        ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.PlugInName == plgin_key).delete(synchronize_session=False)
                        ses.commit()
                else:
                    logger.debug("Scene PID {} has been provided so resetting.".format(scn_pid))
                    if reset_all_plgins:
                        ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == scn_pid).delete(synchronize_session=False)
                    else:
                        scn_plgin_db_objs = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.Scene_PID == scn_pid).all()
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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one_or_none()
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
        query_result = ses.query(EDDGEDI).filter(EDDGEDI.PID == unq_id).one_or_none()
        if query_result is None:
            raise EODataDownException("Scene ('{}') could not be found in database".format(unq_id))
        unq_name = "{}_{}".format(query_result.Product_ID, query_result.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return unq_name

    def get_scn_unq_name_record(self, scn_record):
        """
        A function which returns a name which will be unique using the scene record object passed to the function.

        :param scn_record: the database dict like object representing the scene.
        :return: string with a unique name.

        """
        unq_name = "{}_{}".format(scn_record.Product_ID, scn_record.PID)
        return unq_name

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
        vld_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.Invalid == False).count()
        invld_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.Invalid == True).count()
        dwn_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == True).count()
        ard_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.ARDProduct == True).count()
        dcload_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.DCLoaded == True).count()
        arch_scn_count = ses.query(EDDGEDI).filter(EDDGEDI.Archived == True).count()
        info_dict['n_scenes'] = dict()
        info_dict['n_scenes']['n_valid_scenes'] = vld_scn_count
        info_dict['n_scenes']['n_invalid_scenes'] = invld_scn_count
        info_dict['n_scenes']['n_downloaded_scenes'] = dwn_scn_count
        info_dict['n_scenes']['n_ard_processed_scenes'] = ard_scn_count
        info_dict['n_scenes']['n_dc_loaded_scenes'] = dcload_scn_count
        info_dict['n_scenes']['n_archived_scenes'] = arch_scn_count
        logger.debug("Calculated the scene count.")

        logger.debug("Find the scene file sizes.")
        file_sizes = ses.query(EDDGEDI.Total_Size).filter(EDDGEDI.Invalid == False).all()
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
        scns = ses.query(EDDGEDI).filter(EDDGEDI.Downloaded == True)
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
                scns = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.PlugInName == plgin_key).all()
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
                        info_dict['usr_plugins'][plgin_key]['processing']['time_stdev_secs'] = statistics.stdev(
                            plugin_times)
                    info_dict['usr_plugins'][plgin_key]['processing']['time_median_secs'] = statistics.median(
                        plugin_times)
                    if (len(plugin_times) > 1) and (eodatadown.py_sys_version_flt >= 3.8):
                        info_dict['usr_plugins'][plgin_key]['processing']['time_quartiles_secs'] = statistics.quantiles(
                            plugin_times)
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
            scns = ses.query(EDDGEDIPlugins).filter(EDDGEDIPlugins.PlugInName == plgin_key).all()
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

