#!/usr/bin/env python
"""
EODataDown - an abstract sensor class.
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
# Purpose:  Provides an abstract sensor class.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import sys
import importlib
import logging
import json
import os
import os.path
from abc import ABCMeta, abstractmethod

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDObsDates(Base):
    __tablename__ = "EDDObsDates"
    SensorID = sqlalchemy.Column(sqlalchemy.String, nullable=False, primary_key=True)
    PlatformID = sqlalchemy.Column(sqlalchemy.String, nullable=False, primary_key=True)
    ObsDate = sqlalchemy.Column(sqlalchemy.Date, nullable=False, primary_key=True)
    OverviewCreated = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    NeedUpdate = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Invalid = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Overviews = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)

class EDDObsDatesScns(Base):
    __tablename__ = "EDDObsDatesScns"
    SensorID = sqlalchemy.Column(sqlalchemy.String, nullable=False, primary_key=True)
    PlatformID = sqlalchemy.Column(sqlalchemy.String, nullable=False, primary_key=True)
    ObsDate = sqlalchemy.Column(sqlalchemy.Date, nullable=False, primary_key=True)
    Scene_PID = sqlalchemy.Column(sqlalchemy.Integer, nullable=False, primary_key=True)


class EODataDownSensor (object):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """
    __metaclass__ = ABCMeta

    def __init__(self, db_info_obj):
        self.sensor_name = "AbstractBase"
        self.db_tab_name = "AbstractTableName"
        self.db_info_obj = db_info_obj

        self.baseDownloadPath = None
        self.ardProdWorkPath = None
        self.ardFinalPath = None
        self.ardProdTmpPath = None
        self.quicklookPath = None
        self.tilecachePath = None
        self.analysis_plugins = None

    def get_sensor_name(self):
        """
        Get the name of the sensor this class is representing.
        :return: string
        """
        return self.sensor_name

    def get_db_table_name(self):
        """
        Gets the
        :return:
        """
        return self.db_tab_name

    def parse_output_paths_config(self, params_dict):
        """
        Function which parses the input paths

        :param params_dict: input params from eodatadown:sensor:paths

        """
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        self.baseDownloadPath = json_parse_helper.getStrValue(params_dict, ["download"])
        logger.debug("Download Path: {}".format(self.baseDownloadPath))
        self.ardProdWorkPath = json_parse_helper.getStrValue(params_dict, ["ardwork"])
        logger.debug("ARD Work Path: {}".format(self.ardProdWorkPath))
        self.ardFinalPath = json_parse_helper.getStrValue(params_dict, ["ardfinal"])
        logger.debug("ARD Final Path: {}".format(self.ardFinalPath))
        self.ardProdTmpPath = json_parse_helper.getStrValue(params_dict, ["ardtmp"])
        logger.debug("ARD Temp Path: {}".format(self.ardProdTmpPath))

        if json_parse_helper.doesPathExist(params_dict, ["quicklooks"]):
            self.quicklookPath = json_parse_helper.getStrValue(params_dict, ["quicklooks"])
            logger.debug("Quicklook Path: {}".format(self.quicklookPath))
        else:
            self.quicklookPath = None

        if json_parse_helper.doesPathExist(params_dict, ["tilecache"]):
            self.tilecachePath = json_parse_helper.getStrValue(params_dict, ["tilecache"])
            logger.debug("TileCache Path: {}".format(self.tilecachePath))
        else:
            self.tilecachePath = None

    def parse_plugins_config(self, params_dict):
        """
        Function to parse the json config dict for plugsin creating the
        a list of plugins.

        :param params_dict: input params from eodatadown:sensor:plugins

        """
        self.analysis_plugins = list()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        if json_parse_helper.doesPathExist(params_dict, ["analysis"]):
            for plugin_config in params_dict["analysis"]:
                plugin_path = json_parse_helper.getStrValue(plugin_config, ["path"])
                plugin_path = os.path.abspath(plugin_path)
                plugin_module_name = json_parse_helper.getStrValue(plugin_config, ["module"])
                plugin_cls_name = json_parse_helper.getStrValue(plugin_config, ["class"])
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
                if json_parse_helper.doesPathExist(plugin_config, ["params"]):
                    logger.debug("User params are present for plugin so will test the required keys are present.")
                    plugin_cls_inst.set_users_param(plugin_config["params"])
                    plugin_cls_inst.check_param_keys(raise_except=True)
                    logger.debug("User params for the plugin have the correct keys.")
                self.analysis_plugins.append(plugin_config)

    @abstractmethod
    def parse_sensor_config(self, config_file, first_parse=False): pass

    @abstractmethod
    def init_sensor_db(self): pass

    @abstractmethod
    def check_new_scns(self, check_from_start=False): pass

    @abstractmethod
    def get_scnlist_all(self): pass

    @abstractmethod
    def get_scnlist_download(self): pass

    @abstractmethod
    def has_scn_download(self, unq_id): pass

    @abstractmethod
    def download_scn(self, unq_id): pass

    @abstractmethod
    def download_all_avail(self, n_cores): pass

    @abstractmethod
    def get_scnlist_con2ard(self): pass

    @abstractmethod
    def has_scn_con2ard(self, unq_id): pass

    @abstractmethod
    def scn2ard(self, unq_id): pass

    @abstractmethod
    def scns2ard_all_avail(self, n_cores): pass

    @abstractmethod
    def get_scnlist_usr_analysis(self): pass

    @abstractmethod
    def has_scn_usr_analysis(self, unq_id): pass

    def calc_scn_usr_analysis(self):
        """
        Are there user analysis plugins to be executed.

        :return: boolean

        """
        logger.debug("Going to test whether there are any plugins.")
        if self.analysis_plugins == None:
            logger.debug("There are no plugins and list of None.")
            return False
        elif type(self.analysis_plugins) is not list:
            logger.debug("There are no plugins the variable is not None but not a list: {}.".format(type(self.analysis_plugins)))
            return False
        elif len(self.analysis_plugins) == 0:
            logger.debug("There are no plugins the list has a length of zero.")
            return False
        logger.debug("There are {} plugins.".format(len(self.analysis_plugins)))
        return True

    @abstractmethod
    def run_usr_analysis(self, unq_id): pass

    @abstractmethod
    def run_usr_analysis_all_avail(self, n_cores): pass

    @abstractmethod
    def get_scnlist_datacube(self, loaded=False): pass

    @abstractmethod
    def has_scn_datacube(self, unq_id): pass

    @abstractmethod
    def scn2datacube(self, unq_id): pass

    @abstractmethod
    def scns2datacube_all_avail(self): pass

    @abstractmethod
    def get_scnlist_quicklook(self): pass

    def calc_scn_quicklook(self):
        """
        Is a quicklook image(s) to be calculated.

        :return: boolean

        """
        if self.quicklookPath is None:
            return False
        return True

    @abstractmethod
    def has_scn_quicklook(self, unq_id): pass

    @abstractmethod
    def scn2quicklook(self, unq_id): pass

    @abstractmethod
    def scns2quicklook_all_avail(self): pass

    @abstractmethod
    def get_scnlist_tilecache(self): pass

    def calc_scn_tilecache(self):
        """
        Is a tilecache to be calculated.

        :return: boolean

        """
        if self.tilecachePath is None:
            return False
        return True

    @abstractmethod
    def has_scn_tilecache(self, unq_id): pass

    @abstractmethod
    def scn2tilecache(self, unq_id): pass

    @abstractmethod
    def scns2tilecache_all_avail(self): pass

    @abstractmethod
    def get_scn_record(self, unq_id): pass

    @abstractmethod
    def find_unique_platforms(self): pass

    @abstractmethod
    def query_scn_records_date_count(self, start_date, end_date, valid=True, cloud_thres=None): pass

    @abstractmethod
    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True, cloud_thres=None): pass

    @abstractmethod
    def query_scn_records_date_bbox_count(self, start_date, end_date, bbox, valid=True, cloud_thres=None): pass

    @abstractmethod
    def query_scn_records_date_bbox(self, start_date, end_date, bbox, start_rec=0, n_recs=0, valid=True, cloud_thres=None): pass

    @abstractmethod
    def find_unique_scn_dates(self, start_date, end_date, valid=True, order_desc=True, platform=None): pass

    @abstractmethod
    def get_scns_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None): pass

    @abstractmethod
    def get_scn_pids_for_date(self, date_of_interest, valid=True, ard_prod=True, platform=None): pass

    @abstractmethod
    def create_scn_date_imgs(self, start_date, end_date, img_size, out_img_dir, img_format, vec_file, vec_lyr,
                             tmp_dir, order_desc=True): pass

    @abstractmethod
    def create_multi_scn_visual(self, scn_pids, out_imgs, out_img_sizes, out_extent_vec, out_extent_lyr,
                               gdal_format, tmp_dir):pass

    @abstractmethod
    def query_scn_records_bbox(self, lat_north, lat_south, lon_east, lon_west): pass

    @abstractmethod
    def update_dwnld_path(self, replace_path, new_path): pass

    @abstractmethod
    def update_ard_path(self, replace_path, new_path): pass

    @abstractmethod
    def dwnlds_archived(self, new_path=None): pass

    @abstractmethod
    def export_db_to_json(self, out_json_file): pass

    def update_extended_info_qklook_tilecache_paths(self, extendedInfo, replace_path_dict=None):
        """
        Updates the paths for the quicklook and tilecache paths. Other info in the extend info
        will be lost - however in the future a plugin architecture will be adopted which will
        solve this properly. If replace_path_dict is None then the original extendedInfo will
        be returned.
        :param extendedInfo: dict structure
        :param replace_path_dict: a dictionary of paths (keys are the path to replace and value is the replacement value)
        :return: returns a new dict structure with just the quicklook and tilecache elements, if they exist.
        """
        if extendedInfo is None:
            return None
        if extendedInfo == "":
            return None
        if replace_path_dict is None:
            return extendedInfo
        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        out_extended_info = dict()
        if "quicklook" in extendedInfo:
            out_extended_info["quicklook"] = dict()
            out_extended_info["quicklook"]["quicklookimgs"] = list()
            for qklook_img in extendedInfo["quicklook"]["quicklookimgs"]:
                out_extended_info["quicklook"]["quicklookimgs"].append(eodd_utils.update_file_path(qklook_img, replace_path_dict))
        if "tilecache" in extendedInfo:
            out_extended_info["tilecache"] = dict()
            out_extended_info["tilecache"]["visgtiff"] = eodd_utils.update_file_path(extendedInfo["tilecache"]["visgtiff"], replace_path_dict)
            out_extended_info["tilecache"]["tilecachepath"] = eodd_utils.update_file_path(extendedInfo["tilecache"]["tilecachepath"], replace_path_dict)
        return out_extended_info

    @abstractmethod
    def import_sensor_db(self, input_json_file, replace_path_dict=None): pass

    @abstractmethod
    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False): pass

    @abstractmethod
    def reset_scn(self, unq_id, reset_download=False, reset_invalid=False): pass

    @abstractmethod
    def reset_dc_load(self, unq_id): pass


class EODataDownObsDates (object):

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        self.db_info_obj = db_info_obj
        self.overview_proj_epsg = None
        self.overview_img_base_dir = None
        self.overview_img_sizes = None
        self.overview_extent_vec_file = None
        self.overview_extent_vec_lyr = None

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
            eoddutils = eodatadown.eodatadownutils.EODataDownUtils()

            logger.debug("Testing config file is for 'obsdates'")
            if not json_parse_helper.doesPathExist(config_data, ["eodatadown", "obsdates"]):
                raise EODataDownException("Config file should have top level eodatadown > obsdates.")
            logger.debug("Have the correct config file for 'obsdates'")

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "obsdates", "overviews"]):
                self.overview_proj_epsg = int(json_parse_helper.getNumericValue(config_data,
                                                                                ["eodatadown", "obsdates", "overviews",
                                                                                 "epsg"], 0, 1000000000))
                self.overview_img_base_dir = json_parse_helper.getStrValue(config_data, ["eodatadown", "obsdates",
                                                                                         "overviews", "scn_image_dir"])

                self.overview_tmp_dir = json_parse_helper.getStrValue(config_data, ["eodatadown", "obsdates",
                                                                                         "overviews", "tmp_dir"])

                tmp_overview_img_sizes = json_parse_helper.getListValue(config_data, ["eodatadown", "obsdates",
                                                                                      "overviews", "overviewsizes"])
                self.overview_img_sizes = list()
                for overview_size in tmp_overview_img_sizes:
                    if eoddutils.isNumber(overview_size):
                        self.overview_img_sizes.append(int(overview_size))
                    else:
                        raise EODataDownException("overviewsizes contained a value which was not a number.")

                if json_parse_helper.doesPathExist(config_data, ["eodatadown", "obsdates", "overviews", "extent"]):
                    self.overview_extent_vec_file = json_parse_helper.getStrValue(config_data, ["eodatadown",
                                                                                                "obsdates", "overviews",
                                                                                                "extent", "vec_file"])
                    self.overview_extent_vec_lyr = json_parse_helper.getStrValue(config_data, ["eodatadown",
                                                                                               "obsdates", "overviews",
                                                                                               "extent", "vec_lyr"])
            else:
                raise EODataDownException("No information on eodatadown > obsdates > overviews.")

    def init_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Creating EDDObsDates & EDDObsDatesScns Tables.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def create_obs_date_records(self, sensor_obj, start_date, end_date):
        """
        A function which creates the database records for the observation dates.

        :param sensor_obj: an instance of a EODataDownSensor implementation
        :param start_date: A python datetime object specifying the start date (most recent date)
        :param end_date: A python datetime object specifying the end date (earliest date)

        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        sensor_name = sensor_obj.get_sensor_name()
        platforms = sensor_obj.find_unique_platforms()

        for platform in platforms:
            platform = platform[0]
            unq_dates = sensor_obj.find_unique_scn_dates(start_date, end_date, valid=True, order_desc=True,
                                                         platform=platform)
            db_obsdata_records = list()
            db_scnobsdata_records = list()
            for obs_date in unq_dates:
                obs_date = obs_date[0]
                query_rtn = ses.query(EDDObsDates).filter(EDDObsDates.SensorID == sensor_name,
                                                          EDDObsDates.PlatformID == platform,
                                                          EDDObsDates.ObsDate == obs_date).one_or_none()
                if query_rtn is None:
                    db_obsdata_records.append(EDDObsDates(SensorID=sensor_name, PlatformID=platform,
                                                          ObsDate=obs_date))

                    scn_pids = sensor_obj.get_scn_pids_for_date(obs_date, valid=True, ard_prod=False,
                                                                platform=platform)
                    for scn_pid in scn_pids:
                        db_scnobsdata_records.append(EDDObsDatesScns(SensorID=sensor_name,
                                                                     PlatformID=platform,
                                                                     ObsDate=obs_date,
                                                                     Scene_PID=scn_pid))
            if len(db_obsdata_records) > 0:
                ses.add_all(db_obsdata_records)
                if len(db_scnobsdata_records) > 0:
                    ses.add_all(db_scnobsdata_records)
                ses.commit()
        ses.close()

    def create_obsdate_visual(self, sys_main_obj, sensor):
        """
        A single threaded function to create the overview images for all the scenes which return it.

        :param sys_main_obj: a EODataDownSystemMain instance.
        :param sensor: Optionally a sensor can be specified, in which case the result will just be for that sensor.

        """
        if not sys_main_obj.has_parsed_config():
            raise EODataDownException("The EODataDownSystemMain instance has parsed a "
                                      "config file so it not ready to use.")
        gen_visuals_lst = self.get_lst_obsdates_need_processing(sensor)
        for obs in gen_visuals_lst:
            self.process_obsdata(sys_main_obj, obs[0], obs[1], obs[2])

    def get_lst_obsdates_need_processing(self, sensor=None):
        """
        A function which get the list of scenes which need processing to produce the overview images.

        :param sensor: Optionally a sensor can be specified, in which case the result will just be for that sensor.
        :return: list of lists - each list has [SensorID, PlatformID, ObsDate] 
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        if sensor is not None:
            obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.SensorID == sensor,
                                                        EDDObsDates.OverviewCreated == False).all()
        else:
            obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.OverviewCreated == False).all()
        obsdate_reslts = list()
        for obs in obsdate_qry:
            obsdate_reslts.append([obs.SensorID, obs.PlatformID, obs.ObsDate])
        ses.close()
        return obsdate_reslts

    def process_obsdata(self, sys_main_obj, sensor_id, platform_id, obs_date):
        """
        A function to process a single scene - i.e., produce the over image.

        :param sys_main_obj: a EODataDownSystemMain instance.
        :param sensor_id: specify the sensor
        :param platform_id: specify the platform (i.e., Sentinel-1A, Sentinel-2B, or Landsat-4)
        :param obs_date: specify the date of interest, as a datetime.date object.

        """
        if not sys_main_obj.has_parsed_config():
            raise EODataDownException("The EODataDownSystemMain instance has parsed a "
                                      "config file so it not ready to use.")

        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        obs_qry = ses.query(EDDObsDates).filter(EDDObsDates.SensorID == sensor_id,
                                                EDDObsDates.PlatformID == platform_id,
                                                EDDObsDates.ObsDate == obs_date).one_or_none()
        if obs_qry is not None:
            obsdate_scns_qry = ses.query(EDDObsDatesScns).filter(EDDObsDatesScns.SensorID == obs_qry.SensorID,
                                                                 EDDObsDatesScns.PlatformID == obs_qry.PlatformID,
                                                                 EDDObsDatesScns.ObsDate == obs_qry.ObsDate).all()
            scns_lst = list()
            for scn in obsdate_scns_qry:
                scns_lst.append(scn.Scene_PID)
            print("\t {}".format(scns_lst))
            sensor_obj = sys_main_obj.get_sensor_obj(obs_qry.SensorID)

            obsdate_basename = "{}_{}_{}".format(obs_qry.ObsDate.strftime('%Y%m%d'),
                                                 obs_qry.SensorID, obs_qry.PlatformID)
            obsdate_dir = os.path.join(self.overview_img_base_dir, obsdate_basename)
            if not os.path.exists(obsdate_dir):
                os.mkdir(obsdate_dir)

            out_imgs_dict = dict()
            out_img_files = list()
            for out_img_size in self.overview_img_sizes:
                out_img = os.path.join(obsdate_dir, "{}_{}px.tif".format(obsdate_basename, out_img_size))
                out_img_files.append(out_img)
                out_imgs_dict[out_img_size] = out_img

            # Pass all to sensor function...
            success = sensor_obj.create_multi_scn_visual(scns_lst, out_img_files, self.overview_img_sizes,
                                                         self.overview_extent_vec_file, self.overview_extent_vec_lyr,
                                                         'GTIFF', self.overview_tmp_dir)
            if success:
                obs_qry.OverviewCreated = True
                obs_qry.Overviews = out_imgs_dict
            else:
                obs_qry.OverviewCreated = False
                obs_qry.Invalid = True
            ses.commit()
        ses.close()

    def get_obs_scns(self, start_date, end_date, sensor=None, platform=None, valid=True, order_desc=False):
        """
        A function to get a list of scene objections for a given date range.

        :param start_date: A python datetime object specifying the start date (most recent date)
        :param end_date: A python datetime object specifying the end date (earliest date)
        :param sensor: Optionally specify the sensor of interest.
        :param platform: Optionally specify the platform of the sensor of interest.
        :param valid: If True then only valid scenes with overview images will be returned. Otherwise, all scenes will
                      be returned.
        :param order_desc: If True then returned scenes will be in descending order otherwise ascending.
        :return: A list of EDDObsDates will be returned.
        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        if order_desc:
            if valid:
                if sensor is not None:
                    if platform is not None:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.PlatformID == platform,
                                                                    EDDObsDates.OverviewCreated == True).order_by(
                                                                    EDDObsDates.ObsDate.desc()).all()
                    else:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.OverviewCreated == True).order_by(
                                                                    EDDObsDates.ObsDate.desc()).all()
                else:
                    obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                EDDObsDates.ObsDate > end_date,
                                                                EDDObsDates.OverviewCreated == True).order_by(
                                                                EDDObsDates.ObsDate.desc()).all()
            else:
                if sensor is not None:
                    if platform is not None:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.PlatformID == platform).order_by(
                                                                    EDDObsDates.ObsDate.desc()).all()
                    else:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor).order_by(
                                                                    EDDObsDates.ObsDate.desc()).all()
                else:
                    obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                EDDObsDates.ObsDate > end_date).order_by(
                                                                EDDObsDates.ObsDate.desc()).all()
        else:
            if valid:
                if sensor is not None:
                    if platform is not None:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.PlatformID == platform,
                                                                    EDDObsDates.OverviewCreated == True).order_by(
                                                                    EDDObsDates.ObsDate.asc()).all()
                    else:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.OverviewCreated == True).order_by(
                                                                    EDDObsDates.ObsDate.asc()).all()
                else:
                    obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                EDDObsDates.ObsDate > end_date,
                                                                EDDObsDates.OverviewCreated == True).order_by(
                                                                EDDObsDates.ObsDate.asc()).all()
            else:
                if sensor is not None:
                    if platform is not None:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor,
                                                                    EDDObsDates.PlatformID == platform).order_by(
                                                                    EDDObsDates.ObsDate.asc()).all()
                    else:
                        obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                    EDDObsDates.ObsDate > end_date,
                                                                    EDDObsDates.SensorID == sensor).order_by(
                                                                    EDDObsDates.ObsDate.asc()).all()
                else:
                    obsdate_qry = ses.query(EDDObsDates).filter(EDDObsDates.ObsDate < start_date,
                                                                EDDObsDates.ObsDate > end_date).order_by(
                                                                EDDObsDates.ObsDate.asc()).all()
        return obsdate_qry

    def export_db_to_json(self, out_json_file):
        """
        Export the EDDObsDates and EDDObsDateScns databases to a JSON file.

        :param out_json_file: The output JSON text file.

        """
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_result = ses.query(EDDObsDates).all()
        db_obs_dates_dict = dict()
        pid = 0
        for obsdate in query_result:
            db_obs_dates_dict[pid] = dict()
            db_obs_dates_dict[pid]["SensorID"] = obsdate.SensorID
            db_obs_dates_dict[pid]["PlatformID"] = obsdate.PlatformID
            db_obs_dates_dict[pid]["ObsDate"] = obsdate.ObsDate
            db_obs_dates_dict[pid]["OverviewCreated"] = obsdate.OverviewCreated
            db_obs_dates_dict[pid]["NeedUpdate"] = obsdate.NeedUpdate
            db_obs_dates_dict[pid]["Invalid"] = obsdate.Invalid
            db_obs_dates_dict[pid]["Overviews"] = obsdate.Overviews
            pid = pid + 1

        query_result = ses.query(EDDObsDatesScns).all()
        db_obs_date_scns_dict = dict()
        pid = 0
        for obsdatescns in query_result:
            db_obs_date_scns_dict[pid] = dict()
            db_obs_date_scns_dict[pid]["SensorID"] = obsdatescns.SensorID
            db_obs_date_scns_dict[pid]["PlatformID"] = obsdatescns.PlatformID
            db_obs_date_scns_dict[pid]["ObsDate"] = obsdatescns.ObsDate
            db_obs_date_scns_dict[pid]["Scene_PID"] = obsdatescns.Scene_PID
            pid = pid + 1
        ses.close()

        out_dict = dict()
        out_dict["EDDObsDates"] = db_obs_dates_dict
        out_dict["EDDObsDatesScns"] = db_obs_date_scns_dict

        with open(out_json_file, 'w') as outfile:
            json.dump(out_dict, outfile, indent=4, separators=(',', ': '), ensure_ascii=False)

    def update_overview_file_paths(self, overviews_lst, replace_path_dict=None):
        """
        Update the list of overview file paths.
        :param overviews_lst:
        :param replace_path_dict:
        """
        if replace_path_dict is None:
            return overviews_lst

        eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        out_overviews_lst = list()
        for overview_img in overviews_lst:
            out_overviews_lst.append(eodd_utils.update_file_path(overview_img, replace_path_dict))
        return out_overviews_lst

    def import_obsdates_db(self, input_json_file, replace_path_dict=None):
        """
        This function imports from the database records from the specified input JSON file.

        :param input_json_file: input JSON file with the records to be imported.
        :param replace_path_dict: a dictionary of file paths to be updated, if None then ignored.

        """
        db_obsdate_records = list()
        db_obsdatescn_records = list()

        with open(input_json_file) as json_file_obj:
            obsdate_db_dict = json.load(json_file_obj)
            db_obs_dates_dict = obsdate_db_dict["EDDObsDates"]
            db_obs_date_scns_dict = obsdate_db_dict["EDDObsDatesScns"]

            for pid in db_obs_dates_dict:
                db_obsdate_records.append(EDDObsDates(SensorID=db_obs_dates_dict[pid]["SensorID"],
                                                      PlatformID=db_obs_dates_dict[pid]["PlatformID"],
                                                      ObsDate=db_obs_dates_dict[pid]["ObsDate"],
                                                      OverviewCreated=db_obs_dates_dict[pid]["OverviewCreated"],
                                                      NeedUpdate=db_obs_dates_dict[pid]["NeedUpdate"],
                                                      Invalid=db_obs_dates_dict[pid]["Invalid"],
                                                      Overviews=self.update_overview_file_paths(
                                                                                 db_obs_dates_dict[pid]["Overviews"])))
            for pid in db_obs_dates_dict:
                db_obsdatescn_records.append(EDDObsDatesScns(SensorID=db_obs_date_scns_dict[pid]["SensorID"],
                                                             PlatformID=db_obs_date_scns_dict[pid]["PlatformID"],
                                                             ObsDate=db_obs_date_scns_dict[pid]["ObsDate"],
                                                             Scene_PID=db_obs_date_scns_dict[pid]["Scene_PID"]))

        if len(db_obsdate_records) > 0:
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            ses.add_all(db_obsdate_records)
            ses.commit()
            if len(db_obsdatescn_records) > 0:
                ses.add_all(db_obsdatescn_records)
                ses.commit()
            ses.close()