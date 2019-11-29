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

import logging
import json
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
    def get_scnlist_datacube(self, loaded=False): pass

    @abstractmethod
    def has_scn_datacube(self, unq_id): pass

    @abstractmethod
    def scn2datacube(self, unq_id): pass

    @abstractmethod
    def scns2datacube_all_avail(self): pass

    @abstractmethod
    def get_scnlist_quicklook(self): pass

    @abstractmethod
    def has_scn_quicklook(self, unq_id): pass

    @abstractmethod
    def scn2quicklook(self, unq_id): pass

    @abstractmethod
    def scns2quicklook_all_avail(self): pass

    @abstractmethod
    def get_scnlist_tilecache(self): pass

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
    def query_scn_records_date_count(self, start_date, end_date, valid=True): pass

    @abstractmethod
    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True): pass

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





