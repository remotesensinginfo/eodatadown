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
from abc import ABCMeta, abstractmethod

import eodatadown.eodatadownutils

logger = logging.getLogger(__name__)


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
    def query_scn_records_date_count(self, start_date, end_date, valid=True): pass

    @abstractmethod
    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True): pass

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
    def reset_scn(self, unq_id, reset_download=False): pass

    @abstractmethod
    def reset_dc_load(self, unq_id): pass
