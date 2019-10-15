#!/usr/bin/env python
"""
EODataDown - a set of functions to provide a simplified python interface to ARCSI.
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
# Purpose:  Provides a set of functions to provide a simplified python interface to ARCSI.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 09/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import os.path

from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils

logger = logging.getLogger(__name__)


def run_arcsi_landsat(input_mtl, dem_file, output_dir, tmp_dir, spacecraft_str, sensor_str, reproj_outputs, proj_wkt_file, projabbv):
    """
    A function to run ARCSI for a landsat scene using python rather than
    the command line interface.
    :param spacecraft_str:
    :param sensor_str:
    :param reproj_outputs:
    :param proj_wkt_file:
    :param projabbv:
    :param input_mtl:
    :param dem_file:
    :param output_dir:
    :param tmp_dir:
    :return:
    """
    import arcsilib.arcsirun

    if (spacecraft_str == "LANDSAT_8") and (sensor_str == "OLI_TIRS"):
        arcsi_sensor_str = "ls8"
    elif (spacecraft_str == "LANDSAT_7") and (sensor_str == "ETM"):
        arcsi_sensor_str = "ls7"
    elif (spacecraft_str == "LANDSAT_5") and (sensor_str == "TM"):
        arcsi_sensor_str = "ls5tm"
    elif (spacecraft_str == "LANDSAT_4") and (sensor_str == "TM"):
        arcsi_sensor_str = "ls4tm"
    else:
        logger.error("Did not recognise the spacecraft and sensor combination. (" + spacecraft_str + ", " + sensor_str + ")")
        raise EODataDownException("Did not recognise the spacecraft and sensor combination.")

    if not reproj_outputs:
        proj_wkt_file = None
        projabbv = None

    logger.info("Starting to run ARCSI for: "+input_mtl)
    arcsilib.arcsirun.runARCSI(input_mtl, None, None, arcsi_sensor_str, None, "KEA",
                               output_dir, None, proj_wkt_file, None, projabbv, None, None,
                               ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA"],
                               True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                               "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                               None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                               20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                               True, False, False, None, None, False, None, 'FMASK')
    logger.info("Finished running ARCSI for: " + input_mtl)


def run_arcsi_sentinel2(input_hdr, dem_file, output_dir, tmp_dir, reproj_outputs, proj_wkt_file, projabbv):
    """
    A function to run ARCSI for a landsat scene using python rather than
    the command line interface.
    :param input_hdr:
    :param reproj_outputs:
    :param proj_wkt_file:
    :param projabbv:
    :param dem_file:
    :param output_dir:
    :param tmp_dir:
    :return:
    """
    import arcsilib.arcsirun

    if not reproj_outputs:
        proj_wkt_file = None
        projabbv = None

    logger.info("Starting to run ARCSI for: "+input_hdr)
    arcsilib.arcsirun.runARCSI(input_hdr, None, None, "sen2", None, "KEA",
                               output_dir, None, proj_wkt_file, None, projabbv, None, None,
                               ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA", "SHARP"],
                               True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                               "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                               None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                               20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                               True, False, False, None, None, False, None, 'FMASK')
    logger.info("Finished running ARCSI for: " + input_hdr)


def run_arcsi_rapideye(input_xml, dem_file, output_dir, tmp_dir, reproj_outputs, proj_wkt_file, projabbv):
    """
    A function to run ARCSI for a landsat scene using python rather than
    the command line interface.
    :param input_xml:
    :param reproj_outputs:
    :param proj_wkt_file:
    :param projabbv:
    :param dem_file:
    :param output_dir:
    :param tmp_dir:
    :return:
    """
    import arcsilib.arcsirun

    if not reproj_outputs:
        proj_wkt_file = None
        projabbv = None

    debug_mode = False

    logger.info("Starting to run ARCSI for: "+input_xml)
    arcsilib.arcsirun.runARCSI(input_xml, None, None, "rapideye", None, "KEA",
                               output_dir, None, proj_wkt_file, None, projabbv, None, None,
                               ["DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA"],
                               True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                               "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                               None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                               20, False, debug_mode, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                               True, False, False, None, None, False, None, 'FMASK')
    logger.info("Finished running ARCSI for: " + input_xml)


def run_arcsi_planetscope(input_xml, output_dir, tmp_dir, reproj_outputs, proj_wkt_file, projabbv):
    """
    A function to run ARCSI for a planetscope scene using python rather than
    the command line interface.
    :param reproj_outputs:
    :param proj_wkt_file:
    :param projabbv:
    :param input_xml:
    :param output_dir:
    :param tmp_dir:
    :return:
    """
    import arcsilib.arcsirun

    if not reproj_outputs:
        proj_wkt_file = None
        projabbv = None

    dem_file = None
    debug_mode = True

    logger.info("Starting to run ARCSI for: " + input_xml)
    arcsilib.arcsirun.runARCSI(input_xml, None, None, "planetscope", None, "KEA",
                               output_dir, None, proj_wkt_file, None, projabbv, None, None,
                               ["TOA", "DOS", "SATURATE", "FOOTPRINT", "METADATA"],
                               True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                               "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                               None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                               20, False, debug_mode, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                               True, False, False, None, None, False, None, 'FMASK')
    logger.info("Finished running ARCSI for: " + input_xml)


def move_arcsi_stdsref_products(arcsi_out_dir, ard_products_dir):
    """

    :param arcsi_out_dir:
    :param ard_products_dir:
    :return: bool True - valid result and task completed.
                  False - invalid result ARD not produced (e.g., 100% cloud cover)
    """
    eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
    metadata_file = eoddutils.findFile(arcsi_out_dir, "*meta.json")

    with open(metadata_file) as f:
        meta_data_json = json.load(f)
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()

        if json_parse_helper.doesPathExist(meta_data_json, ["ProductsInfo","ARCSI_CLOUD_COVER"]):
            cloud_cover = json_parse_helper.getNumericValue(meta_data_json, ["ProductsInfo","ARCSI_CLOUD_COVER"], valid_lower=0.0, valid_upper=1.0)

            if cloud_cover < 0.95:
                sref_mskd_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_IMG"])
                eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, sref_mskd_image), ard_products_dir)

                sref_full_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_WHOLE_IMG"])
                eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, sref_full_image), ard_products_dir)

                try:
                    cloud_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "CLOUD_MASK"])
                    eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, cloud_msk_image), ard_products_dir)
                except Exception as e:
                    logger.info("Cloud mask was not available - assume it wasn't calculated")

                valid_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VALID_MASK"])
                eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, valid_msk_image), ard_products_dir)

                topo_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "TOPO_SHADOW_MASK"])
                eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, topo_msk_image), ard_products_dir)

                footprint_shp = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FOOTPRINT"])
                eoddutils.moveFilesWithBase2DIR(os.path.join(arcsi_out_dir, footprint_shp), ard_products_dir)

                metadata_json_file = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
                eoddutils.copyFile2DIR(os.path.join(arcsi_out_dir, metadata_json_file), ard_products_dir)
            else:
                return False
        else:
            return False


def move_arcsi_dos_products(arcsi_out_dir, ard_products_dir):
    """

    :param arcsi_out_dir:
    :param ard_products_dir:
    :return:
    """
    eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
    metadata_file = eoddutils.findFile(arcsi_out_dir, "*meta.json")

    with open(metadata_file) as f:
        meta_data_json = json.load(f)
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()

        dos_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "SREF_DOS_IMG"])
        eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, dos_image), ard_products_dir)

        valid_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VALID_MASK"])
        eoddutils.moveFile2DIR(os.path.join(arcsi_out_dir, valid_msk_image), ard_products_dir)

        footprint_shp = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FOOTPRINT"])
        eoddutils.moveFilesWithBase2DIR(os.path.join(arcsi_out_dir, footprint_shp), ard_products_dir)

        metadata_json_file = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
        eoddutils.copyFile2DIR(os.path.join(arcsi_out_dir, metadata_json_file), ard_products_dir)
