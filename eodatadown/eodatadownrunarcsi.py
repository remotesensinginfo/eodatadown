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

import rsgislib.vectorutils
import rsgislib.imageutils

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
                               True, False, False, None, None, False, None, 'LSMSK')
    logger.info("Finished running ARCSI for: " + input_mtl)


def run_arcsi_sentinel2(input_hdr, dem_file, output_dir, tmp_dir, reproj_outputs, proj_wkt_file, projabbv, low_res=False):
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
    if low_res:
        arcsilib.arcsirun.runARCSI(input_hdr, None, None, "sen2", None, "KEA",
                                   output_dir, None, proj_wkt_file, None, projabbv, None, None,
                                   ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA"],
                                   True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                                   "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                                   None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                                   20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                                   True, False, False, None, None, True, None, 'S2LESSFMSK')
    else:
        arcsilib.arcsirun.runARCSI(input_hdr, None, None, "sen2", None, "KEA",
                                   output_dir, None, proj_wkt_file, None, projabbv, None, None,
                                   ["CLOUDS", "DOSAOTSGL", "STDSREF", "SATURATE", "TOPOSHADOW", "FOOTPRINT", "METADATA", "SHARP"],
                                   True, None, None, arcsilib.DEFAULT_ARCSI_AEROIMG_PATH, arcsilib.DEFAULT_ARCSI_ATMOSIMG_PATH,
                                   "GreenVegetation", 0, None, None, False, None, None, None, None, False,
                                   None, None, tmp_dir, 0.05, 0.5, 0.1, 0.4, dem_file, None, None, True,
                                   20, False, False, 1000, "cubic", "near", 3000, 3000, 1000, 21,
                                   True, False, False, None, None, False, None, 'S2LESSFMSK')
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


def move_arcsi_stdsref_products(arcsi_out_dir, ard_products_dir, use_roi, intersect_vec_file, intersect_vec_lyr,
                                subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir):
    """
    A function to copy the outputs from ARCSI to the appropriate directory for EODataDown.

    :param arcsi_out_dir: the output directory for arcsi where files should be copied from
    :param ard_products_dir: the directory where the appropriate files should be copied too.
    :param use_roi:
    :param intersect_vec_file:
    :param intersect_vec_lyr:
    :param subset_vec_file:
    :param subset_vec_lyr:
    :param mask_outputs:
    :param mask_vec_file:
    :param mask_vec_lyr:
    :param tmp_dir:
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

            out_file_info_dict = dict()
            if cloud_cover < 0.95:
                if use_roi:
                    valid_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VALID_MASK"])
                    if rsgislib.vectorutils.does_vmsk_img_intersect(os.path.join(arcsi_out_dir, valid_msk_image), intersect_vec_file, intersect_vec_lyr, tmp_dir, vec_epsg=None):
                        sref_mskd_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_IMG"])
                        sref_mskd_image_path = os.path.join(arcsi_out_dir, sref_mskd_image)
                        sref_mskd_image_sub_path = os.path.join(tmp_dir, sref_mskd_image)
                        eoddutils.subsetMaskImg(sref_mskd_image_path, sref_mskd_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                        sref_mskd_image_tif = eoddutils.translateCloudOpGTIFF(sref_mskd_image_sub_path, ard_products_dir)
                        out_file_info_dict["STD_SREF_IMG"] = sref_mskd_image_tif

                        sref_full_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_WHOLE_IMG"])
                        sref_full_image_path = os.path.join(arcsi_out_dir, sref_full_image)
                        sref_full_image_sub_path = os.path.join(tmp_dir, sref_full_image)
                        eoddutils.subsetMaskImg(sref_full_image_path, sref_full_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                        sref_full_image_tif = eoddutils.translateCloudOpGTIFF(sref_full_image_sub_path, ard_products_dir)
                        out_file_info_dict["STD_SREF_WHOLE_IMG"] = sref_full_image_tif
                        
                        try:
                            cloud_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "CLOUD_MASK"])
                            cloud_msk_image_path = os.path.join(arcsi_out_dir, cloud_msk_image)
                            cloud_msk_image_sub_path = os.path.join(tmp_dir, cloud_msk_image)
                            eoddutils.subsetMaskImg(cloud_msk_image_path, cloud_msk_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                            cloud_msk_image_tif = eoddutils.translateCloudOpGTIFF(cloud_msk_image_sub_path, ard_products_dir)
                            out_file_info_dict["CLOUD_MASK"] = cloud_msk_image_tif
                        except Exception as e:
                            logger.info("Cloud mask was not available - assume it wasn't calculated")
                        
                        valid_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VALID_MASK"])
                        valid_msk_image_path = os.path.join(arcsi_out_dir, valid_msk_image)
                        valid_msk_image_sub_path = os.path.join(tmp_dir, valid_msk_image)
                        eoddutils.subsetMaskImg(valid_msk_image_path, valid_msk_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                        valid_msk_image_tif = eoddutils.translateCloudOpGTIFF(valid_msk_image_sub_path, ard_products_dir)
                        out_file_info_dict["VALID_MASK"] = valid_msk_image_tif

                        topo_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "TOPO_SHADOW_MASK"])
                        topo_msk_image_path = os.path.join(arcsi_out_dir, topo_msk_image)
                        topo_msk_image_sub_path = os.path.join(tmp_dir, topo_msk_image)
                        eoddutils.subsetMaskImg(topo_msk_image_path, topo_msk_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                        topo_msk_image_tif = eoddutils.translateCloudOpGTIFF(topo_msk_image_sub_path, ard_products_dir)
                        out_file_info_dict["TOPO_SHADOW_MASK"] = topo_msk_image_tif
                        
                        view_angle_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VIEW_ANGLE"])
                        view_angle_image_path = os.path.join(arcsi_out_dir, view_angle_image)
                        view_angle_image_sub_path = os.path.join(tmp_dir, view_angle_image)
                        eoddutils.subsetMaskImg(view_angle_image_path, view_angle_image_sub_path, "KEA", subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                        view_angle_image_tif = eoddutils.translateCloudOpGTIFF(view_angle_image_sub_path, ard_products_dir)
                        out_file_info_dict["VIEW_ANGLE"] = view_angle_image_tif

                        footprint_vec = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FOOTPRINT"])
                        eoddutils.moveFilesWithBase2DIR(os.path.join(arcsi_out_dir, footprint_vec), ard_products_dir)
                        out_file_info_dict["FOOTPRINT"] = footprint_vec

                        out_file_info_dict["METADATA"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
                        out_file_info_dict["ProviderMetadata"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "ProviderMetadata"])
                        out_file_info_dict["FileBaseName"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FileBaseName"])

                        meta_data_json["FileInfo"] = out_file_info_dict

                        metadata_json_file = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
                        output_meta_data_file = os.path.join(arcsi_out_dir, metadata_json_file)
                        with open(output_meta_data_file, 'w') as outfile:
                            json.dump(meta_data_json, outfile, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                    else:
                        return False
                else:
                    sref_mskd_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_IMG"])
                    sref_mskd_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, sref_mskd_image), ard_products_dir)
                    out_file_info_dict["STD_SREF_IMG"] = sref_mskd_image_tif

                    sref_full_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "STD_SREF_WHOLE_IMG"])
                    sref_full_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, sref_full_image), ard_products_dir)
                    out_file_info_dict["STD_SREF_WHOLE_IMG"] = sref_full_image_tif

                    try:
                        cloud_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "CLOUD_MASK"])
                        cloud_msk_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, cloud_msk_image), ard_products_dir)
                        out_file_info_dict["CLOUD_MASK"] = cloud_msk_image_tif
                    except Exception as e:
                        logger.info("Cloud mask was not available - assume it wasn't calculated")

                    valid_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VALID_MASK"])
                    valid_msk_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, valid_msk_image), ard_products_dir)
                    out_file_info_dict["VALID_MASK"] = valid_msk_image_tif

                    topo_msk_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "TOPO_SHADOW_MASK"])
                    topo_msk_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, topo_msk_image), ard_products_dir)
                    out_file_info_dict["TOPO_SHADOW_MASK"] = topo_msk_image_tif

                    view_angle_image = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "VIEW_ANGLE"])
                    view_angle_image_tif = eoddutils.translateCloudOpGTIFF(os.path.join(arcsi_out_dir, view_angle_image), ard_products_dir)
                    out_file_info_dict["VIEW_ANGLE"] = view_angle_image_tif

                    footprint_vec = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FOOTPRINT"])
                    eoddutils.moveFilesWithBase2DIR(os.path.join(arcsi_out_dir, footprint_vec), ard_products_dir)
                    out_file_info_dict["FOOTPRINT"] = footprint_vec

                    out_file_info_dict["METADATA"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
                    out_file_info_dict["ProviderMetadata"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "ProviderMetadata"])
                    out_file_info_dict["FileBaseName"] = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "FileBaseName"])

                    meta_data_json["FileInfo"] = out_file_info_dict

                    metadata_json_file = json_parse_helper.getStrValue(meta_data_json, ["FileInfo", "METADATA"])
                    output_meta_data_file = os.path.join(arcsi_out_dir, metadata_json_file)
                    with open(output_meta_data_file, 'w') as outfile:
                        json.dump(meta_data_json, outfile, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
            else:
                return False
        else:
            return False
        return True


def move_arcsi_dos_products(arcsi_out_dir, ard_products_dir):
    """
    A function to copy the outputs from ARCSI to the appropriate directory for EODataDown.

    :param arcsi_out_dir: the output directory for arcsi where files should be copied from
    :param ard_products_dir: the directory where the appropriate files should be copied too.
    :return: bool True - valid result and task completed.
                  False - invalid result ARD not produced
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

    return True
