#!/usr/bin/env python
"""
EODataDown - an abstract sentinel1 sensor class.
"""
# This file is part of 'EODataDown'
# A tool for automating Earth Observation Data Downloading.
#
# Copyright 2019 Pete Bunting
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
# Purpose:  Provides an abstract sentinel 1 sensor class.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 01/08/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import os.path
import shutil
import glob
import subprocess
import sen1_ard_gamma.sen1_grd_ard_tools
from eodatadown.eodatadownsensor import EODataDownSensor
import eodatadown.eodatadownutils
import rsgislib
import rsgislib.imageutils

logger = logging.getLogger(__name__)


class EODataDownSentinel1ProcessorSensor (EODataDownSensor):
    """
    An abstract class provides functionality which is shared between the
    Sentinel-1 functions.
    """

    def convertSen1ARD(self, input_safe_zipfile, output_dir, work_dir, tmp_dir, dem_img_file, out_img_res,
                       polarisations, out_proj_epsg, out_proj_str, out_proj_img_res=-1, out_proj_interp=None,
                       use_roi=False, intersect_vec_file='', intersect_vec_lyr='', subset_vec_file='',
                       subset_vec_lyr='', mask_outputs=False, mask_vec_file='', mask_vec_lyr=''):
        """

        :param input_safe_zipfile:
        :param output_dir:
        :param work_dir:
        :param tmp_dir:
        :param dem_img_file:
        :param out_img_res:
        :param polarisations:
        :param out_proj_epsg:
        :param out_proj_str:
        :param out_proj_img_res:
        :param out_proj_interp:
        :param use_roi:
        :param intersect_vec_file:
        :param intersect_vec_lyr:
        :param subset_vec_file:
        :param subset_vec_lyr:
        :param mask_outputs:
        :param mask_vec_file:
        :param mask_vec_lyr:

        """
        sen1_ard_success = False
        try:
            eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
            if eodd_utils.isEPSGUTM(out_proj_epsg):
                sen1_out_proj_epsg = out_proj_epsg
                reproj_outputs = False
                if use_roi:
                    out_sen1_files_dir = work_dir
                else:
                    out_sen1_files_dir = output_dir
            elif out_proj_epsg is not None:
                reproj_outputs = True
                sen1_out_proj_epsg = None
                out_sen1_files_dir = work_dir
            else:
                reproj_outputs = False
                sen1_out_proj_epsg = None
                if use_roi:
                    out_sen1_files_dir = work_dir
                else:
                    out_sen1_files_dir = output_dir

            logger.info("Extracting Sentinel-1 file zip.")
            unzip_tmp_dir_created = False
            uid_val = sen1_ard_gamma.sen1_ard_utils.uidGenerator()
            base_file_name = os.path.splitext(os.path.basename(input_safe_zipfile))[0]
            unzip_dir = os.path.join(tmp_dir, "{}_{}".format(base_file_name, uid_val))
            if not os.path.exists(unzip_dir):
                os.makedirs(unzip_dir)
                unzip_tmp_dir_created = True
            current_path = os.getcwd()
            os.chdir(unzip_dir)
            cmd = "unzip {}".format(input_safe_zipfile)
            subprocess.call(cmd, shell=True)
            input_safe_file = os.path.join(unzip_dir, "{}.SAFE".format(base_file_name))
            os.chdir(current_path)
            logger.info("Using Gamma to produce Sentinel-1 Geocoded product.")
            sen1_ard_gamma.sen1_grd_ard_tools.run_sen1_grd_ard_analysis(input_safe_file, out_sen1_files_dir, tmp_dir,
                                                                        dem_img_file, out_img_res, sen1_out_proj_epsg,
                                                                        polarisations, 'GTIFF', False, False,
                                                                        no_dem_check=False, out_int_imgs=True)

            if use_roi:
                sen1_vmsk_img = eodd_utils.findFile(out_sen1_files_dir, '*_vmsk.tif')
                if rsgislib.vectorutils.does_vmsk_img_intersect(sen1_vmsk_img, intersect_vec_file, intersect_vec_lyr,
                                                                tmp_dir, vec_epsg=None):

                    if reproj_outputs:
                        sub_out_dir = os.path.join(tmp_dir, "scn_subset_msk_{}".format(uid_val))
                    else:
                        sub_out_dir = output_dir

                    if not os.path.exists(sub_out_dir):
                        os.makedirs(sub_out_dir)

                    scn_imgs = glob.glob(os.path.join(out_sen1_files_dir, "*.tif"))
                    for c_img in scn_imgs:
                        img_file_name = os.path.splitext(os.path.basename(c_img))
                        out_img = os.path.join(sub_out_dir, img_file_name)
                        eodd_utils.subsetMaskImg(c_img, out_img, "GTIFF", subset_vec_file,
                                                 subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir)
                    out_sen1_files_dir = sub_out_dir
                else:
                    # If not intersecting the ROI
                    sen1_ard_success = False
                    if unzip_tmp_dir_created:
                        shutil.rmtree(unzip_dir)
                    return sen1_ard_success

            if reproj_outputs:
                # Reproject the UTM outputs to required projection.
                rsgis_utils = rsgislib.RSGISPyUtils()
                logger.info("Reprojecting Sentinel-1 ARD product.")
                sen1_out_proj_wkt = eodd_utils.getWKTFromEPSGCode(out_proj_epsg)
                sen1_out_proj_wkt_file = os.path.join(tmp_dir, "{}_{}_wktproj.wkt".format(base_file_name, uid_val))
                eodd_utils.writeList2File([sen1_out_proj_wkt], sen1_out_proj_wkt_file)
                img_interp_alg = 'cubic'
                if out_proj_interp == 'NEAR':
                    img_interp_alg = 'near'
                elif out_proj_interp == 'BILINEAR':
                    img_interp_alg = 'bilinear'
                elif out_proj_interp == 'CUBIC':
                    img_interp_alg = 'cubic'
                fnl_imgs = glob.glob(os.path.join(out_sen1_files_dir, "*.tif"))
                for c_img in fnl_imgs:
                    logger.debug("Reprojecting: {}".format(c_img))
                    img_file_basename = os.path.splitext(os.path.basename(c_img))[0]
                    no_data_val = rsgis_utils.getImageNoDataValue(c_img, 1)
                    out_img_file = os.path.join(output_dir, "{}_{}.tif".format(img_file_basename, out_proj_str))
                    out_file_opts = ["TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES", "INTERLEAVE=PIXEL", "BLOCKXSIZE=256",
                                     "BLOCKYSIZE=256"]
                    interp_alg = img_interp_alg
                    if 'vmsk' in c_img:
                        interp_alg = 'near'
                    rsgislib.imageutils.reprojectImage(c_img, out_img_file, sen1_out_proj_wkt_file, gdalformat='GTIFF',
                                                       interp=interp_alg, inWKT=None, noData=no_data_val,
                                                       outPxlRes=out_proj_img_res, snap2Grid=True, multicore=False,
                                                       gdal_options=out_file_opts)
                    rsgislib.imageutils.assignProj(out_img_file, sen1_out_proj_wkt, "")
                    rsgislib.imageutils.popImageStats(out_img_file, usenodataval=True, nodataval=no_data_val,
                                                      calcpyramids=True)
                    logger.debug("Finished Reprojecting: {}".format(out_img_file))
            if unzip_tmp_dir_created:
                shutil.rmtree(unzip_dir)
            logger.info("Successfully finished processing: '{}'".format(input_safe_file))
            sen1_ard_success = True
        except Exception as e:
            logger.error("Failed in processing: '{}'".format(input_safe_file))
            sen1_ard_success = False
        return sen1_ard_success
