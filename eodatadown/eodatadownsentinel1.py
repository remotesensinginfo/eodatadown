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
                       polarisations, out_proj_epsg, out_proj_str, out_proj_img_res=-1, out_proj_interp=None):
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
        :return:
        """
        sen1_ard_success = False
        try:
            eodd_utils = eodatadown.eodatadownutils.EODataDownUtils()
            sen1_out_proj_epsg = None
            out_sen1_files_dir = work_dir
            if eodd_utils.isEPSGUTM(out_proj_epsg):
                sen1_out_proj_epsg = out_proj_epsg
                out_sen1_files_dir = output_dir

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

            sen1_ard_gamma.sen1_grd_ard_tools.run_sen1_grd_ard_analysis(input_safe_file, out_sen1_files_dir, tmp_dir,
                                                                        dem_img_file, out_img_res, sen1_out_proj_epsg,
                                                                        polarisations, 'GTIFF', False, False,
                                                                        no_dem_check=False)

            if sen1_out_proj_epsg is not None:
                sen1_out_proj_wkt = eodd_utils.getWKTFromEPSGCode(sen1_out_proj_epsg)
                img_interp_alg = 'cubic'
                if out_proj_interp == 'NEAR':
                    img_interp_alg = 'near'
                elif out_proj_interp == 'BILINEAR':
                    img_interp_alg = 'bilinear'
                elif out_proj_interp == 'CUBIC':
                    img_interp_alg = 'cubic'
                fnl_imgs = glob.glob(os.path.join(work_dir, "*.tif"))
                for c_img in fnl_imgs:
                    # Reproject the UTM outputs to required projection.
                    img_file_basename = os.path.splitext(os.path.basename(c_img))[0]
                    out_img_file = os.path.join(output_dir, "{}_{}.tif".format(img_file_basename, out_proj_str))
                    rsgislib.imageutils.reprojectImage(c_img, out_img_file, sen1_out_proj_wkt, gdalformat='GTIFF',
                                                       interp=img_interp_alg, inWKT=None, noData=0.0,
                                                       outPxlRes=out_proj_img_res, snap2Grid=True, multicore=False)
            if unzip_tmp_dir_created:
                shutil.rmtree(unzip_dir)

            sen1_ard_success = True
        except Exception as e:
            logger.error("Failed in processing: '{}'".format(input_safe_file))
            sen1_ard_success = False
        return sen1_ard_success
