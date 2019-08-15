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
import subprocess
import sen1_ard_gamma.sen1_grd_ard_tools

logger = logging.getLogger(__name__)


class EODataDownSentinel1ProcessorSensor (object):
    """
    An abstract class provides functionality which is shared between the
    Sentinel-1 functions.
    """

    def convertSen1ARD(self, input_safe_zipfile, output_dir, tmp_dir, dem_img_file, out_img_res, out_proj_epsg,
                              polarisations):
        """

        :param input_safe_zipfile:
        :param output_dir:
        :param tmp_dir:
        :param dem_img_file:
        :param out_img_res:
        :param out_proj_epsg:
        :param polarisations:

        """
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

        sen1_ard_gamma.sen1_grd_ard_tools.run_sen1_grd_ard_analysis(input_safe_file, output_dir, tmp_dir, dem_img_file,
                                                                    out_img_res, out_proj_epsg, polarisations,
                                                                    'KEA', False, False, no_dem_check=False)
        if unzip_tmp_dir_created:
            shutil.rmtree(unzip_dir)
