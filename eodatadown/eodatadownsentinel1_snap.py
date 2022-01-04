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
# Author: Dan Clewley & Carole Planque
# Email: dac@pml.ac.uk
# Date: 13/11/2021
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import glob
import logging
import os
import sys
import tempfile

from osgeo import gdal
from pyroSAR.snap import geocode

from eodatadown.eodatadownsensor import EODataDownSensor
import eodatadown.eodatadownutils

logger = logging.getLogger(__name__)

class EODataDownSentinel1ProcessorSensor (EODataDownSensor):
    """
    An abstract class provides functionality which is shared between the
    Sentinel-1 functions.
    """
    def _convert_file_to_cog(self, in_file, out_file):
        """
        Convet GDAL readable file into Cloud Optimised GeoTiff
        """
        in_ds = gdal.Open(in_file, gdal.GA_ReadOnly)

        gdal.Translate(out_file, in_ds, format="COG",
                       creationOptions=["COMPRESS=LZW"])

        in_ds = None

    def _reproject_file_to_cog(self, in_file, out_file, out_proj_epsg, out_img_res):
        """
        Reproject file and save to Cloud Optimised Geotiff
        """
        in_ds = gdal.Open(in_file, gdal.GA_ReadOnly)

        gdal.Warp(out_file, in_ds, format="COG",
                  dstSRS=out_proj_epsg,
                  xRes=out_img_res, yRes=out_img_res,
                  srcNodata=0, dstNodata=0,
                  multithread=True,
                  creationOptions=["COMPRESS=LZW"])

        in_ds = None

    def convertSen1ARD(self, input_safe_zipfile, output_dir, work_dir, tmp_dir, dem_img_file, out_img_res,
                       polarisations, out_proj_epsg, out_proj_str, out_proj_img_res=-1, out_proj_interp=None,
                       use_roi=False, intersect_vec_file='', intersect_vec_lyr='', subset_vec_file='',
                       subset_vec_lyr='', mask_outputs=False, mask_vec_file='', mask_vec_lyr=''):
        """
        Convert Sentinel1 data to ARD using SNAP

        TODO:
        * Use supplied DEM in SNAP (currently will download SRTM
        * Add support for different interpolation methods when reprojecting
        * Add ability to mask data

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
            if out_proj_epsg is not None and int(out_proj_epsg) != 4326:
                reproj_outputs = True
                sen1_out_proj_epsg = None
                out_sen1_files_dir = output_dir

            logger.info("Using SNAP to produce Sentinel-1 Geocoded product.")
            geocode(infile=input_safe_zipfile, outdir=out_sen1_files_dir, cleanup=True,
                    scaling="dB", refarea="gamma0",
                    shapefile=subset_vec_file, tr=out_img_res)


            temp_tiff_files = glob.glob(os.path.join(out_sen1_files_dir, "*.tif"))

            for temp_tif in temp_tiff_files:
                # Use either VV or VH as filename as this is needed for ingestion into open data cube
                # FIXME: This is going to break other parts where looking for '*dB*tif' for creating data for
                # visualisation
                if "VV" in os.path.basename(temp_tif):
                    out_tif = os.path.join(out_sen1_files_dir, "VV.tif")
                elif "VH" in os.path.basename(temp_tif):
                    out_tif = os.path.join(out_sen1_files_dir, "VH.tif")

            if reproj_outputs:
                logger.info("Reprojecting Sentinel-1 ARD product {} to {}".format(temp_tif, out_tif))
                self._reproject_file_to_cog(temp_tif, out_tif, out_proj_epsg, out_img_res)
            else:
                logging.info("Converting {} to COG ({})".format(temp_tif, out_tif))
                self._convert_file_to_cog(temp_tif, out_tif)

            # Remove tif file produced by SNAP
            os.remove(temp_tif)

            logger.info("Successfully finished processing: '{}'".format(input_safe_file))
            sen1_ard_success = True

        except Exception as e:
            logger.error("Failed in processing: '{}'".format(input_safe_file))
            raise e
        return sen1_ard_success
