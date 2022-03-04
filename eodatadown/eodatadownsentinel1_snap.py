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

import numpy
from osgeo import gdal
from pyroSAR.snap import geocode
from rios import applier
from rios import cuiprogress

from eodatadown.eodatadownsensor import EODataDownSensor
import eodatadown.eodatadownutils

logger = logging.getLogger(__name__)

def _rios_apply_three_band_stack(info, inputs, outputs):
    """
    Function called by rios applier to create a three band image stack of VV,HV,VV/VH scaled by 1000
    to fit as a UINT16
    """

    input_vv_scaled = numpy.where(numpy.isfinite(inputs.inimage[0]), inputs.inimage[0] * 1000, 0)
    input_vh_scaled = numpy.where(numpy.isfinite(inputs.inimage[1]), inputs.inimage[1] * 1000, 0)

    # Input is in dB so subtract to get ratio
    ratio_image = input_vv_scaled - input_vh_scaled

    outputs.outimage = numpy.vstack([input_vv_scaled, input_vh_scaled, ratio_image]).astype(numpy.uint16)


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

    def _create_three_band_stack(in_vv_file, in_hv_file, out_file):
        """
        Creates a three band image stack of VV,HV,VV/VH scaled by 1000
        to fit as a UINT16
        """
        infiles = applier.FilenameAssociations()
        infiles.inimage = [in_vv_file, in_hv_file]

        outfiles = applier.FilenameAssociations()
        outfiles.outimage = out_file

        controls = applier.ApplierControls()
        controls.progress = cuiprogress.CUIProgressBar()

        controls.setOutputDriverName("GTiff")
        controls.setCreationOptions(["COMPRESS=LZW"])
        controls.setCalcStats(True)

        applier.apply(_rios_apply_three_band_stack, infiles, outfiles, controls=controls)


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

            # Check if output stack file exists, if it does then ARD processing has already run but database hasn't been updated
            out_stack_file = os.path.join(out_sen1_files_dir, "{}_dB_stack.tif".format(out_sen1_files_dir.rstrip("/").split("/")[-1]))

            out_stack_file_glob = os.path.join("{}*".format(out_sen1_files_dir[0:-10]), "*_dB_stack.tif")
            if len(glob.glob(out_stack_file_glob)) > 0:
                logger.info("Stack ARD file exists, processing has already completed. Will update database")
                sen1_ard_success = True
                return sen1_ard_success

            logger.info("Using SNAP to produce Sentinel-1 Geocoded product.")
            geocode(infile=input_safe_zipfile, outdir=out_sen1_files_dir, cleanup=True,
                    scaling="dB", refarea="gamma0",
                    shapefile=subset_vec_file, tr=out_img_res)

            temp_tiff_files = glob.glob(os.path.join(out_sen1_files_dir, "*.tif"))

            vv_tif = None
            vh_tif = None

            for temp_tif in temp_tiff_files:
                # Use either VV or VH as filename as this is needed for ingestion into open data cube
                if "VV" in os.path.basename(temp_tif):
                    out_tif = os.path.join(out_sen1_files_dir, "VV.tif")
                    vv_tif = out_tif
                elif "VH" in os.path.basename(temp_tif):
                    out_tif = os.path.join(out_sen1_files_dir, "VH.tif")
                    vh_tif = out_tif
                if reproj_outputs:
                    logger.info("Reprojecting Sentinel-1 ARD product {} to {}".format(temp_tif, out_tif))
                    self._reproject_file_to_cog(temp_tif, out_tif, out_proj_epsg, out_img_res)
                else:
                    logging.info("Converting {} to COG ({})".format(temp_tif, out_tif))
                    self._convert_file_to_cog(temp_tif, out_tif)

                # Remove tif file produced by SNAP
                os.remove(temp_tif)

            # Create three band stack
            if vv_tif is None or vh_tif is None:
                raise Exception("Couldn't find processed HH or VV files, can't create three band stack")
            else:
                self._create_three_band_stack(vv_tif, vh_tif, out_stack_file)

            logger.info("Successfully finished processing: '{}'".format(input_safe_zipfile))
            sen1_ard_success = True

        except Exception as e:
            logger.error("Failed in processing: '{}'".format(input_safe_zipfile))
            raise e
        return sen1_ard_success
