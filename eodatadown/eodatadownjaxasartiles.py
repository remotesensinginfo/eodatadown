#!/usr/bin/env python
"""
EODataDown - a sensor class for downloading JAXA SAR tiles.
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
import os
import os.path
import datetime
import multiprocessing
import shutil

import osgeo.osr as osr
import osgeo.gdal as gdal
from rios import rat
import numpy

import rsgislib
import rsgislib.imageutils
import rsgislib.imagecalc
import rsgislib.imagefilter
import rsgislib.rastergis

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDJAXASARTiles(Base):
    __tablename__ = "EDDJAXASARTiles"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Tile_Name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Parent_Tile = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Year = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    File_Name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Server_File_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    InstrumentName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Incident_Angle_Low = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Incident_Angle_High = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Remote_URL_MD5 = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Total_Size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Query_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Download_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Download_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Downloaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Download_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    Archived = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    DCLoaded_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    DCLoaded_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    DCLoaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Invalid = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)
    RegCheck = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)


def _download_scn_jaxa(params):
    """
    Function which is used with multiprocessing pool object for downloading landsat data from Google.
    :param params:
    :return:
    """
    server_path = params[0]
    server_url = params[1]
    scn_lcl_dwnld_path = params[2]
    db_info_obj = params[3]

    logger.info("Downloading "+server_path)
    start_date = datetime.datetime.now()
    if os.path.exists(scn_lcl_dwnld_path) and (os.path.getsize(scn_lcl_dwnld_path) >= 1000):
        success = True
    elif os.path.exists(scn_lcl_dwnld_path) and (os.path.getsize(scn_lcl_dwnld_path) < 1000):
        os.remove(scn_lcl_dwnld_path)
        edd_ftp_utils = eodatadown.eodatadownutils.EODDFTPDownload()
        success = edd_ftp_utils.downloadFile(server_url, server_path, scn_lcl_dwnld_path, time_out=1200)
    else:
        edd_ftp_utils = eodatadown.eodatadownutils.EODDFTPDownload()
        success = edd_ftp_utils.downloadFile(server_url, server_path, scn_lcl_dwnld_path, time_out=1200)
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading " + server_path)

    if success:
        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.Server_File_Path == server_path).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + server_path)
        query_result.Downloaded = True
        query_result.Download_Start_Date = start_date
        query_result.Download_End_Date = end_date
        query_result.Download_Path = scn_lcl_dwnld_path
        query_result.Remote_URL = os.path.join(server_url, server_path)
        edd_file_checker = eodatadown.eodatadownutils.EDDCheckFileHash()
        query_result.Remote_URL_MD5 = edd_file_checker.calcMD5Checksum(scn_lcl_dwnld_path)
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")


def _process_to_ard(params):
    """

    :param params:
    :return:
    """
    pid = params[0]
    tile_name = params[1]
    year = params[2]
    db_info_obj = params[3]
    download_path = params[4]
    work_ard_scn_path = params[5]
    tmp_ard_scn_path = params[6]
    final_ard_scn_path = params[7]
    ard_proj_defined = params[8]
    proj_wkt_file = params[9]
    proj_abbv = params[10]
    out_proj_img_res = params[11]
    success = False

    out_msk_file = ""
    out_date_file = ""
    out_linci_file = ""
    sar_stack_file = ""

    rsgis_utils = rsgislib.RSGISPyUtils()

    start_date = datetime.datetime.now()
    try:
        msk_file = ""
        date_file = ""
        linci_file = ""

        ###############################
        ##### Setup file patterns #####
        ###############################
        hh_jers1_file_pattern = '*_sl_HH'

        hh_p1_file_pattern = '*_sl_HH'
        hv_p1_file_pattern = '*_sl_HV'

        hh_p2_file_pattern = '*_sl_HH_F02DAR'
        hv_p2_file_pattern = '*_sl_HV_F02DAR'

        hh_p2_fp_file_pattern = '*_sl_HH_FP6QAR'
        hv_p2_fp_file_pattern = '*_sl_HV_FP6QAR'

        mask_file_pattern = '*_mask'
        date_file_pattern = '*_date'
        linci_file_pattern = '*_linci'

        mask_p2_file_pattern = '*_mask_F02DAR'
        date_p2_file_pattern = '*_date_F02DAR'
        linci_p2_file_pattern = '*_linci_F02DAR'
        ###############################

        unique_base_name = "{0}_{1}".format(tile_name, year)

        edd_utils = eodatadown.eodatadownutils.EODataDownUtils()
        edd_utils.extractGZTarFile(download_path, work_ard_scn_path)

        if year == 1996:
            # JERS-1
            # File file
            ref_launch_date = datetime.datetime(1992, 2, 11)
            try:
                hh_file = edd_utils.findFile(work_ard_scn_path, hh_jers1_file_pattern)
            except Exception as e:
                logger.error("Could not find HH file in '{}'".format(work_ard_scn_path))
                raise e

            sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + '_hh.tif')
            if ard_proj_defined:
                sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_hh.tif')
                rsgislib.imageutils.reprojectImage(hh_file, sar_stack_file, proj_wkt_file, gdalformat='GTIFF', interp='cubic', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
            else:
                rsgislib.imagecalc.imageMath(hh_file, sar_stack_file, "b1", "GTIFF", rsgis_utils.getGDALDataTypeFromImg(hh_file))
            rsgislib.imageutils.popImageStats(sar_stack_file, usenodataval=True, nodataval=0, calcpyramids=True)

            try:
                msk_file = edd_utils.findFile(work_ard_scn_path, mask_file_pattern)
            except Exception as e:
                logger.error("Could not find Mask file in '{}'".format(work_ard_scn_path))
                raise e

            try:
                date_file = edd_utils.findFile(work_ard_scn_path, date_file_pattern)
            except Exception as e:
                logger.error("Could not find Date file in '{}'".format(work_ard_scn_path))
                raise e

            try:
                linci_file = edd_utils.findFile(work_ard_scn_path, linci_file_pattern)
            except Exception as e:
                logger.error("Could not find Incidence Angle file in '{}'".format(work_ard_scn_path))
                raise e

        elif year in [2007, 2008, 2009, 2010]:
            # ALOS PALSAR
            # File files
            ref_launch_date = datetime.datetime(2006, 1, 24)
            try:
                hh_file = edd_utils.findFile(work_ard_scn_path, hh_p1_file_pattern)
            except Exception as e:
                logger.error("Could not find HH file in '{}'".format(work_ard_scn_path))
                raise e
            try:
                hv_file = edd_utils.findFile(work_ard_scn_path, hv_p1_file_pattern)
            except Exception as e:
                logger.error("Could not find HV file in '{}'".format(work_ard_scn_path))
                raise e

            # Add HH/HV ratio.
            hhhv_file = os.path.join(tmp_ard_scn_path, unique_base_name + '_hhhv.tif')
            band_defns = [rsgislib.imagecalc.BandDefn('hh', hh_file, 1), rsgislib.imagecalc.BandDefn('hv', hv_file, 1)]
            rsgislib.imagecalc.bandMath(hhhv_file, 'hv==0?0:(hh/hv)*1000', 'GTIFF', rsgislib.TYPE_16UINT, band_defns)

            # Stack Image bands
            sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + '_HHHV.tif')
            if ard_proj_defined:
                sar_stack_tmp_file = os.path.join(tmp_ard_scn_path, unique_base_name + '_HHHV.tif')
                rsgislib.imageutils.stackImageBands([hh_file, hv_file, hhhv_file], ["HH", "HV", "HH/HV"], sar_stack_tmp_file, None, 0, 'GTIFF', rsgislib.TYPE_16UINT)
                sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_HHHV.tif')
                rsgislib.imageutils.reprojectImage(sar_stack_tmp_file, sar_stack_file, proj_wkt_file, gdalformat='GTIFF', interp='cubic', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
            else:
                rsgislib.imageutils.stackImageBands([hh_file, hv_file, hhhv_file], ["HH", "HV", "HH/HV"], sar_stack_file, None, 0, 'GTIFF', rsgislib.TYPE_16UINT)
            rsgislib.imageutils.popImageStats(sar_stack_file, usenodataval=True, nodataval=0, calcpyramids=True)

            try:
                msk_file = edd_utils.findFile(work_ard_scn_path, mask_file_pattern)
            except Exception as e:
                logger.error("Could not find Mask file in '{}'".format(work_ard_scn_path))
                raise e

            try:
                date_file = edd_utils.findFile(work_ard_scn_path, date_file_pattern)
            except Exception as e:
                logger.error("Could not find Date file in '{}'".format(work_ard_scn_path))
                raise e

            try:
                linci_file = edd_utils.findFile(work_ard_scn_path, linci_file_pattern)
            except Exception as e:
                logger.error("Could not find Incidence Angle file in '{}'".format(work_ard_scn_path))
                raise e
        else:
            # ALOS-2 PALSAR-2
            # Find files.
            ref_launch_date = datetime.datetime(2014, 5, 24)
            try:
                hh_file = edd_utils.findFile(work_ard_scn_path, hh_p2_file_pattern)
            except Exception:
                try:
                    hh_file = edd_utils.findFile(work_ard_scn_path, hh_p2_fp_file_pattern)
                except Exception as e:
                    logger.error("Could not find HH file in '{}'".format(work_ard_scn_path))
                    raise e

            try:
                hv_file = edd_utils.findFile(work_ard_scn_path, hv_p2_file_pattern)
            except Exception:
                try:
                    hv_file = edd_utils.findFile(work_ard_scn_path, hv_p2_fp_file_pattern)
                except Exception as e:
                    logger.error("Could not find HV file in '{}'".format(work_ard_scn_path))
                    raise e

            # Add HH/HV ratio.
            hhhv_file = os.path.join(tmp_ard_scn_path, unique_base_name + '_hhhv.tif')
            band_defns = [rsgislib.imagecalc.BandDefn('hh', hh_file, 1), rsgislib.imagecalc.BandDefn('hv', hv_file, 1)]
            rsgislib.imagecalc.bandMath(hhhv_file, 'hv==0?0:(hh/hv)*1000', 'GTIFF', rsgislib.TYPE_16UINT, band_defns)

            # Stack Image bands
            sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + '_HHHV.tif')
            if ard_proj_defined:
                sar_stack_tmp_file = os.path.join(tmp_ard_scn_path, unique_base_name + '_HHHV.tif')
                rsgislib.imageutils.stackImageBands([hh_file, hv_file, hhhv_file], ["HH", "HV", "HH/HV"], sar_stack_tmp_file, None, 0, 'GTIFF', rsgislib.TYPE_16UINT)
                sar_stack_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_HHHV.tif')
                rsgislib.imageutils.reprojectImage(sar_stack_tmp_file, sar_stack_file, proj_wkt_file, gdalformat='GTIFF', interp='cubic', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
            else:
                rsgislib.imageutils.stackImageBands([hh_file, hv_file, hhhv_file], ["HH", "HV", "HH/HV"], sar_stack_file, None, 0, 'GTIFF', rsgislib.TYPE_16UINT)
            rsgislib.imageutils.popImageStats(sar_stack_file, usenodataval=True, nodataval=0, calcpyramids=True)

            try:
                msk_file = edd_utils.findFile(work_ard_scn_path, mask_file_pattern)
            except Exception:
                try:
                    msk_file = edd_utils.findFile(work_ard_scn_path, mask_p2_file_pattern)
                except Exception as e:
                    logger.error("Could not find Mask file in '{}'".format(work_ard_scn_path))
                    raise e

            try:
                date_file = edd_utils.findFile(work_ard_scn_path, date_file_pattern)
            except Exception:
                try:
                    date_file = edd_utils.findFile(work_ard_scn_path, date_p2_file_pattern)
                except Exception as e:
                    logger.error("Could not find Date file in '{}'".format(work_ard_scn_path))
                    raise e

            try:
                linci_file = edd_utils.findFile(work_ard_scn_path, linci_file_pattern)
            except Exception:
                try:
                    linci_file = edd_utils.findFile(work_ard_scn_path, linci_p2_file_pattern)
                except Exception as e:
                    logger.error("Could not find Incidence Angle file in '{}'".format(work_ard_scn_path))
                    raise e

        out_msk_file = os.path.join(final_ard_scn_path, unique_base_name + '_mask.tif')
        if ard_proj_defined:
            out_msk_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_mask.tif')
            rsgislib.imageutils.reprojectImage(msk_file, out_msk_file, proj_wkt_file, gdalformat='GTIFF', interp='near', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
        else:
            rsgislib.imagecalc.imageMath(msk_file, out_msk_file, "b1", "GTIFF", rsgis_utils.getGDALDataTypeFromImg(msk_file))
        rsgislib.rastergis.populateStats(out_msk_file, True, True, True)

        ####### Add Mask descriptions ##########
        rat_img_dataset = gdal.Open(out_msk_file, gdal.GA_Update)
        Histogram = rat.readColumn(rat_img_dataset, "Histogram")
        Mask_Type_Names = numpy.empty_like(Histogram, dtype=numpy.dtype('a255'))
        Mask_Type_Names[...] = ""
        Mask_Type_Names[0] = "No data"
        if Mask_Type_Names.shape[0] >= 50:
            Mask_Type_Names[50] = "Ocean and water"
        if Mask_Type_Names.shape[0] >= 100:
            Mask_Type_Names[100] = "Lay over"
        if Mask_Type_Names.shape[0] >= 150:
            Mask_Type_Names[150] = "Shadowing"
        if Mask_Type_Names.shape[0] >= 255:
            Mask_Type_Names[255] = "Land"
        rat.writeColumn(rat_img_dataset, "Type", Mask_Type_Names)
        rat_img_dataset = None
        ########################################

        out_date_file = os.path.join(final_ard_scn_path, unique_base_name + '_date.tif')
        if ard_proj_defined:
            out_date_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_date.tif')
            rsgislib.imageutils.reprojectImage(date_file, out_date_file, proj_wkt_file, gdalformat='GTIFF', interp='near', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
        else:
            rsgislib.imagecalc.imageMath(date_file, out_date_file, "b1", "GTIFF", rsgis_utils.getGDALDataTypeFromImg(date_file))
        rsgislib.rastergis.populateStats(out_date_file, True, True, True)

        ##################### Define Dates #######################
        rat_img_dataset = gdal.Open(out_date_file, gdal.GA_Update)
        Histogram = rat.readColumn(rat_img_dataset, "Histogram")
        Aqu_Date_Day = numpy.zeros_like(Histogram, dtype=int)
        Aqu_Date_Month = numpy.zeros_like(Histogram, dtype=int)
        Aqu_Date_Year = numpy.zeros_like(Histogram, dtype=int)

        ID = numpy.arange(Histogram.shape[0])
        ID = ID[Histogram > 0]
        min_date_day_launch = numpy.min(ID) # For date range in db
        max_date_day_launch = numpy.max(ID) # For date range in db

        for tmp_day_count in ID:
            try:
                tmp_date = ref_launch_date + datetime.timedelta(days=float(tmp_day_count))
            except Exception as e:
                logger.error("Could not create datetime object '{}'".format(e))
                raise e
            Aqu_Date_Day[tmp_day_count] = tmp_date.day
            Aqu_Date_Month[tmp_day_count] = tmp_date.month
            Aqu_Date_Year[tmp_day_count] = tmp_date.year

        rat.writeColumn(rat_img_dataset, "Day", Aqu_Date_Day)
        rat.writeColumn(rat_img_dataset, "Month", Aqu_Date_Month)
        rat.writeColumn(rat_img_dataset, "Year", Aqu_Date_Year)
        rat_img_dataset = None
        #############################################################

        out_linci_file = os.path.join(final_ard_scn_path, unique_base_name + '_linci.tif')
        if ard_proj_defined:
            out_linci_file = os.path.join(final_ard_scn_path, unique_base_name + "_" + proj_abbv + '_linci.tif')
            rsgislib.imageutils.reprojectImage(linci_file, out_linci_file, proj_wkt_file, gdalformat='GTIFF', interp='near', inWKT=None, noData=0.0, outPxlRes=str(out_proj_img_res), snap2Grid=True, multicore=False)
        else:
            rsgislib.imagecalc.imageMath(linci_file, out_linci_file, "b1", "GTIFF", rsgis_utils.getGDALDataTypeFromImg(linci_file))
        rsgislib.rastergis.populateStats(out_linci_file, True, True, True)
        success = True
    except Exception as e:
        success = False
    end_date = datetime.datetime.now()

    if success:
        # Remove Remaining files.
        shutil.rmtree(work_ard_scn_path)
        shutil.rmtree(tmp_ard_scn_path)

        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == pid).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + pid)

        ##################### Find Date Range #######################
        try:
            query_result.Start_Date = ref_launch_date + datetime.timedelta(days=float(min_date_day_launch))
            query_result.End_Date = ref_launch_date + datetime.timedelta(days=float(max_date_day_launch))
        except Exception as e:
            logger.error("Could not create datetime object '{}'".format(e))
            raise e
        #############################################################

        ############## Find Incident Angle Range ####################
        rat_img_dataset = gdal.Open(out_linci_file, gdal.GA_ReadOnly)
        Histogram = rat.readColumn(rat_img_dataset, "Histogram")
        rat_img_dataset = None
        ID = numpy.arange(Histogram.shape[0])
        ID = ID[Histogram > 0]
        query_result.Incident_Angle_Low = numpy.min(ID)
        query_result.Incident_Angle_High = numpy.max(ID)
        #############################################################

        ############## Find input image BBOX ####################
        bbox = rsgis_utils.getImageBBOX(sar_stack_file)
        query_result.North_Lat = bbox[2]
        query_result.South_Lat = bbox[3]
        query_result.East_Lon = bbox[1]
        query_result.West_Lon = bbox[0]

        in_img_epsg = int(rsgis_utils.getEPSGCode(sar_stack_file))
        if in_img_epsg != 4326:
            tlx = bbox[0]
            tly = bbox[2]
            brx = bbox[1]
            bry = bbox[3]

            source_osr = osr.SpatialReference()
            source_osr.ImportFromEPSG(in_img_epsg)

            target_osr = osr.SpatialReference()
            target_osr.ImportFromEPSG(4326)

            north_lat, west_lon = rsgis_utils.reprojPoint(source_osr, target_osr, tlx, tly)
            south_lat, east_lon = rsgis_utils.reprojPoint(source_osr, target_osr, brx, bry)

            query_result.North_Lat = north_lat
            query_result.South_Lat = south_lat
            query_result.East_Lon = east_lon
            query_result.West_Lon = west_lon
        #########################################################

        query_result.ARDProduct = True
        query_result.ARDProduct_Start_Date = start_date
        query_result.ARDProduct_End_Date = end_date
        query_result.ARDProduct_Path = sar_stack_file
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")
    else:
        # Remove Remaining files.
        shutil.rmtree(work_ard_scn_path)
        shutil.rmtree(tmp_ard_scn_path)
        shutil.rmtree(final_ard_scn_path)
        logger.debug("Removed tmp, work and final paths are scene wasn't successfully processed.")



class EODataDownJAXASARTileSensor (EODataDownSensor):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "JAXASARTiles"
        self.db_tab_name = "EDDJAXASARTiles"
        self.avail_years = [1996, 2007, 2008, 2009, 2010, 2015, 2016, 2017]
        self.jaxa_ftp = "ftp.eorc.jaxa.jp"
        self.ftp_paths = dict()
        self.ftp_paths[1996] = "/pub/ALOS-2/JERS-1_MSC/25m_MSC/1996/"
        self.ftp_paths[2007] = "/pub/ALOS/ext1/PALSAR_MSC/25m_MSC/2007"
        self.ftp_paths[2008] = "/pub/ALOS/ext1/PALSAR_MSC/25m_MSC/2008"
        self.ftp_paths[2009] = "/pub/ALOS/ext1/PALSAR_MSC/25m_MSC/2009"
        self.ftp_paths[2010] = "/pub/ALOS/ext1/PALSAR_MSC/25m_MSC/2010"
        self.ftp_paths[2015] = "/pub/ALOS-2/ext1/PALSAR-2_MSC/25m_MSC/2015"
        self.ftp_paths[2016] = "/pub/ALOS-2/ext1/PALSAR-2_MSC/25m_MSC/2016"
        self.ftp_paths[2017] = "/pub/ALOS-2/ext2/PALSAR-2_MSC/25m_MSC/2017"
        self.instrument_name = dict()
        self.instrument_name[1996] = "JERS-1"
        self.instrument_name[2007] = "ALOS PALSAR"
        self.instrument_name[2008] = "ALOS PALSAR"
        self.instrument_name[2009] = "ALOS PALSAR"
        self.instrument_name[2010] = "ALOS PALSAR"
        self.instrument_name[2015] = "ALOS-2 PALSAR-2"
        self.instrument_name[2016] = "ALOS-2 PALSAR-2"
        self.instrument_name[2017] = "ALOS-2 PALSAR-2"

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
            logger.debug("Testing config file is for 'JAXASARTiles'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'JAXASARTiles'")

            logger.debug("Find ARD processing params from config file")
            self.projEPSG = -1
            self.projabbv = ""
            self.outImgRes = 25
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "proj",
                                                                       "epsg"], 0, 1000000000))
                self.outImgRes = float(json_parse_helper.getNumericValue(config_data,
                                                                         ["eodatadown", "sensor", "ardparams", "proj",
                                                                          "outres"], 0, 1000000))
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data,
                                                                  ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data,
                                                                 ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data,
                                                                ["eodatadown", "sensor", "paths", "ardtmp"])

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths", "quicklooks"]):
                self.quicklookPath = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "paths", "quicklooks"])
            else:
                self.quicklookPath = None

            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "paths", "tilecache"]):
                self.tilecachePath = json_parse_helper.getStrValue(config_data,
                                                                    ["eodatadown", "sensor", "paths", "tilecache"])
            else:
                self.tilecachePath = None

            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            self.lcl_jaxa_lst = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "download", "jaxa_file_listing"])
            if self.lcl_jaxa_lst == "":
                self.lcl_jaxa_lst = None
            self.tile_lst = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor", "download", "tiles"])
            self.all_jaxa_tiles = False
            if len(self.tile_lst) == 0:
                self.all_jaxa_tiles = True

            self.years_of_interest = json_parse_helper.getListValue(config_data,
                                                                    ["eodatadown", "sensor", "download", "years"])
            if len(self.years_of_interest) == 0:
                raise EODataDownException("Must specify at least one year")

            for year in self.years_of_interest:
                if year not in self.avail_years:
                    raise EODataDownException(
                        "The year ({0}) specified is not within the list of available years.".format(year))
            logger.debug("Found search params from config file")

    def init_sensor_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Creating JAXASARTiles Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_rtn = ses.query(EDDJAXASARTiles.Year).group_by(EDDJAXASARTiles.Year).all()
        years_in_db = list()
        for result in query_rtn:
            years_in_db.append(result[0])

        years_to_dwn = list()
        for year_tmp in self.years_of_interest:
            if year_tmp not in years_in_db:
                years_to_dwn.append(year_tmp)
        new_scns_avail = False
        if len(years_to_dwn) > 0:
            if self.lcl_jaxa_lst is None:
                edd_ftp_utils = eodatadown.eodatadownutils.EODDFTPDownload()
                new_scns_avail = False
                for cyear in years_to_dwn:
                    logger.info("Processing {} from remote server.".format(cyear))
                    file_dict, file_lst = edd_ftp_utils.getFTPFileListings(self.jaxa_ftp, self.ftp_paths[cyear], "", "",
                                                                           ftp_timeout=None)
                    db_records = list()
                    for file_path in file_lst:
                        file_base_path = os.path.split(file_path)[0]
                        parent_tile = os.path.basename(file_base_path)
                        file_name = os.path.split(file_path)[1]
                        tile_name = file_name.split("_")[0]
                        if ("FNF" not in file_name) and (parent_tile.strip() != str(cyear)):
                            db_records.append(EDDJAXASARTiles(Tile_Name=tile_name, Parent_Tile=parent_tile, Year=cyear,
                                                              File_Name=file_name, Server_File_Path=file_path,
                                                              InstrumentName=self.instrument_name[cyear],
                                                              Query_Date=datetime.datetime.now()))
                    if len(db_records) > 0:
                        ses.add_all(db_records)
                        ses.commit()
                        new_scns_avail = True
            else:
                json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
                jaxa_file_lst = json_parse_helper.readGZIPJSON(self.lcl_jaxa_lst)

                for cyear in years_to_dwn:
                    logger.info("Processing {} from local file specified.".format(cyear))
                    db_records = list()
                    for file_path in jaxa_file_lst[str(cyear)]:
                        file_base_path = os.path.split(file_path)[0]
                        parent_tile = os.path.basename(file_base_path)
                        file_name = os.path.split(file_path)[1]
                        tile_name = file_name.split("_")[0]
                        if ("FNF" not in file_name) and (parent_tile.strip() != str(cyear)):
                            db_records.append(EDDJAXASARTiles(Tile_Name=tile_name, Parent_Tile=parent_tile, Year=cyear,
                                                              File_Name=file_name, Server_File_Path=file_path,
                                                              InstrumentName=self.instrument_name[cyear],
                                                              Query_Date=datetime.datetime.now()))
                    if len(db_records) > 0:
                        ses.add_all(db_records)
                        ses.commit()
                        new_scns_avail = True

        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked for availability of new scenes", sensor_val=self.sensor_name,
                               updated_lcl_db=True, scns_avail=new_scns_avail)

    def get_scnlist_all(self):
        """
        A function which returns a list of the unique IDs for all the scenes within the database.

        :return: list of integers
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDJAXASARTiles).all()
        scns = list()
        if query_result is not None:
            for record in query_result:
                scns.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns

    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        raise EODataDownException("Not implemented.")

    def has_scn_download(self, unq_id):
        """
        A function which checks whether an individual scene has been downloaded.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: boolean (True for downloaded; False for not downloaded)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.Downloaded

    def download_scn(self, unq_id):
        """
        A function which downloads an individual scene and updates the database if download is successful.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: returns boolean indicating successful or otherwise download.
        """
        raise EODataDownException("Not implemented.")

    def download_all_avail(self, n_cores):
        """
        Queries the database to find all scenes which have not been downloaded and then downloads them.
        This function uses the python multiprocessing Pool to allow multiple simultaneous downloads to occur.
        Be careful not use more cores than your internet connection and server can handle.
        :param n_cores: The number of scenes to be simultaneously downloaded.
        """
        if not os.path.exists(self.baseDownloadPath):
            raise EODataDownException("The download path does not exist, please create and run again.")
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.Downloaded == False).all()
        download_new_scns = False
        if query_result is not None:
            logger.debug("Build download file list.")
            dwnld_params = list()
            for record in query_result:
                if self.all_jaxa_tiles or (record.Tile_Name in self.tile_lst):
                    logger.debug("Building download info for '"+record.File_Name+"'")
                    local_file_path = os.path.join(self.baseDownloadPath, record.File_Name)
                    dwnld_params.append([record.Server_File_Path, self.jaxa_ftp, local_file_path, self.db_info_obj])
        else:
            logger.info("There are no scenes to be downloaded.")

        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        if len(dwnld_params) > 0:
            with multiprocessing.Pool(processes=n_cores) as pool:
                pool.map(_download_scn_jaxa, dwnld_params)
            download_new_scns = True
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked downloaded new scenes.", sensor_val=self.sensor_name, updated_lcl_db=True, downloaded_new_scns=download_new_scns)

    def get_scnlist_con2ard(self):
        """
        A function which queries the database to find scenes which have been downloaded but have not yet been
        processed to an analysis ready data (ARD) format.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to process.
        """
        raise EODataDownException("Not implemented.")

    def has_scn_con2ard(self, unq_id):
        """
        A function which checks whether a scene has been converted to an ARD product.
        :param unq_id: the unique ID of the scene of interest.
        :return: boolean (True: has been converted. False: Has not been converted)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return (query_result.ARDProduct == True) and (EDDJAXASARTiles.Invalid == False)

    def scn2ard(self, unq_id):
        """
        A function which processes a single scene to an analysis ready data (ARD) format.
        :param unq_id: the unique ID of the scene to be processed.
        :return: returns boolean indicating successful or otherwise processing.
        """
        raise EODataDownException("Not implemented.")

    def scns2ard_all_avail(self, n_cores):
        """
        Queries the database to find all scenes which have been downloaded but not processed to an
        analysis ready data (ARD) format and then processed them to an ARD format.
        This function uses the python multiprocessing Pool to allow multiple simultaneous processing
        of the scenes using a single core for each scene.
        Be careful not use more cores than your system has or have I/O capacity for. The processing being
        undertaken is I/O heavy in the ARD Work and tmp paths. If you have high speed storage (e.g., SSD)
        available it is recommended the ARD work and tmp paths are located on this volume.
        :param n_cores: The number of scenes to be simultaneously processed.
        """
        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdWorkPath):
            raise EODataDownException("The ARD working path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdTmpPath):
            raise EODataDownException("The ARD tmp path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.Downloaded == True,
                                                         EDDJAXASARTiles.ARDProduct == False,
                                                         EDDJAXASARTiles.Invalid == False).all()

        proj_wkt_file = None
        if self.ardProjDefined:
            rsgis_utils = rsgislib.RSGISPyUtils()
            proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

        if query_result is not None:
            logger.debug("Create the specific output directories for the ARD processing.")
            dt_obj = datetime.datetime.now()

            work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(work_ard_path):
                os.mkdir(work_ard_path)

            tmp_ard_path = os.path.join(self.ardProdTmpPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(tmp_ard_path):
                os.mkdir(tmp_ard_path)

            ard_params = list()
            for record in query_result:
                if self.all_jaxa_tiles or (record.Tile_Name in self.tile_lst):
                    logger.debug("Create info for running ARD analysis for scene: {0} in {1}".format(record.Tile_Name, record.Year))
                    unique_name = "{0}_{1}".format(record.Tile_Name, record.Year)
                    final_ard_scn_path = os.path.join(self.ardFinalPath, unique_name)
                    if not os.path.exists(final_ard_scn_path):
                        os.mkdir(final_ard_scn_path)

                    work_ard_scn_path = os.path.join(work_ard_path, unique_name)
                    if not os.path.exists(work_ard_scn_path):
                        os.mkdir(work_ard_scn_path)

                    tmp_ard_scn_path = os.path.join(tmp_ard_path, unique_name)
                    if not os.path.exists(tmp_ard_scn_path):
                        os.mkdir(tmp_ard_scn_path)

                    if self.ardProjDefined:
                        proj_wkt_file = os.path.join(work_ard_scn_path, unique_name + "_wkt.wkt")
                        rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                    ard_params.append([record.PID, record.Tile_Name, record.Year, self.db_info_obj, record.Download_Path, work_ard_scn_path,
                         tmp_ard_scn_path, final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv, self.outImgRes])
        else:
            logger.info("There are no scenes which have been downloaded but not processed to an ARD product.")
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start processing the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_process_to_ard, ard_params)
        logger.info("Finished processing the scenes.")

        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Processed scenes to an ARD product.", sensor_val=self.sensor_name,
                               updated_lcl_db=True, convert_scns_ard=True)

    def get_scnlist_datacube(self, loaded=False):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.ARDProduct == True,
                                                         EDDJAXASARTiles.DCLoaded == loaded).all()
        scns2dcload = list()
        if query_result is not None:
            for record in query_result:
                scns2dcload.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2dcload

    def has_scn_datacube(self, unq_id):
        """
        A function to find whether a scene has been loaded in the DataCube.
        :param unq_id: the unique ID of the scene.
        :return: boolean (True: Loaded in DataCube. False: Not loaded in DataCube)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scene.")
        query_result = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == unq_id).one()
        ses.close()
        logger.debug("Closed the database session.")
        return query_result.DCLoaded

    def scn2datacube(self, unq_id):
        """
        A function which loads a single scene into the datacube system.
        :param unq_id: the unique ID of the scene to be loaded.
        :return: returns boolean indicating successful or otherwise loading into the datacube.
        """
        raise EODataDownException("Not implemented.")

    def scns2datacube_all_avail(self):
        """
        Queries the database to find all scenes which have been processed to an ARD format but not loaded
        into the datacube and then loads these scenes into the datacube.
        """
        raise EODataDownException("Not implemented.")

    def get_scnlist_quicklook(self):
        """
        Get a list of all scenes which have not had a quicklook generated.

        :return: list of unique IDs
        """
        raise EODataDownException('get_scnlist_quicklook not implemented')

    def has_scn_quicklook(self, unq_id):
        """
        Check whether the quicklook has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has quicklook. False = has not got a quicklook)
        """
        raise EODataDownException('has_scn_quicklook not implemented')

    def scn2quicklook(self, unq_id):
        """
        Generate the quicklook image for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        raise EODataDownException('scn2quicklook not implemented')

    def scns2quicklook_all_avail(self):
        """
        Generate the quicklook images for the scenes for which a quicklook image do not exist.

        """
        raise EODataDownException('scns2quicklook_all_avail not implemented')

    def get_scnlist_tilecache(self):
        """
        Get a list of all scenes for which a tile cache has not been generated.

        :return: list of unique IDs
        """
        raise EODataDownException('get_scnlist_tilecache not implemented')

    def has_scn_tilecache(self, unq_id):
        """
        Check whether a tile cache has been generated for an individual scene.

        :param unq_id: integer unique ID for the scene.
        :return: boolean (True = has quicklook. False = has not got a quicklook)
        """
        raise EODataDownException('has_scn_tilecache not implemented')

    def scn2tilecache(self, unq_id):
        """
        Generate the tile cache for the scene.

        :param unq_id: integer unique ID for the scene.
        """
        raise EODataDownException('scn2tilecache not implemented')

    def scns2tilecache_all_avail(self):
        """
        Generate the tile cache for the scenes for which a tile cache does not exist.

        """
        raise EODataDownException('scns2tilecache_all_avail not implemented')

    def get_scn_record(self, unq_id):
        """
        A function which queries the database using the unique ID of a scene returning the record
        :param unq_id:
        :return: Returns the database record object
        """
        raise EODataDownException("Not implemented.")

    def query_scn_records_date_count(self, start_date, end_date, valid=True):
        """
        A function which queries the database to find scenes within a specified date range
        and returns the number of records available.

        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :param valid: If True only valid scene records will be returned (i.e., has been processed to an ARD product)
        :return: count of records available
        """
        raise Exception("Not Implemented")

    def query_scn_records_date(self, start_date, end_date, start_rec=0, n_recs=0, valid=True):
        """
        A function which queries the database to find scenes within a specified date range.
        :param start_date: A python datetime object specifying the start date
        :param end_date: A python datetime object specifying the end date
        :return: list of database records.
        """
        raise EODataDownException("Not implemented.")

    def query_scn_records_bbox(self, lat_north, lat_south, lon_east, lon_west):
        """
        A function which queries the database to find scenes within a specified bounding box.
        :param lat_north: double with latitude north
        :param lat_south: double with latitude south
        :param lon_east: double with longitude east
        :param lon_west: double with longitude west
        :return: list of database records.
        """
        raise EODataDownException("Not implemented.")

    def update_dwnld_path(self, replace_path, new_path):
        """
        If the path to the downloaded files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not implemented.")

    def update_ard_path(self, replace_path, new_path):
        """
        If the path to the ARD files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not implemented.")

    def dwnlds_archived(self, replace_path=None, new_path=None):
        """
        This function identifies scenes which have been downloaded but the download is no longer available
        in the download path. It will set the archived option on the database for these files. It is expected
        that these files will have been move to an archive location (e.g., AWS glacier or tape etc.) but they
        could have just be deleted. There is an option to update the path to the downloads if inputs are not
        None but a check will not be performed as to whether the data is present at the new path.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files are located.
        """
        raise EODataDownException("Not implemented.")

    def export_db_to_json(self, out_json_file):
        """
        This function exports the database table to a JSON file.
        :param out_json_file: output JSON file path.
        """
        raise EODataDownException("Not implemented.")

    def import_sensor_db(self, input_json_file):
        """
        This function imports from the database records from the specified input JSON file.
        The database table is expected to be empty and an error will be raised if there are . This might be used if data was processed
        on another system (e.g., HPC cluster).
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        raise EODataDownException("Not implemented.")

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        raise EODataDownException("Not Implemented")

    def reset_scn(self, unq_id, reset_download=False):
        """
        A function which resets an image. This means any downloads and products are deleted
        and the database fields are reset to defaults. This allows the scene to be re-downloaded
        and processed.
        :param unq_id: unique id for the scene to be reset.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == unq_id).one_or_none()

        if scn_record is None:
            ses.close()
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

        if scn_record.DCLoaded:
            # How to remove from datacube?
            scn_record.DCLoaded_Start_Date = None
            scn_record.DCLoaded_End_Date = None
            scn_record.DCLoaded = False

        if scn_record.ARDProduct:
            ard_path = scn_record.ARDProduct_Path
            if os.path.exists(ard_path):
                shutil.rmtree(ard_path)
            scn_record.ARDProduct_Start_Date = None
            scn_record.ARDProduct_End_Date = None
            scn_record.ARDProduct_Path = ""
            scn_record.ARDProduct = False

        if scn_record.Downloaded and reset_download:
            dwn_path = scn_record.Download_Path
            if os.path.exists(dwn_path):
                shutil.rmtree(dwn_path)
            scn_record.Download_Start_Date = None
            scn_record.Download_End_Date = None
            scn_record.Download_Path = ""
            scn_record.Downloaded = False

        ses.commit()
        ses.close()

    def reset_dc_load(self, unq_id):
        """
        A function which resets whether an image has been loaded into a datacube
        (i.e., sets the flag to False).
        :param unq_id: unique id for the scene to be reset.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Perform query to find scene.")
        scn_record = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.PID == unq_id).one_or_none()

        if scn_record is None:
            ses.close()
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

        if scn_record.DCLoaded:
            scn_record.DCLoaded_Start_Date = None
            scn_record.DCLoaded_End_Date = None
            scn_record.DCLoaded = False

        ses.commit()
        ses.close()
