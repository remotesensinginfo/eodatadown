#!/usr/bin/env python
"""
EODataDown - a sensor class for a generic dataset.
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
# Purpose:  Provides a sensor class for a generic dataset
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 15/09/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import glob
import os
import os.path
import datetime
import multiprocessing
import shutil
import rsgislib
from osgeo import osr
from osgeo import ogr
from osgeo import gdal

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

def _process_to_ard(params):
    """

    :param params:
    """
    base_name = params[0]
    db_info_obj = params[1]
    download_file = params[2]
    ard_work_path = params[3]
    ard_final_path = params[4]
    ard_proj_defined = params[5]
    proj_wkt_file = params[6]
    projabbv = params[7]
    out_proj_img_res = params[8]
    reproj_interp = params[9]
    generic_table = params[10]
    success = False

    start_date = datetime.datetime.now()
    rsgis_utils = rsgislib.RSGISPyUtils()
    no_data_val = rsgis_utils.getImageNoDataValue(download_file)
    try:
        download_path = os.path.dirname(download_file)
        if os.path.abspath(download_path) == os.path.abspath(ard_final_path):
            ard_file = download_file
            rsgislib.imageutils.popImageStats(ard_file, usenodataval=True, nodataval=no_data_val, calcpyramids=True)
        else:
            in_datatype = rsgis_utils.getGDALDataTypeFromImg(download_file)
            if ard_proj_defined:
                ard_file = os.path.join(ard_final_path, base_name +'_'+projabbv+ '.tif')
                rsgislib.imageutils.reprojectImage(download_file, ard_file, proj_wkt_file, gdalformat='GTIFF', interp=reproj_interp,
                                                   inWKT=None, noData=no_data_val, outPxlRes=str(out_proj_img_res), snap2Grid=True,
                                                   multicore=False)
            else:
                ard_file = os.path.join(ard_final_path, base_name+'.tif')
                rsgislib.imagecalc.imageMath(download_file, ard_file, 'b1', 'GTIFF', in_datatype)
            rsgislib.imageutils.popImageStats(ard_file, usenodataval=True, nodataval=no_data_val, calcpyramids=True)
        success = True
        logger.debug("Processed to ARD Product.")
    except Exception as e:
        success = False
        logger.error("Could not convert '{}' to an ARD image.".format(download_file))
        raise e
    end_date = datetime.datetime.now()

    if success:
        logger.debug("Get bounding box for image.")
        min_x, max_x, min_y, max_y = rsgis_utils.getImageBBOX(ard_file)
        img_epsg_code = int(rsgis_utils.getEPSGCode(ard_file))
        if img_epsg_code == 4326:
            north_lat = max_y
            south_lat = min_y
            east_lon = max_x
            west_lon = min_x
        else:
            source_osr = osr.SpatialReference()
            source_osr.ImportFromEPSG(img_epsg_code)

            target_osr = osr.SpatialReference()
            target_osr.ImportFromEPSG(4326)
            north_lat, west_lon = rsgis_utils.reprojPoint(source_osr, target_osr, min_x, max_y)
            south_lat, east_lon = rsgis_utils.reprojPoint(source_osr, target_osr, max_x, min_y)
        logger.debug("Got bounding box for image.")


        logger.debug("Set up database connection and update record.")
        db_engine = sqlalchemy.create_engine(db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_obj = generic_table.update().where(generic_table.c.Base_Name == base_name).values(North_Lat=north_lat,
                                                                                                South_Lat=south_lat,
                                                                                                East_Lon=east_lon,
                                                                                                West_Lon=west_lon,
                                                                                                ARDProduct=True,
                                                                                                ARDProduct_Start_Date=start_date,
                                                                                                ARDProduct_End_Date=end_date,
                                                                                                ARDProduct_Path=ard_file)
        ses.execute(query_obj)
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")
    shutil.rmtree(ard_work_path)


class EODataDownGenericDatasetSensor (EODataDownSensor):
    """
    An class which represents a the Landsat sensor being downloaded from the Google Cloud.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensor_name = "GenericDataset"
        self.metadata =  sqlalchemy.MetaData()

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
            logger.debug("Testing config file is for 'GenericDataset'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensor_name])
            logger.debug("Have the correct config file for 'GenericDataset'")

            logger.debug("Get database table name from  config file.")
            self.db_tab_name = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "dbtabname"])
            logger.debug("Got database table name from  config file.")

            logger.debug("Find ARD processing params from config file")
            self.projEPSG = -1
            self.projabbv = ""
            self.outImgRes = 0.0
            self.reprojInterp = "near"
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
                self.reprojInterp = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "ardparams", "proj", "interp"])
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            self.downloadSearchPath = json_parse_helper.getStrValue(config_data,
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

            logger.debug("Read info params from config file")
            self.sensorInfoName = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "info", "sensor"])

            self.sensorSource = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "info", "source"])

            self.sensorDateParse = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "info", "dateparse"])

            self.sensorDate = json_parse_helper.getDateValue(config_data, ["eodatadown", "sensor", "info", "date"], self.sensorDateParse)
            logger.debug("Found info params from config file")

            self.generic_table = sqlalchemy.Table(self.db_tab_name, self.metadata,
                                                  sqlalchemy.Column('PID', sqlalchemy.Integer, primary_key=True),
                                                  sqlalchemy.Column('Base_Name', sqlalchemy.String, nullable=False),
                                                  sqlalchemy.Column('Sensor', sqlalchemy.String, nullable=False),
                                                  sqlalchemy.Column('Source', sqlalchemy.String, nullable=False),
                                                  sqlalchemy.Column('Source_Date', sqlalchemy.DateTime, nullable=False),
                                                  sqlalchemy.Column('North_Lat', sqlalchemy.Float, default=0.0),
                                                  sqlalchemy.Column('South_Lat', sqlalchemy.Float, default=0.0),
                                                  sqlalchemy.Column('East_Lon', sqlalchemy.Float, default=0.0),
                                                  sqlalchemy.Column('West_Lon', sqlalchemy.Float, default=0.0),
                                                  sqlalchemy.Column('Query_Date', sqlalchemy.DateTime, nullable=False),
                                                  sqlalchemy.Column('Downloaded', sqlalchemy.Boolean, nullable=False,
                                                                    default=False),
                                                  sqlalchemy.Column('Download_Path', sqlalchemy.String, nullable=False),
                                                  sqlalchemy.Column('Archived', sqlalchemy.Boolean, nullable=False,
                                                                    default=False),
                                                  sqlalchemy.Column('ARDProduct_Start_Date', sqlalchemy.DateTime,
                                                                    nullable=True),
                                                  sqlalchemy.Column('ARDProduct_End_Date', sqlalchemy.DateTime,
                                                                    nullable=True),
                                                  sqlalchemy.Column('ARDProduct', sqlalchemy.Boolean, nullable=False,
                                                                    default=False),
                                                  sqlalchemy.Column('ARDProduct_Path', sqlalchemy.String,
                                                                    nullable=False,
                                                                    default=""),
                                                  sqlalchemy.Column('DCLoaded_Start_Date', sqlalchemy.DateTime,
                                                                    nullable=True),
                                                  sqlalchemy.Column('DCLoaded_End_Date', sqlalchemy.DateTime,
                                                                    nullable=True),
                                                  sqlalchemy.Column('DCLoaded', sqlalchemy.Boolean, nullable=False,
                                                                    default=False),
                                                  sqlalchemy.Column('Invalid', sqlalchemy.Boolean, nullable=False,
                                                                    default=False),
                                                  sqlalchemy.Column('ExtendedInfo', sqlalchemy.dialects.postgresql.JSONB,
                                                                    nullable=True),
                                                  sqlalchemy.Column('RegCheck', sqlalchemy.Boolean, nullable=False,
                                                                    default=False)
                                                  )


    def init_sensor_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        self.metadata.drop_all(db_engine)

        logger.debug("Creating {} Database.".format(self.db_tab_name))
        self.metadata.bind = db_engine
        self.metadata.create_all()

    def check_new_scns(self, check_from_start=False):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        file_lst = glob.glob(self.downloadSearchPath)
        new_scns_avail = False
        for tmp_file in file_lst:
            base_name = os.path.splitext(os.path.basename(tmp_file))[0]
            query_obj = sqlalchemy.select([self.generic_table]).where(self.generic_table.c.Base_Name == base_name)
            query_rtn = ses.execute(query_obj).fetchall()
            if len(query_rtn) == 0:
                new_scns_avail = True
                ins_sql = self.generic_table.insert().values(Base_Name=base_name, Sensor=self.sensorInfoName,
                                                             Source=self.sensorSource, Source_Date=self.sensorDate,
                                                             Query_Date=datetime.datetime.now(), Downloaded=True,
                                                             Download_Path=tmp_file)
                ses.execute(ins_sql)
        ses.commit()
        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Checked for availability of new scenes", sensor_val=self.db_tab_name,
                               updated_lcl_db=True, scns_avail=new_scns_avail)

    def get_scnlist_all(self):
        """
        A function which returns a list of the unique IDs for all the scenes within the database.

        :return: list of integers
        """
        raise EODataDownException("Not Implemented.")

    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        return list()

    def has_scn_download(self, unq_id):
        """
        A function which checks whether an individual scene has been downloaded.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: boolean (True for downloaded; False for not downloaded)
        """
        raise EODataDownException("Not Implemented.")

    def download_scn(self, unq_id):
        """
        A function which downloads an individual scene and updates the database if download is successful.
        :param unq_id: the unique ID of the scene to be downloaded.
        :return: returns boolean indicating successful or otherwise download.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        query_obj = sqlalchemy.select([self.generic_table]).where(self.generic_table.c.PID == unq_id)
        query_rtn = ses.execute(query_obj).fetchall()
        if len(query_rtn) == 1:
            downloaded = query_rtn[0].Downloaded
        else:
            raise EODataDownException("Could not find a record for PID {}.".format(unq_id))
        return downloaded

    def download_all_avail(self, n_cores):
        """
        Queries the database to find all scenes which have not been downloaded and then downloads them.
        This function uses the python multiprocessing Pool to allow multiple simultaneous downloads to occur.
        Be careful not use more cores than your internet connection and server can handle.
        :param n_cores: The number of scenes to be simultaneously downloaded.
        """
        logger.info("This sensor class does not download scenes.")

    def get_scnlist_con2ard(self):
        """
        A function which queries the database to find scenes which have been downloaded but have not yet been
        processed to an analysis ready data (ARD) format.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to process.
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Perform query to find scenes which need downloading.")
        query_obj = sqlalchemy.select([self.generic_table]).where(self.generic_table.c.Downloaded == True, self.generic_table.c.ARDProduct == False)
        query_rtn = ses.execute(query_obj).fetchall()

        scns2ard = list()
        if len(query_rtn) > 0:
            for record in query_rtn:
                scns2ard.append(record.PID)
        ses.close()
        logger.debug("Closed the database session.")
        return scns2ard

    def has_scn_con2ard(self, unq_id):
        """
        A function which checks whether a scene has been converted to an ARD product.
        :param unq_id: the unique ID of the scene of interest.
        :return: boolean (True: has been converted. False: Has not been converted)
        """
        raise EODataDownException("Not Implemented")

    def scn2ard(self, unq_id):
        """
        A function which processes a single scene to an analysis ready data (ARD) format.
        :param unq_id: the unique ID of the scene to be processed.
        :return: returns boolean indicating successful or otherwise processing.
        """
        if not os.path.exists(self.ardFinalPath):
            raise EODataDownException("The ARD final path does not exist, please create and run again.")

        if not os.path.exists(self.ardProdWorkPath):
            raise EODataDownException("The ARD work path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_obj = sqlalchemy.select([self.generic_table]).where(self.generic_table.c.PID == unq_id)
        query_rtn = ses.execute(query_obj).fetchall()
        if len(query_rtn) > 0:
            if len(query_rtn) == 1:
                record = query_rtn[0]

                dt_obj = datetime.datetime.now()

                work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
                if not os.path.exists(work_ard_path):
                    os.mkdir(work_ard_path)

                work_ard_scn_path = os.path.join(work_ard_path, record.Base_Name)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                proj_wkt_file = None
                if self.ardProjDefined:
                    rsgis_utils = rsgislib.RSGISPyUtils()
                    proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Base_Name + "_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                _process_to_ard([record.Base_Name, self.db_info_obj, record.Download_Path, work_ard_scn_path,
                                 self.ardFinalPath, self.ardProjDefined, proj_wkt_file, self.projabbv,
                                 self.outImgRes, self.reprojInterp, self.generic_table])
            else:
                logger.error("PID {0} has returned more than 1 scene - must be unique something really wrong.".format(unq_id))
                raise EODataDownException("There was more than 1 scene which has been found - soomething has gone really wrong!")
        else:
            logger.error("PID {0} has not returned a scene - check inputs.".format(unq_id))
            raise EODataDownException("PID {0} has not returned a scene - check inputs.".format(unq_id))

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
            raise EODataDownException("The ARD work path does not exist, please create and run again.")

        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        query_obj = sqlalchemy.select([self.generic_table]).where(self.generic_table.c.Downloaded == True and
                                                                  self.generic_table.c.ARDProduct == False)
        query_rtn = ses.execute(query_obj).fetchall()
        if len(query_rtn) > 0:
            dt_obj = datetime.datetime.now()
            work_ard_path = os.path.join(self.ardProdWorkPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(work_ard_path):
                os.mkdir(work_ard_path)

            if self.ardProjDefined:
                rsgis_utils = rsgislib.RSGISPyUtils()
                proj_wkt = rsgis_utils.getWKTFromEPSGCode(self.projEPSG)

            ard_params = list()
            for record in query_rtn:
                work_ard_scn_path = os.path.join(work_ard_path, record.Base_Name)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                proj_wkt_file = None
                if self.ardProjDefined:
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Base_Name + "_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                ard_params.append([record.Base_Name, self.db_info_obj, record.Download_Path, work_ard_scn_path,
                             self.ardFinalPath, self.ardProjDefined, proj_wkt_file, self.projabbv,
                             self.outImgRes, self.reprojInterp, self.generic_table])
        else:
            logger.info("There are no scenes which have been downloaded but not processed to an ARD product.")
        ses.close()
        logger.debug("Closed the database session.")

        if len(ard_params) > 0:
            logger.info("Start processing the scenes.")
            with multiprocessing.Pool(processes=n_cores) as pool:
                pool.map(_process_to_ard, ard_params)
            logger.info("Finished processing the scenes.")

        edd_usage_db = EODataDownUpdateUsageLogDB(self.db_info_obj)
        edd_usage_db.add_entry(description_val="Processed scenes to an ARD product.", sensor_val=self.db_tab_name,
                               updated_lcl_db=True, convert_scns_ard=True)

    def get_scnlist_datacube(self, loaded=False):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        raise EODataDownException("Not Implemented")

    def has_scn_datacube(self, unq_id):
        """
        A function to find whether a scene has been loaded in the DataCube.
        :param unq_id: the unique ID of the scene.
        :return: boolean (True: Loaded in DataCube. False: Not loaded in DataCube)
        """
        raise EODataDownException("Not Implemented")

    def scn2datacube(self, unq_id):
        """
        A function which loads a single scene into the datacube system.
        :param unq_id: the unique ID of the scene to be loaded.
        :return: returns boolean indicating successful or otherwise loading into the datacube.
        """
        raise EODataDownException("Not Implemented")

    def scns2datacube_all_avail(self):
        """
        Queries the database to find all scenes which have been processed to an ARD format but not loaded
        into the datacube and then loads these scenes into the datacube.
        """
        raise EODataDownException("Not Implemented")

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
        raise EODataDownException("Not Implemented")

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
        raise EODataDownException("Not Implemented")

    def find_unique_scn_dates(self, start_date, end_date, valid=True):
        """
        A function which returns a list of unique dates on which acquisitions have occurred.
        :param start_date: A python datetime object specifying the start date (most recent date)
        :param end_date: A python datetime object specifying the end date (earliest date)
        :param valid: If True only valid observations are considered.
        :return: List of datetime objects. Might return None.
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
        raise EODataDownException("Not Implemented")

    def update_dwnld_path(self, replace_path, new_path):
        """
        If the path to the downloaded files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not Implemented")

    def update_ard_path(self, replace_path, new_path):
        """
        If the path to the ARD files is updated then this function will update the database
        replacing the part of the path which has been changed. The files will also be moved (if they have
        not already been moved) during the processing. If they are no present at the existing location
        in the database or at the new path then this process will not complete.
        :param replace_path: The existing path to be replaced.
        :param new_path: The new path where the downloaded files will be located.
        """
        raise EODataDownException("Not Implemented")

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
        raise EODataDownException("Not Implemented")

    def export_db_to_json(self, out_json_file):
        """
        This function exports the database table to a JSON file.
        :param out_json_file: output JSON file path.
        """
        raise EODataDownException("Not Implemented")

    def import_sensor_db(self, input_json_file, replace_path_dict=None):
        """
        This function imports from the database records from the specified input JSON file.
        The database table checks are not made for duplicated as records are just appended 
        to the table with a new PID.
        :param input_json_file: input JSON file with the records to be imported.
        :param replace_path_dict: a dictionary of file paths to be updated, if None then ignored.
        """
        raise EODataDownException("Not Implemented")

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='GPKG', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        try:
            gdal.UseExceptions()

            vec_osr = osr.SpatialReference()
            vec_osr.ImportFromEPSG(4326)

            driver = ogr.GetDriverByName(driver_name)
            if os.path.exists(file_path) and add_lyr:
                out_data_source = gdal.OpenEx(file_path, gdal.OF_UPDATE)
            elif os.path.exists(file_path):
                driver.DeleteDataSource(file_path)
                out_data_source = driver.CreateDataSource(file_path)
            else:
                out_data_source = driver.CreateDataSource(file_path)

            out_vec_lyr = out_data_source.GetLayerByName(lyr_name)
            if out_vec_lyr is None:
                out_vec_lyr = out_data_source.CreateLayer(lyr_name, srs=vec_osr, geom_type=ogr.wkbPolygon )

            basename_field_defn = ogr.FieldDefn("BaseName", ogr.OFTString)
            basename_field_defn.SetWidth(128)
            if out_vec_lyr.CreateField(basename_field_defn) != 0:
                raise EODataDownException("Could not create 'BaseName' field in output vector lyr.")

            sensor_field_defn = ogr.FieldDefn("Sensor", ogr.OFTString)
            sensor_field_defn.SetWidth(128)
            if out_vec_lyr.CreateField(sensor_field_defn) != 0:
                raise EODataDownException("Could not create 'Sensor' field in output vector lyr.")

            source_field_defn = ogr.FieldDefn("Source", ogr.OFTString)
            source_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(source_field_defn) != 0:
                raise EODataDownException("Could not create 'Source' field in output vector lyr.")

            date_field_defn = ogr.FieldDefn("Date", ogr.OFTString)
            date_field_defn.SetWidth(32)
            if out_vec_lyr.CreateField(date_field_defn) != 0:
                raise EODataDownException("Could not create 'Date' field in output vector lyr.")

            down_file_field_defn = ogr.FieldDefn("DownFile", ogr.OFTString)
            down_file_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(down_file_field_defn) != 0:
                raise EODataDownException("Could not create 'DownFile' field in output vector lyr.")

            ard_file_field_defn = ogr.FieldDefn("ARDFile", ogr.OFTString)
            ard_file_field_defn.SetWidth(256)
            if out_vec_lyr.CreateField(ard_file_field_defn) != 0:
                raise EODataDownException("Could not create 'ARDFile' field in output vector lyr.")

            north_field_defn = ogr.FieldDefn("North_Lat", ogr.OFTReal)
            if out_vec_lyr.CreateField(north_field_defn) != 0:
                raise EODataDownException("Could not create 'North_Lat' field in output vector lyr.")

            south_field_defn = ogr.FieldDefn("South_Lat", ogr.OFTReal)
            if out_vec_lyr.CreateField(south_field_defn) != 0:
                raise EODataDownException("Could not create 'South_Lat' field in output vector lyr.")

            east_field_defn = ogr.FieldDefn("East_Lon", ogr.OFTReal)
            if out_vec_lyr.CreateField(east_field_defn) != 0:
                raise EODataDownException("Could not create 'East_Lon' field in output vector lyr.")

            west_field_defn = ogr.FieldDefn("West_Lon", ogr.OFTReal)
            if out_vec_lyr.CreateField(west_field_defn) != 0:
                raise EODataDownException("Could not create 'West_Lon' field in output vector lyr.")

            # Get the output Layer's Feature Definition
            feature_defn = out_vec_lyr.GetLayerDefn()

            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()

            query_obj = sqlalchemy.select([self.generic_table])
            query_rtn = ses.execute(query_obj).fetchall()

            if len(query_rtn) > 0:
                for record in query_rtn:
                    geo_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                    geo_bbox.setBBOX(record.North_Lat, record.South_Lat, record.West_Lon, record.East_Lon)
                    bboxs = geo_bbox.getGeoBBoxsCut4LatLonBounds()

                    for bbox in bboxs:
                        poly = bbox.getOGRPolygon()
                        # Add to output shapefile.
                        out_feat = ogr.Feature(feature_defn)
                        out_feat.SetField("BaseName", record.Base_Name)
                        out_feat.SetField("Sensor", record.Sensor)
                        out_feat.SetField("Source", record.Source)
                        out_feat.SetField("Date", record.Source_Date.strftime('%Y-%m-%d'))
                        out_feat.SetField("DownFile", record.Download_Path)
                        if record.ARDProduct:
                            out_feat.SetField("ARDFile", record.ARDProduct_Path)
                        else:
                            out_feat.SetField("ARDFile", "")
                        out_feat.SetField("North_Lat", record.North_Lat)
                        out_feat.SetField("South_Lat", record.South_Lat)
                        out_feat.SetField("East_Lon", record.East_Lon)
                        out_feat.SetField("West_Lon", record.West_Lon)
                        out_feat.SetGeometry(poly)
                        out_vec_lyr.CreateFeature(out_feat)
                        out_feat = None
            out_vec_lyr = None
            out_data_source = None
        except Exception as e:
            raise e

    def reset_scn(self, unq_id, reset_download=False, reset_invalid=False):
        """
        A function which resets an image. This means any downloads and products are deleted
        and the database fields are reset to defaults. This allows the scene to be re-downloaded
        and processed.
        :param unq_id: unique id for the scene to be reset.
        """
        raise EODataDownException("Not Implemented")

    def reset_dc_load(self, unq_id):
        """
        A function which resets whether an image has been loaded into a datacube
        (i.e., sets the flag to False).
        :param unq_id: unique id for the scene to be reset.
        """
        raise EODataDownException("Not Implemented")
