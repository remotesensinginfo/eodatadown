#!/usr/bin/env python
"""
EODataDown - a sensor class for Rapideye data downloaded from Planet.
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
# Purpose:  Provides a sensor class for Rapideye data downloaded from Planet.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 11/10/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import requests
import os
import os.path
import datetime
import multiprocessing
import rsgislib
import shutil
import xml.etree.ElementTree as ET

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB
from eodatadown.eodatadownrapideye import PlanetImageDownloadReference
import eodatadown.eodatadownrunarcsi

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDPlanetScope(Base):
    __tablename__ = "EDDPlanetScope"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Scene_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False, unique=True)
    Satellite_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Strip_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Grid_Cell = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Item_Type = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Provider = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    File_Identifier = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Acquired = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Published = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Updated = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Anomalous_Pixels = sqlalchemy.Column(sqlalchemy.Float, nullable=False, default=0.0)
    Black_Fill = sqlalchemy.Column(sqlalchemy.Float, nullable=False, default=0.0)
    Usable_Data = sqlalchemy.Column(sqlalchemy.Float, nullable=False, default=1.0)
    Cloud_Cover = sqlalchemy.Column(sqlalchemy.Float, nullable=False, default=0.0)
    EPSG_Code = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    Ground_Control = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    GSD = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Origin_X = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Origin_Y = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Pixel_Res = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Sun_Azimuth = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Sun_Elevation = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    View_Angle = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=False)
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


def _getPSIdentifierFileSize(xml_file):
    """

    :param xml_file:
    :return:
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    schemaURL = root.attrib['{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'].strip().split()[0]
    planetscopeUrl = '{' + schemaURL + '}'
    metaDataProperty = root.find('{http://www.opengis.net/gml}metaDataProperty')
    eoMetaData = metaDataProperty.find(planetscopeUrl + 'EarthObservationMetaData')
    scn_identifier = eoMetaData.find('{http://earth.esa.int/eop}identifier').text.strip()
    productInfo = root.find('{http://www.opengis.net/gml}resultOf').find(planetscopeUrl + 'EarthObservationResult').find('{http://earth.esa.int/eop}product').find(planetscopeUrl + 'ProductInformation')
    fileName = productInfo.find('{http://earth.esa.int/eop}fileName').text.strip()
    return scn_identifier, fileName

def _download_scn_planet(params):
    """

    :param params:
    :return:
    """
    scene_id = params[0]
    analytic_img_dwn_url = params[1]
    analytic_img_md5 = params[2]
    analytic_xml_dwn_url = params[3]
    analytic_xml_md5 = params[4]
    dbInfoObj = params[5]
    lcl_dwnld_path = params[6]
    planetAPIKey = params[7]

    logger.info("Downloading " + scene_id)
    start_date = datetime.datetime.now()
    eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()
    xml_file_out = os.path.join(lcl_dwnld_path, scene_id + ".xml")
    success_xml = eodd_http_downloader.downloadFile(analytic_xml_dwn_url, analytic_xml_md5, xml_file_out, planetAPIKey, "")
    if success_xml:
        scn_identifier, tif_file_name = _getPSIdentifierFileSize(xml_file_out)
        xml_file_out_tmp = xml_file_out
        xml_file_out = os.path.join(lcl_dwnld_path, scn_identifier + "_metadata.xml")
        os.rename(xml_file_out_tmp, xml_file_out)

        img_file_out = os.path.join(lcl_dwnld_path, scn_identifier + ".tif")
        success_img = eodd_http_downloader.downloadFile(analytic_img_dwn_url, analytic_img_md5, img_file_out, planetAPIKey, "")
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading " + scene_id)

    if success_img and success_xml:
        logger.debug("Set up database connection and update record.")
        dbEng = sqlalchemy.create_engine(dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()
        query_result = ses.query(EDDPlanetScope).filter(EDDPlanetScope.Scene_ID == scene_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + scene_id)
        query_result.Downloaded = True
        query_result.Download_Start_Date = start_date
        query_result.Download_End_Date = end_date
        query_result.Download_Path = lcl_dwnld_path
        query_result.File_Identifier = scn_identifier
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")

def _process_to_ard(params):
    """
    A function which is used with the python multiprocessing pool feature to convert a scene to an ARD product
    using multiple processing cores.
    :param params:
    :return:
    """
    scene_id = params[0]
    file_identifier = params[1]
    dbInfoObj = params[2]
    scn_path = params[3]
    output_dir = params[4]
    tmp_dir = params[5]
    final_ard_path = params[6]
    reproj_outputs = params[7]
    proj_wkt_file = params[8]
    projabbv = params[9]

    try:
        eddUtils = eodatadown.eodatadownutils.EODataDownUtils()
        input_xml = eddUtils.findFile(scn_path, file_identifier+"*metadata.xml")

        start_date = datetime.datetime.now()
        eodatadown.eodatadownrunarcsi.run_arcsi_planetscope(input_xml, output_dir, tmp_dir, reproj_outputs, proj_wkt_file, projabbv)

        logger.debug("Move final ARD files to specified location.")
        # Move ARD files to be kept.
        eodatadown.eodatadownrunarcsi.move_arcsi_dos_products(output_dir, final_ard_path)
        # Remove Remaining files.
        shutil.rmtree(output_dir)
        shutil.rmtree(tmp_dir)
        logger.debug("Moved final ARD files to specified location.")
        end_date = datetime.datetime.now()

        logger.debug("Set up database connection and update record.")
        dbEng = sqlalchemy.create_engine(dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()
        query_result = ses.query(EDDPlanetScope).filter(EDDPlanetScope.Scene_ID == scene_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + scene_id)
            raise EODataDownException("Could not find the scene within local database: " + scene_id)
        query_result.ARDProduct = True
        query_result.ARDProduct_Start_Date = start_date
        query_result.ARDProduct_End_Date = end_date
        query_result.ARDProduct_Path = final_ard_path
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")
    except Exception as e:
        logger.error(e.__str__())


class EODataDownPlanetScopeSensor (EODataDownSensor):
    """
    A class which represents a the Rapideye sensor being downloaded via the Planet API.
    """

    def __init__(self, db_info_obj):
        """
        Function to initial the sensor.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        EODataDownSensor.__init__(self, db_info_obj)
        self.sensorName = "PlanetScope"
        self.dbTabName = "EDDPlanetScope"

    def parse_sensor_config(self, config_file, first_parse=False):
        """
        Parse the JSON configuration file. If first_parse=True then a signature file will be created
        which will be checked each time the system runs to ensure changes are not back to the
        configuration file. If the signature does not match the input file then an expection will be
        thrown. To update the configuration (e.g., extent date range or spatial area) run with first_parse=True.
        :param config_file: string with the path to the JSON file.
        :param first_parse: boolean as to whether the file has been previously parsed.
        """
        eddFileChecker = eodatadown.eodatadownutils.EDDCheckFileHash()
        # If it is the first time the config_file is parsed then create the signature file.
        if first_parse:
            eddFileChecker.createFileSig(config_file)
            logger.debug("Created signature file for config file.")

        if not eddFileChecker.checkFileSig(config_file):
            raise EODataDownException("Input config did not match the file signature.")

        with open(config_file) as f:
            config_data = json.load(f)
            json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
            logger.debug("Testing config file is for 'PlanetScope'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensorName])
            logger.debug("Have the correct config file for 'PlanetScope'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.projEPSG = -1
            self.projabbv = ""
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data,
                                                              ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data,
                                                                      ["eodatadown", "sensor", "ardparams", "proj",
                                                                       "epsg"], 0, 1000000000))
            logger.debug("Found ARD processing params from config file")

            logger.debug("Find paths from config file")
            self.baseDownloadPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "download"])
            self.ardProdWorkPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardwork"])
            self.ardFinalPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardfinal"])
            self.ardProdTmpPath = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "paths", "ardtmp"])
            logger.debug("Found paths from config file")

            logger.debug("Find search params from config file")
            geo_bounds_lst = json_parse_helper.getListValue(config_data, ["eodatadown", "sensor", "download", "geobounds"])
            if not len(geo_bounds_lst) > 0:
                raise EODataDownException("There must be at least 1 geographic boundary given.")

            self.geoBounds = []
            for geo_bound_json in geo_bounds_lst:
                edd_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                edd_bbox.setNorthLat(json_parse_helper.getNumericValue(geo_bound_json, ["north_lat"], -90, 90))
                edd_bbox.setSouthLat(json_parse_helper.getNumericValue(geo_bound_json, ["south_lat"], -90, 90))
                edd_bbox.setWestLon(json_parse_helper.getNumericValue(geo_bound_json, ["west_lon"], -180, 180))
                edd_bbox.setEastLon(json_parse_helper.getNumericValue(geo_bound_json, ["east_lon"], -180, 180))
                self.geoBounds.append(edd_bbox)
            self.cloudCoverThres = int(json_parse_helper.getNumericValue(config_data, ["eodatadown", "sensor", "download", "cloudcover"], 0, 100))
            self.startDate = json_parse_helper.getDateTimeValue(config_data, ["eodatadown", "sensor", "download", "startdate"], "%Y-%m-%d")
            logger.debug("Found search params from config file")

            logger.debug("Find Planet Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.planetAPIKey = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "planetaccount", "apikey"]))
            logger.debug("Found Planet Account params from config file")

    def init_sensor_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(dbEng)

        logger.debug("Creating EDDPlanetScope Database.")
        Base.metadata.bind = dbEng
        Base.metadata.create_all()

    def check_new_scns(self):
        """
        Check whether there is new data available which is not within the existing database.
        Scenes not within the database will be added.
        """
        import planet.api
        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()

        logger.debug(
            "Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if ses.query(EDDPlanetScope).first() is not None:
            query_date = ses.query(EDDPlanetScope).order_by(EDDPlanetScope.Acquired.desc()).first().Acquired
        logger.info("Query with start at date: " + str(query_date))

        os.environ['PL_API_KEY'] = self.planetAPIKey
        client = planet.api.ClientV1()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        new_scns_avail = False
        for geo_bound in self.geoBounds:
            roi_json = geo_bound.getGeoJSONPolygon()
            roi_json_str = geo_bound.getGeoJSONPolygonStr()
            logger.info("Checking for available scenes for \"" + roi_json_str + "\"")

            query = planet.api.filters.and_filter(planet.api.filters.geom_filter(roi_json), planet.api.filters.date_range('acquired', gt=query_date), planet.api.filters.range_filter('cloud_cover', lt=(float(self.cloudCoverThres) / 100.0)))#, planet.api.filters.permission_filter('assets:download'))

            request = planet.api.filters.build_search_request(query, ["PSOrthoTile"])
            results = client.quick_search(request)

            db_records = []
            # items_iter returns an iterator over API response pages
            for record in results.items_iter(limit=1000000000):
                if json_parse_helper.doesPathExist(record, ["id"]):
                    scene_id = json_parse_helper.getStrValue(record, ["id"])
                    query_rtn = ses.query(EDDPlanetScope).filter(
                        EDDPlanetScope.Scene_ID == scene_id).one_or_none()
                    if query_rtn is None:
                        satellite_id = json_parse_helper.getStrValue(record, ["properties", "satellite_id"])
                        strip_id = json_parse_helper.getStrValue(record, ["properties", "strip_id"])
                        grid_cell = json_parse_helper.getStrValue(record, ["properties", "grid_cell"])
                        item_type = json_parse_helper.getStrValue(record, ["properties", "item_type"])
                        provider = json_parse_helper.getStrValue(record, ["properties", "provider"])
                        acquired_str = json_parse_helper.getStrValue(record, ["properties", "acquired"]).replace('Z','')[:-1]
                        acquired = datetime.datetime.strptime(acquired_str, "%Y-%m-%dT%H:%M:%S.%f")
                        published_str = json_parse_helper.getStrValue(record, ["properties", "published"]).replace('Z','')[:-1]
                        published = datetime.datetime.strptime(published_str, "%Y-%m-%dT%H:%M:%S")
                        updated_str = json_parse_helper.getStrValue(record, ["properties", "updated"]).replace('Z', '')[:-1]
                        updated = datetime.datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S")
                        anomalous_pixels = json_parse_helper.getNumericValue(record, ["properties", "anomalous_pixels"])
                        black_fill = json_parse_helper.getNumericValue(record, ["properties", "black_fill"])
                        usable_data = json_parse_helper.getNumericValue(record, ["properties", "usable_data"])
                        cloud_cover = json_parse_helper.getNumericValue(record, ["properties", "cloud_cover"])
                        epsg_code = int(json_parse_helper.getNumericValue(record, ["properties", "epsg_code"]))
                        ground_control = json_parse_helper.getBooleanValue(record, ["properties", "ground_control"])
                        gsd = json_parse_helper.getNumericValue(record, ["properties", "gsd"])
                        origin_x = json_parse_helper.getNumericValue(record, ["properties", "origin_x"])
                        origin_y = json_parse_helper.getNumericValue(record, ["properties", "origin_y"])
                        pixel_res = json_parse_helper.getNumericValue(record, ["properties", "pixel_resolution"])
                        sun_azimuth = json_parse_helper.getNumericValue(record, ["properties", "sun_azimuth"])
                        sun_elevation = json_parse_helper.getNumericValue(record, ["properties", "sun_elevation"])
                        view_angle = json_parse_helper.getNumericValue(record, ["properties", "view_angle"])
                        geom_coords_str = json_parse_helper.getStrValue(record, ["geometry"])
                        edd_bbox = eodatadown.eodatadownutils.EDDGeoBBox()
                        edd_bbox.parseGeoJSONPolygon(geom_coords_str)
                        north_lat = edd_bbox.getNorthLat()
                        south_lat = edd_bbox.getSouthLat()
                        east_lon = edd_bbox.getEastLon()
                        west_lon = edd_bbox.getWestLon()
                        remote_url = json_parse_helper.getStrValue(record, ["_links", "assets"])

                        db_records.append(
                            EDDPlanetScope(Scene_ID=scene_id, Satellite_ID=satellite_id,
                                              Strip_ID=strip_id, Grid_Cell=grid_cell, Item_Type=item_type,
                                              Provider=provider,
                                              Acquired=acquired, Published=published, Updated=updated,
                                              Anomalous_Pixels=anomalous_pixels, Black_Fill=black_fill,
                                              Usable_Data=usable_data, Cloud_Cover=cloud_cover, EPSG_Code=epsg_code,
                                              Ground_Control=ground_control, GSD=gsd, Origin_X=origin_x,
                                              Origin_Y=origin_y,
                                              Pixel_Res=pixel_res, Sun_Azimuth=sun_azimuth, Sun_Elevation=sun_elevation,
                                              View_Angle=view_angle, North_Lat=north_lat, South_Lat=south_lat,
                                              East_Lon=east_lon, West_Lon=west_lon, Remote_URL=remote_url,
                                              Query_Date=datetime.datetime.now(), Download_Start_Date=None,
                                              Download_End_Date=None, Downloaded=False, Download_Path="",
                                              ARDProduct_Start_Date=None, ARDProduct_End_Date=None, ARDProduct=False,
                                              ARDProduct_Path=""))

            if len(db_records) > 0:
                ses.add_all(db_records)
                ses.commit()
                new_scns_avail = True
            logger.info("Found and added to database for available scenes for \"" + roi_json_str + "\"")

        ses.close()
        logger.debug("Closed Database session")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked for availability of new scenes", sensor_val=self.sensorName,
                              updated_lcl_db=True, scns_avail=new_scns_avail)

    def parse_http_download_response_json(self, http_json):
        """
        A function which parses the Planet API's JSON HTTP response.
        :param http_json:
        :return:
        """
        dwnld_img_obj = PlanetImageDownloadReference()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        analytic_status = json_parse_helper.getStrValue(http_json, ["analytic", "status"], ["active", "inactive", "activating"])
        analytic_xml_status = json_parse_helper.getStrValue(http_json, ["analytic_xml", "status"], ["active", "inactive", "activating"])
        if analytic_status == "inactive" or analytic_xml_status == "inactive":
            logger.info("Download is inactive and needs to be activated.")
            dwnld_img_obj.activated = "inactive"
            dwnld_img_obj.analytic_img_act_url = json_parse_helper.getStrValue(http_json, ["analytic", "_links", "activate"])
            dwnld_img_obj.analytic_xml_act_url = json_parse_helper.getStrValue(http_json, ["analytic_xml", "_links", "activate"])
        elif analytic_status == "activating" or analytic_xml_status == "activating":
            logger.info("Download is activating.")
            dwnld_img_obj.activated = "activating"
        else:
            logger.info("Download is activate.")
            dwnld_img_obj.activated = "active"
            dwnld_img_obj.analytic_img_dwn_url = json_parse_helper.getStrValue(http_json,["analytic", "location"])
            dwnld_img_obj.analytic_img_md5 = json_parse_helper.getStrValue(http_json,["analytic", "md5_digest"])
            dwnld_img_obj.analytic_xml_dwn_url = json_parse_helper.getStrValue(http_json,["analytic_xml", "location"])
            dwnld_img_obj.analytic_xml_md5 = json_parse_helper.getStrValue(http_json, ["analytic_xml", "md5_digest"])

        analytic_img_dwn_perm_str_arr = json_parse_helper.getStrListValue(http_json, ["analytic", "_permissions"])
        if "download" in analytic_img_dwn_perm_str_arr:
            logger.debug("Permission to download image is available.")
            dwnld_img_obj.analytic_img_dwn_perm = True
        else:
            logger.debug("Permission for image file not available to download")
            dwnld_img_obj.analytic_img_dwn_perm = False

        analytic_xml_dwn_perm_str_arr = json_parse_helper.getStrListValue(http_json, ["analytic_xml", "_permissions"])
        if "download" in analytic_xml_dwn_perm_str_arr:
            logger.debug("Permission to download xml is available.")
            dwnld_img_obj.analytic_xml_dwn_perm = True
        else:
            logger.debug("Permission for XML Header not available to download")
            dwnld_img_obj.analytic_xml_dwn_perm = False
        return dwnld_img_obj

    def get_scnlist_download(self):
        """
        A function which queries the database to retrieve a list of scenes which are within the
        database but have yet to be downloaded.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to download.
        """
        raise EODataDownException("Not implemented.")

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

        logger.debug("Creating HTTP Session Object.")
        session = requests.Session()
        session.auth = (self.planetAPIKey, "")
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session.headers["User-Agent"] = user_agent

        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()
        downloaded_new_scns = False

        logger.debug("Perform query to find scenes which need downloading.")
        query_result = ses.query(EDDPlanetScope).filter(EDDPlanetScope.Downloaded == False).all()
        eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()

        dwnld_params = []
        if query_result is not None:
            logger.debug("Create the output directory for this download.")
            dt_obj = datetime.datetime.now()

            for record in query_result:
                logger.debug("Testing if available to download : "+ record.Scene_ID)

                http_resp = session.get(record.Remote_URL)
                eodd_http_downloader.checkResponse(http_resp, record.Remote_URL)
                dwnld_obj = self.parse_http_download_response_json(http_resp.json())

                if (not dwnld_obj.analytic_img_dwn_perm) or (not dwnld_obj.analytic_xml_dwn_perm):
                    logger.debug("Do not have permission to download scene : " + record.Scene_ID)
                    raise EODataDownException("Permission to download is not available for image/xml file(s) for scene: {}".format(record.Scene_ID))

                if dwnld_obj.activated == "inactive":
                    act_img_http_resp = session.get(dwnld_obj.analytic_img_act_url)
                    eodd_http_downloader.checkResponse(act_img_http_resp, dwnld_obj.analytic_img_act_url)
                    act_xml_http_resp = session.get(dwnld_obj.analytic_xml_act_url)
                    eodd_http_downloader.checkResponse(act_xml_http_resp, dwnld_obj.analytic_xml_act_url)
                elif dwnld_obj.activated == "active":
                    lcl_dwnld_scn_path = os.path.join(self.baseDownloadPath, record.Scene_ID)
                    if not os.path.exists(lcl_dwnld_scn_path):
                        os.mkdir(lcl_dwnld_scn_path)
                    downloaded_new_scns = True
                    dwnld_params.append([record.Scene_ID, dwnld_obj.analytic_img_dwn_url, dwnld_obj.analytic_img_md5, dwnld_obj.analytic_xml_dwn_url, dwnld_obj.analytic_xml_md5, self.dbInfoObj, lcl_dwnld_scn_path, self.planetAPIKey])
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=n_cores) as pool:
            pool.map(_download_scn_planet, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked downloaded new scenes.", sensor_val=self.sensorName, updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)


    def get_scnlist_con2ard(self):
        """
        A function which queries the database to find scenes which have been downloaded but have not yet been
        processed to an analysis ready data (ARD) format.
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to process.
        """
        raise EODataDownException("Not implemented.")

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
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()

        logger.debug("Perform query to find scenes which need converting to ARD.")
        query_result = ses.query(EDDPlanetScope).filter(EDDPlanetScope.Downloaded == True, EDDPlanetScope.ARDProduct == False).all()

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

            ard_params = []
            for record in query_result:
                logger.debug("Create info for running ARD analysis for scene: " + record.Scene_ID)
                final_ard_scn_path = os.path.join(self.ardFinalPath, record.Scene_ID)
                if not os.path.exists(final_ard_scn_path):
                    os.mkdir(final_ard_scn_path)

                work_ard_scn_path = os.path.join(work_ard_path, record.Scene_ID)
                if not os.path.exists(work_ard_scn_path):
                    os.mkdir(work_ard_scn_path)

                tmp_ard_scn_path = os.path.join(tmp_ard_path, record.Scene_ID)
                if not os.path.exists(tmp_ard_scn_path):
                    os.mkdir(tmp_ard_scn_path)

                if self.ardProjDefined:
                    proj_wkt_file = os.path.join(work_ard_scn_path, record.Scene_ID+"_wkt.wkt")
                    rsgis_utils.writeList2File([proj_wkt], proj_wkt_file)

                ard_params.append([record.Scene_ID, record.File_Identifier, self.dbInfoObj, record.Download_Path, work_ard_scn_path, tmp_ard_scn_path, final_ard_scn_path, self.ardProjDefined, proj_wkt_file, self.projabbv])
        else:
            logger.info("There are no scenes which have been downloaded but not processed to an ARD product.")
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start processing the scenes.")
        if len(ard_params) > 0:
            with multiprocessing.Pool(processes=n_cores) as pool:
                pool.map(_process_to_ard, ard_params)
        logger.info("Finished processing the scenes.")

        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Processed scenes to an ARD product.", sensor_val=self.sensorName, updated_lcl_db=True, convert_scns_ard=True)

    def get_scnlist_add2datacube(self):
        """
        A function which queries the database to find scenes which have been processed to an ARD format
        but have not yet been loaded into the system datacube (specifed in the configuration file).
        :return: A list of unq_ids for the scenes. The list will be empty if there are no scenes to be loaded.
        """
        raise EODataDownException("Not implemented.")

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

    def get_scn_record(self, unq_id):
        """
        A function which queries the database using the unique ID of a scene returning the record
        :param unq_id:
        :return: Returns the database record object
        """
        raise EODataDownException("Not implemented.")

    def query_scn_records_date(self, start_date, end_date):
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

    def export2db(self, db_info_obj):
        """
        This function exports the existing database to the database specified by the
        input database info object.
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        raise EODataDownException("Not implemented.")

    def import_append_db(self, db_info_obj):
        """
        This function imports from the database specified by the input database info object
        and appends the data to the exisitng database. This might be used if data was processed
        on another system (e.g., HPC cluster).
        :param db_info_obj: Instance of a EODataDownDatabaseInfo object
        """
        raise EODataDownException("Not implemented.")

    def create_gdal_gis_lyr(self, file_path, lyr_name, driver_name='SQLite', add_lyr=False):
        """
        A function to export the outlines and some attributes to a GDAL vector layer.
        :param file_path: path to the output file.
        :param lyr_name: the name of the layer within the output file.
        :param driver_name: name of the gdal driver
        :param add_lyr: add the layer to the file
        """
        raise EODataDownException("Not Implemented")
