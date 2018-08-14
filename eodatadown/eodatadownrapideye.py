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
# Date: 12/08/2018
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
import pprint

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownutils import EODataDownResponseException
from eodatadown.eodatadownsensor import EODataDownSensor
from eodatadown.eodatadownusagedb import EODataDownUpdateUsageLogDB

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy

import planet.api

logger = logging.getLogger(__name__)

Base = declarative_base()

class EDDRapideyePlanet(Base):
    __tablename__ = "EDDRapideyePlanet"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Scene_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Catalog_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Satellite_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Strip_ID = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Grid_Cell = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Item_Type = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Provider = sqlalchemy.Column(sqlalchemy.String, nullable=False)
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
    ARDProduct_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")

class PlanetImageDownloadReference(object):

    def __init__(self):
        self.analytic_img_dwn_url = ""
        self.analytic_img_md5 = ""
        self.analytic_img_dwn_perm = False
        self.analytic_xml_dwn_url = ""
        self.analytic_xml_md5 = ""
        self.analytic_xml_dwn_perm = False
        self.analytic_img_act_url = ""
        self.analytic_xml_act_url = ""
        self.activated = "inactive"


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

    print(analytic_img_dwn_url)
    print(analytic_xml_dwn_url)

    img_file_out = os.path.join(lcl_dwnld_path, "RE_" + scene_id + ".tif")
    xml_file_out = os.path.join(lcl_dwnld_path, "RE_" + scene_id + ".xml")

    logger.info("Downloading " + scene_id)
    start_date = datetime.datetime.now()
    eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()
    success_img = eodd_http_downloader.downloadFile(analytic_img_dwn_url, analytic_img_md5, img_file_out, planetAPIKey, "")
    success_xml = eodd_http_downloader.downloadFile(analytic_xml_dwn_url, analytic_xml_md5, xml_file_out, planetAPIKey, "")
    end_date = datetime.datetime.now()
    logger.info("Finished Downloading " + scene_id)

    if success_img and success_xml:
        logger.debug("Set up database connection and update record.")
        dbEng = sqlalchemy.create_engine(dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()
        query_result = ses.query(EDDRapideyePlanet).filter(EDDRapideyePlanet.Scene_ID == scene_id).one_or_none()
        if query_result is None:
            logger.error("Could not find the scene within local database: " + scene_id)
        query_result.Downloaded = True
        query_result.Download_Start_Date = start_date
        query_result.Download_End_Date = end_date
        query_result.Download_Path = lcl_dwnld_path
        ses.commit()
        ses.close()
        logger.debug("Finished download and updated database.")


class EODataDownRapideyeSensor (EODataDownSensor):
    """
    A class which represents a the Rapideye sensor being downloaded via the Planet API.
    """

    def __init__(self, dbInfoObj):
        EODataDownSensor.__init__(self, dbInfoObj)
        self.sensorName = "RapideyePlanet"

    def getSensorName(self):
        return self.sensorName

    def parseSensorConfig(self, config_file, first_parse=False):
        """
        A function to parse the RapideyePlanet JSON config file.
        :param config_file: 
        :param first_parse: 
        :return: 
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
            logger.debug("Testing config file is for 'RapideyePlanet'")
            json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "name"], [self.sensorName])
            logger.debug("Have the correct config file for 'RapideyePlanet'")

            logger.debug("Find ARD processing params from config file")
            self.demFile = json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "ardparams", "dem"])
            self.projEPSG = -1
            self.projabbv = ""
            self.ardProjDefined = False
            if json_parse_helper.doesPathExist(config_data, ["eodatadown", "sensor", "ardparams", "proj"]):
                self.ardProjDefined = True
                self.projabbv = json_parse_helper.getStrValue(config_data,  ["eodatadown", "sensor", "ardparams", "proj", "projabbv"])
                self.projEPSG = int(json_parse_helper.getNumericValue(config_data, ["eodatadown", "sensor", "ardparams", "proj",  "epsg"], 0, 1000000000))
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

            logger.debug("Find ESA Account params from config file")
            edd_pass_encoder = eodatadown.eodatadownutils.EDDPasswordTools()
            self.planetAPIKey = edd_pass_encoder.unencodePassword(json_parse_helper.getStrValue(config_data, ["eodatadown", "sensor", "planetaccount", "apikey"]))
            logger.debug("Found ESA Account params from config file")

    def initSensorDB(self):
        """
        Initialise the sensor database table.
        :return:
        """
        logger.debug("Creating Database Engine.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(dbEng)

        logger.debug("Creating EDDRapideyePlanet Database.")
        Base.metadata.bind = dbEng
        Base.metadata.create_all()

    def check4NewData(self):
        """
        A function which queries the planet API to find if scenes are available.
        :return:
        """
        logger.debug("Creating Database Engine and Session.")
        dbEng = sqlalchemy.create_engine(self.dbInfoObj.dbConn)
        Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
        ses = Session()

        logger.debug("Find the start date for query - if table is empty then using config date otherwise date of last acquried image.")
        query_date = self.startDate
        if ses.query(EDDRapideyePlanet).first() is not None:
            query_date = ses.query(EDDRapideyePlanet).order_by(EDDRapideyePlanet.Acquired.desc()).first().Acquired
        logger.info("Query with start at date: " + str(query_date))

        os.environ['PL_API_KEY'] = self.planetAPIKey
        client = planet.api.ClientV1()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        new_scns_avail = False
        for geo_bound in self.geoBounds:
            roi_json = geo_bound.getGeoJSONPolygon()
            roi_json_str = geo_bound.getGeoJSONPolygonStr()
            logger.info("Checking for available scenes for \"" + roi_json_str + "\"")

            query = planet.api.filters.and_filter(planet.api.filters.geom_filter(roi_json),
                                                  planet.api.filters.date_range('acquired', gt=query_date),
                                                  planet.api.filters.range_filter('cloud_cover', lt=(float(self.cloudCoverThres)/100.0)),
                                                  planet.api.filters.permission_filter('assets:download'))

            request = planet.api.filters.build_search_request(query, ["REOrthoTile"])
            results = client.quick_search(request)

            db_records = []
            # items_iter returns an iterator over API response pages
            for record in results.items_iter(limit=1000000000):
                if json_parse_helper.doesPathExist(record, ["id"]):
                    scene_id = json_parse_helper.getStrValue(record, ["id"])
                    query_rtn = ses.query(EDDRapideyePlanet).filter(EDDRapideyePlanet.Scene_ID == scene_id).one_or_none()
                    if query_rtn is None:
                        catalog_id = json_parse_helper.getStrValue(record, ["properties", "catalog_id"])
                        satellite_id = json_parse_helper.getStrValue(record, ["properties", "satellite_id"])
                        strip_id  = json_parse_helper.getStrValue(record, ["properties", "strip_id"])
                        grid_cell = json_parse_helper.getStrValue(record, ["properties", "grid_cell"])
                        item_type = json_parse_helper.getStrValue(record, ["properties", "item_type"])
                        provider = json_parse_helper.getStrValue(record, ["properties", "provider"])
                        acquired_str = json_parse_helper.getStrValue(record, ["properties", "acquired"]).replace('Z', '')[:-1]
                        acquired = datetime.datetime.strptime(acquired_str, "%Y-%m-%dT%H:%M:%S")
                        published_str = json_parse_helper.getStrValue(record, ["properties", "published"]).replace('Z', '')[:-1]
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
                            EDDRapideyePlanet(Scene_ID=scene_id, Catalog_ID=catalog_id, Satellite_ID=satellite_id,
                                              Strip_ID=strip_id, Grid_Cell=grid_cell, Item_Type=item_type, Provider=provider,
                                              Acquired=acquired, Published=published, Updated=updated,
                                              Anomalous_Pixels=anomalous_pixels, Black_Fill=black_fill,
                                              Usable_Data=usable_data, Cloud_Cover=cloud_cover, EPSG_Code=epsg_code,
                                              Ground_Control=ground_control, GSD=gsd, Origin_X=origin_x, Origin_Y=origin_y,
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
        edd_usage_db.addEntry(description_val="Checked for availability of new scenes", sensor_val=self.sensorName, updated_lcl_db=True, scns_avail=new_scns_avail)


    def parseHTTPDownloadResponseJSON(self, http_json):
        """

        :param http_json:
        :return:
        """
        dwnld_img_obj = PlanetImageDownloadReference()
        json_parse_helper = eodatadown.eodatadownutils.EDDJSONParseHelper()
        analytic_status = json_parse_helper.getStrValue(http_json, ["analytic", "status"], ["active", "inactive", "activating"])
        analytic_xml_status = json_parse_helper.getStrValue(http_json, ["analytic_xml", "status"], ["active", "inactive", "activating"])
        if analytic_status == "inactive" or analytic_xml_status == "inactive":
            logger.debug("Download is inactive and needs to be activated.")
            dwnld_img_obj.activated = "inactive"
            dwnld_img_obj.analytic_img_act_url = json_parse_helper.getStrValue(http_json, ["analytic", "_links", "activate"])
            dwnld_img_obj.analytic_xml_act_url = json_parse_helper.getStrValue(http_json, ["analytic_xml", "_links", "activate"])
        elif analytic_status == "activating" or analytic_xml_status == "activating":
            logger.debug("Download is activating.")
            dwnld_img_obj.activated = "activating"
        else:
            logger.debug("Download is activate.")
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

    def downloadNewData(self, ncores):
        """
        A function which downloads the scenes which are within the database but not downloaded.
        :param ncores:
        :return:
        """
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
        query_result = ses.query(EDDRapideyePlanet).filter(EDDRapideyePlanet.Downloaded == False).all()
        dwnld_params = []
        eodd_http_downloader = eodatadown.eodatadownutils.EDDHTTPDownload()

        if query_result is not None:
            logger.debug("Create the output directory for this download.")
            dt_obj = datetime.datetime.now()
            lcl_dwnld_path = os.path.join(self.baseDownloadPath, dt_obj.strftime("%Y-%m-%d"))
            if not os.path.exists(lcl_dwnld_path):
                os.mkdir(lcl_dwnld_path)

            for record in query_result:
                logger.debug("Testing if available to download : "+ record.Scene_ID)
                http_resp = session.get(record.Remote_URL)
                eodd_http_downloader.checkResponse(http_resp, record.Remote_URL)
                dwnld_obj = self.parseHTTPDownloadResponseJSON(http_resp.json())

                if (not dwnld_obj.analytic_img_dwn_perm) or (not dwnld_obj.analytic_xml_dwn_perm):
                    logger.debug("Do not have permission to download scene : " + record.Scene_ID)
                    raise EODataDownException("Permission to download is not available for image/xml file(s) for scene: {}".format(record.Scene_ID))

                if dwnld_obj.activated == "inactive":
                    act_img_http_resp = session.get(dwnld_obj.analytic_img_act_url)
                    eodd_http_downloader.checkResponse(act_img_http_resp, dwnld_obj.analytic_img_act_url)
                    act_xml_http_resp = session.get(dwnld_obj.analytic_xml_act_url)
                    eodd_http_downloader.checkResponse(act_xml_http_resp, dwnld_obj.analytic_xml_act_url)
                elif dwnld_obj.activated == "active":
                    lcl_dwnld_scn_path = os.path.join(lcl_dwnld_path, record.Scene_ID)
                    if not os.path.exists(lcl_dwnld_scn_path):
                        os.mkdir(lcl_dwnld_scn_path)
                    downloaded_new_scns = True
                    dwnld_params.append([record.Scene_ID, dwnld_obj.analytic_img_dwn_url, dwnld_obj.analytic_img_md5, dwnld_obj.analytic_xml_dwn_url, dwnld_obj.analytic_xml_md5, self.dbInfoObj, lcl_dwnld_scn_path, self.planetAPIKey])
        ses.close()
        logger.debug("Closed the database session.")

        logger.info("Start downloading the scenes.")
        with multiprocessing.Pool(processes=ncores) as pool:
            pool.map(_download_scn_planet, dwnld_params)
        logger.info("Finished downloading the scenes.")
        edd_usage_db = EODataDownUpdateUsageLogDB(self.dbInfoObj)
        edd_usage_db.addEntry(description_val="Checked downloaded new scenes.", sensor_val=self.sensorName, updated_lcl_db=True, downloaded_new_scns=downloaded_new_scns)

    def convertNewData2ARD(self, ncores):
        """

        :param ncores:
        :return:
        """
        raise EODataDownException("Not Implemented")

