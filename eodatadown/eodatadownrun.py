#!/usr/bin/env python
"""
EODataDown - run the EODataDown System.
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
# Purpose:  Run the EODataDown System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 19/10/2018
# Version: 2.0
#
# History:
# Version 1.0 - Created.
# Version 2.0 - Removed class structure to make it easier to integrate with other systems.

import logging
import json
import multiprocessing

from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils
import eodatadown.eodatadownsystemmain

logger = logging.getLogger(__name__)


def find_new_downloads(config_file, sensors, check_from_start=False):
    """
    A function to run the process of finding new data to download.
    :param config_file: The EODataDown configuration file path.
    :param sensors: list of sensor names.
    :param check_from_start: If True the search from new downloads will be from the configured start date.
                             If False the search will be from the most recent scene in the database.

    """
    logger.info("Running process to find new downloads.")
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Finding Available Downloads.", start_block=True)

    for sensor in sensors:
        sensor_obj = sys_main_obj.get_sensor_obj(sensor)
        sensor_obj.check_new_scns(check_from_start)
        sensor_obj.rm_scns_intersect()

    edd_usage_db.add_entry("Finished: Finding Available Downloads.", end_block=True)


def rm_scn_intersect(config_file, sensors, check_all=False):
    """
    A function which tests whether a scenes bounding box (BBOX) intersects with a
    vector layer provided. If there is no intersection then the scene will be deleted
    from the database. This function only performs an operation if the configure file
    provides a vector layer for the intersection.

    :param config_file: The EODataDown configuration file path.
    :param sensors: list of sensor names.
    :param check_all: If True all scenes within the database will be checked.
                      If False then only those scenes which have not been downloaded
                      will be tested.

    """
    logger.info("Running process to remove scenes which do not intersect with an ROI vector layer.")
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Removing Scenes with No Intersection.", start_block=True)

    for sensor in sensors:
        sensor_obj = sys_main_obj.get_sensor_obj(sensor)
        sensor_obj.rm_scns_intersect(check_all)

    edd_usage_db.add_entry("Finished: Removing Scenes with No Intersection.", end_block=True)


def get_sensor_obj(config_file, sensor):
    """
    A function to get a sensor object.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor
    :return: instance of a EODataDownSensor
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    sensor_obj_to_process = sys_main_obj.get_sensor_obj(sensor)

    return sensor_obj_to_process


def perform_downloads(config_file, n_cores, sensors):
    """
    A function which runs the process of performing the downloads of available scenes
    which have not yet been downloaded.
    :param config_file: The EODataDown configuration file path.
    :param n_cores:
    :param sensors: List of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Downloading Available Scenes.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.download_all_avail(n_cores)
        except Exception as e:
            logger.error("Error occurred while downloading for sensor: "+sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Downloading Available Scenes.", end_block=True)


def perform_scene_download(config_file, sensor, scene_id):
    """
    A function which performs the download for the specified scene.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor.
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Downloading Specified Scene ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        sensor_obj_to_process.download_scn(scene_id)
    except Exception as e:
        logger.error("Error occurred while downloading scene ({0}) for sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Downloading Specified Scene ({0}: {1}).".format(sensor, scene_id), end_block=True)


def process_data_ard(config_file, n_cores, sensors):
    """
    A function which runs the process of converting the downloaded scenes to an ARD product.
    :param config_file: The EODataDown configuration file path.
    :param n_cores:
    :param sensors: List of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Converting Available Scenes to ARD Product.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.scns2ard_all_avail(n_cores)
        except Exception as e:
            logger.error("Error occurred while converting to ARD for sensor: " + sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Converting Available Scenes to ARD Product.", end_block=True)


def process_scene_ard(config_file, sensor, scene_id):
    """
    A function which runs the process of converting the specified scene to an ARD product.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Converting Specified Scene to ARD ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        sensor_obj_to_process.scn2ard(scene_id)
    except Exception as e:
        logger.error("Error occurred while converting scene ({0}) to ARD for sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Converting Specified Scene to ARD ({0}: {1}).".format(sensor, scene_id), end_block=True)


def datacube_load_data(config_file, sensors):
    """

    :param config_file: The EODataDown configuration file path.
    :param sensors: List of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Loading Available Scenes to Data Cube.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.scns2datacube_all_avail()
        except Exception as e:
            logger.error("Error occurred while converting to ARD for sensor: " + sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Loading Available Scenes to Data Cube.", end_block=True)


def datacube_load_scene(config_file, sensor, scene_id):
    """
    A function which runs the process of converting the specified scene to an ARD product.
    :param config_file: The EODataDown configuration file path.
    :param sensor: List of sensor names.
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Load Specified Scene into DataCube ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        sensor_obj_to_process.scn2datacube(scene_id)
    except Exception as e:
        logger.error("Error occurred while loading scene ({0}) into DataCube for sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Load Specified Scene into DataCube ({0}: {1}).".format(sensor, scene_id), end_block=True)


def gen_quicklook_images(config_file, sensors):
    """

    :param config_file: The EODataDown configuration file path.
    :param sensors: list of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Generating Quicklook Image for Available Scenes.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.scns2quicklook_all_avail()
        except Exception as e:
            logger.error("Error occurred while generating quicklook images for sensor: " + sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Generating Quicklook Image for Available Scenes.", end_block=True)


def gen_quicklook_scene(config_file, sensor, scene_id):
    """
    A function which runs the process of generating quicklook image for an input scene.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Generate quicklook image for specified scene ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        sensor_obj_to_process.scn2quicklook(scene_id)
    except Exception as e:
        logger.error("Error occurred while generating quicklook for scene ({0}) from sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Generate quicklook image for specified scene ({0}: {1}).".format(sensor, scene_id), end_block=True)


def gen_tilecache_images(config_file, sensors):
    """

    :param config_file: The EODataDown configuration file path.
    :param sensors: List of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Generating TileCache Image for Available Scenes.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.scns2tilecache_all_avail()
        except Exception as e:
            logger.error("Error occurred while tile cache for sensor: " + sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Generating TileCache Image for Available Scenes.", end_block=True)


def gen_scene_tilecache(config_file, sensor, scene_id):
    """
    A function which runs the process of generating a tilecache for an input scene.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Generate tilecache for specified scene ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        sensor_obj_to_process.scn2tilecache(scene_id)
    except Exception as e:
        logger.error("Error occurred while generating tilecache for scene ({0}) from sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Generate tilecache for specified scene ({0}: {1}).".format(sensor, scene_id), end_block=True)


def run_user_plugins(config_file, sensors):
    """

    :param config_file: The EODataDown configuration file path.
    :param sensors: List of sensor names.
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Running User Plugins for Available Scenes.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_objs_to_process = list()
    for sensor_obj in sensor_objs:
        process_sensor = False
        if sensors is None:
            process_sensor = True
        if sensors is not None:
            if sensor_obj.get_sensor_name() in sensors:
                process_sensor = True
        if process_sensor:
            sensor_objs_to_process.append(sensor_obj)

    for sensor_obj in sensor_objs_to_process:
        try:
            sensor_obj.run_usr_analysis_all_avail(1)
        except Exception as e:
            logger.error("Error occurred while running user plugins for sensor: " + sensor_obj.get_sensor_name())
            logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Running User Plugins for Available Scenes.", end_block=True)


def run_user_plugins_scene(config_file, sensor, scene_id):
    """
    A function which runs the process of generating a tilecache for an input scene.
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor
    :param scene_id:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Running User Plugins for specified scene ({0}: {1}).".format(sensor, scene_id), start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        logger.debug("Testing Sensor Object: '{}'.".format(sensor_obj.get_sensor_name()))
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            logger.debug("Found Sensor Object.")
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    try:
        logger.debug("Going to try running the user analysis plugins for scene '{}'".format(scene_id))
        sensor_obj_to_process.run_usr_analysis(scene_id)
        logger.debug("Finished to try running the user analysis plugins for scene '{}'".format(scene_id))
    except Exception as e:
        logger.error("Error occurred while running user plugins for scene ({0}) from sensor: ({1})".format(
                     scene_id, sensor_obj_to_process.get_sensor_name()))
        logger.debug(e.__str__(), exc_info=True)
    edd_usage_db.add_entry("Finished: Running User Plugins for specified scene ({0}: {1}).".format(sensor, scene_id), end_block=True)


def export_image_footprints_vector(config_file, sensor, table, vector_file, vector_lyr, vector_driver, add_layer):
    """

    :param table:
    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor to be exported.
    :param vector_file:
    :param vector_lyr:
    :param vector_driver:
    :param add_layer:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Export vector footprints.", start_block=True)

    sensor_objs = sys_main_obj.get_sensors()
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            if (table == "") or (table is None) or (sensor_obj.get_db_table_name() == table):
                try:
                    logger.info("Exporting footprints for {}.".format(sensor))
                    sensor_obj.create_gdal_gis_lyr(vector_file, vector_lyr, vector_driver, add_layer)
                    logger.info("Exported footprints for {}.".format(sensor))
                except Exception as e:
                    logger.error("Error occurred while Export vector footprints for sensor: {}".format(
                                sensor_obj.get_sensor_name()))
                    logger.debug(e.__str__(), exc_info=True)
                break
    edd_usage_db.add_entry("Finished: Export vector footprints.", end_block=True)


def export_sensor_database(config_file, sensor, out_json_file):
    """
    A function to export a sensors database table to a JSON file.

    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor to be exported.
    :param out_json_file: the file path of the output JSON file.

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Export {} sensors database table.".format(sensor), start_block=True)

    sensor_obj = sys_main_obj.get_sensor_obj(sensor)
    sensor_obj.export_db_to_json(out_json_file)

    edd_usage_db.add_entry("Finished: Export {} sensors database table.".format(sensor), end_block=True)


def import_sensor_database(config_file, sensor, in_json_file, replace_path_jsonfile):
    """
    A function to import a sensors database table from a JSON file.

    :param config_file: The EODataDown configuration file path.
    :param sensor: the string name of the sensor to be exported.
    :param in_json_file: the file path of the output JSON file.
    :param replace_path_jsonfile: a JSON file with pairs of paths to be updated during import.

    """
    if replace_path_jsonfile is None:
        replace_path_dict = None
    else:
        json_file_obj = open(replace_path_jsonfile)
        replace_path_dict = json.load(json_file_obj)

    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Import {} sensors database table.".format(sensor), start_block=True)

    sensor_obj = sys_main_obj.get_sensor_obj(sensor)
    sensor_obj.import_sensor_db(in_json_file, replace_path_dict)

    edd_usage_db.add_entry("Finished: Import {} sensors database table.".format(sensor), end_block=True)


def export_obsdate_database(config_file, out_json_file):
    """
    A function to export the observation dates database tables to a JSON file.

    :param config_file: The EODataDown configuration file path.
    :param out_json_file: the file path of the output JSON file.

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Export obsdate database table.", start_block=True)

    obsdates_obj = sys_main_obj.get_obsdates_obj()
    if obsdates_obj is None:
        logger.error("Error occurred and the observation date object could not be created.")
    obsdates_obj.export_db_to_json(out_json_file)

    edd_usage_db.add_entry("Finished: Export obsdate database table.", end_block=True)


def import_obsdate_database(config_file, in_json_file, replace_path_jsonfile):
    """
    A function to import the observation dates database table from a JSON file.

    :param config_file: The EODataDown configuration file path.
    :param in_json_file: the file path of the output JSON file.
    :param replace_path_jsonfile: a JSON file with pairs of paths to be updated during import.

    """
    if replace_path_jsonfile is None:
        replace_path_dict = None
    else:
        json_file_obj = open(replace_path_jsonfile)
        replace_path_dict = json.load(json_file_obj)

    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Starting: Import obsdate database tables.", start_block=True)

    obsdates_obj = sys_main_obj.get_obsdates_obj()
    if obsdates_obj is None:
        logger.error("Error occurred and the observation date object could not be created.")
    obsdates_obj.import_obsdates_db(in_json_file, replace_path_dict)

    edd_usage_db.add_entry("Finished: Import obsdate database table.", end_block=True)


def run_scn_analysis(params):
    """
    This function runs a full per scene analysis and will check whether the scene has been: downloaded,
    converted to ARD, quicklook generated and tilecache generated.

    The function takes an array of values as input so it can be used within multiprocessing.Pool.

    :param params: an array with [config_file, scn_sensor, scn_id].

    """
    config_file = params[0]
    scn_sensor = params[1]
    scn_id = params[2]

    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)

    try:
        sensor_obj = sys_main_obj.get_sensor_obj(scn_sensor)
        process_complete = False
        if not sensor_obj.has_scn_download(scn_id):
            logger.debug("Downloading {} scene {}.".format(scn_sensor, scn_id))
            sensor_obj.download_scn(scn_id)
            logger.debug("Downloading complete for {} scene {}.".format(scn_sensor, scn_id))
        if sensor_obj.has_scn_download(scn_id):
            if not sensor_obj.has_scn_con2ard(scn_id):
                logger.debug("Starting conversion to ARD for {} scene {}.".format(scn_sensor, scn_id))
                sensor_obj.scn2ard(scn_id)
                logger.debug("Completed conversion to ARD for {} scene {}.".format(scn_sensor, scn_id))
            if sensor_obj.has_scn_con2ard(scn_id):
                if sensor_obj.calc_scn_quicklook():
                    process_complete = False
                    if not sensor_obj.has_scn_quicklook(scn_id):
                        logger.debug("Starting quicklook generation {} scene {}.".format(scn_sensor, scn_id))
                        sensor_obj.scn2quicklook(scn_id)
                        logger.debug("Completed quicklook generation {} scene {}.".format(scn_sensor, scn_id))
                    if sensor_obj.has_scn_quicklook(scn_id):
                        process_complete = True
                        logger.debug("Processing quicklook complete for {} scene {}.".format(scn_sensor, scn_id))

                if sensor_obj.calc_scn_tilecache():
                    process_complete = False
                    if not sensor_obj.has_scn_tilecache(scn_id):
                        logger.debug("Starting tilecache generation {} scene {}.".format(scn_sensor, scn_id))
                        sensor_obj.scn2tilecache(scn_id)
                        logger.debug("Completed tilecache generation {} scene {}.".format(scn_sensor, scn_id))
                    if sensor_obj.has_scn_tilecache(scn_id):
                        process_complete = True
                        logger.debug("Processing tilecache complete for {} scene {}.".format(scn_sensor, scn_id))

                if sensor_obj.calc_scn_usr_analysis():
                    process_complete = False
                    if not sensor_obj.has_scn_usr_analysis(scn_id):
                        logger.debug("Starting user plugins processing {} scene {}.".format(scn_sensor, scn_id))
                        sensor_obj.run_usr_analysis(scn_id)
                        logger.debug("Completed user plugins processing {} scene {}.".format(scn_sensor, scn_id))
                    if sensor_obj.has_scn_usr_analysis(scn_id):
                        process_complete = True
                        logger.debug("Completed all user analysis plugins for {} scene {}.".format(scn_sensor, scn_id))

                if process_complete:
                    logger.info("All processing steps are complete for {} scene {}.".format(scn_sensor, scn_id))
                else:
                    logger.error("Need to re-run as processing did not complete for {} scene {}.".format(scn_sensor, scn_id))
    except Exception as e:
        # Exceptions are caught so processing is not stopped by an error.
        logger.error("An error has occurred for {} scene {}.\n Exception: {}".format(scn_sensor, scn_id, e), exc_info=True)


def process_scenes_all_steps(config_file, sensors, ncores=1):
    """
    A function which undertakes all the processing steps for all the scenes which haven't yet been undertaken.
    This is per scene processing rather than per step processing in the functions above.

    Steps include:
      * Download
      * ARD Production
      * Generating Tile Cache
      * Generating Quicklook images

    :param config_file: The EODataDown configuration file path.
    :param sensors: list of sensor string names to be processed.
    :param ncores: the number of cores to use for the analysis - one core per scene.

    """
    tasks = get_scenes_need_processing(config_file, sensors)
    if len(tasks) > 0:
        with multiprocessing.Pool(processes=ncores) as pool:
            pool.map(run_scn_analysis, tasks)


def get_scenes_need_processing(config_file, sensors):
    """
    A function which finds all the processing steps for all the scenes which haven't yet been undertaken.
    This is per scene processing rather than per step processing in the functions above.

    Steps include:
      * Download
      * ARD Production
      * Generating Tile Cache
      * Generating Quicklook images

    :param config_file: The EODataDown configuration file path.
    :param sensors: list of sensor string names to be processed.
    :returns: a list of lists where each scn has [config_file, scn_sensor, scn_id]

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)

    tasks = []
    for sensor in sensors:
        sensor_obj = sys_main_obj.get_sensor_obj(sensor)
        scn_ids = []
        scns = sensor_obj.get_scnlist_usr_analysis()
        for scn in scns:
            if scn not in scn_ids:
                tasks.append([config_file, sensor, scn])
                scn_ids.append(scn)

        scns = sensor_obj.get_scnlist_quicklook()
        for scn in scns:
            if scn not in scn_ids:
                tasks.append([config_file, sensor, scn])
                scn_ids.append(scn)

        scns = sensor_obj.get_scnlist_tilecache()
        for scn in scns:
            if scn not in scn_ids:
                tasks.append([config_file, sensor, scn])
                scn_ids.append(scn)

        scns = sensor_obj.get_scnlist_con2ard()
        for scn in scns:
            if scn not in scn_ids:
                tasks.append([config_file, sensor, scn])
                scn_ids.append(scn)

        scns = sensor_obj.get_scnlist_download()
        for scn in scns:
            if scn not in scn_ids:
                tasks.append([config_file, sensor, scn])
                scn_ids.append(scn)
    return tasks


def create_date_report(config_file, pdf_report_file, start_date, end_date, sensor, platform, order_desc=False,
                       record_db=False):
    """
    A function to create a date report (i.e., quicklooks of all the acquisitions for a particular date)
    as a PDF.

    :param config_file: The EODataDown configuration file path.
    :param pdf_report_file: The output PDF file.
    :param start_date: A python datetime date object specifying the start date (most recent date)
    :param end_date: A python datetime date object specifying the end date (earliest date)
    :param sensor: The sensor to process (if None then all)
    :param platform: The platform to process (if None then all)
    :param order_desc: If True the report will be in descending order.
    :param record_db: If True a record will be written to the database for the report.

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    report_obj = sys_main_obj.get_date_report_obj()
    if report_obj is None:
        logger.error("Error occurred and the date report object could not be created.")

    obsdates_obj = sys_main_obj.get_obsdates_obj()
    if obsdates_obj is None:
        logger.error("Error occurred and the observation date object could not be created.")

    report_obj.create_date_report(obsdates_obj, pdf_report_file, sensor, platform, start_date, end_date,
                                  order_desc, record_db)


def build_obs_date_db(config_file, sensor, start_date, end_date):
    """
    A function which builds the observation date database table for a given sensor within a given
    date range. If the observation date is already in the database then it is ignored and not
    updated.

    :param config_file: The EODataDown configuration file path.
    :param sensor: The sensor to process
    :param start_date: A python datetime date object specifying the start date (most recent date)
    :param end_date: A python datetime date object specifying the end date (earliest date)

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    obsdates_obj = sys_main_obj.get_obsdates_obj()
    sensor_obj = sys_main_obj.get_sensor_obj(sensor)
    obsdates_obj.create_obs_date_records(sensor_obj, start_date, end_date)


def create_obs_date_visuals(config_file, sensor):
    """
    A function which creates all the observations visualisation images.

    :param config_file: The EODataDown configuration file path.
    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    obsdates_obj = sys_main_obj.get_obsdates_obj()
    obsdates_obj.create_obsdate_visual(sys_main_obj, sensor)


def create_obs_date_visuals(config_file, sensor, platform, obs_date):
    """
    A function to create the visual overviews for a particular observation date.

    :param config_file: The EODataDown configuration file path.
    :param sensor: The sensor for the observation of interest.
    :param platform: The platform for the observation of interest.
    :param obs_date: The date of the observation of interest.

    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    obsdates_obj = sys_main_obj.get_obsdates_obj()
    obsdates_obj.process_obsdata(sys_main_obj, sensor, platform, obs_date)

def get_obs_dates_need_processing(config_file, sensor):
    """
    A function to get a list of the observation dates which are in need of processing.

    :param config_file: The EODataDown configuration file path.
    :param sensor: Optionally a sensor can be specified, in which case the result will just be for that sensor.
    :return: list of lists - each list has [SensorID, PlatformID, ObsDate]
    """
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    obsdates_obj = sys_main_obj.get_obsdates_obj()
    obs_dates_list = obsdates_obj.get_lst_obsdates_need_processing(sensor)
    return obs_dates_list


def get_scenes_need_processing_date_order(config_file, sensors):
    """
    This function returns a list of dicts with sensor, scene PID and observation date
    sorted to ascending order (i.e., earliest first). This can be used for a single
    sensor or multiple sensors where the the scenes are merged from the different
    sensors.

    The outputs of this function can be used to call the run_scn_analysis function.

    :param config_file: The EODataDown configuration file path.
    :param sensors: a list of sensors
    :return: a list of lists where each scn has [config_file, scn_sensor, scn_id]

    """
    import datetime
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)

    scns = get_scenes_need_processing(config_file, sensors)

    sen_objs = dict()
    for sensor_str in sensors:
        sen_objs[sensor_str] = sys_main_obj.get_sensor_obj(sensor_str)

    scns_dict = dict()
    for scn in scns:
        scn_datetime = sen_objs[scn[1]].get_scn_obs_date(scn[2])
        if type(scn_datetime) is datetime.date:
            scn_datetime = datetime.datetime(scn_datetime.year, scn_datetime.month, scn_datetime.day)
        logger.debug("Scene {} ({}) acq datetime: {}".format(scn[2], scn[1], scn_datetime.isoformat()))
        scns_dict[scn_datetime] = scn

    out_scns = list()
    for scn_key in sorted(list(scns_dict.keys())):
        print("{}: {} - {}".format(scn_key.isoformat(), scns_dict[scn_key][1], scns_dict[scn_key][2]))
        out_scns.append(scns_dict[scn_key])

    return out_scns


def does_scn_need_processing(config_file, sensor, unq_id):
    """
    A function which tests whether a scene required processing or if processing has been completed.

    :param config_file: The EODataDown configuration file path.
    :param sensor: The sensor for the observation of interest.
    :param unq_id: Unique integer ID for the scene to be checked.
    :return: boolean. True IS processing is required. False is processing is NOT required.

    """
    logger.debug("Testing whether scene ({}) from {} has completed all processing.".format(unq_id, sensor))
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)

    sensor_obj = sys_main_obj.get_sensor_obj(sensor)
    logger.debug("Got sensor object for scene {} from sensor {}.".format(unq_id, sensor))

    processing_required = False
    if not sensor_obj.has_scn_download(unq_id):
        processing_required = True
        logger.debug("Scene has not been downloaded.")
    elif not sensor_obj.has_scn_con2ard(unq_id):
        processing_required = True
        logger.debug("Scene has not been processed to an ARD product.")

    if (not processing_required) and sensor_obj.calc_scn_quicklook():
        logger.debug("Generation of a quicklook product is required.")
        if not sensor_obj.has_scn_quicklook(unq_id):
            processing_required = True
            logger.debug("A quicklook product has not be generated.")

    if (not processing_required) and sensor_obj.calc_scn_tilecache():
        logger.debug("Generation of a tilecache product is required.")
        if not sensor_obj.has_scn_tilecache(unq_id):
            processing_required = True
            logger.debug("A tilecache product has not be generated.")

    if (not processing_required) and sensor_obj.calc_scn_usr_analysis():
        logger.debug("There are user defined processing plugins to execute.")
        if not sensor_obj.has_scn_usr_analysis(unq_id):
            processing_required = True
            logger.debug("The user defined plugins have not been (fully) executed.")

    if processing_required:
        logger.debug("Processing is required for scene ({}) from {}.".format(unq_id, sensor))
    else:
        logger.debug("Processing is complete for scene ({}) from {}.".format(unq_id, sensor))

    return processing_required
