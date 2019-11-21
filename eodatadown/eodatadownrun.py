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
import multiprocessing
from eodatadown.eodatadownutils import EODataDownException
import eodatadown.eodatadownutils
import eodatadown.eodatadownsystemmain

logger = logging.getLogger(__name__)


# Start: Function for Pool
def _check_new_data_qfunc(sensor_obj_params):
    sensor_obj_params[0].check_new_scns(sensor_obj_params[1])
# End: Function for Pool


def find_new_downloads(config_file, n_cores, sensors, check_from_start=False):
    """
    A function to run the process of finding new data to download.
    :param config_file:
    :param n_cores:
    :param sensors:
    :return:
    """
    logger.info("Running process to find new downloads.")
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    edd_usage_db = sys_main_obj.get_usage_db_obj()
    edd_usage_db.add_entry("Started: Finding Available Downloads.", start_block=True)

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
            sensor_objs_to_process.append([sensor_obj, check_from_start])

    with multiprocessing.Pool(processes=n_cores) as pool:
        pool.map(_check_new_data_qfunc, sensor_objs_to_process)
    edd_usage_db.add_entry("Finished: Finding Available Downloads.", end_block=True)


def get_sensor_obj(config_file, sensor):
    """
    A function to get a sensor object.
    :param config_file:
    :param sensor:
    :return:
    """
    # Create the System 'Main' object and parse the configuration file.
    sys_main_obj = eodatadown.eodatadownsystemmain.EODataDownSystemMain()
    sys_main_obj.parse_config(config_file)
    logger.debug("Parsed the system configuration.")

    sensor_objs = sys_main_obj.get_sensors()
    sensor_obj_to_process = None
    for sensor_obj in sensor_objs:
        if sensor_obj.get_sensor_name() == sensor:
            sensor_obj_to_process = sensor_obj
            break

    if sensor_obj_to_process is None:
        logger.error("Error occurred could not find sensor object for '{}'".format(sensor))
        raise EODataDownException("Could not find sensor object for '{}'".format(sensor))

    return sensor_obj_to_process


def perform_downloads(config_file, n_cores, sensors):
    """
    A function which runs the process of performing the downloads of available scenes
    which have not yet been downloaded.
    :param config_file:
    :param n_cores:
    :param sensors:
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
    :param config_file:
    :param sensor:
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
    :param config_file:
    :param n_cores:
    :param sensors:
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
    :param config_file:
    :param sensor:
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

    :param config_file:
    :param sensors:
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
    :param config_file:
    :param sensor:
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

    :param config_file:
    :param sensors:
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
    :param config_file:
    :param sensor:
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

    :param config_file:
    :param sensors:
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
    :param config_file:
    :param sensor:
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


def export_image_footprints_vector(config_file, sensor, table, vector_file, vector_lyr, vector_driver, add_layer):
    """

    :param table:
    :param config_file:
    :param sensor:
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