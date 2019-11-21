#!/usr/bin/env python
"""
EODataDown - load the product specifications into datacube.
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
# Purpose:  Command line tool for running the system.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 02/05/2019
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import argparse
import logging
import os
import os.path
import subprocess

from eodatadown import EODATADOWN_SENSORS_LIST
from eodatadown import eodd_install_prefix

logger = logging.getLogger('eoddinitdatacube.py')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="", help="Path to the datacube config file.")
    parser.add_argument("-s", "--sensor", type=str, default=None, choices=EODATADOWN_SENSORS_LIST,
                        help='''Specify the sensors for which this process should be executed, 
                                if not specified then processing is executed for all.''')
    args = parser.parse_args()

    datacube_cmd_path = 'datacube'
    datacube_cmd_path_env_value = os.getenv('DATACUBE_CMD_PATH', None)
    if datacube_cmd_path_env_value is not None:
        datacube_cmd_path = datacube_cmd_path_env_value

    config_file = args.config
    main_config_value = os.getenv('DATACUBE_CONFIG_PATH', None)
    if (config_file == '') and (main_config_value is not None):
        config_file = main_config_value

    print("Data Cube Config File: '" + config_file + "'")

    if not os.path.exists(config_file):
        logger.info("The config file does not exist: '" + config_file + "'")
        raise Exception("Config file does not exist")

    sensors = list()
    if args.sensor == None:
        sensors = EODATADOWN_SENSORS_LIST
    else:
        sensors.append(args.sensor)
    sentinel1_complete = False

    cmd_base = '{0} product add {1}'
    try:
        logger.info('Running process to add product specifications to the datacube.')
        for sensor in sensors:
            if sensor == "LandsatGOOG":
                # Landsat 4
                ls4_prod_spec = os.path.join(eodd_install_prefix, 'share', 'eodatadown', 'dc_prod_specs',
                                              'arcsi_ls4_prod_spec.yaml')
                if os.path.exists(ls4_prod_spec):
                    cmd = cmd_base.format(datacube_cmd_path, ls4_prod_spec)
                    try:
                        subprocess.call(cmd, shell=True)
                    except Exception as e:
                        logger.debug(e, exc_info=True)
                        raise Exception("Failed to add Landsat-4 product specification to the datacube.")
                else:
                    raise Exception("Could not find Landsat-4 Product Specification: '{}'".format(ls4_prod_spec))

                # Landsat 5
                ls5_prod_spec = os.path.join(eodd_install_prefix, 'share', 'eodatadown', 'dc_prod_specs',
                                             'arcsi_ls5_prod_spec.yaml')
                if os.path.exists(ls5_prod_spec):
                    cmd = cmd_base.format(datacube_cmd_path, ls5_prod_spec)
                    try:
                        subprocess.call(cmd, shell=True)
                    except Exception as e:
                        logger.debug(e, exc_info=True)
                        raise Exception("Failed to add Landsat-5 product specification to the datacube.")
                else:
                    raise Exception("Could not find Landsat-5 Product Specification: '{}'".format(ls5_prod_spec))

                # Landsat 7
                ls7_prod_spec = os.path.join(eodd_install_prefix, 'share', 'eodatadown', 'dc_prod_specs',
                                             'arcsi_ls7_prod_spec.yaml')
                if os.path.exists(ls7_prod_spec):
                    cmd = cmd_base.format(datacube_cmd_path, ls7_prod_spec)
                    try:
                        subprocess.call(cmd, shell=True)
                    except Exception as e:
                        logger.debug(e, exc_info=True)
                        raise Exception("Failed to add Landsat-7 product specification to the datacube.")
                else:
                    raise Exception("Could not find Landsat-7 Product Specification: '{}'".format(ls7_prod_spec))

                # Landsat 8
                ls8_prod_spec = os.path.join(eodd_install_prefix, 'share', 'eodatadown', 'dc_prod_specs',
                                             'arcsi_ls8_prod_spec.yaml')
                if os.path.exists(ls8_prod_spec):
                    cmd = cmd_base.format(datacube_cmd_path, ls8_prod_spec)
                    try:
                        subprocess.call(cmd, shell=True)
                    except Exception as e:
                        logger.debug(e, exc_info=True)
                        raise Exception("Failed to add Landsat-8 product specification to the datacube.")
                else:
                    raise Exception("Could not find Landsat-8 Product Specification: '{}'".format(ls8_prod_spec))

                cmd = cmd_base.format(datacube_cmd_path, '')
            elif sensor == "Sentinel2GOOG":
                sen2_prod_spec = os.path.join(eodd_install_prefix, 'share', 'eodatadown', 'dc_prod_specs',
                                              'arcsi_sen2_prod_spec.yaml')
                if os.path.exists(sen2_prod_spec):
                    cmd = cmd_base.format(datacube_cmd_path, sen2_prod_spec)
                    try:
                        subprocess.call(cmd, shell=True)
                    except Exception as e:
                        logger.debug(e, exc_info=True)
                        raise Exception("Failed to add sentinel-2 product specification to the datacube.")
                else:
                    raise Exception("Could not find Sentinel-2 Product Specification: '{}'".format(sen2_prod_spec))
            elif (sensor == "Sentinel1ESA" or sensor == "Sentinel1ASF") and (not sentinel1_complete):
                print("Product Specification has not yet implemented for Sentinel-1.")
                sentinel1_complete = True
            elif sensor == "RapideyePlanet":
                print("Product Specification has not yet implemented for Rapideye data.")
            elif sensor == "PlanetScope":
                print("Product Specification has not yet implemented for Planet Scope Imagery.")
            elif sensor == "JAXASARTiles":
                print("Product Specification has not yet implemented for JAXA SAR Tiles.")
            elif sensor == "GenericDataset":
                print("Generic Datasets cannot be loaded into the datacube - need more information so needs to be done manual.")
            else:
                raise Exception("Sensor specified ('{}') if not known.".format(sensor))
        logger.info('Finished process to add product specifications to the datacube.')
    except Exception as e:
        logger.error("{}".format(e.message()), exc_info=True)

