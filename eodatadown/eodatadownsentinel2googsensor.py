#!/usr/bin/env python
"""
EODataDown - a sensor class for Sentinel-2 data downloaded from the Google Cloud.
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
# Purpose:  Provides a sensor class for Sentinel-2 data downloaded from the Google Cloud.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
from abc import ABCMeta, abstractmethod, abstractproperty
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor


logger = logging.getLogger(__name__)


class EODataDownSentinel2GoogSensor (EODataDownSensor):
    """
    An class which represents a the Sentinel-2 sensor being downloaded from the Google Cloud.
    """

    def __init__(self, dbInfoObj):
        EODataDownSensor.__init__(self, dbInfoObj)
        self.sensorName = "Sentinel-2 Google"

    def parseSensorConfig(self, config_file, first_parse=False):
        """

        :param config_file:
        :param first_parse:
        :return:
        """
        raise EODataDownException("EODataDownSentinel2GoogSensor::parseSensorConfig not implemented")

    def initSensorDB(self):
        """

        :return:
        """
        raise EODataDownException("EODataDownSentinel2GoogSensor::initSensorDB not implemented")

    def check4NewData(self):
        """

        :return:
        """
        raise EODataDownException("EODataDownSentinel2GoogSensor::check4NewData not implemented")

    def downloadNewData(self):
        """

        :return:
        """
        raise EODataDownException("EODataDownSentinel2GoogSensor::downloadNewData not implemented")

    def convertNewData2ARD(self):
        """

        :return:
        """
        raise EODataDownException("EODataDownSentinel2GoogSensor::convertNewData2ARD not implemented")
