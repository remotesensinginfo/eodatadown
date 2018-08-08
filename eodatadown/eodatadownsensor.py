#!/usr/bin/env python
"""
EODataDown - an abstract sensor class.
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
from abc import ABCMeta, abstractmethod, abstractproperty

logger = logging.getLogger(__name__)


class EODataDownSensor (object):
    """
    An abstract class which represents a sensor and defines the functions a sensor must have.
    """
    __metaclass__ = ABCMeta

    def __init__(self, dbInfoObj, ncores):
        self.sensorName = "AbstractBase"
        self.dbInfoObj = dbInfoObj
        self.ncores = ncores

    @abstractmethod
    def parseSensorConfig(self, config_file, first_parse=False): pass

    @abstractmethod
    def initSensorDB(self): pass

    @abstractmethod
    def check4NewData(self): pass

    @abstractmethod
    def downloadNewData(self): pass

    @abstractmethod
    def convertNewData2ARD(self): pass
