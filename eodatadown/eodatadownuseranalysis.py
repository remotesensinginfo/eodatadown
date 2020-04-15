#!/usr/bin/env python
"""
EODataDown - an abstract base class for running user analysis following ARD processing
"""
# This file is part of 'EODataDown'
# A tool for automating Earth Observation Data Downloading.
#
# Copyright 2020 Pete Bunting
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
# Purpose:  Provides an abstract base class for running user analysis following ARD processing
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 10/04/2020
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class EODataDownUserAnalysis (object):
    """
    An abstract base class for running user analysis following ARD processing
    """
    __metaclass__ = ABCMeta

    def __init__(self, analysis_name, params=None):
        """
        A class to do some analysis defined by the user. Implemented
        as a plugin provided through the sensor configuration.

        The params for the class are passed when the class is instantiated
        and the user parses for the sensor parsed.

        :param analysis_name: A name for the analysis, must be unique for sensor.
        :param params: a dict of user params for the class

        """
        self.analysis_name = analysis_name
        self.params = params

    def get_analysis_name(self):
        """
        A function which returns the name of the user analysis.

        :return: a string with name of analysis class

        """
        return self.analysis_name

    def set_users_param(self, params):
        """
        Provide any user params to the class.

        :param params: a dict of params

        """
        self.params = params

    @abstractmethod
    def perform_analysis(self, scn_obj, sen_obj):
        """
        A function which needs to be implemented by the user to perform the analysis.
        The object for the scene representing the database record is provided to the function.

        The function must return a set of a boolean and dict (bool, dict). If True, then the
        dict return will be used to replace JSON field for the scene within the database.
        If False, then the dict will be added to the scene JSON field using the name of the
        analysis (i.e., provided within the constructor) as the key to uniquely identify the
        information. If None is returned then nothing will be written to the scene database.

        The function cannot alter the other database fields, only the JSON field.

        :param scn_obj: The scene record from the database.
        :param scn_obj: An instance of a eodatadownsensor object related to the sensor for the scene.
        :return: (bool, dict) or None. See description above.

        """
        pass

