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

    def __init__(self, params=None, analysis_name=None):
        """
        A class to do some analysis defined by the user. Implemented
        as a plugin provided through the sensor configuration.

        The params for the class are passed when the class is instantiated
        and the user parses for the sensor parsed.

        This function initialises an attribute self.analysis_name to None unless an value is passed. However,
        this attribute requires a value which must not be None. When creating an instance of this function you
        must either pass a value to this function or define this value in your constructor after this parent
        constructor has been called.

        :param params: a dict of user params for the class
        :param analysis_name: A name for the analysis, must be unique for sensor. Must not be None.

        """
        self.analysis_name = analysis_name
        self.params = params

    def set_analysis_name(self, analysis_name):
        """
        A function which sets the name of the user analysis.

        :param analysis_name: A name for the analysis, must be unique for sensor. Must not be None.

        """
        self.analysis_name = analysis_name

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

    def get_ext_info_key(self):
        """
        Provide the key used for the extended info for this plugin. This function returns the analysis_name

        :return: string with key.

        """
        if self.analysis_name is None:
            raise Exception("The analysis name is None and must have a value provided.")

        return self.analysis_name

    @abstractmethod
    def perform_analysis(self, scn_db_obj, sen_obj):
        """
        A function which needs to be implemented by the user to perform the analysis.
        The object for the scene representing the database record is provided to the function.

        The function must return a set of with a boolean and dict (bool, dict).
        The boolean represents whether the analysis was successfully completed.
        The dict will be added to the database record JSON field (ExtendedInfo)
        using the key (defined by get_ext_info_key). If the dict is None then
        and the processing successfully completed then a simple value of True
        will be written as the key value.

        The function cannot alter the other database fields, only the JSON field.

        :param scn_db_obj: The scene record from the database.
        :param sen_obj: An instance of a eodatadownsensor object related to the sensor for the scene.
        :return: (bool, dict); dict can be None. See description above.

        """
        pass

