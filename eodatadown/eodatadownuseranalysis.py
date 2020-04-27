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
from eodatadown.eodatadownutils import EODataDownException

logger = logging.getLogger(__name__)


class EODataDownUserAnalysis (object):
    """
    An abstract base class for running user analysis following ARD processing
    """
    __metaclass__ = ABCMeta

    def __init__(self, analysis_name, req_keys=None):
        """
        A class to do some analysis defined by the user. Implemented
        as a plugin provided through the sensor configuration.

        Any params inputted via the EODataDown configuration script for the plugin are passed
        to the class using the set_users_param function.

        This function initialises an attribute self.analysis_name to None unless an value is passed. However,
        this attribute requires a value which must not be None. When creating an instance of this function you
        must either pass a value to this function or define this value in your constructor after this parent
        constructor has been called.

        :param analysis_name: A name for the analysis, must be unique for sensor. Must not be None. This is
                              expected to be passed to the parent from the child constructor. No value will
                              be passed through from the EODataDown sensor when the plugin is instantiated.
        :param req_keys: A list of required keys within the parameters (optionally) provided from the EODataDown
                         configuration file. If None then it will be assume no parameters are being provided to
                         the class.

        """
        if analysis_name is None:
            raise EODataDownException("The analysis name was None, a value is required.")
        self.analysis_name = analysis_name
        self.req_keys = req_keys
        self.params = None

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

    def set_required_keys(self, req_keys=None):
        """
        A function which sets the required keys for the class parameters.

        :param req_keys: A list of required keys within the parameters (optionally) provided from the EODataDown
                         configuration file. If None then it will be assume no parameters are being provided to
                         the class.

        """
        self.req_keys = req_keys

    def get_required_keys(self):
        """
        A function which returns the list of require keys for the parameters dict.

        :return: a list of strings or None.

        """
        return self.req_keys

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

    def check_param_keys(self, req_keys=None, raise_except=False):
        """
        Check that the parameters have the req_keys for the analysis

        :param req_keys: list of keys, if None then the class version (defined in constructor) will be used.
        :return: boolean (True if all present)

        """
        keys_present = True
        if req_keys is None:
            req_keys = self.req_keys
        for key in req_keys:
            if key not in self.params:
                keys_present = False
                logger.debug("'{}' key NOT present.".format(key))
                if raise_except:
                    raise EODataDownException("Parameters did not have expected key '{}'".format(key))
                break
            else:
                logger.debug("'{}' key present.".format(key))
        return keys_present

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

