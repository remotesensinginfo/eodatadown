#!/usr/bin/env python
"""
Setup script for EODataDown. Use like this for Unix:

$ python setup.py install

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
# Purpose:  Installation of the EODataDown software
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

from distutils.core import setup
import os

setup(name='EODataDown',
    version='2.1.0',
    description='A tool for automating Earth Observation Data Downloading.',
    author='Pete Bunting',
    author_email='pfb@aber.ac.uk',
    scripts=['bin/eoddsetup.py', 'bin/eoddrun.py', 'bin/eoddpassencode.py', 'bin/eoddexport.py',
             'bin/eoddgenscncmds.py', 'bin/eoddsenroi.py', 'bin/eoddresetimgs.py', 'bin/eoddinitdatacube.py',
             'bin/eoddexportdb.py', 'bin/eoddimportdb.py', 'bin/eoddgenmonscncmds.py', 'bin/eoddrunmonitoring.py',
             'bin/eoddrunscnmonitoring.py', 'bin/eoddchknewscns.py', 'bin/eoddcreatereport.py',
             'bin/eoddobsdatetools.py', 'bin/eoddgenobsdatecmds.py'],
    packages=['eodatadown'],
    package_dir={'eodatadown': 'eodatadown'},
    package_data={'eodatadown': ['templates/*.jinja2']},
    data_files=[(os.path.join('share','eodatadown'),
                [os.path.join('share','eodatadown', 'loggingconfig.json'),
                 os.path.join('share', 'eodatadown', 'sensor_scn_lut.gpkg')]),
                (os.path.join('share','eodatadown', 'dc_prod_specs'),
                [os.path.join('share','eodatadown', 'dc_prod_specs', 'arcsi_ls4_prod_spec.yaml'),
                 os.path.join('share','eodatadown', 'dc_prod_specs', 'arcsi_ls5_prod_spec.yaml'),
                 os.path.join('share','eodatadown', 'dc_prod_specs', 'arcsi_ls7_prod_spec.yaml'),
                 os.path.join('share','eodatadown', 'dc_prod_specs', 'arcsi_ls8_prod_spec.yaml'),
                 os.path.join('share','eodatadown', 'dc_prod_specs', 'arcsi_sen2_prod_spec.yaml')])],
    license='LICENSE.txt',
    url='https://www.remotesensing.info/eodatadown',
    classifiers=['Intended Audience :: Developers',
                 'Intended Audience :: Remote Sensing Scientists',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 3.5',
                 'Programming Language :: Python :: 3.6',
                 'Programming Language :: Python :: 3.7',
                 'Programming Language :: Python :: 3.8'])
