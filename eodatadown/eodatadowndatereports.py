#!/usr/bin/env python
"""
EODataDown - a sensor class for Landsat data downloaded from the Google Cloud.
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
# Purpose:  Provides a sensor class for Landsat data downloaded from the Google Cloud.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import logging
import json
import os
import os.path
import datetime
import shutil
import rsgislib
import subprocess

from osgeo import osr
from osgeo import ogr
from osgeo import gdal

import eodatadown.eodatadownutils
from eodatadown.eodatadownutils import EODataDownException
from eodatadown.eodatadownsensor import EODataDownSensor

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.types
import sqlalchemy.dialects.postgresql
from sqlalchemy.orm.attributes import flag_modified

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDDateReports(Base):
    __tablename__ = "EDDDateReports"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    File_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Start_Date = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    End_Date = sqlalchemy.Column(sqlalchemy.Date, nullable=False)
    Production_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Sensor = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Scn_Images = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)
    ExtendedInfo = sqlalchemy.Column(sqlalchemy.dialects.postgresql.JSONB, nullable=True)


class EODataDownDateReports (object):

    def __init__(self, db_info_obj, scn_image_dir):
        self.db_info_obj = db_info_obj
        self.scn_image_dir = scn_image_dir

    def init_db(self):
        """
        A function which initialises the database use the db_info_obj passed to __init__.
        Be careful as running this function drops the table if it already exists and therefore
        any data would be lost.
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop system table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Creating EDDDateReports Database Table.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()

    def create_date_report(self, sensor_obj, pdf_report_file, start_date, end_date, vec_file, vec_lyr, tmp_dir):
        """
        A function to create a date report (i.e., quicklooks of all the acquisitions for a particular date)
        as a PDF.

        :param sensor_obj: An instance of a EODataDownSensor object
        :param pdf_report_file: The output PDF file.
        :param start_date: A python datetime date object specifying the start date (most recent date)
        :param end_date: A python datetime date object specifying the end date (earliest date)
        :param vec_file: A vector file (polyline) which can be overlaid for context.
        :param vec_lyr: The layer in the vector file.
        :param tmp_dir: A temp directory for intermediate files.

        """
        from weasyprint import HTML, CSS
        import jinja2

        pdf_report_file = os.path.abspath(pdf_report_file)
        sensor_name = sensor_obj.get_sensor_name()
        eoddutils = eodatadown.eodatadownutils.EODataDownUtils()
        uid_str = eoddutils.uidGenerator()
        out_pdf_basename = eoddutils.get_file_basename(pdf_report_file, checkvalid=True)
        c_tmp_dir = os.path.join(tmp_dir, '{}_{}_{}'.format(out_pdf_basename, sensor_name, uid_str))
        if not os.path.exists(c_tmp_dir):
            os.mkdir(c_tmp_dir)

        out_img_dir = os.path.join(self.scn_image_dir, '{}_{}'.format(out_pdf_basename, uid_str))
        if not os.path.exists(out_img_dir):
            os.mkdir(out_img_dir)

        # Generate the images for the report.
        date_scns_dict = sensor_obj.create_scn_date_imgs(start_date, end_date, 500, out_img_dir, 'PNG', vec_file,
                                                         vec_lyr, tmp_dir)

        date_scn_imgs_dict = dict()
        for scn_key in date_scns_dict:
            date_scn_imgs_dict[scn_key] = dict()
            date_scn_imgs_dict[scn_key]['qkimage'] = date_scns_dict['qkimage']
            date_scn_imgs_dict[scn_key]['date_str'] = date_scns_dict['scn_date'].strftime('%Y-%m-%d')

        # Process the report template
        css_fields = dict()
        css_fields['header_title'] = 'EODataDown Date Report'
        css_fields['info_footer'] = 'See https://eodatadown.remotesensing.info for background.'
        css_fields['copyright_footer'] = 'Copyright Aberystwyth University'

        html_fields=dict()
        html_fields['page_title'] = "Sensor for Period"
        html_fields['second_title'] = "Report from EODataDown"
        html_fields['scns'] = date_scn_imgs_dict

        template_loader = jinja2.PackageLoader('eodatadown')

        template_env = jinja2.Environment(loader=template_loader)
        html_template = template_env.get_template('report_scn_html.jinja2')
        css_template = template_env.get_template('report_scn_css.jinja2')

        report_html_str = html_template.render(html_fields)
        report_css_str = css_template.render(css_fields)

        # Generate the PDF report.
        report_html = HTML(report_html_str)
        report_css = CSS(report_css_str)
        report_html.write_pdf(pdf_report_file, stylesheets=[report_css])

        #shutil.rmtree(c_tmp_dir)
        """
        logger.debug("Creating Database Engine and Session.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        db_records = list()
        db_records.append(EDDDateReports(File_Path=pdf_report_file,
                                         Start_Date=start_date,
                                         End_Date=end_date,
                                         Production_Date=datetime.datetime.now(),
                                         Sensor=sensor_name,
                                         Scn_Images=date_scn_imgs_dict
                                         ))
        ses.add_all(db_records)
        ses.commit()
        """