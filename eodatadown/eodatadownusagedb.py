#!/usr/bin/env python
"""
EODataDown - provide access to the usage database.
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
# Purpose:  Provide the main class where the functionality of EODataDown is accessed.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.


import logging
import datetime

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import sqlalchemy.orm

logger = logging.getLogger(__name__)

Base = declarative_base()


class EDDUsageLog(Base):
    __tablename__ = "EDDUsageLog"

    ID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Sensor = sqlalchemy.Column(sqlalchemy.String, default="NA")
    Update = sqlalchemy.Column(sqlalchemy.DateTime)
    Description = sqlalchemy.Column(sqlalchemy.String)
    UpdatedLclDB = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    FoundNewScns = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    NewScnsAvail = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    DownloadedNewScns = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    ConvertNewScnsARD = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    IngestNewScnsToDC = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    StartBlock = sqlalchemy.Column(sqlalchemy.Boolean, default=False)
    EndBlock = sqlalchemy.Column(sqlalchemy.Boolean, default=False)

class EODataDownUpdateUsageLogDB(object):

    def __init__(self, db_info_obj):
        self.db_info_obj = db_info_obj

    def init_usage_log_db(self):
        """
        A function which will setup the system data base for each of the sensors.
        Note. this function should only be used to initialing the system.
        :return:
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Drop usage table if within the existing database.")
        Base.metadata.drop_all(db_engine)

        logger.debug("Creating Usage Database.")
        Base.metadata.bind = db_engine
        Base.metadata.create_all()



    def add_entry(self, description_val, sensor_val="NA", updated_lcl_db=False, scns_avail=False, downloaded_new_scns=False, convert_scns_ard=False, ingest_scns_dc=False, start_block=False, end_block=False):
        """
        Function to add an entry into the usage log database.
        :param start_block:
        :param end_block:
        :param description_val:
        :param sensor_val:
        :param updated_lcl_db:
        :param scns_avail:
        :param downloaded_new_scns:
        :param convert_scns_ard:
        :param ingest_scns_dc:
        :return:
        """
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.db_info_obj.dbConn)

        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()

        logger.debug("Adding Update to Database.")
        ses.add(EDDUsageLog(Sensor=sensor_val, Update=datetime.datetime.now(), Description=description_val,
                            UpdatedLclDB=updated_lcl_db, FoundNewScns=scns_avail, NewScnsAvail=scns_avail,
                            DownloadedNewScns=downloaded_new_scns, ConvertNewScnsARD=convert_scns_ard,
                            IngestNewScnsToDC=ingest_scns_dc, StartBlock=start_block, EndBlock=end_block))
        ses.commit()
        ses.close()
        logger.debug("Committed and closed db session.")
