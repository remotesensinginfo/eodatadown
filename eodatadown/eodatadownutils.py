#!/usr/bin/env python
"""
EODataDown - Utilities used within the EODataDown System.
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
# Purpose:  Utilities used within the EODataDown System.
#
# Author: Pete Bunting
# Email: pfb@aber.ac.uk
# Date: 07/08/2018
# Version: 1.0
#
# History:
# Version 1.0 - Created.

import Crypto.Cipher.AES
import base64
import hashlib
import os.path
import datetime
import logging
import shutil
import requests
import glob

import eodatadown

logger = logging.getLogger(__name__)


class EODataDownException(Exception):

    def __init__(self, value):
        """
        Init for the EODataDownException class
        """
        self.value = value

    def __str__(self):
        """
        Return a string representation of the exception
        """
        return repr(self.value)

class EODataDownResponseException(EODataDownException):

    def __init__(self, value, response=None):
        """
        Init for the EODataDownResponseException class
        """
        self.value = value
        self.response = response

    def __str__(self):
        """
        Return a string representation of the exception
        """
        return "HTTP status {0} {1}: {2}".format(self.response.status_code, self.response.reason, repr(self.value))


class EODataDownUtils(object):

    def findFile(self, dirPath, fileSearch):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        files = glob.glob(os.path.join(dirPath, fileSearch))
        if len(files) != 1:
            raise EODataDownException("Could not find a single file (" + fileSearch + "); found " + str(len(files)) + " files.")
        return files[0]

    def moveFile2DIR(self, in_file, out_dir):
        """
        A function which moves a file to the specified output directory.
        :param in_file:
        :param out_dir:
        :return:
        """
        file_name = os.path.split(in_file)[1]
        out_file_path = os.path.join(out_dir, file_name)
        shutil.move(in_file, out_file_path)

    def moveFilesWithBase2DIR(self, in_base_file, out_dir):
        """
        A function which moves all the files with the same basename
        (i.e., different extension)to the specified output directory.
        :param in_base_file:
        :param out_dir:
        :return:
        """
        file_name = os.path.splitext(in_base_file)[0]
        in_files = glob.glob(file_name+".*")
        for file in in_files:
            self.moveFile2DIR(file, out_dir)

    def copyFile2DIR(self, in_file, out_dir):
        """
        A function which moves a file to the specified output directory.
        :param in_file:
        :param out_dir:
        :return:
        """
        file_name = os.path.split(in_file)[1]
        out_file_path = os.path.join(out_dir, file_name)
        shutil.copyfile(in_file, out_file_path)


class EODataDownDatabaseInfo(object):

    def __init__(self, dbConn, dbUser, dbPass, dbName):
        self.dbConn = dbConn
        self.dbUser = dbUser
        self.dbPass = dbPass
        self.dbName = dbName

    def getDBConnection(self):
        return self.dbConn

    def getDBUser(self):
        return self.dbUser

    def getDBPass(self):
        return self.dbPass

    def getEncodedDBPass(self):
        eddPassEncoder = EDDPasswordTools()
        encodedPass = eddPassEncoder.encodePassword(self.dbPass)
        return encodedPass

    def getDBName(self):
        return self.dbName


class EDDPasswordTools(object):
    secret_key = "I7Cpan66nlslFqKyuUIkc1puFzeUHlg4"

    def encodePassword(self, plaintxt):
        if len(plaintxt) % 16 != 0:
            raise EODataDownException("Number of characters within the text must be a multiple of 16.")
        cipher = Crypto.Cipher.AES.new(self.secret_key, Crypto.Cipher.AES.MODE_ECB)
        txtencoded = base64.b64encode(cipher.encrypt(plaintxt)).decode()
        return txtencoded

    def unencodePassword(self, txtencoded):
        cipher = Crypto.Cipher.AES.new(self.secret_key, Crypto.Cipher.AES.MODE_ECB)
        plaintxt = cipher.decrypt(base64.b64decode(txtencoded)).decode()
        return plaintxt


class EDDCheckFileHash(object):

    def getSigFilePath(self, input_file):
        sig_file = os.path.splitext(input_file)[0]+".sig"
        logger.debug("Signature File Path: '" + sig_file + "'")
        return sig_file

    def createFileHash(self, input_file):
        filehash = hashlib.md5()
        filehash.update(open(input_file).read().encode())
        return filehash.hexdigest()

    def createFileSig(self, input_file):
        hash_sig = self.createFileHash(input_file)
        logger.debug("Created signature for input file: '" + input_file + "'")
        sig_file = self.getSigFilePath(input_file)
        f = open(sig_file, "w")
        f.write(hash_sig+"\n")
        f.close()
        logger.debug("Written to signature file: '" + sig_file + "'")

    def checkFileSig(self, input_file):
        sig_file = self.getSigFilePath(input_file)
        if not os.path.exists(sig_file):
            raise EODataDownException("Signature file could not be found.")

        f = open(sig_file, "r")
        in_hash_sig = f.read().strip()
        f.close()
        logger.debug("Read file signature: '" + in_hash_sig + "'")
        calcd_hash_sig = self.createFileHash(input_file)
        logger.debug("Calculated file signature: '" + calcd_hash_sig + "'")
        if calcd_hash_sig == in_hash_sig:
            logger.debug("Signatures Match")
            return True
        logger.info("Signature Does Not Match: " + input_file + " '" +calcd_hash_sig+ "'")
        return False

    def check_checksum(self, input_file, checksum, block_size=2 ** 13):
        """
        Compare a given MD5 checksum with one calculated from a file.
        :param input_file:
        :param checksum:
        :param block_size:
        :return:
        """
        md5 = hashlib.md5()
        with open(input_file, "rb") as f:
            while True:
                block_data = f.read(block_size)
                if not block_data:
                    break
                md5.update(block_data)
        return md5.hexdigest().lower() == checksum.lower()


class EDDJSONParseHelper(object):

    def doesPathExist(self, json_obj, tree_sequence):
        """
        A function which tests whether a path exists within JSON file.
        :param json_obj:
        :param tree_sequence: list of strings
        :return: boolean
        """
        curr_json_obj = json_obj
        steps_str = ""
        pathExists = True
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                pathExists = False
                break
        return pathExists

    def getStrValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a single string value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")
        if valid_values is not None:
            if curr_json_obj not in valid_values:
                raise EODataDownException("'"+curr_json_obj+"' is not within the list of valid values.")
        return curr_json_obj

    def getDateValue(self, json_obj, tree_sequence, date_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")
        try:
            out_date_obj = datetime.datetime.strptime(curr_json_obj, date_format).date()
        except Exception as e:
            raise EODataDownException(e)
        return out_date_obj

    def getDateTimeValue(self, json_obj, tree_sequence, date_time_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")
        try:
            out_datetime_obj = datetime.datetime.strptime(curr_json_obj, date_time_format)
        except Exception as e:
            raise EODataDownException(e)
        return out_datetime_obj

    def getStrListValue(self, json_obj, tree_sequence, valid_values=None):
        """
        A function which retrieves a list of string values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")

        if type(curr_json_obj).__name__ != "list":
            raise EODataDownException("Retrieved value is not a list.")
        if valid_values is not None:
            for val in curr_json_obj:
                if type(val).__name__ != "str":
                    raise EODataDownException("'" + val + "' is not of type string.")
                if val not in valid_values:
                    raise EODataDownException("'"+val+"' is not within the list of valid values.")
        return curr_json_obj

    def getNumericValue(self, json_obj, tree_sequence, valid_lower=None, valid_upper=None):
        """
        A function which retrieves a single numeric value from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :param valid_values:
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")

        out_value = 0.0
        if (type(curr_json_obj).__name__ == "int") or (type(curr_json_obj).__name__ == "float"):
            out_value = curr_json_obj
        elif type(curr_json_obj).__name__ == "str":
            if curr_json_obj.isnumeric():
                out_value = float(curr_json_obj)
            else:
                raise EODataDownException("The identified value is not numeric '" + steps_str + "'")
        else:
            raise EODataDownException("The identified value is not numeric '" + steps_str + "'")

        if valid_lower is not None:
            if out_value < valid_lower:
                raise EODataDownException("'"+str(out_value)+"' is less than the defined valid range.")
        if valid_upper is not None:
            if out_value > valid_upper:
                raise EODataDownException("'"+str(out_value)+"' is higher than the defined valid range.")
        return out_value

    def getListValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a list of values from a JSON structure.
        :param json_obj:
        :param tree_sequence: list of strings
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '"+steps_str+"'")

        if type(curr_json_obj).__name__ != "list":
            raise EODataDownException("Retrieved value is not a list.")
        return curr_json_obj

    def findStringValueESALst(self, lst_json_obj, name):
        """

        :param lst_json_obj:
        :param name:
        :return: [found, value]
        """
        value = ""
        found = False
        for json_obj in lst_json_obj:
            if json_obj["name"] == name:
                value = json_obj["content"]
                found = True
                break
        return [found, value]

    def findIntegerValueESALst(self, lst_json_obj, name):
        """

        :param lst_json_obj:
        :param name:
        :return: [found, value]
        """
        value = 0
        found = False
        for json_obj in lst_json_obj:
            if json_obj["name"] == name:
                value = int(json_obj["content"])
                found = True
                break
        return [found, value]


class EDDGeoBBox(object):

    def __init__(self):
        """
        Default constructor without setting values.
        """
        self.north_lat = 0.0
        self.south_lat = 0.0
        self.west_lon = 0.0
        self.east_lon = 0.0

    def setBBOX(self, north_lat, south_lat, west_lon, east_lon):
        """

        :param north_lat:
        :param south_lat:
        :param west_lon:
        :param east_lon:
        """
        self.north_lat = north_lat
        self.south_lat = south_lat
        self.west_lon = west_lon
        self.east_lon = east_lon

    def setNorthLat(self, north_lat):
        """

        :param north_lat:
        :return:
        """
        self.north_lat = north_lat

    def getNorthLat(self):
        """

        :return:
        """
        return self.north_lat

    def setSouthLat(self, south_lat):
        """

        :param south_lat:
        :return:
        """
        self.south_lat = south_lat

    def getSouthLat(self):
        """

        :return:
        """
        return self.south_lat

    def setWestLon(self, west_lon):
        """

        :param west_lon:
        :return:
        """
        self.west_lon = west_lon

    def getWestLon(self):
        """

        :return:
        """
        return self.west_lon

    def setEastLon(self, east_lon):
        """

        :param east_lon:
        :return:
        """
        self.east_lon = east_lon

    def getEastLon(self):
        """

        :return:
        """
        return self.east_lon

    def getWKTPolygon(self):
        """
        Get the bounding bbox represented as a polygon as a WKT string.
        :return:
        """
        wkt_str = "POLYGON ((" + str(self.west_lon) + " " + str(self.north_lat) + ", " + str(self.east_lon) + " " + str(
            self.north_lat) + ", " + str(self.east_lon) + " " + str(self.south_lat) + ", " + str(
            self.west_lon) + " " + str(self.south_lat) + ", " + str(self.west_lon) + " " + str(self.north_lat) + "))"
        return wkt_str

    def parseWKTPolygon(self, wkt_poly):
        """
        Populate the object from the WKT polygon.
        :param wkt_poly:
        :return:
        """
        wkt_poly = wkt_poly.replace("POLYGON ((", "").replace("))", "")
        pts = wkt_poly.split(",")
        min_lon = 0.0
        max_lon = 0.0
        min_lat = 0.0
        max_lat = 0.0
        first = True
        for pt in pts:
            lon, lat = pt.split(" ")
            lat_val = float(lat)
            lon_val = float(lon)
            if first:
                min_lon = lon_val
                max_lon = lon_val
                min_lat = lat_val
                max_lat = lat_val
                first = False
            else:
                if lon_val < min_lon:
                    min_lon = lon_val
                if lon_val > max_lon:
                    max_lon = lon_val
                if lat_val < min_lat:
                    min_lat = lat_val
                if lat_val > max_lat:
                    max_lat = lat_val

        self.north_lat = max_lat
        self.south_lat = min_lat
        self.west_lon = min_lon
        self.east_lon = max_lon



class EDDHTTPDownload(object):

    def checkResponse(self, response, url):
        """
        Check the HTTP response and raise an exception with appropriate error message
        if request was not successful.
        :param response:
        :param url:
        :return:
        """
        try:
            response.raise_for_status()
            success = True
        except (requests.HTTPError, ValueError):
            success = False
            excpt_msg = "Invalid API response."
            try:
                excpt_msg = response.headers["cause-message"]
            except:
                try:
                    excpt_msg = response.json()["error"]["message"]["value"]
                except:
                    excpt_msg = "Unknown error ('{0}'), check url in a web browser: '{1}'".format(response.reason, url)
            api_error = EODataDownResponseException(excpt_msg, response)
            api_error.__cause__ = None
            raise api_error
        return success

    def downloadFile(self, input_url, input_url_md5, out_file_path, username, password, exp_file_size, continue_download=True):
        """

        :param input_url:
        :param input_url_md5:
        :param out_file_path:
        :param username:
        :param password:
        :param exp_file_size:
        :param continue_download:
        :return:
        """
        logger.debug("Creating HTTP Session Object.")
        session = requests.Session()
        session.auth = (username, password)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session.headers["User-Agent"] = user_agent

        eddFileChecker = EDDCheckFileHash()

        temp_dwnld_path = out_file_path + '.incomplete'
        needs_downloading = True
        if os.path.exists(temp_dwnld_path) and continue_download:
            if os.path.getsize(temp_dwnld_path) > exp_file_size:
                os.remove(temp_dwnld_path)
                needs_downloading = True
                logger.debug("There was an existing file but too large removed and starting download again: " + out_file_path)
            elif os.path.getsize(temp_dwnld_path) == exp_file_size:
                md5_match = eddFileChecker.check_checksum(temp_dwnld_path, input_url_md5)
                if md5_match:
                    needs_downloading = False
                    os.rename(temp_dwnld_path, out_file_path)
                    logger.debug("There was an existing file and the MD5 matched so renamed and not downloading: " + out_file_path)
                else:
                    os.remove(temp_dwnld_path)
                    needs_downloading = True
                    logger.debug("There was an existing file but the MD5 did not matched so removed and starting download again: " + out_file_path)
            else:
                logger.debug("There was an existing temp file which was incomplete so will try to continue from where is was: " + out_file_path)
                needs_downloading = True

        if needs_downloading:
            continuing_download = False
            headers = {}
            downloaded_bytes = 0
            if os.path.exists(temp_dwnld_path):
                continuing_download = True
                logger.debug("Continuing the Download")
                downloaded_bytes = os.path.getsize(temp_dwnld_path)
                headers = {'Range': 'bytes={}-'.format(downloaded_bytes)}

            usr_update_step = exp_file_size/10
            next_update = downloaded_bytes
            usr_step_feedback = round((downloaded_bytes/exp_file_size)*100, 0)

            with session.get(input_url, stream=True, auth=session.auth, headers=headers) as r:
                self.checkResponse(r, input_url)
                chunk_size = 2 ** 20
                if continuing_download:
                    mode = 'ab'
                else:
                    mode = 'wb'
                with open(temp_dwnld_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            downloaded_bytes = downloaded_bytes + len(chunk)

                            if downloaded_bytes > next_update:
                                usr_step_feedback = round((downloaded_bytes / exp_file_size) * 100, 0)
                                logger.info("Downloaded {} % of {}".format(usr_step_feedback, temp_dwnld_path))
                                next_update = next_update + usr_update_step
            logger.info("Download Complete: ".format(temp_dwnld_path))
            md5_match = eddFileChecker.check_checksum(temp_dwnld_path, input_url_md5)
            if md5_match:
                os.rename(temp_dwnld_path, out_file_path)
                logger.info("MD5 Matched Renamed download: ".format(out_file_path))
                return True
            else:
                logger.info("MD5 did not match: ".format(temp_dwnld_path))
            return False

