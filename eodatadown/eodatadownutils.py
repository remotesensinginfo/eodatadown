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

import base64
import hashlib
import os.path
import datetime
import logging
import shutil
import requests
import glob
import json
import ftplib
import time
import gzip
import pycurl
import subprocess
import rsgislib
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

    def readTextFileNoNewLines(self, file):
        """
        Read a text file into a single string
        removing new lines.

        :return: string

        """
        txtStr = ""
        try:
            dataFile = open(file, 'r')
            for line in dataFile:
                txtStr += line.strip()
            dataFile.close()
        except Exception as e:
            raise e
        return txtStr

    def readTextFile2List(self, file):
        """
        Read a text file into a list where each line
        is an element in the list.

        :return: list

        """
        outList = list()
        try:
            dataFile = open(file, 'r')
            for line in dataFile:
                line = line.strip()
                if line != "":
                    outList.append(line)
            dataFile.close()
        except Exception as e:
            raise e
        return outList

    def writeList2File(self, dataList, outFile):
        """
        Write a list a text file, one line per item.

        """
        try:
            f = open(outFile, 'w')
            for item in dataList:
               f.write(str(item)+'\n')
            f.flush()
            f.close()
        except Exception as e:
            raise e

    def findFile(self, dirPath, fileSearch, recursive=False):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        if recursive:
            files = glob.glob(os.path.join(dirPath, '**', fileSearch), recursive=True)
        else:
            files = glob.glob(os.path.join(dirPath, fileSearch))

        if len(files) != 1:
            raise EODataDownException("Could not find a single file ({0}) in {1}; found {2} files.".format(fileSearch, dirPath, len(files)))
        return files[0]

    def findFileNone(self, dirPath, fileSearch, recursive=False):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return: the found file or None rather than raising exception.
        """
        try:
            file = self.findFile(dirPath, fileSearch, recursive)
        except:
            file = None
        return file

    def findFileMultiPaths(self, dirPaths, fileSearch):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        c_dirPath = None
        for dirPath in dirPaths:
            files = glob.glob(os.path.join(dirPath, fileSearch))
            if len(files) > 0:
                c_dirPath = dirPath
                break
        if len(files) != 1:
            raise EODataDownException("Could not find a single file ({0}) in {1}; found {2} files.".format(fileSearch, c_dirPath, len(files)))
        return files[0]

    def findFirstFile(self, dirPath, fileSearch):
        """
        Search for a single file with a path using glob. Therefore, the file
        path returned is a true path. Within the fileSearch provide the file
        name with '*' as wildcard(s).
        :param dirPath:
        :param fileSearch:
        :return:
        """
        c_dirPath = dirPath
        for root, dirs, files in os.walk(dirPath):
            files = glob.glob(os.path.join(root, fileSearch))
            if len(files) > 0:
                c_dirPath = root
                break

        if len(files) != 1:
            raise EODataDownException("Could not find a single file ({0}) in {1}; found {2} files.".format(fileSearch, dirPath, len(files)))
        return files[0]

    def findFilesRecurse(self, dir_path, file_ext):
        """
        Recursively search a directory for files with the provided extension.
        Ignores directories which have the same extension.
        :param dir_path: directory file path
        :param file_ext: file extension (include dot; e.g., .kea)
        :return: returns list of files.
        """
        found_files = list()
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith(file_ext):
                    found_file = os.path.join(root, file)
                    if os.path.isfile(found_file):
                        found_files.append(found_file)
        return found_files

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

    def extractGZTarFile(self, in_file, out_dir):
        """

        :param in_file:
        :param out_dir:
        :return:
        """
        cwd = os.getcwd()
        os.chdir(out_dir)
        logger.debug("Executing tar (gz) data extraction.")
        cmd = 'tar -xzf ' + in_file
        logger.debug("Command is: '{}'.".format(cmd))
        try:
            subprocess.call(cmd, shell=True)
        except Exception as e:
            logger.error("Failed to run command: {}".format(cmd))
            raise e
        logger.debug("Executed tar (gz) data extraction.")
        os.chdir(cwd)

    def uidGenerator(self, size=6):
        """
        A function which will generate a 'random' string of the specified length based on the UUID
        
        :param size: the length of the returned string.
        :return: string of length size.

        """
        import uuid
        randomStr = str(uuid.uuid4())
        randomStr = randomStr.replace("-","")
        return randomStr[0:size]

    def isNumber(self, str_val):
        """
        A function which tests whether the input string contains a number of not.
        """
        try:
            float(str_val)  # for int, long and float
        except ValueError:
            try:
                complex(str_val)  # for complex
            except ValueError:
                return False
        return True

    def isEPSGUTM(self, epsg_code):
        """
        Function to test whether the specified epsg code is a UTM WGS84 zone.
        :param epsg_code: EPSG code to check.
        :return: boolean;
        """
        utm_epsg_codes = [32601, 32602, 32603, 32604, 32605, 32606, 32607, 32608, 32609, 32610, 32611, 32612, 32613,
                          32614, 32615, 32616, 32617, 32618, 32619, 32620, 32621, 32622, 32623, 32624, 32625, 32626,
                          32627, 32628, 32629, 32630, 32631, 32632, 32633, 32634, 32635, 32636, 32637, 32638, 32639,
                          32640, 32641, 32642, 32643, 32644, 32645, 32646, 32647, 32648, 32649, 32650, 32651, 32652,
                          32653, 32654, 32655, 32656, 32657, 32658, 32659, 32660, 32701, 32702, 32703, 32704, 32705,
                          32706, 32707, 32708, 32709, 32710, 32711, 32712, 32713, 32714, 32715, 32716, 32717, 32718,
                          32719, 32720, 32721, 32722, 32723, 32724, 32725, 32726, 32727, 32728, 32729, 32730, 32731,
                          32732, 32733, 32734, 32735, 32736, 32737, 32738, 32739, 32740, 32741, 32742, 32743, 32744,
                          32745, 32746, 32747, 32748, 32749, 32750, 32751, 32752, 32753, 32754, 32755, 32756, 32757,
                          32758, 32759, 32760]
        return epsg_code in utm_epsg_codes

    def getWKTFromEPSGCode(self, epsgCode):
        """
        Using GDAL to return the WKT string for inputted EPSG Code.
        :param epsgCode: integer variable of the epsg code.
        :return: string with WKT representation of the projection.
        """
        wktString = None
        try:
            from osgeo import osr
            spatRef = osr.SpatialReference()
            spatRef.ImportFromEPSG(epsgCode)
            wktString = spatRef.ExportToWkt()
        except Exception:
            wktString = None
        return wktString

    def translateCloudOpGTIFF(self, input_img, out_dir):
        """
        Using GDAL translate to convert input image to a cloud optimised GeoTIFF.
        :param input_img: Input image which is GDAL readable.
        :param out_dir: Output image directory. The output file name will be the same as the input with the extension
                        replaced with .tif
        :return: returns the output image filename and path.
        """
        import osgeo.gdal
        basename = os.path.splitext(os.path.basename(input_img))[0]
        output_img = os.path.join(out_dir, "{}.tif".format(basename))

        rsgis_utils = rsgislib.RSGISPyUtils()
        no_data_val = rsgis_utils.getImageNoDataValue(input_img)
        options = "-co TILED=YES -co INTERLEAVE=PIXEL -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -co COMPRESS=LZW -co BIGTIFF=YES -co COPY_SRC_OVERVIEWS=YES"
        trans_opt = osgeo.gdal.TranslateOptions(format='GTIFF', noData=no_data_val, options=options)
        osgeo.gdal.Translate(output_img, input_img, options=trans_opt)
        return output_img

    def subsetMaskImg(self, input_img, output_img, gdal_format, subset_vec_file, subset_vec_lyr, mask_outputs, mask_vec_file, mask_vec_lyr, tmp_dir):
        """
        Apply commands to subset and optionally mask the input image.

        :param input_img:
        :param output_img:
        :param intersect_vec_file:
        :param intersect_vec_lyr:
        :param subset_vec_file:
        :param subset_vec_lyr:
        :param mask_outputs:
        :param mask_vec_file:
        :param mask_vec_lyr:
        :param tmp_dir:

        """
        rsgis_utils = rsgislib.RSGISPyUtils()
        no_data_val = rsgis_utils.getImageNoDataValue(input_img)
        if mask_outputs:
            in_image_base = self.get_file_basename(input_img)
            sub_img_file = os.path.join(tmp_dir, "{}_sub.kea".format(in_image_base))
            rsgislib.imageutils.subset_to_vec(input_img, sub_img_file, gdal_format, subset_vec_file, subset_vec_lyr)
            sub_msk_img_file = os.path.join(tmp_dir, "{}_sub_msk.kea".format(in_image_base))
            rsgislib.imageutils.mask_img_with_vec(sub_img_file, output_img, gdal_format, mask_vec_file,
                                                  mask_vec_lyr, tmp_dir, outvalue=no_data_val)
        else:
            rsgislib.imageutils.subset_to_vec(input_img, output_img, gdal_format, subset_vec_file, subset_vec_lyr)

        rsgislib.imageutils.popImageStats(output_img, usenodataval=True, nodataval=no_data_val, calcpyramids=True)


    def getDateTimeAsString(self, date_time_obj):
        """
        Returns the datetime, date, or time object as an iso string. If None then an empty string will be returned.
        :param date_time_obj: a datetime, date or time object.
        :return: iso string representation of the date and/or time.
        """
        if date_time_obj is None:
            return ''
        else:
            return date_time_obj.isoformat()

    def getDateTimeFromISOString(self, datetimeisostr):
        """
        A function which returns a datetime object from a datetime ISO string.
        :param datetimeisostr: string representation, in ISO format, of the datetime. If the string is blank or None,
                               then None will be returned.
        :return: a Python datetime object. Can return None.
        """
        if datetimeisostr is None:
            return None
        if datetimeisostr == "":
            return None
        return datetime.datetime.fromisoformat(datetimeisostr)

    def getDateFromISOString(self, dateisostr):
        """
        A function which returns a datetime object from a date ISO string.
        :param dateisostr: string representation, in ISO format, of the datetime. If the string is blank or None,
                               then None will be returned.
        :return: a Python date object. Can return None.
        """
        if dateisostr is None:
            return None
        if dateisostr == "":
            return None
        return datetime.date.fromisoformat(dateisostr)

    def update_file_path(self, input_path, paths_dict):
        """
        A function which updates the input file path by replacing
        using one of the input paths in the dict. If the dict is
        None then ignored and input_path returned. If paths dict
        doesn't contain a path which matches the input_path then
        the input path is returned.
        :param input_path: The input path within which part may by replaced.
        :param paths_dict: a dictionary of paths (keys are the path to replace and value is the replacement value)
        :return: returns path updated is a match was found.
        """
        if paths_dict is None:
            return input_path
        out_path = input_path
        for path_val in paths_dict:
            if path_val in input_path:
                out_path = input_path.replace(path_val, paths_dict[path_val])
                break
        return out_path

    def get_file_basename(self, filepath, checkvalid=False, n_comps=0):
        """
        Uses os.path module to return file basename (i.e., path and extension removed)
        :param filepath: string for the input file name and path
        :param checkvalid: if True then resulting basename will be checked for punctuation
                            characters (other than underscores) and spaces, punctuation
                            will be either removed and spaces changed to an underscore.
                           (Default = False)
        :param n_comps: if > 0 then the resulting basename will be split using underscores
                        and the return based name will be defined using the n_comps
                        components split by under scores.
        :return: basename for file
        """
        import string
        basename = os.path.splitext(os.path.basename(filepath))[0]
        if checkvalid:
            basename = basename.replace(' ', '_')
            for punct in string.punctuation:
                if (punct != '_') and (punct != '-'):
                    basename = basename.replace(punct, '')
        if n_comps > 0:
            basename_split = basename.split('_')
            if len(basename_split) < n_comps:
                raise Exception(
                    "The number of components specified is more than the number of components in the basename.")
            out_basename = ""
            for i in range(n_comps):
                if i == 0:
                    out_basename = basename_split[i]
                else:
                    out_basename = out_basename + '_' + basename_split[i]
            basename = out_basename
        return basename

    def remove_punctuation(self, input_str):
        """
        A function which removes punctuation from an input string. '_' and '-' are converted to spaces and the
        rest will just be removed.

        :param input_str: input string which might have punctuation.
        :return: string with punctuation removed.
        """
        import string
        out_str = input_str.replace('_', ' ')
        out_str = out_str.replace('-', ' ')
        for punct in string.punctuation:
            if (punct != '_') and (punct != '-'):
                out_str = out_str.replace(punct, '')
        return out_str

    def get_file_lock(self, input_file, sleep_period=1, wait_iters=120, use_except=False):
        """
        A function which gets a lock on a file.

        The lock file will be a unix hidden file (i.e., starts with a .) and it will have .lok added to the end.
        E.g., for input file hello_world.txt the lock file will be .hello_world.txt.lok. The contents of the lock
        file will be the time and date of creation.

        Using the default parameters (sleep 1 second and wait 120 iterations) if the lock isn't available
        it will be retried every second for 120 seconds (i.e., 2 mins).

        :param input_file: The input file for which the lock will be created.
        :param sleep_period: time in seconds to sleep for, if the lock isn't available. (Default=1 second)
        :param wait_iters: the number of iterations to wait for before giving up. (Default=120)
        :param use_except: Boolean. If True then an exception will be thrown if the lock is not
                           available. If False (default) False will be returned if the lock is
                           not successful.
        :return: boolean. True: lock was successfully gained. False: lock was not gained.

        """
        file_path, file_name = os.path.split(input_file)
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(file_path, lock_file_name)

        got_lock = False
        for i in range(wait_iters+1):
            if not os.path.exists(lock_file_path):
                got_lock = True
                break
            time.sleep(sleep_period)

        if got_lock:
            c_datetime = datetime.datetime.now()
            f = open(lock_file_path, 'w')
            f.write('{}\n'.format(c_datetime.isoformat()))
            f.flush()
            f.close()
        elif use_except:
            raise EODataDownException("Lock could not be gained for file: {}".format(input_file))

        return got_lock

    def release_file_lock(self, input_file):
        """
        A function which releases a lock file for the input file.

        :param input_file: The input file for which the lock will be created.

        """
        file_path, file_name = os.path.split(input_file)
        lock_file_name = ".{}.lok".format(file_name)
        lock_file_path = os.path.join(file_path, lock_file_name)
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)

    def clean_file_locks(self, dir_path, timeout=3600):
        """
        A function which cleans up any remaining lock file (i.e., if an application has crashed).
        The timeout time will be compared with the time written within the file.

        :param dir_path: the file path to search for lock files (i.e., ".*.lok")
        :param timeout: the time (in seconds) for the timeout. Default: 3600 (1 hours)

        """
        c_dateime = datetime.datetime.now()
        lock_files = glob.glob(os.path.join(dir_path, ".*.lok"))
        for lock_file_path in lock_files:
            create_date_str = self.readTextFileNoNewLines(lock_file_path)
            create_date = datetime.datetime.fromisoformat(create_date_str)
            time_since_create = (c_dateime - create_date).total_seconds()
            if time_since_create > timeout:
                os.remove(lock_file_path)



class EODataDownDatabaseInfo(object):

    def __init__(self, dbConn):
        self.dbConn = dbConn

    def getDBConnection(self):
        return self.dbConn


class EDDPasswordTools(object):

    def encodePassword(self, plaintxt):
        txtencoded = base64.b64encode(plaintxt.encode()).decode()
        return txtencoded

    def unencodePassword(self, txtencoded):
        plaintxt = base64.b64decode(txtencoded.encode()).decode()
        return plaintxt


class EDDCheckFileHash(object):

    def getSigFilePath(self, input_file):
        sig_file = os.path.splitext(input_file)[0]+".sig"
        logger.debug("Signature File Path: '{}'".format(sig_file))
        return sig_file

    def createFileHash(self, input_file):
        filehash = hashlib.md5()
        filehash.update(open(input_file).read().encode())
        return filehash.hexdigest()

    def createFileSig(self, input_file):
        hash_sig = self.createFileHash(input_file)
        logger.debug("Created signature for input file: '{}'".format(input_file))
        sig_file = self.getSigFilePath(input_file)
        f = open(sig_file, "w")
        f.write(hash_sig+"\n")
        f.close()
        logger.debug("Written to signature file: '{}'".format(sig_file))

    def checkFileSig(self, input_file):
        sig_file = self.getSigFilePath(input_file)
        if not os.path.exists(sig_file):
            raise EODataDownException("Signature file could not be found.")

        f = open(sig_file, "r")
        in_hash_sig = f.read().strip()
        f.close()
        logger.debug("Read file signature: '{}'".format(in_hash_sig))
        calcd_hash_sig = self.createFileHash(input_file)
        logger.debug("Calculated file signature: '{}'".format(calcd_hash_sig))
        if calcd_hash_sig == in_hash_sig:
            logger.debug("Signatures Match")
            return True
        logger.info("Signature Does Not Match: '{}' '{}'".format(input_file, calcd_hash_sig))
        return False

    def check_checksum(self, input_file, checksum, block_size=2 ** 13):
        """
        Compare a given MD5 checksum with one calculated from a file.
        :param input_file:
        :param checksum:
        :param block_size:
        :return:
        """
        lcl_checksum = self.calcMD5Checksum(input_file, block_size)
        return lcl_checksum.lower() == checksum.lower()

    def calcMD5Checksum(self, input_file, block_size=2 ** 13):
        """

        :param input_file:
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
        return md5.hexdigest()


class EDDJSONParseHelper(object):

    def readGZIPJSON(self, file_path):
        """
        Function to read a gzipped JSON file returning the data structure produced
        :param file_path:
        :return:
        """
        with gzip.GzipFile(file_path, "r") as fin:  # 4. gzip
            json_bytes = fin.read()                 # 3. bytes (i.e. UTF-8)

        json_str = json_bytes.decode("utf-8")       # 2. string (i.e. JSON)
        data = json.loads(json_str)                 # 1. data
        return data

    def writeGZIPJSON(self, data, file_path):
        """
        Function to write a gzipped json file.
        :param data:
        :param file_path:
        :return:
        """
        json_str = json.dumps(data) + "\n"           # 1. string (i.e. JSON)
        json_bytes = json_str.encode("utf-8")        # 2. bytes (i.e. UTF-8)

        with gzip.GzipFile(file_path, "w") as fout:  # 3. gzip
            fout.write(json_bytes)

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
                raise EODataDownException("Could not find '{}'".format(steps_str))
        if valid_values is not None:
            if curr_json_obj not in valid_values:
                raise EODataDownException("'{}' is not within the list of valid values.".format(curr_json_obj))
        return curr_json_obj

    def getBooleanValue(self, json_obj, tree_sequence):
        """
        A function which retrieves a single boolean value from a JSON structure.
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
        if type(curr_json_obj).__name__ == "bool":
            rtn_bool = curr_json_obj
        else:
            raise EODataDownException("'{}' is not 'True' or 'False'.".format(curr_json_obj))
        return rtn_bool

    def getDateValue(self, json_obj, tree_sequence, date_format="%Y-%m-%d"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :param date_format: a string or list of strings for the date/time format
                                 to be parsed by datetime.datetime.strptime.
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str+":"+tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '{}'".format(steps_str))

        if type(date_format) is str:
            try:
                out_date_obj = datetime.datetime.strptime(curr_json_obj, date_format).date()
            except Exception as e:
                raise EODataDownException(e)
        elif type(date_format) is list:
            found = False
            except_obj = None
            for date_format_str in date_format:
                try:
                    out_date_obj = datetime.datetime.strptime(curr_json_obj, date_format_str).date()
                    found = True
                    break
                except Exception as e:
                    except_obj = e
            if not found:
                raise EODataDownException(except_obj)
        else:
            raise EODataDownException("Do not know what the type is of date_format variable.")

        return out_date_obj

    def getDateTimeValue(self, json_obj, tree_sequence, date_time_format="%Y-%m-%dT%H:%M:%S.%f"):
        """
        A function which retrieves a single date value from a JSON structure.
        :param date_time_format:
        :param json_obj:
        :param tree_sequence: list of strings
        :param date_time_format: a string or list of strings for the date/time format
                                 to be parsed by datetime.datetime.strptime.
        :return:
        """
        curr_json_obj = json_obj
        steps_str = ""
        for tree_step in tree_sequence:
            steps_str = steps_str + ":" + tree_step
            if tree_step in curr_json_obj:
                curr_json_obj = curr_json_obj[tree_step]
            else:
                raise EODataDownException("Could not find '{}'".format(steps_str))

        curr_json_obj = curr_json_obj.replace('Z', '')
        if type(date_time_format) is str:
            try:
                out_datetime_obj = datetime.datetime.strptime(curr_json_obj, date_time_format)
            except Exception as e:
                raise EODataDownException(e)
        elif type(date_time_format) is list:
            found = False
            except_obj = None
            for date_time_format_str in date_time_format:
                try:
                    out_datetime_obj = datetime.datetime.strptime(curr_json_obj, date_time_format_str)
                    found = True
                    break
                except Exception as e:
                    except_obj = e
            if not found:
                raise EODataDownException(except_obj)
        else:
            raise EODataDownException("Do not know what the type is of date_time_format variable.")

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
                raise EODataDownException("Could not find '{}'".format(steps_str))

        if type(curr_json_obj).__name__ != "list":
            raise EODataDownException("Retrieved value is not a list.")
        if valid_values is not None:
            for val in curr_json_obj:
                if type(val).__name__ != "str":
                    raise EODataDownException("'{}' is not of type string.".format(val))
                if val not in valid_values:
                    raise EODataDownException("'{}' is not within the list of valid values.".format(val))
        return curr_json_obj

    def getNumericValue(self, json_obj, tree_sequence, valid_lower=None, valid_upper=None):
        """
        A function which retrieves a single numeric value from a JSON structure.
        :param valid_lower:
        :param valid_upper:
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
                raise EODataDownException("Could not find '{}'".format(steps_str))

        out_value = 0.0
        if (type(curr_json_obj).__name__ == "int") or (type(curr_json_obj).__name__ == "float"):
            out_value = curr_json_obj
        elif type(curr_json_obj).__name__ == "str":
            if curr_json_obj.isnumeric():
                out_value = float(curr_json_obj)
            else:
                try:
                    out_value = float(curr_json_obj)
                except:
                    raise EODataDownException("The identified value is not numeric '{}'".format(steps_str))
        else:
            raise EODataDownException("The identified value is not numeric '{}'".format(steps_str))

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
                raise EODataDownException("Could not find '{}'".format(steps_str))

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

    def getGeoBBoxsCut4LatLonBounds(self, thres=90):
        """
        Where the polygons go over the boundary and therefore looping around the world the wrong way (i.e., creating
        a large polygon).
        :param thres: the threshold over which to define a 'large' polygon.
        :return: a list of EDDGeoBBox objects (must have at least 1).
        """
        # Does East / West cross -180/180 border.
        cut_east_west = False
        cut_east_west_lgr_poly = False
        if (self.east_lon - self.west_lon) > thres:
            cut_east_west = True
            cut_east_west_lgr_poly = True

        # Does North/South cross -90/90 border.
        cut_north_south = False
        cut_north_south_lgr_poly = False
        if (self.north_lat - self.south_lat) > thres:
            cut_north_south = True
            cut_north_south_lgr_poly = True

        out_bboxs = list()
        if cut_east_west or cut_north_south:
            out_tmp_bboxs = list()
            if cut_east_west_lgr_poly:
                geoBBOXWest = EDDGeoBBox()
                geoBBOXWest.setBBOX(self.north_lat, self.south_lat, -180, self.west_lon)
                out_tmp_bboxs.append(geoBBOXWest)
                geoBBOXEast = EDDGeoBBox()
                geoBBOXEast.setBBOX(self.north_lat, self.south_lat, self.east_lon, 180)
                out_tmp_bboxs.append(geoBBOXEast)
            if cut_north_south_lgr_poly:
                for tmpBBOX in out_tmp_bboxs:
                    geoBBOXSouth = EDDGeoBBox()
                    geoBBOXSouth.setBBOX(90, self.north_lat, tmpBBOX.west_lon, tmpBBOX.east_lon)
                    out_bboxs.append(geoBBOXSouth)
                    geoBBOXNorth = EDDGeoBBox()
                    geoBBOXNorth.setBBOX(self.south_lat, -90, tmpBBOX.west_lon, tmpBBOX.east_lon)
                    out_bboxs.append(geoBBOXNorth)
            else:
                out_bboxs = out_tmp_bboxs
        else:
            out_bboxs.append(self)

        return out_bboxs

    def getOGRPolygon(self):
        """
        Create an OGR polygon object.
        :return: OGR Polygon.
        """
        import osgeo.ogr as ogr
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(self.west_lon, self.north_lat)
        ring.AddPoint(self.east_lon, self.north_lat)
        ring.AddPoint(self.east_lon, self.south_lat)
        ring.AddPoint(self.west_lon, self.south_lat)
        ring.AddPoint(self.west_lon, self.north_lat)
        # Create polygon.
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly

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
        wkt_poly = wkt_poly.upper()
        logger.debug("Input wkt_poly = {}".format(wkt_poly))
        if ("MULTIPOLYGON (((" in wkt_poly) or ("MULTIPOLYGON(((" in wkt_poly):
            wkt_poly = wkt_poly.replace("MULTIPOLYGON (((", "")
            wkt_poly = wkt_poly.replace("MULTIPOLYGON(((", "")
            wkt_poly = wkt_poly.replace(")))", "")
        elif ("POLYGON ((" in wkt_poly) or ("POLYGON((" in wkt_poly):
            wkt_poly = wkt_poly.replace("POLYGON ((", "")
            wkt_poly = wkt_poly.replace("POLYGON((", "")
            wkt_poly = wkt_poly.replace("))", "")
        else:
            raise Exception("Did not recongise WKT string - simple function can be applied to POLYGON or MULTIPOLYGON.")
        logger.debug("Edited wkt_poly = {}".format(wkt_poly))
        pts = wkt_poly.split(",")
        min_lon = 0.0
        max_lon = 0.0
        min_lat = 0.0
        max_lat = 0.0
        first = True
        for pt in pts:
            pt = pt.strip()
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


    def getGeoJSONPolygonStr(self, pretty_print=False):
        """

        :return:

        """
        json_dict = dict()
        json_dict["type"] = "Polygon"
        json_dict["coordinates"] = [[[self.west_lon, self.north_lat], [self.east_lon, self.north_lat], [self.east_lon, self.south_lat], [self.west_lon, self.south_lat], [self.west_lon, self.north_lat]]]
        if pretty_print:
            return json.dumps(json_dict, sort_keys=True,indent=4)
        else:
            return json.dumps(json_dict)

    def getGeoJSONPolygon(self):
        """

        :return:

        """
        json_dict = dict()
        json_dict["type"] = "Polygon"
        json_dict["coordinates"] = [[[self.west_lon, self.north_lat], [self.east_lon, self.north_lat], [self.east_lon, self.south_lat], [self.west_lon, self.south_lat], [self.west_lon, self.north_lat]]]
        return json_dict

    def parseGeoJSONPolygon(self, geo_json_poly):
        """
        Populate the object from coordinates dictionary.
        :param geo_json_poly:
        :return:

        """
        min_lon = 0.0
        max_lon = 0.0
        min_lat = 0.0
        max_lat = 0.0

        if (geo_json_poly["type"].lower() == "polygon") or (geo_json_poly["type"].lower() == "multipolygon"):
            first = True
            for pts in geo_json_poly["coordinates"]:
                if geo_json_poly["type"].lower() == "multipolygon":
                    pts = pts[0]
                for pt in pts:
                    lon = pt[0]
                    lat = pt[1]
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
        else:
            raise EODataDownException("GeoJSON should be of type polygon.")

        self.north_lat = max_lat
        self.south_lat = min_lat
        self.west_lon = min_lon
        self.east_lon = max_lon

    def getCSVPolygon(self):
        """
        Get the bounding bbox represented as a polygon as a CSV string.
        :return:

        """
        csv_str = str(self.west_lon) + "," + str(self.north_lat) + "," + \
                  str(self.east_lon) + "," + str(self.north_lat) + "," + \
                  str(self.east_lon) + "," + str(self.south_lat) + "," + \
                  str(self.west_lon) + "," + str(self.south_lat) + "," + \
                  str(self.west_lon) + "," + str(self.north_lat)
        return csv_str

    def getSimpleBBOXStr(self):
        """
        Gets the bounding bbox represented as a simple string [upper-left and lower-right]
        :return: [north, west, south, east]

        """
        bbox_str = "{},{},{},{}".format(self.north_lat, self.west_lon, self.south_lat, self.east_lon)
        return bbox_str

    def getBBOXLLURStr(self):
        """
        Gets the bounding bbox represented as a simple string [upper-left and lower-right]
        :return: [north, west, south, east]

        """
        bbox_str = "{},{},{},{}".format(self.west_lon, self.south_lat, self.east_lon, self.north_lat)
        return bbox_str


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

    def downloadFileContinue(self, input_url, input_url_md5, out_file_path, username, password, exp_file_size, continue_download=True):
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

        edd_file_checker = EDDCheckFileHash()
        if os.path.exists(out_file_path):
            logger.debug("Output file is already present, checking MD5.")
            md5_match = edd_file_checker.check_checksum(out_file_path, input_url_md5)
            if not md5_match:
                logger.debug("MD5 did not match for the existing file so deleted and will download.")
                os.remove(out_file_path)
            else:
                logger.info("The output file already exists and the MD5 matched so not downloading: {}".format(out_file_path))
                return True

        logger.debug("Creating HTTP Session Object.")
        session_http = requests.Session()
        session_http.auth = (username, password)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_http.headers["User-Agent"] = user_agent

        temp_dwnld_path = out_file_path + '.incomplete'
        needs_downloading = True
        if os.path.exists(temp_dwnld_path) and continue_download:
            if os.path.getsize(temp_dwnld_path) > exp_file_size:
                os.remove(temp_dwnld_path)
                needs_downloading = True
                logger.debug("There was an existing file but too large removed and starting download again: " + out_file_path)
            elif os.path.getsize(temp_dwnld_path) == exp_file_size:
                md5_match = edd_file_checker.check_checksum(temp_dwnld_path, input_url_md5)
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

            with session_http.get(input_url, stream=True, auth=session_http.auth, headers=headers) as r:
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
            md5_match = edd_file_checker.check_checksum(temp_dwnld_path, input_url_md5)
            if md5_match:
                os.rename(temp_dwnld_path, out_file_path)
                logger.info("MD5 Matched Renamed download: ".format(out_file_path))
                return True
            else:
                logger.info("MD5 did not match: ".format(temp_dwnld_path))
            return False

    def downloadFile(self, input_url, input_url_md5, out_file_path, username, password):
        """

        :param input_url:
        :param input_url_md5:
        :param out_file_path:
        :param username:
        :param password:
        :return:
        """
        edd_file_checker = EDDCheckFileHash()
        if os.path.exists(out_file_path):
            logger.debug("Output file is already present, checking MD5.")
            md5_match = edd_file_checker.check_checksum(out_file_path, input_url_md5)
            if not md5_match:
                logger.debug("MD5 did not match for the existing file so deleted and will download.")
                os.remove(out_file_path)
            else:
                logger.info(
                    "The output file already exists and the MD5 matched so not downloading: {}".format(out_file_path))
                return True

        logger.debug("Creating HTTP Session Object.")
        session_http = requests.Session()
        session_http.auth = (username, password)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_http.headers["User-Agent"] = user_agent

        temp_dwnld_path = out_file_path + '.incomplete'

        headers = {}
        downloaded_bytes = 0

        usr_update_step = 5000000
        next_update = usr_update_step

        with session_http.get(input_url, stream=True, auth=session_http.auth, headers=headers) as r:
            self.checkResponse(r, input_url)
            chunk_size = 2 ** 20
            mode = 'wb'

            with open(temp_dwnld_path, mode) as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        downloaded_bytes = downloaded_bytes + len(chunk)
                        if downloaded_bytes > next_update:
                            logger.info("Downloaded {} of {}".format(downloaded_bytes, temp_dwnld_path))
                            next_update = next_update + usr_update_step
        logger.info("Download Complete: ".format(temp_dwnld_path))
        md5_match = edd_file_checker.check_checksum(temp_dwnld_path, input_url_md5)
        if md5_match:
            os.rename(temp_dwnld_path, out_file_path)
            logger.info("MD5 Matched Renamed download: ".format(out_file_path))
            return True
        else:
            logger.info("MD5 did not match: ".format(temp_dwnld_path))
        return False

    def downloadFileNoMD5Continue(self, input_url, out_file_path, username, password, exp_file_size, check_file_size_exists=True, continue_download=True):
        """

        :param check_file_size_exists:
        :param input_url:
        :param out_file_path:
        :param username:
        :param password:
        :param exp_file_size:
        :param continue_download:
        :return:
        """
        if os.path.exists(out_file_path):
            logger.debug("Output file is already present")
            if check_file_size_exists:
                logger.debug("Checking file size")
                file_size = os.path.getsize(out_file_path)
                if file_size == exp_file_size:
                    logger.info("The output file already exists and the file size matched so not downloading: {}".format(out_file_path))
                    return True
                else:
                    logger.debug("The file exists and the file size did not match so deleting ready for download.")
                    os.remove(out_file_path)
            else:
                logger.debug("The file exists so deleting ready for download.")
                os.remove(out_file_path)


        logger.debug("Creating HTTP Session Object.")
        session_http = requests.Session()
        session_http.auth = (username, password)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_http.headers["User-Agent"] = user_agent

        temp_dwnld_path = out_file_path + '.incomplete'
        needs_downloading = True
        if os.path.exists(temp_dwnld_path) and continue_download:
            if os.path.getsize(temp_dwnld_path) > exp_file_size:
                os.remove(temp_dwnld_path)
                needs_downloading = True
                logger.debug("There was an existing file but too large removed and starting download again: " + out_file_path)
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

            with session_http.get(input_url, stream=True, auth=session_http.auth, headers=headers) as r:
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
            if os.path.getsize(temp_dwnld_path) >= exp_file_size:
                logger.info("File size is at least as big as expected: ".format(out_file_path))
                os.rename(temp_dwnld_path, out_file_path)
                logger.info("Renamed download: ".format(out_file_path))
                return True
            return False

    def downloadFileNoMD5(self, input_url, out_file_path, username, password):
        """

        :param input_url:
        :param input_url_md5:
        :param out_file_path:
        :param username:
        :param password:
        :return:
        """
        print("HERE")
        logger.debug("Creating HTTP Session Object.")
        session_http = requests.Session()
        session_http.auth = (username, password)
        user_agent = "eoedatadown/" + str(eodatadown.EODATADOWN_VERSION)
        session_http.headers["User-Agent"] = user_agent

        temp_dwnld_path = out_file_path + '.incomplete'

        headers = {}
        downloaded_bytes = 0

        usr_update_step = 5000000
        next_update = usr_update_step

        try:
            with session_http.get(input_url, stream=True, auth=session_http.auth, headers=headers) as r:
                self.checkResponse(r, input_url)
                chunk_size = 2 ** 20
                mode = 'wb'

                with open(temp_dwnld_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            downloaded_bytes = downloaded_bytes + len(chunk)
                            if downloaded_bytes > next_update:
                                logger.info("Downloaded {} of {}".format(downloaded_bytes, temp_dwnld_path))
                                next_update = next_update + usr_update_step
            logger.info("Download Complete: ".format(temp_dwnld_path))
            os.rename(temp_dwnld_path, out_file_path)
        except Exception as e:
            print(e)
            return False
        return True


class EODDFTPDownload(object):

    def traverseFTP(self, ftp_conn, ftp_path, ftp_files, try_n_times):
        """

        :param ftp_conn:
        :param ftp_path:
        :param ftp_files: dictionary
        :param try_n_times: if server connection fails try again (sleeping for 5 secs in between) n times for failing.
        :return:
        """
        dirs = list()
        nondirs = list()
        if ftp_path not in ftp_files:
            ftp_files[ftp_path] = list()
        count = 0
        for i in range(try_n_times):
            if count > try_n_times:
                break
            count = count + 1
            try:
                dir_lst = ftp_conn.mlsd(ftp_path, ["type"])
                break
            except Exception as e:
                logger.error("FTP connection failed but trying again: {0}".format(e))
                time.sleep(5)
                continue
        if count > try_n_times:
            raise EODataDownException("Tried multiple times which failed to get directory listing on FTP server so failing.")

        for item in dir_lst:
            if (item[1]['type'] == 'dir') and ((item[0][0] == 'S') or (item[0][0] == 'N')):
                c_dir = os.path.join(ftp_path, item[0])
                dirs.append(c_dir)
                if c_dir not in ftp_files:
                    ftp_files[c_dir] = list()
                logger.debug("Found a directory: {}".format(c_dir))
            elif not ((item[0] == '.') or (item[0] == '..')):
                c_file = os.path.join(ftp_path, item[0])
                nondirs.append(c_file)
                ftp_files[ftp_path].append(c_file)
                logger.debug("Found a file: {}".format(c_file))

        if not nondirs:
            return nondirs

        for subdir in sorted(dirs):
            tmpFilesLst = self.traverseFTP(ftp_conn, subdir, ftp_files, try_n_times)
            nondirs = nondirs + tmpFilesLst
        return nondirs

    def getFTPFileListings(self, ftp_url, ftp_path, ftp_user, ftp_pass, ftp_timeout=None, try_n_times=5):
        """
        Traverse the FTP server directory structure to create a list of all the files (full paths)
        :param ftp_url:
        :param ftp_path:
        :param ftp_user:
        :param ftp_pass:
        :param ftp_timeout: in seconds (None and system default will be used; system defaults are usual aboue 300 seconds)
        :param try_n_times: if server connection fails try again (sleeping for 5 secs in between) n times for failing.
        :return: directory by directory and simple list of files as tuple
        """
        ftp_files = dict()
        logger.debug("Opening FTP Connection to {}".format(ftp_url))
        ftp_conn = ftplib.FTP(ftp_url, user=ftp_user, passwd=ftp_pass, timeout=ftp_timeout)
        ftp_conn.login()
        logger.info("Traverse the file system and get a list of paths")
        nondirslst = self.traverseFTP(ftp_conn, ftp_path, ftp_files, try_n_times)
        logger.info("Fiinshed traversing the ftp server file system.")
        return ftp_files, nondirslst

    def downloadFile(self, url, remote_path, local_path, time_out=None, username=None, password=None):
        """

        :param url:
        :param remote_path:
        :param local_path:
        :param time_out: (default 300 seconds if None)
        :param username:
        :param password:
        :return:
        """
        full_path_url = url+remote_path
        success = False
        try:
            if time_out is None:
                time_out = 300

            fp = open(local_path, "wb")
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL, full_path_url)
            curl.setopt(pycurl.FOLLOWLOCATION, True)
            curl.setopt(pycurl.NOPROGRESS, 0)
            curl.setopt(pycurl.FOLLOWLOCATION, 1)
            curl.setopt(pycurl.MAXREDIRS, 5)
            curl.setopt(pycurl.CONNECTTIMEOUT, 50)
            curl.setopt(pycurl.TIMEOUT, time_out)
            curl.setopt(pycurl.FTP_RESPONSE_TIMEOUT, 600)
            curl.setopt(pycurl.NOSIGNAL, 1)
            if (not username is None) and (not password is None):
                curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_ANY)
                curl.setopt(pycurl.USERPWD, username + ':' + password)
            curl.setopt(pycurl.WRITEDATA, fp)
            logger.info("Starting download of {}".format(full_path_url))
            curl.perform()
            logger.info("Finished download in {0} of {1} bytes for {2}".format(curl.getinfo(curl.TOTAL_TIME), curl.getinfo(curl.SIZE_DOWNLOAD), full_path_url))
            success = True
        except:
            logger.error("An error occurred when downloading {}.".format(os.path.join(url, remote_path)))
            success = False
        return success


class EODDWGetDownload(object):

    def downloadFile(self, input_url, out_file_path, username=None, password=None, try_number="10", time_out="60", input_url_md5=None):
        """
        A function which downloads a file from a url using the wget command line tool.
        If a username or password are provided then both must be provided.
        :param input_url_md5:
        :param input_url: string with the URL to be downloaded.
        :param out_file_path: output file name and path.
        :param username: username for the download, if required. Default is None meaning it will be ignored.
        :param password: password for the download, if required. Default is None meaning it will be ignored.
        :param try_number: number of attempts at the download. Default is 10.
        :param time_out: number of seconds to time out Default is 60.
        :return: value 1 as success and 0 as not successful.
        """
        try_number = str(try_number)
        time_out = str(time_out)
        success = False
        command = ["wget", "-c", "-P", out_file_path, "-t", try_number, "-T", time_out, "--no-check-certificate"]
        if (username is not None) and (password is not None):
            command.append("--user")
            command.append(username)
            command.append("--password")
            command.append(password)
        command.append(input_url)
        download_state = -1
        try:
            download_state = subprocess.call(command)
        except Exception as e:
            logger.debug(e.__str__())
            logger.info("Download of file ({0}) failed.".format(out_file_path))
        if download_state == 0:
            logger.info("Successfully downloaded file: {}".format(out_file_path))
            if input_url_md5 is not None:
                edd_file_checker = EDDCheckFileHash()
                md5_match = edd_file_checker.check_checksum(out_file_path, input_url_md5)
                if md5_match:
                    logger.info("MD5 matches for the downloaded file: {}".format(out_file_path))
                    success = True
                else:
                    logger.info("MD5 does not match for the downloaded file: {}".format(out_file_path))
                    success = False
            else:
                success = True
        else:
            success = False
            logger.info("File being downloaded did not successfully complete: {}".format(out_file_path))
        return success

class EODDDefineSensorROI(object):

    def findSensorROI(self, sensor_lut_file, sensor_lst, roi_vec_file, roi_vec_lyr, output_file):
        """
        A function which uses a vector ROI to find the sensor location definitions.
        :param sensor_lut_file:
        :param sensor_lst:
        :param roi_vec_file:
        :param roi_vec_lyr:
        :param output_file:
        :return:
        """
        try:
            import rsgislib.vectorutils
            vec_wkt_str = rsgislib.vectorutils.getProjWKTFromVec(roi_vec_file, roi_vec_lyr)
            rsgis_utils = rsgislib.RSGISPyUtils()
            epsg_code = rsgis_utils.getEPSGCodeFromWKT(vec_wkt_str)
            if epsg_code == 4326:
                raise Exception("The input ROI vector layer should be in WGS 84 projection (EPSG 4326).")

            outvalsdict = dict()
            if 'Landsat' in sensor_lst:
                lsatts = rsgislib.vectorutils.getAttLstSelectFeats(sensor_lut_file, 'landsat_wrs2_lut', ['PATH', 'ROW'],
                                                                   roi_vec_file, roi_vec_lyr)
                lstiles = list()
                for tile in lsatts:
                    lstiles.append({"path":tile['PATH'], "row":tile['ROW']})
                outvalsdict['landsat'] = lstiles

            if 'Sentinel2' in sensor_lst:
                sen2atts = rsgislib.vectorutils.getAttLstSelectFeats(sensor_lut_file, 'sen2_tiles_lut', ['Name'],
                                                                     roi_vec_file, roi_vec_lyr)
                sen2tiles = list()
                for tile in sen2atts:
                    sen2tiles.append(tile['Name'])
                sen2tilesset = set(sen2tiles)
                outvalsdict['sentinel2'] = list(sen2tilesset)

            if 'JAXADegTiles' in sensor_lst:
                raise Exception("The polygons for the JAXA tiles need adding to the database.")

            if 'OtherBBOX' in sensor_lst:
                envs = rsgislib.vectorutils.getFeatEnvs(roi_vec_file, roi_vec_lyr)
                bboxlst = list()
                for env in envs:
                    env_dict = dict()
                    env_dict["north_lat"] = env[3]
                    env_dict["south_lat"] = env[2]
                    env_dict["east_lon"] = env[1]
                    env_dict["west_lon"] = env[0]
                    bboxlst.append(env_dict)
                outvalsdict['other'] = bboxlst

            with open(output_file, 'w') as outfile:
                json.dump(outvalsdict, outfile, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)

        except Exception as e:
            logger.error("Failed to create sensor ROI file using LUT ({0}) for ROI ({1}).".format(sensor_lut_file,
                                                                                                  roi_vec_file))
            raise(e)


