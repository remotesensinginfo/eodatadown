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
import logging

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
    secret_key = 'I7Cpan66nlslFqKyuUIkc1puFzeUHlg4'

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
        sig_file = os.path.splitext(input_file)[0]+'.sig'
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
        f.write(hash_sig+'\n')
        f.close()
        logger.debug("Written to signature file: '" + sig_file + "'")

    def checkFileSig(self, input_file):
        sig_file = self.getSigFilePath(input_file)
        if not os.path.exists(sig_file):
            raise EODataDownException('Signature file could not be found.')

        f = open(sig_file, "r")
        in_hash_sig = f.read().strip()
        f.close()
        logger.debug("Read file signature: '" + in_hash_sig + "'")
        calcd_hash_sig = self.createFileHash(input_file)
        logger.debug("Calculated file signature: '" + calcd_hash_sig + "'")
        if calcd_hash_sig == in_hash_sig:
            logger.debug("Signatures Match")
            return True
        logger.debug("Signatures Do Not Match: " + input_file)
        return False
