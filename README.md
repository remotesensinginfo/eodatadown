# EODataDown

A tool for automatically downloading Earth Observation imagery and products.

## Depdencies

* PyCrypto [https://www.dlitz.net/software/pycrypto/]
* SQLAlchemy [http://www.sqlalchemy.org]

## Configuration

There are a number of configuration files which need to created for the system to work

### Overall System

An example is within the share directory of the source code.

### Logging

EODataDown uses the python logging library. A general configuration suitable for most users is installed alongside the source <install_path>/share/eodatadown. This can be edited or the EDD_LOG_CFG variable used to use a different configuration file. 


## Environmental Variables

* EDD_LOG_CFG - specify the location of a JSON file configuring the python logging system.
