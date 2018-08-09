# EODataDown

A tool for automatically downloading Earth Observation imagery and products.

## Dependencies

* PyCrypto [https://www.dlitz.net/software/pycrypto/]
* SQLAlchemy [http://www.sqlalchemy.org]
* RSGISLib [http://www.rsgislib.org]
* ARCSI [http://arcsi.remotesensing.info]

### Google Services
For google services the following dependencies need to installed. 

* google-api-core 
* google-cloud-core
* google-auth
* google-cloud-bigquery

[https://github.com/GoogleCloudPlatform/google-cloud-python]

## Installation

Using conda and conda-forge the dependencies can be installed within the following command:

conda install -c conda-forge pycrypto sqlalchemy google-cloud-bigquery arcsi

## Configuration

There are a number of configuration files which need to created for the system to work

### Overall System

An example is within the share directory of the source code.

### Logging

EODataDown uses the python logging library. A general configuration suitable for most users is installed alongside the source <install_path>/share/eodatadown. This can be edited or the EDD_LOG_CFG variable used to use a different configuration file. 


## Environmental Variables

* EDD_MAIN_CFG - specify the location of a JSON file configuring the system.
* EDD_LOG_CFG - specify the location of a JSON file configuring the python logging system.
* EDD_NCORES - specify the number of cores to use when running jobs which can use multiple cores.


## Notes for Sensors

### Landsat From Google.
To use this sensor you need to set up a google account and project. While the data and download is free an account is still required. See https://cloud.google.com/resource-manager/docs/creating-managing-projects

Details of the account will need to be specified in the JSON config file for the LandsatGOOG sensor.


## Setting up Google Accounts

See the following page for setting up a project: 

https://cloud.google.com/resource-manager/docs/creating-managing-projects 

And then the following page for setting up your credentials:
  
https://cloud.google.com/bigquery/docs/reference/libraries

