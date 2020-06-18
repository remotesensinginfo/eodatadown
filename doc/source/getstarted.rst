Get Started
===========

Install
--------

The easiest way to get started it to our the docker image (petebunting/eodatadown) already built and available for download. 

Pull Docker Image::
    
    docker pull petebunting/eodatadown

If you are on a shared system (e.g., cluster) then it is probably beneficial to use Singularity::

    singularity build eodatadown.sif docker://petebunting/eodatadown


Setup Database
---------------
EODataDown uses a `postgreSQL <https://www.postgresql.org>`_ database to store the system data. Assuming you already have a postgreSQL installation available and the server is running you need to create the database for EODataDown to use.

Create database (assume you are running this on the machine with the database server running)::

    createdb eodd_test_db

Create user within the database:

Run psql::
    
    psql eodd_test_db
    
Setup User::

    CREATE USER eodduser WITH PASSWORD 'userpass';
    GRANT ALL PRIVILEGES ON DATABASE eodd_test_db TO eodduser;
    

Create Configuration Files
---------------------------

EODataDown is reliant on a set of configuration files to run. The overall configuration file links to the others and is passed to all the command line tools (or environmental variable EDD_MAIN_CFG can be defined). 

Main configuration file example::

    {
        "eodatadown":
        {
            "details":
            {
                "name":"Demo System",
                "description":"System to demonstrate how to create a simple EODataDown instance."
            },
            "database":
            {
                "connection":"postgresql://eodduser:userpass@127.0.0.1:5432/eodd_test_db"
            },
            "sensors":
            {
                "LandsatGOOG":
                {
                    "config":"/home/eodatadown/config/LandsatGoog.json"
                },
                "Sentinel2GOOG":
                {
                    "config":"/home/eodatadown/config/Sentinel2Goog.json"
                }
            }
        }
    }



