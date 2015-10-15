#### Setting up your Environment

* Install Ruby 2.2.0

#### Getting Started

* Add Two files
  * `environment.json` which will have content `"current_environment" : "development"`
  * `development.json` which will have connection details for two databases

    {
        "datahub": {
            "port": ,
            "database": "datastory2",
            "host": "",
            "username": "",
            "password": ""
        }
    }
    
  * The file name should be same as the environment you set. The possible values for environment are `development|production|staging`

Login to pgAdmin and manually create database with name: datastory_datasets and run pg_custom_functions.sql

$ git clone git@github.com:europeana/europeana-statistics-grape.git

$ cd europeana-statistics-grape

$ rackup
