# Shp-To-Socrata

## Takes geo shp files and migrates them up to the Socrata Geo API

### The primary goal of this repo is to migrate geo shape files to the Socrata naive geo API.

The city of SF has about 62 datasets that were hosted on Socrata as zipped shape files
We wanted these to be availible to users as geojson, and other formats.
This is the script we used to migrate them. 

### This repo does the following:
* downloads a shapefile from an URL
* opens the zipp file and reads it to a geopandas df
* reprojects the geodataset to web mercator
* Creates the initial dataset schema in Socrata from the schema found in the shape file
* Converts the geodata in the shape file to geojson
* Inserts/Uploads data from the shape file to Socrata using the socrata API; sends the data in chunks as json post requests as opposed to try to upload an entire file.
* Supports multiple retries; checks to make sure all the data in the file made it up there.
* Sends email notifications to receipts to let users know if the job was successful or notifications
* Also includes scripts for updating/upserting the geodatasets

### Other features of this library include:
* config files to set up socrata client and emailer
* config file to set up schema of initial dataset load list and various directories used in the scripts
* Important to note, you will need to provide a csv that outlines the geodatasets that your are trying to migrate/update. See the file configs/sample_csv_geodataset_list.csv


