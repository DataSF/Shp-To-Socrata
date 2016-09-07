#!/bin/bash
echo "Starting uploading daily geoshapes: $(date)"
/home/ubuntu/miniconda2/bin/python /home/ubuntu/geoShps/pydev/ShapeFileToSocrataUpdateLoad.py -u daily > /home/ubuntu/geoShps/logs/dailyShps_log.txt