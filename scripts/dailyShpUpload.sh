#!/bin/bash
echo "Starting uploading annual geoshapes: $(date)"
/home/ubuntu/miniconda2/bin/python /home/ubuntu/geoShps/pydev/ShapeFileToSocrataUpdateLoad.py -u annual > /home/ubuntu/geoShps/logs/annualShps_log.txt