#!/bin/bash
echo "Starting upload weeekly geoshapes: $(date)"
/home/ubuntu/miniconda2/bin/python /home/ubuntu/geoShps/pydev/ShapeFileToSocrataUpdateLoad.py -u weekly > /home/ubuntu/geoShps/logs/weeklyShps_log.txt