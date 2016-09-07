#!/bin/bash
echo "Starting uploading monthly geoshapes: $(date)"
/home/ubuntu/miniconda2/bin/python /home/ubuntu/geoShps/pydev/ShapeFileToSocrataUpdateLoad.py -u monthly > /home/ubuntu/geoShps/logs/montlyShps_log.txt