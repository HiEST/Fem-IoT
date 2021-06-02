#!/bin/sh

# Do local work
#HDFS_SERVER=''
#IHS=file:///home/data/femiot/anon_ihs_test.csv
#AIS=file:///home/data/femiot/anon_2016-01.csv
#CSV=file:///home/data/femiot/calc_emis

# Connect to the namenode
HDFS_SERVER=hdfs://172.15.1.10:9000
IHS=hdfs:///data/femiot/anon_ihs_test.csv
AIS=hdfs:///data/femiot/anon_2016-01.csv
CSV=hdfs:///data/femiot/calc_emis


mlflow run . --no-conda \
    -P hdfs=$HDFS_SERVER \
    -P ihs_path=$IHS \
    -P ais_path=$AIS \
    -P model="STEAM,STEAM2" -P export_db=False -P unit="kg" -P step=60 \
    -P ae_on_lim=0 \
    -P csv_output=$CSV
