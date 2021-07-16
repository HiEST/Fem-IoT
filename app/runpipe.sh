#!/bin/sh

# Configurable variables
# Local
# HDFS_SERVER="hdfs://172.15.1.10:9000"
# IHS="hdfs:///data/anon_ihs_test.csv"
# AIS="hdfs:///data/anon_2016-01.csv"
# TMP="hdfs:///data/calc/"
# CSV="hdfs:///data/calc_emis"

# ENMA
HDFS_SERVER="hdfs://master1.internal:9000"
IHS="hdfs:///user/ubuntu/emis_femiot/data/anon_ihs_test.csv"
AIS="hdfs:///user/ubuntu/emis_femiot/data/anon_2016-01.csv"
TMP="hdfs:///user/ubuntu/emis_femiot/data/calc/"
CSV="hdfs:///user/ubuntu/emis_femiot/data/calc_emis"


# Use distributed or not (Spark submit or direct execution of the script)
DISTRIBUTED=false


# Include functions
runjob_local() {
    printf " with args locally: $1n"
    export PATH=$PATH+":$SPARK_HOME/bin"
    #../submit.sh
    python src/main.py --hdfs ${HDFS_SERVER} $1
}

hassucceed() {
    if [ $1 -ne 0 ]; then
        printf "Application failed at $2\n"
        exit
    fi
}

runjob() {
    printf "Running stage $1"
    COM="--job $1 --job-args $2"
    if [ "$DISTRIBUTED" = true ] ; then
        runjob_docker "$COM"
    else 
        runjob_local "$COM"
    fi
    hassucceed $? $1
}



############################## PIPE ############################################


# Load Metadata
runjob "load_metadata" "file=$IHS output_file=${TMP}/ihs.parquet"

## Ingest CSV
runjob "ingest_csv" "file=$AIS output_file=${TMP}/ais.parquet"

# Compute emissions (15 minute interpolation)
runjob "compute_emissions" "model=STEAM2 input_data=${TMP}/ais.parquet
        input_metadata=${TMP}/ihs.parquet output_file=${TMP}/emis.parquet
        step=10 interpolation_lim=900"

## Write CSV
runjob "export_csv" "input_file=${TMP}/emis.parquet output_file=${CSV}"
