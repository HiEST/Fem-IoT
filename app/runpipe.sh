#!/bin/bash

# Configurable variables
# Local
# HDFS_SERVER="hdfs://172.15.1.10:9000"
# IHS="hdfs:///data/anon_ihs_test.csv"
# AIS="hdfs:///data/anon_2016-01.csv"
# TMP="hdfs:///data/calc/"
# OUT="hdfs:///data/calc_emis"

# ENMA
# HDFS_SERVER="hdfs://master1.internal:9000"
# IHS="hdfs:///user/ubuntu/emis_femiot/data/anon_ihs_test.csv"
# AIS="hdfs:///user/ubuntu/emis_femiot/data/anon_2016-01.csv"
# TMP="hdfs:///user/ubuntu/emis_femiot/data/calc/"
# OUT="hdfs:///user/ubuntu/emis_femiot/data/calc_emis"


PARAM_COUNT=0
NUM_PARAM=5

# Use distributed or not (Spark submit or direct execution of the script)
DISTRIBUTED=false

# Argument parsing
for ARGUMENT in "$@"
do

    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)   

    case "$KEY" in
        HDFS_SERVER)    HDFS_SERVER=${VALUE}; ((PARAM_COUNT++));;
        IHS)            IHS=${VALUE}; ((PARAM_COUNT++));;
        AIS)            AIS=${VALUE}; ((PARAM_COUNT++));;
        TMP)            TMP=${VALUE}; ((PARAM_COUNT++));;
        OUT)            OUT=${VALUE}; ((PARAM_COUNT++));;
        *)   
    esac    


done

echo $PARAM_COUNT
# Exit if params are not given
if [ $PARAM_COUNT -lt $NUM_PARAM ]
then
    echo "Incorrect number of paremeters"
    echo "Usage: ./runpipe.sh HDFS_SERVER=\"Server\" IHS=\"IHS file\" AIS=\"AIS File\" TMP=\"Temporary Folder\" OUT=\"Output Folder\""
    exit
fi

echo
echo "Running pipeline..."
echo " - Server: $HDFS_SERVER"
echo " - IHS Input: $IHS"
echo " - AIS Input: $AIS"
echo " - Temporary folder: $TMP"
echo " - Output folder: $OUT"
echo


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
runjob "export_csv" "input_file=${TMP}/emis.parquet output_file=${OUT}"

## Write summary
runjob "compute_summary" "input_file=${TMP}/emis.parquet hdfs_path=${OUT}"
