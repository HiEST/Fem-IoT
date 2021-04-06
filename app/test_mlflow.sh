#!/bin/sh

mlflow run . --no-conda\
    -P ihs_path=file:///home/data/anon_ihs_test.csv \
    -P ais_path=file:///home/data/anon_2016-01.csv \
    -P model="STEAM,STEAM2" -P export_db=False -P unit="kg" -P step=60 \
    -P ae_on_lim=0 \
    -P csv_output=file:///home/data/calc_emis
