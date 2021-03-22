#!/bin/sh

# HERMES
# mlflow run . --no-conda -P ihs_path=file:///home/fubu/data/IHS/ihs_data.csv \
#     -P ais_path=file:///home/fubu/data/AIS/AIS2016/abel_2016-01.csv \
#     -P model="STEAM,STEAM2" -P export_db=False -P unit="kg" -P step=60 \
#     -P \
#     hermes_file=file:///home/fubu/data/HERMES/day_summary.csv \
#     -P ae_on_lim=0
# Remember that there is a --no-conda parameter!

# NO HERMES
mlflow run . \
    -P ihs_path=file:///home/fubu/data/IHS/2021-02-24-ihs_data_basic.csv \
    -P ais_path=file:///home/fubu/data/AIS/AIS2016/abel_2016-01.csv \
    -P model="STEAM,STEAM2" -P export_db=False -P unit="kg" -P step=60 \
    -P ae_on_lim=0


