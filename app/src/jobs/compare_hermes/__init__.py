# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, from_unixtime, month, sum,
                                   count)

import tempfile
import sys
import pandas as pd
from shared.msg_types import prt_warn, prt_high

import seaborn as sns
import matplotlib.pyplot as plt


from shared.reporting.reporting_funs import log_dataframe_metric

try:
    import mlflow
except ImportError:
    prt_warn("MLFlow is not installed. Not tracking the experiment.")


__author__ = 'agutierrez'


from time import gmtime, strftime


def emis_summary(emis):
    summ = emis.withColumn('nox', col('nox_me') + col('nox_ae'))\
        .withColumn('sox', col('sox_me') + col('sox_ae'))\
        .withColumn('co2', col('co2_me') + col('co2_ae'))\
        .withColumn('time', to_date(from_unixtime(col('time'))))\
        .withColumn('month', month(col('time')))

    print(strftime("%z", gmtime()))
    print(summ.groupby(col('month')).agg(count(col('month'))).show())

    grp = summ.groupby(["hermes_type", "month"])\
        .agg(
            sum(col('nox')).alias('NOx'),
            sum(col('sox')).alias('SOx'),
            sum(col('co2')).alias('CO2')
         ).withColumnRenamed("hermes_type", "type").toPandas()

    return(grp)


def process_hermes(csv_file):
    hermes = pd.read_csv(csv_file)
    hermes["timest"] = pd.to_datetime(hermes["timest"])
    hermes["month"] = hermes["timest"].dt.month

    hermes_grp = hermes.groupby(["type", "month"]).sum()
    return(hermes_grp)


def emis_diff(emis, hermes, model_suffix="model"):
    model_suffix = '_' + model_suffix
    diff = emis.join(hermes, on=["type", "month"], lsuffix=model_suffix,
                     rsuffix='_hermes')
    diff['NOx_diff'] = diff['NOx'+model_suffix] - diff['NOx_hermes']
    diff['SOx_diff'] = diff['SOx'+model_suffix] - diff['SOx_hermes']
    diff['CO2_diff'] = diff['CO2'+model_suffix] - diff['CO2_hermes']

    diff['NOx_perc'] = diff['NOx'+model_suffix] / diff['NOx_hermes'] * 100
    diff['SOx_perc'] = diff['SOx'+model_suffix] / diff['SOx_hermes'] * 100
    diff['CO2_perc'] = diff['CO2'+model_suffix] / diff['CO2_hermes'] * 100
    return(diff)


def log_diff_by_type(diff):
    types = diff.type.unique()
    for t in types:  # for each type
        dt = diff[diff.type == t].drop(columns='type')
        dt = dt.add_prefix(t+'_')
        log_dataframe_metric(dt, t+'_'+'month')


def pivot_diff_df(data, models, pols):
    polmod = pd.DataFrame(
            columns=['type', 'month', 'pollutant', 'model', 'value'])

    for _, row in data.iterrows():
        for m in models:
            for p in pols:
                var = p+'_'+m
                polmod = polmod.append(
                        {
                            'type': row['type'],
                            'pollutant': p,
                            'model': m,
                            'month': row['month'],
                            'value': row[var]
                        },
                        ignore_index=True)
    return(polmod)


def pol_barplot_by_type(data, types, pollutants, output_folder="barplot"):
    import os
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for t in types:
        for pol in pollutants:
            pol_data = data[(data['pollutant'] == pol) & (data['type'] == t)]
            plt.figure()
            sns.barplot(y="value", x="month", hue="model",
                        data=pol_data).set_title(t+"_"+pol)\
               .get_figure().savefig(output_folder+'/'+t+"_"+pol+".png")


def analyze(spark: SparkSession, input_file='/ships_data/IHSData.txt',
            hermes_file='/ships_data/day_summary.csv', model='',
            output_file='hermes_comparison.csv'):
    if not output_file:  # No path, use tmp
        output_file = tempfile.mkdtemp() + '/hermes_comparison.csv'
    prt_high(
        """
        Running comparison with HERMES
        ##################################
        Parameters
         - Input file:  {}
         - HERMES file:  {}
         - Model: {}
         - Output file: {}
        ##################################
        """.format(input_file, hermes_file, model, output_file))

    emis = spark.read.parquet(input_file)
    summ = emis_summary(emis)
    hermes = process_hermes(hermes_file)
    diff = emis_diff(summ, hermes, model)
    diff.to_csv(output_file)

    if 'mlflow' in sys.modules:
        prt_high("Logging MLFlow artifacts")
        mlflow.log_artifact(output_file, "hermes_comparison.csv")
        log_diff_by_type(diff)

        pols = ["NOx", "SOx", "CO2"]
        models = ["hermes", model]
        piv = pivot_diff_df(diff, models, pols)
        types = diff.type.unique()
        pol_barplot_by_type(piv, types, pols)
        mlflow.log_artifact("barplot")

    return
