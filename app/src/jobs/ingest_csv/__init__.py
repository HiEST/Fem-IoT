# pylint: disable=no-name-in-module
import calendar
import time
import sys


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col
from pyspark.sql.types import StringType, BooleanType, IntegerType, FloatType
from shared.data_cleaning import change_column_type, ensure_columns_type

from shared.msg_types import prt_warn, prt_high

import tempfile

try:
    import mlflow
except ImportError:
    prt_warn("MLFlow is not installed. Not tracking the experiment.")


from numpy import nan

__author__ = 'denisovlev & agutierrez'


def analyze(spark: SparkSession, file='/ships_data/PortSubset-small.csv',
            output_file='ais_data.parquet'):
    if not output_file:  # No path, use tmp
        output_file = tempfile.mkdtemp() + '/ais_data.parquet'
    prt_high(
        """
        Running csv ingestion
        ##################################
        Parameters
         - Input file:  {}
         - Output file: {}
        ##################################
        """.format(file, output_file)
        )

    ships_table = spark.read.format('com.databricks.spark.csv')\
        .options(header='true', inferschema='false', delimiter=';').load(file)
    ships_table = clean_data(ships_table)
    ships_table = cast_data(ships_table)
    ships_table = ships_table.na.drop()
    ships_table = ensure_columns_type(ships_table)

    # New features

    ships_table = ships_table.withColumn(
            'ais_navstatus',
            (
                when(col("sog") < 1, 'HOT')
                .when(col("sog") <= 5, 'MAN')
                .otherwise('CRU')
            )
    )

    ships_table.write.mode('overwrite').parquet(output_file)

    # If we have mlflow, log the result
    if 'mlflow' in sys.modules:
        prt_high("Logging MLFlow artifacts")
        mlflow.log_artifacts(output_file, "ais_data.parquet")

    return


def clean_data(df):
    for cl in df.columns:
        # replace_value = '-1'
        replace_value = nan
        if SHIPS_COL_TYPES[cl] == StringType():
            continue
        if SHIPS_COL_TYPES[cl] == BooleanType():
            replace_value = 'FALSE'
        df = df.withColumn(cl, when((
            df[cl] == 'NA') | (df[cl].isNull()), replace_value)
            .otherwise(df[cl]))
    return df


def convert_date_s(s_time):
    date = time.strptime(s_time, '%Y-%m-%d %H:%M:%S')
    return calendar.timegm(date)


def cast_data(df):
    for col_name, col_type in SHIPS_COL_TYPES.items():
        df = change_column_type(df, col_name, col_type)

    date_converter = udf(convert_date_s, IntegerType())
    df = df.withColumn('time', date_converter(df['fechahora']))\
        .drop('fechahora')

    return df


SHIPS_COL_TYPES = {
    'nombre': StringType(),
    'imo': IntegerType(),
    'mmsi': IntegerType(),
    'size_a': IntegerType(),
    'size_b': IntegerType(),
    'size_c': IntegerType(),
    'size_d': IntegerType(),
    'eslora': IntegerType(),
    'manga': IntegerType(),
    'draught': FloatType(),
    'sog': FloatType(),
    'cog': IntegerType(),
    'rot': IntegerType(),
    'heading': IntegerType(),
    'navstatus': IntegerType(),
    'typeofshipandcargo': IntegerType(),
    'latitude': FloatType(),
    'longitude': FloatType(),
    'fechahora': StringType()
}
