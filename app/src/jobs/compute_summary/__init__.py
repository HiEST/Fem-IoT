# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession

from shared.msg_types import prt_high

#from shared.reporting.reporting_funs import log_emission_summary
from shared.reporting.reporting_funs import log_emission_summary_csv


def analyze(spark: SparkSession, input_file='emissions.parquet', path='../output'):
    prt_high(
            """
            Running Summarize Emissions
            ##################################
            Parameters
             - Input file: {}
             - Output path file: {}
            ##################################
            """.format(input_file, path)
            )

    # import os
    # os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64"

    emis = spark.read.parquet(input_file)
    #log_emission_summary(emis)
    log_emission_summary_csv(emis, path)

    return
