# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession

from shared.msg_types import prt_high

from shared.reporting.reporting_funs import log_emission_summary


def analyze(spark: SparkSession, input_data='emissions.parquet'):
    prt_high(
            """
            Running Summarize Emissions
            ##################################
            Parameters
             - Input file: {}
            ##################################
            """.format(input_data)
            )

    # import os
    # os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64"

    emis = spark.read.parquet(input_data)
    log_emission_summary(emis)

    return
