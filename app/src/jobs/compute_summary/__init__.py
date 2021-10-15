# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession

from shared.msg_types import prt_high

#from shared.reporting.reporting_funs import log_emission_summary
from shared.reporting.reporting_funs import log_emission_summary_csv


def analyze(spark: SparkSession, input_file='emissions.parquet', hdfs_path='hdfs://', plot_path='../output'):
    prt_high(
            """
            Running Summarize Emissions
            ##################################
            Parameters
             - Input file: {}
             - Output HDFS path: {}
             - Output plot path: {}
            ##################################
            """.format(input_file, hdfs_path, plot_path)
            )

    # import os
    # os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64"

    emis = spark.read.parquet(input_file)
    #log_emission_summary(emis)
    log_emission_summary_csv(emis, hdfs_path, plot_path)

    return
