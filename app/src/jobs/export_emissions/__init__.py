from pyspark.sql import SparkSession
from shared.msg_types import prt_high


def analyze(spark: SparkSession, input_file='emissions.parquet',
            output_file='emissions.csv'):
    prt_high(
            """
            Running export to CSV
            ##################################
            Parameters
             - Input file: {}
             - Output file: {}
            ##################################
            """.format(input_file, output_file)
            )

    df = spark.read.parquet(input_file)

    df.orderBy('time', 'imo')\
        .write.csv(output_file, header=True, mode='overwrite')

    return
