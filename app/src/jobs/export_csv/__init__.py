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
        .coalesce(1)\
        .write.csv(output_file, header=True, mode='overwrite')
    # Coalesce(1) so that we write one big csv.

    # Pandas version
    # pdf = df.orderBy('time', 'imo').toPandas()
    # pdf.to_csv(output_file)

    return
