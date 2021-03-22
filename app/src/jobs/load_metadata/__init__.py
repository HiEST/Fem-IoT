from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, BooleanType,\
    StructType, StructField

import tempfile
import sys
from shared.msg_types import prt_warn, prt_high

try:
    import mlflow
except ImportError:
    prt_warn("MLFlow is not installed. Not tracking the experiment.")


__author__ = 'agutierrez & denisovlev'

schema = StructType([
    StructField('imo', IntegerType()),
    StructField('name', StringType()),
    StructField('type', StringType()),
    StructField('me_stroke', IntegerType()),
    StructField('me_rpm', IntegerType()),
    StructField('prop_rpm', IntegerType()),
    StructField('loa', FloatType()),
    StructField('lbp', FloatType()),
    StructField('l', FloatType()),
    StructField('b', FloatType()),
    StructField('t', FloatType()),
    StructField('los', FloatType()),
    StructField('lwl', FloatType()),
    StructField('ta', FloatType()),
    StructField('tf', FloatType()),
    StructField('bulbous_bow', BooleanType()),
    StructField('n_thru', IntegerType()),
    StructField('inst_pow_me', FloatType()),
    StructField('n_inst_me', IntegerType()),
    StructField('single_pow_me', FloatType()),
    StructField('eng_type', StringType()),
    StructField('inst_pow_ae', FloatType()),
    StructField('design_speed', FloatType()),
    StructField('n_ref_teu', IntegerType()),
    StructField('n_cabin', IntegerType()),
    StructField('serv_pow_me', FloatType()),
    StructField('serv_single_pow_me', FloatType()),
    StructField('ae_rpm', IntegerType()),
    StructField('n_screw', IntegerType()),
    StructField('n_rudd', IntegerType()),
    StructField('n_brac', IntegerType()),
    StructField('n_boss', IntegerType()),
    StructField('design_draft', BooleanType()),
    StructField('build_year', IntegerType()),
    StructField('fuel_type', StringType()),
    StructField('vapour_recovery', StringType()),
    StructField('ae_stroke', StringType()),
    StructField('trozzi_fuel_type', StringType()),
    StructField('hermes_type', StringType()),
    StructField('trozzi_type', StringType()),
    StructField('eng_type2', StringType()),
    StructField('naei_sfoc_me', IntegerType()),
    StructField('naei_sfoc_ae', IntegerType()),
    StructField('steam_sfocbase_me', IntegerType()),
    StructField('steam_sfocbase_ae', IntegerType()),
    StructField('waterline', FloatType()),
    StructField('dp', FloatType()),
    StructField('wet_surf_a3', FloatType()),
    StructField('wet_surf_k', FloatType()),
    StructField('cr_nofn', FloatType()),
    StructField('qpc', FloatType())
])


def clean_data(ihs_table):
    # NOTE: IHS is cleaned on IHS processor
    # ihs_table = ihs_table.na.fill(-1)       # -1 to numeric NA
    ihs_table = ihs_table.na.fill(False)    # False to boolean NA

    return ihs_table


def analyze(spark: SparkSession, file='/ships_data/IHSData.txt',
            output_file='ihs_metadata.parquet'):
    if not output_file:  # No path, use tmp
        output_file = tempfile.mkdtemp() + '/ihs_metadata.parquet'
    prt_high(
        """
        Running metadata ingestion
        ##################################
        Parameters
         - Input file:  {}
         - Output file: {}
        ##################################
        """.format(file, output_file))

    ihs_table = spark.read.csv(file, header=True, schema=schema, sep='\t',
                               nullValue='NA')
    ihs_table = clean_data(ihs_table)
    ihs_table.write.mode('overwrite').parquet(output_file)

    if 'mlflow' in sys.modules:
        prt_high("Logging MLFlow artifacts")
        mlflow.log_artifacts(output_file, "ihs_data.parquet")

    return
