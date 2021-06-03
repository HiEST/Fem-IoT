# pylint: disable=redundant-keyword-arg
# pylint: disable=no-name-in-module

import math
from itertools import chain
import tempfile
from shared.msg_types import prt_warn, prt_high, prt_info
import sys

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import udf, col, when

from pyspark.sql.types import (
        FloatType, DoubleType, LongType, StructType, StructField, StringType,
        BooleanType)

from shared.data_cleaning import ensure_columns_type, count_nulls_steam2
from shared.equations import (
        calcSOxEmissionFactor, calcCO2EmissionFactor, calcNOxEmissionFactor,
        estimateEmission)

from shared.models.steam import transient_power_me_steam,\
        transient_power_ae_steam
from shared.models.steam2 import transient_power_me_steam2,\
        transient_power_ae_steam2

from shared.reporting.reporting_funs import log_emission_summary

# try:
#     import mlflow
# except ImportError:
#     prt_warn("MLFlow is not installed. Not tracking the experiment.")


__author__ = 'agutierrez & denisovlev'


# input_data: File that contains the AIS ship data
# input_metadata: File that contains the information about the ships (IHS)
# output_file: Path to file that will contain the estimated emissions
# interpolation: Interpolation upper threshold. If the difference of time
#   between samples is greater than the threshold, there is no interpolation.
# step: how much space between samples we want (in seconds)

def transform_grouped(key_value, step, interpolation):
    interp = interp_seq(key_value[1], step, interpol_limit=interpolation)
    deltas_ac = deltas_accumulators_by_timeframe(interp)
    flat = chain.from_iterable(deltas_ac)
    return to_row(flat)


def deltas_accumulators_by_timeframe(seqs):
    # compute deltas only inside continuous time frames
    for seq in seqs:
        yield compute_deltas_and_accumulators(seq)


def compute_deltas_and_accumulators(seq):
    # WARNING: This function is order sensitive when declaring new elements in
    # the dictionary.
    prev = None
    for dict in seq:
        if prev is None:
            dict['last_move'] = 0
            prev = dict
        # Assumption: We consider that a ship doesn't move if the speed is
        # lower than VSafety (Jalkanen 2009)
        if dict['sog'] < 0.5:
            dict['last_move'] = prev['last_move'] + dict['time'] - prev['time']
        else:
            dict['last_move'] = 0
        d_lat = dict['latitude'] - prev['latitude']
        d_lon = dict['longitude'] - prev['longitude']
        amp_v = math.sqrt(d_lat * d_lat + d_lon * d_lon)
        dict['d_lat'] = d_lat
        dict['d_lon'] = d_lon
        # use ship speed from data?
        dict['amp_v'] = amp_v

        prev = dict
        yield dict


def to_row(seq):
    for dict in seq:
        yield Row(**dict)


def copy_row(row, **kwargs):
    dict = {}
    for attr in list(row.__fields__):
        dict[attr] = row[attr]

    for key, value in kwargs.items():
        dict[key] = value

    return dict


# interpol_limit: Interpolation upper threshold. If the difference of time
# between samples is greater than the threshold, there is no interpolation.
# step: Number of seconds between samples
def interp_seq(seq, step=60, interpol_limit=15*60):
    cur_time_slice = []
    next_time_slice = []
    seq = list(seq)
    seq.sort(key=lambda row: row['time'])
    seq = iter(seq)

    while (True):
        for r in seq:
            if len(cur_time_slice) == 0:
                cur_time_slice.append(r)
                continue
            prev = cur_time_slice[-1]
            if r['time'] - prev['time'] > interpol_limit:
                next_time_slice.append(r)
                break
            cur_time_slice.append(r)

        if len(cur_time_slice) == 0:
            break
        if len(cur_time_slice) < 2:
            cur_time_slice = next_time_slice
            next_time_slice = []
            continue

        start = cur_time_slice[0]
        end = cur_time_slice[-1]
        if start['time'] % step == 0:
            start_time = start['time']
        else:
            start_time = start['time'] - (start['time'] % step) + step
        x = np.arange(start_time, end['time'], step)
        timeX = [row['time'] for row in cur_time_slice]
        latY = [row['latitude'] for row in cur_time_slice]
        lonY = [row['longitude'] for row in cur_time_slice]
        sogY = [row['sog'] for row in cur_time_slice]

        lat = np.interp(x, timeX, latY)
        lon = np.interp(x, timeX, lonY)
        sog = np.interp(x, timeX, sogY)
        interpolated = zip(x, lat, lon, sog)

        rows = []
        for i in interpolated:
            copyRow = copy_row(
                start, time=i[0].item(), latitude=i[1].item(),
                longitude=i[2].item(), sog=i[3].item())
            rows.append(copyRow)
        yield rows

        cur_time_slice = next_time_slice
        next_time_slice = []


def analyze(spark: SparkSession, input_data='ships_data.parquet',
            input_metadata='ships_metadata.parquet',
            output_file='emissions.parquet',
            step=60, interpolation_lim=15*60, unit="kg",
            sfoc="NAEI", model="STEAM", ae_on_lim=24*60*60):
    if not output_file:  # No path, use tmp
        output_file = tempfile.mkdtemp() + '/emissions.parquet'
    prt_high("""
    Running compute emissions
    ##################################
    Parameters
     - Input data: {}
     - Input metadata (IHS): {}
     - Output file: {}
     - Interpolation limit: {} (s)
     - Interpolation step: {} (s)
     - Aux. Eng. at berth limit: {} (s)
     - Unit: {}/{}s
     - SFOC: {}
     - Emission Model: {}
    ##################################
    """.format(input_data, input_metadata, output_file, interpolation_lim,
               step, ae_on_lim, unit, step, sfoc, model))
    # Rename stage
    if 'mlflow' in sys.modules:
        mlflow.set_tag(
            "mlflow.runName", "compute_emissions_{}_{}".format(model, sfoc))

    # Cast parameters
    interpolation_lim = int(interpolation_lim)
    ae_on_lim = int(ae_on_lim)
    step = int(step)

    if unit == "kg":
        prt_info("Setting unit to kilograms")
        unit = 1000
    else:
        prt_info("Setting unit to grams")
        unit = 1

    df = spark.read.parquet(input_data)
    ihs_df = spark.read.parquet(input_metadata)

    # IHS processing

    # Filter desired SFOC
    if sfoc == "NAEI":
        print("Using NAEI SFOC estimation")
        ihs_df = ihs_df.withColumnRenamed("naei_sfoc_me", "sfoc_me")\
            .withColumnRenamed("naei_sfoc_ae", "sfoc_ae")
    else:
        # Other is STEAM by default
        print("Using STEAM SFOC estimation")
        ihs_df = ihs_df.withColumnRenamed("steam_sfocbase_me", "sfoc_me")\
            .withColumnRenamed("steam_sfocbase_ae", "sfoc_ae")

    # Variable filtering and function preparation
    if model == "STEAM2":
        transientPowerMEFunc = udf(transient_power_me_steam2, FloatType())
        transientPowerAEFunc = udf(transient_power_ae_steam2, FloatType())

        ihs_df = ihs_df.select('imo',
                               'l',
                               'b',
                               't',
                               'qpc',
                               'wet_surf_k',
                               'wet_surf_a3',
                               'cr_nofn',
                               'n_screw',
                               'n_cabin',
                               'n_ref_teu',
                               'design_draft',
                               'waterline',
                               'type',
                               'hermes_type',
                               'me_rpm',
                               'ae_rpm',
                               'inst_pow_me',
                               'inst_pow_ae',
                               'design_speed',
                               'sfoc_me',
                               'sfoc_ae'
                               ).cache()
        # Which model is used? If there any nulls on the selected attrs
        # we use STEAM (except for inst_pow)
        ihs_df = count_nulls_steam2(ihs_df)\
            .withColumn("model", (col("nulls") == 0).cast('integer')+1)

    else:   # Default is STEAM
        transientPowerMEFunc = udf(transient_power_me_steam, FloatType())
        transientPowerAEFunc = udf(transient_power_ae_steam, FloatType())

        ihs_df = ihs_df.select('imo',
                               'type',
                               'hermes_type',
                               'me_rpm',
                               'ae_rpm',
                               'inst_pow_me',
                               'inst_pow_ae',
                               'design_speed',
                               'sfoc_me',
                               'sfoc_ae'
                               ).cache()

    # Dataset joining and SFOC selection
    joined = df.select('nombre', 'imo', 'sog', 'latitude', 'longitude',
                       'time')\
               .join(ihs_df, ['imo'], 'inner')

    # Interpolation
    grouped = joined.rdd.groupBy(lambda record: record['imo'])
    interpolated = grouped.flatMap(
        lambda d: transform_grouped(d, step, interpolation_lim))
    # new_df = interpolated.toDF()

    if model == "STEAM2":
        interp_schema = StructType([
            StructField('imo',          LongType(),     True),
            StructField('nombre',       StringType(),   True),
            StructField('sog',          DoubleType(),   True),
            StructField('latitude',     DoubleType(),   True),
            StructField('longitude',    DoubleType(),   True),
            StructField('time',         LongType(),     True),
            StructField('l',            DoubleType(),   True),
            StructField('b',            DoubleType(),   True),
            StructField('t',            DoubleType(),   True),
            StructField('qpc',          DoubleType(),   True),
            StructField('wet_surf_k',   DoubleType(),   True),
            StructField('wet_surf_a3',  DoubleType(),   True),
            StructField('cr_nofn',      DoubleType(),   True),
            StructField('n_screw',      LongType(),     True),
            StructField('n_cabin',      LongType(),     True),
            StructField('n_ref_teu',    LongType(),     True),
            StructField('design_draft', BooleanType(),  True),
            StructField('waterline',    DoubleType(),   True),
            StructField('type',         StringType(),   True),
            StructField('hermes_type',  StringType(),   True),
            StructField('me_rpm',       LongType(),     True),
            StructField('ae_rpm',       LongType(),     True),
            StructField('inst_pow_me',  DoubleType(),   True),
            StructField('inst_pow_ae',  DoubleType(),   True),
            StructField('design_speed', DoubleType(),   True),
            StructField('sfoc_me',      LongType(),     True),
            StructField('sfoc_ae',      LongType(),     True),
            StructField('nulls',        LongType(),     True),
            StructField('model',        LongType(),     True),
            StructField('last_move',    LongType(),     True),
            StructField('d_lat',        DoubleType(),   True),
            StructField('d_lon',        DoubleType(),   True),
            StructField('amp_v',        DoubleType(),   True)
        ])
    else:
        # Steam 1
        interp_schema = StructType([
            StructField('imo',          LongType(),     True),
            StructField('nombre',       StringType(),   True),
            StructField('sog',          DoubleType(),   True),
            StructField('latitude',     DoubleType(),   True),
            StructField('longitude',    DoubleType(),   True),
            StructField('time',         LongType(),     True),
            StructField('type',         StringType(),   True),
            StructField('hermes_type',  StringType(),   True),
            StructField('me_rpm',       LongType(),     True),
            StructField('ae_rpm',       LongType(),     True),
            StructField('inst_pow_me',  DoubleType(),   True),
            StructField('inst_pow_ae',  DoubleType(),   True),
            StructField('design_speed', DoubleType(),   True),
            StructField('sfoc_me',      LongType(),     True),
            StructField('sfoc_ae',      LongType(),     True),
            StructField('last_move',    LongType(),     True),
            StructField('d_lat',        DoubleType(),   True),
            StructField('d_lon',        DoubleType(),   True),
            StructField('amp_v',        DoubleType(),   True)
        ])

    new_df = spark.createDataFrame(data=interpolated, schema=interp_schema)

    # Setting the schema for the new data
    # new_df = change_column_type(new_df, 'time', IntegerType(), True)
    # new_df = change_column_type(new_df, 'latitude', FloatType(), True)
    # new_df = change_column_type(new_df, 'longitude', FloatType(), True)
    # new_df = change_column_type(new_df, 'sog', FloatType(), True)
    # new_df = change_column_type(new_df, 'imo', IntegerType(), True)
    # new_df = change_column_type(new_df, 'd_lat', FloatType(), True)
    # new_df = change_column_type(new_df, 'd_lon', FloatType(), True)
    # new_df = change_column_type(new_df, 'amp_v', FloatType(), True)

    if model == "STEAM2":
        # Transient power calculation
        new_df = new_df.withColumn(
            'trans_p_me', transientPowerMEFunc(
                new_df['model'], new_df['sog'], new_df['design_speed'],
                new_df['inst_pow_me'], new_df['l'], new_df['b'], new_df['t'],
                new_df['qpc'], new_df['wet_surf_k'], new_df['wet_surf_a3'],
                new_df['cr_nofn'], new_df['n_screw'], new_df['design_draft'],
                new_df['waterline'])
        )
        new_df = new_df.withColumn(
            'trans_p_ae', transientPowerAEFunc(
                new_df['sog'], new_df['type'], new_df['inst_pow_ae'],
                new_df['n_cabin'], new_df['n_ref_teu'])
        )
    else:
        # Transient power calculation
        new_df = new_df.withColumn(
            'trans_p_me', transientPowerMEFunc(
                new_df['sog'], new_df['design_speed'], new_df['inst_pow_me'])
        )
        new_df = new_df.withColumn(
            'trans_p_ae', transientPowerAEFunc(
                new_df['sog'], new_df['type'], new_df['inst_pow_ae'])
        )

    # Deactivate AE if the ship has been at berth more than 24h
    if ae_on_lim > 0:
        new_df = new_df.withColumn(
                "trans_p_ae",
                when(col('last_move') < ae_on_lim, col("trans_p_ae"))
                .otherwise(0))

    calcSOxEmissionFactorFunc = udf(calcSOxEmissionFactor, FloatType())
    calcCO2EmissionFactorFunc = udf(calcCO2EmissionFactor, FloatType())
    calcNOxEmissionFactorFunc = udf(calcNOxEmissionFactor, FloatType())

    # TODO: Maybe this shouldn't be a UDF
    estimateEmissionFunc = udf(
            lambda fact, pow: estimateEmission(fact, pow, step, unit),
            FloatType())

    # Emission factor calculation
    # TODO: Move this to R script
    new_df = new_df.withColumn(
            'sox_fact_me', calcSOxEmissionFactorFunc(new_df['sfoc_me']))
    new_df = new_df.withColumn(
            'sox_fact_ae', calcSOxEmissionFactorFunc(new_df['sfoc_ae']))
    new_df = new_df.withColumn(
            'co2_fact_me', calcCO2EmissionFactorFunc(new_df['sfoc_me']))
    new_df = new_df.withColumn(
            'co2_fact_ae', calcCO2EmissionFactorFunc(new_df['sfoc_ae']))
    new_df = new_df.withColumn(
            'nox_fact_me', calcNOxEmissionFactorFunc(new_df['me_rpm']))
    new_df = new_df.withColumn(
            'nox_fact_ae', calcNOxEmissionFactorFunc(new_df['ae_rpm']))

    # Emission calculation
    new_df = new_df.withColumn(
            'sox_me', estimateEmissionFunc(
                new_df['sox_fact_me'], new_df['trans_p_me']))
    new_df = new_df.withColumn(
            'sox_ae', estimateEmissionFunc(
                new_df['sox_fact_ae'], new_df['trans_p_ae']))
    new_df = new_df.withColumn(
            'co2_me', estimateEmissionFunc(
                new_df['co2_fact_me'], new_df['trans_p_me']))
    new_df = new_df.withColumn(
            'co2_ae', estimateEmissionFunc(
                new_df['co2_fact_ae'], new_df['trans_p_ae']))
    new_df = new_df.withColumn(
            'nox_me', estimateEmissionFunc(
                new_df['nox_fact_me'], new_df['trans_p_me']))
    new_df = new_df.withColumn(
            'nox_ae', estimateEmissionFunc(
                new_df['nox_fact_ae'], new_df['trans_p_ae']))

    new_df = ensure_columns_type(new_df)
    new_df.write.mode('overwrite').parquet(output_file)

    if 'mlflow' in sys.modules:
        prt_high("Logging MLFlow artifacts")
        prt_high("- emissions.parquet")
        mlflow.log_artifacts(output_file, "emissions.parquet")
        prt_high("- emissions summary")
        log_emission_summary(new_df)

    return
