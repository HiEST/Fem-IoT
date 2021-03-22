# pylint: disable=redundant-keyword-arg
# pylint: disable=no-member

from functools import partial

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import udf, first
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import Row

from shared.data_cleaning import change_column_type, ensure_columns_type
from shared.msg_types import prt_high, prt_info, prt_warn

import numpy as np


def meters_to_deg(m):
    # Approximated conversion using 1º longitude in the equator as reference.
    # https://www.quora.com/How-many-meters-make-up-a-degree-of-longitude-latitude-on-Earth
    # https://en.wikipedia.org/wiki/Decimal_degrees
    return(m/113320)    # 113.32km


# Default granularity: hours
def analyze(spark: SparkSession, input_file='emissions.parquet',
            output_file='rasters.parquet', time_granularity=600,
            num_cols=100, cell_size=None, use_type=False
            ):
    prt_high(
            """
            Running compute emissions.
            ##################################
            Parameters
             - Input file: {}
             - Output file: {}
             - Time granularity: {}
             - Cell size: {}
             - Number of raster columns: {}
             - Rasters by type: {}
            ##################################

            """.format(input_file, output_file, time_granularity, cell_size,
                       num_cols, use_type)
            )

    cell_size_meters = None
    # Process parameters
    if cell_size is not None:
        prt_high("Info: Cell size set in parameters, using it")
        if cell_size[-1] == 'm':    # Meters
            cell_size_meters = int(cell_size[:-1])
            cell_size = meters_to_deg(cell_size_meters)
            # All except the last char
        else:
            cell_size = int(cell_size)
    else:
        num_cols = int(num_cols)
    time_granularity = int(time_granularity)

    # TODO: FILL NUM_VARS automatically
    prt_warn("WARNING: Number of variables is manually set to 10")
    num_vars = 10

    df = spark.read.parquet(input_file)

    min_max_lat_lon = df.agg(F.min(df.latitude), F.max(df.latitude),
                             F.min(df.longitude), F.max(df.longitude))
    min_max_time = df.agg(F.min(df.time), F.max(df.time))

    min_max_row = min_max_lat_lon.first()
    min_lat = min_max_row[0]
    max_lat = min_max_row[1]
    min_lon = min_max_row[2]
    max_lon = min_max_row[3]

    min_max_time = min_max_time.first()
    min_time = min_max_time[0]
    max_time = min_max_time[1]

    if cell_size is None:
        # Calculate cell dimension and number of rows using the number of
        # columns defined
        cell_size = (max_lon - min_lon) / num_cols
    else:
        num_cols = int(np.ceil((max_lon-min_lon)/cell_size))

    num_rows = int(np.ceil((max_lat-min_lat)/cell_size))
    prt_info("NUM COLS: " + str(num_cols))
    prt_info("NUM ROWS: " + str(num_rows))
    prt_info("CELL DIM: " + str(cell_size))

    # Create metadata file
    prt_info("Building metadata")
    meta = [(num_cols, num_rows, num_vars, min_lat, max_lat, min_lon, max_lon,
            cell_size, cell_size_meters, min_time, max_time, time_granularity)]
    rdd = spark.sparkContext.parallelize(meta)
    metarow = rdd.map(lambda x: Row(
        num_cols=int(x[0]), num_rows=int(x[1]), num_vars=int(x[2]),
        min_lat=float(x[3]), max_lat=float(x[4]),
        min_lon=float(x[5]), max_lon=float(x[6]),
        cell_size=float(x[7]), cell_size_meters=float(x[8]),
        min_time=int(x[9]), max_time=int(x[10]),
        time_granularity=int(x[11])
    ))
    metadf = spark.createDataFrame(metarow)

    # Create rasters
    prt_info("Building rasters")
    # TODO: Implement a way to define cell size
    to_cell = partial(lat_lon_to_cell, min_lon, min_lat, num_cols, num_rows,
                      cell_size)
    convertToCellFunc = udf(to_cell, IntegerType())

    to_hours = partial(to_time_resolution, time_granularity)
    convertToHours = udf(to_hours, IntegerType())

    df_cell = df.withColumn('cell', convertToCellFunc(df['longitude'],
                                                      df['latitude']))
    df_cell = df_cell.withColumn('hour', convertToHours(df['time']))

    # use desc order and first to get the last value
    # https://stackoverflow.com/questions/43114445/how-to-use-first-and-last-function-in-pyspark
    w = Window().partitionBy('imo', 'hour').orderBy(df_cell.time.desc())

    # lower than any possible sin/cos value so max only gets valid values

    df_cell = df_cell.withColumn('last_time', first("time").over(w))
    # PLACEHOLDER = -9999.0
    # df_cell = df_cell.withColumn('last_amp_v',
    #                             when(df_cell['last_time'] == df_cell['time'],
    #                                  df_cell['amp_v']).otherwise(PLACEHOLDER))
    # df_cell = df_cell.withColumn('last_cos',
    #                             when(df_cell['last_time'] == df_cell['time'],
    #                                  df_cell['cos_v']).otherwise(PLACEHOLDER))
    # df_cell = df_cell.withColumn('last_sin',
    #                             when(df_cell['last_time'] == df_cell['time'],
    #                                  df_cell['sin_v']).otherwise(PLACEHOLDER))

    # potential bug: max(last_amp_v), max('last_cos'), max('last_sin'). Each
    # may be from different ships if they happen to be in the same raster cell
    if use_type:
        raster = df_cell.groupBy("cell", "hour", "type")
    else:
        raster = df_cell.groupBy("cell", "hour")

    raster = raster.agg(F.sum("sox_me").alias("sox_me"),
                        F.sum("sox_ae").alias("sox_ae"),
                        F.sum('co2_me').alias('co2_me'),
                        F.sum("co2_ae").alias("co2_ae"),
                        F.sum("nox_me").alias("nox_me"),
                        F.sum('nox_ae').alias('nox_ae'),
                        # F.max('last_amp_v').alias('last_amp_v'),
                        # F.max('last_cos').alias('last_cos'),
                        # F.max('last_sin').alias('last_sin'),
                        F.count('*').alias('sample_count'))

    # Lower limit for these attributes
    # raster = raster.withColumn(
    #    'last_amp_v', when(raster['last_amp_v'] <= (PLACEHOLDER + 1), 0)
    #    .otherwise(raster['last_amp_v']))
    # raster = raster.withColumn(
    #    'last_cos', when(raster['last_cos'] <= (PLACEHOLDER + 1), 0)
    #    .otherwise(raster['last_cos']))
    # raster = raster.withColumn(
    #    'last_sin', when(raster['last_sin'] <= (PLACEHOLDER + 1), 0)
    #    .otherwise(raster['last_sin']))

    raster = change_column_type(raster, 'sample_count', IntegerType(),
                                force=True)
    raster = change_column_type(raster, 'sox_me', FloatType(), force=True)
    raster = change_column_type(raster, 'sox_ae', FloatType(), force=True)
    raster = change_column_type(raster, 'co2_me', FloatType(), force=True)
    raster = change_column_type(raster, 'co2_ae', FloatType(), force=True)
    raster = change_column_type(raster, 'nox_me', FloatType(), force=True)
    raster = change_column_type(raster, 'nox_ae', FloatType(), force=True)
    # raster = change_column_type(raster, 'last_amp_v', FloatType(),
    #                             force=True)
    # raster = change_column_type(raster, 'last_cos', FloatType(), force=True)
    # raster = change_column_type(raster, 'last_sin', FloatType(), force=True)

    raster = ensure_columns_type(raster)

    # Write rasters
    raster.write.mode('overwrite').parquet(output_file)

    # Write metadata
    metadf.write.mode('overwrite').parquet(output_file+'.meta')
    return


def lat_lon_to_cell(min_lon, min_lat, ncol, nrow, cell_size, lon, lat):
    # TODO: Improve this to handle coordinate (180º-0º-180º)
    h_pos = int((lon - min_lon) / cell_size)
    v_pos = int((lat - min_lat) / cell_size)
    # return (nrow-v_pos) * ncol + h_pos
    # We need nrow to invert the plot (latitude is inverted in our cuadrant)
    return v_pos * ncol + h_pos


def to_time_resolution(resolution, timestamp):
    return timestamp - (timestamp % resolution)
