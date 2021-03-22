import os
import numpy as np
import rasterio as rio
from rasterio.transform import from_origin
from pyspark.sql import SparkSession

# Cython
# import pyximport
# pyximport.install()
# TODO: Check why import module not found when using shared.rastering
# (rastering.pyx - Cython). Test compiling the module?
# from shared.rastering import pandas_to_raster
from shared.rastering_slow import pandas_to_raster

from shared.msg_types import prt_high, prt_info, prt_warn


def create_band_tiff(r, transform, band_id, path):
    new_dataset = rio.open(
        path, 'w', driver='GTiff',
        height=r.shape[0],
        width=r.shape[1],
        count=1, dtype=str(r.dtype),
        crs='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
        transform=transform,
        nodata=0
    )

    new_dataset.write(r[:, :, band_id], 1)
    new_dataset.close()
    return


def analyze(spark: SparkSession, input_file='rasters.parquet',
            output_folder='/tmp/img', units='kg'):
    prt_high(
            """
            Running image generation.")
            ##################################")
            Parameters")
             - Input file: {}
             - Output folder: {}
            """.format(input_file, output_folder)
            )

    df = spark.read.parquet(input_file)
    meta = spark.read.parquet(input_file+'.meta').toPandas()

    pol_vars = ["sox_me", "sox_ae", "co2_me", "co2_ae", "nox_me", "nox_ae"]
    pol_vars.extend(['hour', 'cell'])

    n_cols = meta.num_cols[0]
    n_rows = meta.num_rows[0]

    # Define the transformation of data (Bounding box)
    transform = from_origin(meta.min_lon[0], meta.max_lat[0],
                            meta.cell_size[0], meta.cell_size[0])

    # Produce a GeoTIFF and PDF per pollutant (Including ME, AE and
    # joint(ME+AE))
    timestamps = df.select('hour').distinct().collect()
    for t in timestamps:
        # Generate the rasters for this timestep
        timestamp = t['hour']
        data = df.select(pol_vars).filter(df.hour == t['hour']).toPandas()
        r = pandas_to_raster(data, pol_vars, n_rows, n_cols)
        if units == 'kg':
            r = r/(1000 * meta.cell_size_meters[0]**2)
        prt_info("Calculated raster sum: ", r.sum(axis=(0, 1, 2)))
        # joint raster ME+AE
        # TODO: r_me_ae declaration can be done previously
        pol_vars_me_ae = ['sox_', 'co2_', 'nox_']
        shp = list(r.shape)
        shp[2] = shp[2]//2
        shp = tuple(shp)
        r_me_ae = np.zeros(shp, dtype=np.float32)

        for i in range(0, len(pol_vars_me_ae)):
            r_me_ae[:, :, i] = r[:, :, i*2] + r[:, :, i*2+1]

        # Save the rasters to GeoTIFF
        file_path = output_folder + '/' + str(timestamp) + '/'
        try:
            os.makedirs(file_path)
        except OSError as e:
            prt_warn(str(e))
            prt_warn("Warning: folder exists" + file_path)
        for p in range(0, len(pol_vars)):
            raster_path = file_path + pol_vars[p]
            create_band_tiff(r, transform, p, raster_path + '.tif')

        for i in range(0, len(pol_vars_me_ae)):
            raster_path = file_path + pol_vars_me_ae[i]
            create_band_tiff(r_me_ae, transform, i, raster_path + '.tif')

    return
