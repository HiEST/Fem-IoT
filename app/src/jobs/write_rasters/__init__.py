from pyspark.sql import SparkSession
import numpy as np
import h5py

from shared.msg_types import prt_high, prt_info


def analyze(spark: SparkSession, input_file='rasters.parquet',
            output_file='/home/rasters.hdf5'):
    prt_high(
            """
            ##################################
            Parameters
             - Input file: {}
             - Output file: {}
            ##################################
            """.format(input_file, output_file)
            )

    df = spark.read.parquet(input_file)

    # Metadata processing
    metadf = spark.read.parquet(input_file+".meta").collect()[0]
    num_vars = metadf['num_vars']
    num_cols = metadf['num_cols']
    num_rows = metadf['num_rows']

    # 'last_amp_v', 'last_cos', 'last_sin')\
    by_time = df.select('hour', 'cell', 'sample_count', 'nox_me', 'nox_ae')\
        .rdd.map((lambda row: (row['hour'], row))).groupByKey()\
        .sortByKey(ascending=True).cache()

    n_rasters = by_time.count()

    filename = output_file
    f = h5py.File(filename, 'w', libver='latest')
    shape = (n_rasters, num_rows, num_cols, num_vars)
    dset = f.create_dataset('dataset', shape, dtype='f', compression="gzip",
                            chunks=(1, num_rows, num_cols, num_vars))

    for i, raster_tuple in enumerate(by_time.toLocalIterator()):
        dset[i] = to_raster(raster_tuple[1], num_rows, num_cols, num_vars)

    # Global aggregates
    # Aggregates by cell
    ds_min = np.min(dset, axis=(0))
    prt_info('min:', ds_min)
    ds_max = np.max(dset, axis=(0))
    prt_info('max:', ds_max)
    ds_mean = np.mean(dset, axis=(0))
    prt_info('mean:', ds_mean)
    ds_std = np.std(dset, axis=(0))
    prt_info('std:', ds_std)

    f.create_dataset('min', data=ds_min)
    f.create_dataset('max', data=ds_max)
    f.create_dataset('mean', data=ds_mean)
    f.create_dataset('std', data=ds_std)

    prt_info("Cell metadata shape:")
    prt_info(f['min'].shape)
    prt_info(f['max'].shape)
    prt_info(f['mean'].shape)
    prt_info(f['std'].shape)

    # Global aggregates
    ds_min = np.min(ds_min, axis=(0, 1))
    # We get min and max from the previous result!
    prt_info('min:', ds_min)

    ds_max = np.max(ds_max, axis=(0, 1))
    prt_info('max:', ds_max)

    ds_mean = np.mean(dset, axis=(0, 1, 2))
    prt_info('mean:', ds_mean)

    ds_std = np.std(dset, axis=(0, 1, 2))
    prt_info('std:', ds_std)

    dset.attrs['min'] = ds_min
    dset.attrs['max'] = ds_max
    dset.attrs['mean'] = ds_mean
    dset.attrs['std'] = ds_std
    f.close()


def to_raster(rows, nrows, ncols, nvars):
    raster = np.zeros((nrows, ncols, nvars))
    for row in rows:
        # image is flipped for some reason? flip it back
        x = (nrows - 1) - (row['cell'] // ncols)
        y = row['cell'] % ncols
        raster[x, y, 0] = row['sample_count']
        raster[x, y, 1] = row['nox_me']
        raster[x, y, 2] = row['nox_ae']
        # raster[x, y, 3] = row['last_amp_v']
        # raster[x, y, 4] = row['last_cos']
        # raster[x, y, 5] = row['last_sin']
    return raster
