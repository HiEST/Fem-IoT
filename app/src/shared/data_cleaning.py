# CSV types
# Deprecated
# This was used after CSV reading.
# Now CSV reader has an option to add a schema.
from pyspark.sql.functions import col


def change_column_type(df, col_name, new_col_type, force=False):
    # Check if the type of the column is already what we want
    if not force:
        old_col_type = next(
            field.dataType for field in df.schema.fields
            if field.name == col_name
        )
        if old_col_type == new_col_type:
            return df
    # Temporal column creation
    tmp_col_name = "{}_tmp".format(col_name)
    # Casting
    df2 = df.withColumn(tmp_col_name, df[col_name].cast(new_col_type)) \
        .drop(col_name) \
        .withColumnRenamed(tmp_col_name, col_name)
    return df2


def ensure_columns_type(df):
    for field in df.schema.fields:
        df = change_column_type(df, field.name, field.dataType, force=True)
    return df


# NAs

def na_num(c):
    return col(c).isNull().cast('integer')


def count_nulls_steam2(meta):
    meta = meta.withColumn(
            "nulls",
            na_num("l") +
            na_num('b') +
            na_num('t') +
            na_num('qpc') +
            na_num('wet_surf_k') +
            na_num('wet_surf_a3') +
            na_num('cr_nofn') +
            na_num('n_screw') +
            na_num('n_cabin') +
            na_num('n_ref_teu') +
            na_num('design_draft') +
            na_num('waterline') +
            na_num('type') +
            na_num('me_rpm') +
            na_num('ae_rpm') +
            # na_num('inst_pow_me') +
            # na_num('inst_pow_ae') +
            na_num('design_speed')
    )
    return(meta)
