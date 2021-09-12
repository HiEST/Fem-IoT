# pylint: disable=no-name-in-module
# pylint: disable=import-error
from shared.msg_types import prt_warn, prt_high

from pyspark.sql.functions import dayofyear, weekofyear, month, dayofweek
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import sum, col

import seaborn as sns
import matplotlib.pyplot as plt

import os

# try:
#     import mlflow
# except ImportError:
#     prt_warn("MLFlow is not installed. Not tracking the experiment.")


def group_emis(data, by):
    data = data\
            .groupBy(by)\
            .agg(
                sum(col('trans_p')).alias('trans_p'),
                sum(col('trans_p_me')).alias('trans_p_me'),
                sum(col('trans_p_ae')).alias('trans_p_ae'),
                sum(col('nox')).alias('nox'),
                sum(col('sox')).alias('sox'),
                sum(col('co2')).alias('co2'),
                sum(col('nox_me')).alias('nox_me'),
                sum(col('sox_me')).alias('sox_me'),
                sum(col('co2_me')).alias('co2_me'),
                sum(col('nox_ae')).alias('nox_ae'),
                sum(col('sox_ae')).alias('sox_ae'),
                sum(col('co2_ae')).alias('co2_ae'))\
            .orderBy(by)\
            .toPandas()
    data = data.add_prefix(by+'_')
    return(data)


def log_dataframe_metric(df, column):
    for _, row in df.iterrows():
        row = row.to_dict()
        step = int(row[column])
        del row[column]
        mlflow.log_metrics(row, step=step)


def log_emission_summary(emis):
    # Add ME and AE
    emis = emis\
            .withColumn('trans_p', col('trans_p_me') + col('trans_p_ae'))\
            .withColumn('nox', col('nox_me') + col('nox_ae'))\
            .withColumn('sox', col('sox_me') + col('sox_ae'))\
            .withColumn('co2', col('co2_me') + col('co2_ae'))

    # Log total
    all = emis.agg(
                sum(col('trans_p')).alias('total_trans_p'),
                sum(col('trans_p_me')).alias('total_trans_p_me'),
                sum(col('trans_p_ae')).alias('total_trans_p_ae'),
                sum(col('nox')).alias('total_nox'),
                sum(col('sox')).alias('total_sox'),
                sum(col('co2')).alias('total_co2'),
                sum(col('nox_me')).alias('total_nox_me'),
                sum(col('sox_me')).alias('total_sox_me'),
                sum(col('co2_ae')).alias('total_co2_me'),
                sum(col('nox_ae')).alias('total_nox_ae'),
                sum(col('sox_ae')).alias('total_sox_ae'),
                sum(col('co2_ae')).alias('total_co2_ae')).toPandas()
    mlflow.log_metrics(all.iloc[0].to_dict())

    # Generate time features
    emis = emis.withColumn('time', emis.time.cast(dataType=TimestampType()))
    emis = emis\
        .withColumn('day', dayofyear(col('time')))\
        .withColumn('week', weekofyear(col('time')))\
        .withColumn('month', month(col('time')))\
        .withColumn('dayofweek', dayofweek(col('time'))).cache()

    day_df = group_emis(emis, 'day')
    week_df = group_emis(emis, 'week')
    month_df = group_emis(emis, 'month')
    dayofweek_df = group_emis(emis, 'dayofweek')

    prt_high("Generated summary. Logging it.")
    # Log everything
    log_dataframe_metric(day_df, 'day_day')
    log_dataframe_metric(week_df, 'week_week')
    log_dataframe_metric(month_df, 'month_month')
    log_dataframe_metric(dayofweek_df, 'dayofweek_dayofweek')


def plot_summary(df, folder, x):
    for colu in df.columns[2:]:
        plt.figure(colu)
        sns.lineplot(data=df, x=x, y=colu)
        plt.savefig(folder+"/lineplot_"+colu+".png")


def mkdir_if_not_exist(path):
    if not os.path.exists(path):
        os.mkdir(path)


def log_emission_summary_csv(emis, path):
    # Add ME and AE
    emis = emis\
            .withColumn('trans_p', col('trans_p_me') + col('trans_p_ae'))\
            .withColumn('nox', col('nox_me') + col('nox_ae'))\
            .withColumn('sox', col('sox_me') + col('sox_ae'))\
            .withColumn('co2', col('co2_me') + col('co2_ae'))

    # Log total
    all = emis.agg(
                sum(col('trans_p')).alias('total_trans_p'),
                sum(col('trans_p_me')).alias('total_trans_p_me'),
                sum(col('trans_p_ae')).alias('total_trans_p_ae'),
                sum(col('nox')).alias('total_nox'),
                sum(col('sox')).alias('total_sox'),
                sum(col('co2')).alias('total_co2'),
                sum(col('nox_me')).alias('total_nox_me'),
                sum(col('sox_me')).alias('total_sox_me'),
                sum(col('co2_ae')).alias('total_co2_me'),
                sum(col('nox_ae')).alias('total_nox_ae'),
                sum(col('sox_ae')).alias('total_sox_ae'),
                sum(col('co2_ae')).alias('total_co2_ae')).toPandas()

    # Generate time features
    emis = emis.withColumn('time', emis.time.cast(dataType=TimestampType()))
    emis = emis\
        .withColumn('day', dayofyear(col('time')))\
        .withColumn('week', weekofyear(col('time')))\
        .withColumn('month', month(col('time')))\
        .withColumn('dayofweek', dayofweek(col('time'))).cache()

    day_df = group_emis(emis, 'day')
    week_df = group_emis(emis, 'week')
    month_df = group_emis(emis, 'month')
    dayofweek_df = group_emis(emis, 'dayofweek')

    prt_high("Generated summary. Logging it.")

    # Make directories
    mkdir_if_not_exist(path)
    mkdir_if_not_exist(path+"/plot")
    mkdir_if_not_exist(path+"/plot/day")
    mkdir_if_not_exist(path+"/plot/week")
    mkdir_if_not_exist(path+"/plot/month")
    mkdir_if_not_exist(path+"/plot/dayofweek")

    # Log everything
    # Save emis, day_df, week_df, month_df, dayofweek_df (pandas df)
    all.to_csv(path+"/emis.csv")

    day_df.to_csv(path+"/day.csv")
    plot_summary(day_df, path+"/plot/day", x="day_day")

    week_df.to_csv(path+"/week.csv")
    plot_summary(week_df, path+"/plot/week", x="week_week")

    month_df.to_csv(path+"/month.csv")
    plot_summary(month_df, path+"/plot/month", x="month_month")

    dayofweek_df.to_csv(path+"/dayofweek.csv")
    plot_summary(dayofweek_df, path+"/plot/dayofweek", x="dayofweek_dayofweek")
