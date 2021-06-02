import argparse
import importlib
import time
import os
import sys
from shared.msg_types import prt_warn, prt_high, prt_info, prt_err

# PRINT ENV
try:
    from pyspark.sql import SparkSession
except ImportError:
    prt_warn("PySpark not found, using findspark")
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

try:
    import mlflow
except ImportError:
    prt_warn("MLFlow is not installed. Not tracking the experiment.")

__author__ = 'ekampf & agutierrez'


def main(args):
    prt_info("Going to run Spark Job")
    # Change path to the current file's path
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    # environment = {
    #     'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    # }

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        prt_info('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}

    prt_info('\nRunning job %s...\nenvironment is %s\n'
             % (args.job_name, str(job_args)))

    # Start Spark
    spark = SparkSession.builder\
        .appName(args.job_name)\
        .config("spark.jars", args.extra_jars)

    if args.hdfs:
        spark = spark.config("spark.hadoop.fs.defaultFS", args.hdfs)

   spark = spark.getOrCreate()

    # Set timezone to UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    prt_warn("-> For some reason we get the spark SQLContext deprecated\
             warning, but we're already using SparkSession. Ignore.")
    prt_info("Spark context started. Going to import jobs.")
    prt_info("Setting log level to {}".format(args.log_level))
    spark.sparkContext.setLogLevel(args.log_level)

    # Import Module
    try:
        job_module = importlib.import_module('jobs.%s' % args.job_name)
    except Exception as e:
        prt_err(str(e))
        prt_err("Error, couldnt load module %s" % args.job_name)
        exit(1)

    # Execute Module
    start = time.time()
    job_module.analyze(spark, **job_args)
    end = time.time()
    prt_high("\nExecution of job %s took %s seconds"
             % (args.job_name, end-start))


if __name__ == '__main__':
    try:
        prt_info("Now Running")

        # load modules
        if os.path.exists('libs.zip'):
            sys.path.insert(0, 'libs.zip')
        else:
            prt_warn("Libs zip not found. Using folder.")
            sys.path.insert(0, './libs')

        if os.path.exists('jobs.zip'):
            sys.path.insert(0, 'jobs.zip')
        else:
            prt_warn("Job zip not found. Using folder.")
            sys.path.insert(0, './jobs')

        # Argument parser
        parser = argparse.ArgumentParser(description='Run a PySpark job')
        parser.add_argument(
            '--job', type=str, required=True, dest='job_name',
            help="The name of the job module you want to run. (ex: poc will run \
            job on jobs.poc package)")
        parser.add_argument(
            '--job-args', nargs='*', help="Extra arguments to send to the PySpark \
            job (example: --job-args template=manual-email1 foo=bar")
        parser.add_argument(
            '--log', type=str, dest='log_level', default='WARN',
            help="Level of Spark logging (default = WARN).")
        parser.add_argument(
            '--extra-jars', type=str, dest='extra_jars', default='',
            help="Extra java jars to be added")
        parser.add_argument(
            '--hdfs', type=str, dest='hdfs', default='',
            help="HDFS endpoint")


        args = parser.parse_args()
        prt_info("Called with arguments: %s" % args)

        # Run main
        if 'mlflow' in sys.modules:
            prt_high("- Running with MLFlow")
            mlflow.start_run()  # Setting run_name in start run doesn't work
            mlflow.set_tag("mlflow.runName", args.job_name)
        main(args)
        if 'mlflow' in sys.modules:
            prt_high("MLFlow: Shutting down run.")
            mlflow.end_run()
    except KeyboardInterrupt:
        print('Interrupted :(')
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
