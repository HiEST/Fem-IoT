# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql.functions import col

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from shared.msg_types import prt_warn

import sys
# try:
#     import mlflow
# except ImportError:
#     prt_warn("MLFlow is not installed. Not tracking the experiment.")

from shared.msg_types import prt_high, prt_info


def add_geometry_field(conn, table, lon, lat, field='geom'):
    cur = conn.cursor()
    cur.execute(
            "ALTER TABLE {} ADD COLUMN {} geometry(Point, 4326);"
            .format(table, field))
    cur.close()

    cur = conn.cursor()
    cur.execute(
            "UPDATE {} SET {} = ST_SetSRID(ST_MakePoint({}, {}), 4326);"
            .format(table, field, lon, lat))
    cur.close()
    idx = table + '_geom_idx'
    cur = conn.cursor()
    cur.execute(
            "CREATE INDEX {} ON {} USING GIST ({});"
            .format(idx, table, field))
    cur.close()

    conn.commit()


def add_indices(conn, table, idx_fields="(imo, type)"):
    idx = table + '_idx'
    # Build column list
    cur = conn.cursor()
    cur.execute("CREATE INDEX {} ON {}{};".format(idx, table, idx_fields))
    cur.close()
    conn.commit()


def add_in_port_attr(conn, table):
    q = "ALTER TABLE {} ADD COLUMN in_port boolean DEFAULT FALSE;"\
        .format(table)
    cur = conn.cursor()
    cur.execute(q)
    cur.close()

    q = """UPDATE {} s
        SET in_port=TRUE
        FROM port_shape shp WHERE ST_WITHIN(s.geom, shp.geom);
        """.format(table)
    cur = conn.cursor()
    cur.execute(q)
    cur.close()
    conn.commit()


def clean_table_and_derived(conn, table):
    q = "DROP TABLE IF EXISTS {} CASCADE;".format(table)

    cur = conn.cursor()
    cur.execute(q)
    cur.close()
    conn.commit()


def create_ais_info_view(conn, table, ihs_table):
    q = """CREATE MATERIALIZED VIEW {}_info AS
        SELECT imo, nombre, mmsi, count, count_port,
            COALESCE(type, typeofshipandcargo::text) AS type
        FROM
        (SELECT DISTINCT nombre, imo, mmsi, typeofshipandcargo,
            count(*) as count, sum(in_port::int) as count_port FROM {}
            GROUP BY (nombre, imo, mmsi, typeofshipandcargo)) as ais
        left join
        (
            SELECT imo, type FROM {}
        ) as ihs USING(imo);
        """.format(table, table, ihs_table)

    cur = conn.cursor()
    cur.execute(q)
    cur.close()
    conn.commit()


def analyze(spark: SparkSession, input_data='emissions.parquet', db='ais',
            table='emis', host='localhost', port=5431,  user='agutierrez',
            passwd='pass', table_type='ais', time_col='time', lon='longitude',
            lat='latitude', idx_fields="(imo, type)", ihs_table="ihs"):
    prt_high(
            """
            Running export to PostreSQL
            ##################################
            Parameters
             - Input file: {}
             - User: {}
             - Password: Censored :P
             - Database: {}
             - Host: {}
             - Table: {} (type: {})
             - Port: {}
             - Time column: {}
             - Latitude: {}
             - Longitude: {}
             - Idx Fields: {}
             - IHS table: {}
            ##################################
            """.format(input_data, user, db, host, table, table_type, port,
                       time_col, lat, lon, str(idx_fields), ihs_table)
            )
    # Rename stage
    if 'mlflow' in sys.modules:
        mlflow.set_tag(
            "mlflow.runName", "export_postgis_{}".format(table))

    # Connections
    mode = "overwrite"
    url = "jdbc:postgresql://{}:{}/{}".format(host, port, db)
    properties = {
            "user": user,
            "password": passwd,
            "driver": "org.postgresql.Driver",
            }

    prt_info("Processing parquet file")
    emis = spark.read.parquet(input_data)

    conn = psycopg2.connect(
        "dbname='{}' user='{}' host='{}' password='{}' port={}"
        .format(db, user, host, passwd, port)
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Allow CREATE INDEX

    # Remove materialized view if exists
    if table_type == "ais" or table_type == "ihs":
        clean_table_and_derived(conn, table)

    # Write table
    if table_type != "ihs":
        emis = emis.withColumn(
                time_col, col(time_col).cast(dataType=t.TimestampType()))
    prt_info("Exporting to JDBC")
    emis.write.jdbc(url=url, table=table, mode=mode, properties=properties)

    # Create indices
    conn = psycopg2.connect(
        "dbname='{}' user='{}' host='{}' password='{}' port={}"
        .format(db, user, host, passwd, port)
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Allow CREATE INDEX

    if table_type != "ihs":
        add_indices(conn, table, idx_fields)
        add_geometry_field(conn, table, lon, lat)
        add_in_port_attr(conn, table)
        if table_type == "ais":
            create_ais_info_view(conn, table, ihs_table)

    conn.close()

    return
