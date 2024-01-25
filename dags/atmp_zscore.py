import re
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import MetaData, Table, and_, create_engine, select
from statsmodels.tsa.seasonal import STL
from scipy.stats import zscore


# TODO DRY
@task()
def list_stations():
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()
    engine = create_engine(uri)

    metadata = MetaData()
    table = Table("stations", metadata, autoload=True, autoload_with=engine)
    stmt = select(table.c.station_code)

    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


@task()
def calculate_atmp_zsore(station: str):
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()
    engine = create_engine(uri)

    metadata = MetaData()
    table = Table("latest_history", metadata, autoload=True, autoload_with=engine)
    stmt = (
        select(table.c.timestamp, table.c.ATMP)
        .where(table.c.station_code == station)
        .where(table.c.ATMP != None)
    )

    print(station)
    df = pd.read_sql_query(stmt, engine, params={"station_code": station})

    if df.empty:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    df = df.resample("H").first().interpolate()

    stl = STL(df, robust=True, period=24)
    result = stl.fit()

    df_deseasonalized = df.join(result.seasonal)
    df_deseasonalized = df_deseasonalized.join(result.trend)
    df_deseasonalized["deseasonalized"] = (
        df_deseasonalized["ATMP"] - df_deseasonalized["season"]
    )
    df_deseasonalized["detrended"] = (
        df_deseasonalized["deseasonalized"] - df_deseasonalized["trend"]
    )
    df_deseasonalized["detrended_z"] = zscore(df_deseasonalized["detrended"])
    print(df_deseasonalized.tail())

    return df_deseasonalized["detrended_z"].iloc[-1]


@dag(
    dag_id="atmp_zscore",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=timedelta(hours=6),
    tags=["history"],
)
def fetch_history():
    # stations = list_stations()
    extract = calculate_atmp_zsore.expand(station=["41038"])


fetch_history()
