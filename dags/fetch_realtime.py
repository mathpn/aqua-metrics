import re
from datetime import datetime, timedelta
from io import StringIO

import polars as pl
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

schema = {
    "STN": pl.String,
    "LAT": pl.Float64,
    "LON": pl.Float64,
    "YY": pl.Int64,
    "MM": pl.Int64,
    "DD": pl.Int64,
    "hh": pl.Int64,
    "mm": pl.Int64,
    "WDIR": pl.Int64,
    "WSPD": pl.Float64,
    "GST": pl.Float64,
    "WVHT": pl.Float64,
    "DPD": pl.Float64,
    "APD": pl.Float64,
    "MWD": pl.Float64,
    "PRES": pl.Float64,
    "PTDY": pl.Float64,
    "ATMP": pl.Float64,
    "WTMP": pl.Float64,
    "DEWP": pl.Float64,
    "VIS": pl.Float64,
    "TIDE": pl.Float64,
}


@task()
def extract_latest_observations():
    res = requests.get("https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt")
    data = "\n".join(
        re.sub(r"\s+", ";", line) for line in res.content.decode("utf-8").splitlines()
    )

    buffer = StringIO(data)
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()
    print(uri)

    df = pl.read_csv(
        buffer,
        separator=";",
        skip_rows_after_header=1,
        null_values=["MM"],
        schema=schema,
    )

    df = df.with_columns(
        [pl.datetime("YY", "MM", "DD", "hh", "mm", 0).alias("timestamp")]
    ).drop(["YY", "MM", "DD", "hh", "mm"])

    df = df.with_columns([pl.lit(datetime.now()).alias("ingestion_ts")])

    df.write_database(
        table_name="realtime_data", connection=uri, if_table_exists="append"
    )
    return f"written dataframe with shape {df.shape} to database"


@dag(
    dag_id="fetch_realtime",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5),
    tags=["realtime"],
)
def fetch_realtime():
    extract_latest_observations()
    SQLExecuteQueryOperator(
        task_id="fill_stations",
        sql="sql/insert_stations.sql",
        conn_id="aqua_metrics_sqlite",
    )


fetch_realtime()
