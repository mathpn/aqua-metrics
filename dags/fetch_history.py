import re
from datetime import datetime, timedelta
from io import StringIO

import polars as pl
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import MetaData, Table, and_, create_engine, select

schema = {
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
    "ATMP": pl.Float64,
    "WTMP": pl.Float64,
    "DEWP": pl.Float64,
    "VIS": pl.Float64,
    "PTDY": pl.Float64,
    "TIDE": pl.Float64,
}


@task()
def extract_history(station: str) -> None:
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()

    res = requests.get(f"https://www.ndbc.noaa.gov/data/realtime2/{station}.txt")
    # TODO refactor to function
    data = res.content.decode("utf-8")

    if data.startswith("<!DOCTYPE HTML"):
        raise AirflowSkipException(
            f"skipping station {station}: HTML returned instead of CSV"
        )

    data = "\n".join(re.sub(r"\s+", ";", line) for line in data.splitlines())

    buffer = StringIO(data)

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
        table_name="history_data", connection=uri, if_table_exists="append"
    )


@task()
def list_stations():
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()
    engine = create_engine(uri)

    metadata = MetaData()
    table = Table("stations", metadata, autoload=True, autoload_with=engine)
    stmt = select(table.c.station_code).limit(5) # XXX limit

    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


@dag(
    dag_id="fetch_history",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=15),
    tags=["realtime"],
)
def fetch_history():
    stations = list_stations()
    out = extract_history.expand(station=stations)


fetch_history()