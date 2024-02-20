import re
from datetime import datetime, timedelta
from io import StringIO

import polars as pl
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, create_engine, select


@task()
def list_stations():
    uri = PostgresHook(postgres_conn_id="aqua_metrics_db").get_uri()
    engine = create_engine(uri)

    metadata = MetaData()
    table = Table("stations", metadata, autoload=True, autoload_with=engine)
    stmt = select(table.c.station_code)

    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]


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
    uri = PostgresHook(postgres_conn_id="aqua_metrics_db").get_uri()

    res = requests.get(
        f"https://www.ndbc.noaa.gov/data/realtime2/{station}.txt", timeout=15
    )
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
        [pl.datetime("YY", "MM", "DD", "hh", "mm", 0).alias("timestamp").cast(pl.Utf8)]
    ).drop(["YY", "MM", "DD", "hh", "mm"])

    # TODO foreign key
    df = df.with_columns([pl.lit(datetime.now()).alias("ingestion_ts")])
    df = df.with_columns([pl.lit(station).alias("station_code")])

    df.write_database(
        table_name="history_data", connection=uri, if_table_exists="append"
    )


@dag(
    dag_id="fetch_history",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=timedelta(hours=6),
    tags=["history"],
)
def fetch_history():
    stations = list_stations()
    extract = extract_history.expand(station=["42055"])

    latest_history = SQLExecuteQueryOperator(
        task_id="latest_history",
        sql="sql/latest_history.sql",
        split_statements=True,
        autocommit=False,
        conn_id="aqua_metrics_db",
        trigger_rule="all_done",
    )

    extract >> latest_history


fetch_history()
