from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from scipy.stats import zscore
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.dialects.sqlite import insert
from statsmodels.tsa.seasonal import STL

from common import list_stations


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

    df = pd.read_sql_query(stmt, engine, params={"station_code": station})

    if df.empty:
        return

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

    zscore_table = Table("z_scores", metadata, autoload=True, autoload_with=engine)

    z_score = df_deseasonalized["detrended_z"].iloc[-1]
    insert_stmt = insert(
        zscore_table, values={"station_code": station, "ATMP": z_score}
    ).on_conflict_do_update(index_elements=["station_code"], set_={"ATMP": z_score})

    with engine.begin() as conn:
        conn.execute(insert_stmt)


@dag(
    dag_id="atmp_zscore",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=timedelta(hours=6),
    tags=["history"],
)
def fetch_history():
    stations = list_stations()
    calculate_atmp_zsore.expand(station=stations)


fetch_history()
