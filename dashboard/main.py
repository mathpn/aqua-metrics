import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.sql import functions as func


def load_data(conn, metadata, metric: str) -> pd.DataFrame:
    realtime_table = Table("realtime_data", metadata, autoload_with=conn)
    station_table = Table("stations", metadata, autoload_with=conn)

    stmt = (
        select(station_table.c.lat, station_table.c.lon, realtime_table.c[metric])
        .join(
            station_table, realtime_table.c.station_code == station_table.c.station_code
        )
        .where(
            realtime_table.c.ingestion_ts
            == select(func.max(realtime_table.c.ingestion_ts)).scalar_subquery(),
            realtime_table.c[metric] != None,
        )
    )
    return pd.read_sql(stmt, conn)


def load_ascore_atmp_data(conn, metadata, metric: str) -> pd.DataFrame:
    z_scores_table = Table("z_scores", metadata, autoload_with=conn)
    station_table = Table("stations", metadata, autoload_with=conn)

    stmt = select(
        station_table.c.lat,
        station_table.c.lon,
        z_scores_table.c[metric].label("z-score"),
    ).join(station_table, z_scores_table.c.station_code == station_table.c.station_code)
    return pd.read_sql(stmt, conn)


METRIC_NAMES = {
    "WSPD": "Wind speed (m/s)",
    "ATMP": "Atmospheric pressure",
    "PRES": "Sea level pressure (hPa)",
    "WVHT": "Significant wave height (meters)",
}


def main():
    engine = create_engine(
        "postgresql+psycopg2://aqua:password42@data-postgres/aqua_metrics"
    )
    metadata = MetaData()

    metric = st.selectbox("Choose a metric", METRIC_NAMES.keys(), index=1)

    with engine.connect() as conn:
        df = load_data(conn, metadata, metric)
        df_zscore = load_ascore_atmp_data(conn, metadata, metric)

    st.title("Buoy data dashboard")
    st.subheader(METRIC_NAMES[metric])

    print(df.head())
    df["size"] = 10
    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        color=metric,
        opacity=0.5,
        color_continuous_scale="viridis",
        size="size",
        zoom=1,
    )

    fig.update_layout(mapbox_style="carto-darkmatter", mapbox_center_lon=0)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    st.plotly_chart(fig)

    st.subheader("Deviation from expected value (z-score)")
    df_zscore["z-score"] = df_zscore["z-score"].round(2)
    print(df_zscore.head())

    df_zscore["size"] = 10
    fig = px.scatter_mapbox(
        df_zscore,
        lat="lat",
        lon="lon",
        color="z-score",
        opacity=0.5,
        color_continuous_scale="viridis",
        size="size",
        zoom=1,
    )

    fig.update_layout(mapbox_style="carto-darkmatter", mapbox_center_lon=0)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    st.plotly_chart(fig)


if __name__ == "__main__":
    main()
