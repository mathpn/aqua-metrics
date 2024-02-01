import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.title("Buoy data dashboard")


def load_data(conn: sqlite3.Connection, metric: str):
    df = pd.read_sql_query(
        f"""
        SELECT LAT, LON, {metric} 
        FROM realtime_data
        WHERE ingestion_ts = (SELECT MAX(ingestion_ts) FROM realtime_data)
        AND {metric} IS NOT NULL
        """,
        conn,
    )
    return df


def load_ascore_atmp_data(conn: sqlite3.Connection, metric: str):
    df = pd.read_sql_query(
        f"""
        SELECT lat, lon, {metric} 
        FROM z_scores AS t1
        JOIN stations AS t2
        ON t1.station_code = t2.station_code
        """,
        conn,
    )
    return df


METRIC_NAMES = {
    "WSPD": "Wind speed (m/s)",
    "ATMP": "Atmospheric pressure",
    "PRES": "Sea level pressure (hPa)",
    "WVHT": "Significant wave height (meters)",
}


def main():
    conn = sqlite3.connect("file:data/database.db?mode=ro", uri=True)

    metric = st.selectbox("Choose a metric", METRIC_NAMES.keys(), index=1)

    df = load_data(conn, metric)
    print(df.sample(10))

    st.subheader(METRIC_NAMES[metric])

    df["size"] = 10
    fig = px.scatter_mapbox(
        df,
        lat="LAT",
        lon="LON",
        color=metric,
        opacity=0.5,
        color_continuous_scale="viridis",
        size="size",
        zoom=1,
    )

    fig.update_layout(mapbox_style="carto-darkmatter", mapbox_center_lon=0)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    st.plotly_chart(fig)

    df_zscore = load_ascore_atmp_data(conn, metric)
    df_zscore[metric] = df_zscore[metric].round(2)
    print(df_zscore)

    df_zscore["size"] = 10
    fig = px.scatter_mapbox(
        df_zscore,
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


if __name__ == "__main__":
    main()
