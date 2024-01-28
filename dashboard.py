import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.title("Buoy data dashboard")


def load_data(conn: sqlite3.Connection):
    df = pd.read_sql_query(
        """
        SELECT LAT, LON, ATMP
        FROM realtime_data
        WHERE ingestion_ts = (SELECT MAX(ingestion_ts) FROM realtime_data)
        AND ATMP IS NOT NULL
        """,
        conn,
    )
    return df


def load_ascore_atmp_data(conn: sqlite3.Connection):
    df = pd.read_sql_query(
        """
        SELECT lat, lon, ATMP
        FROM z_scores AS t1
        JOIN stations AS t2
        ON t1.station_code = t2.station_code
        """,
        conn,
    )
    return df



conn = sqlite3.connect("file:data/database.db?mode=ro", uri=True)

df = load_data(conn)
print(df.sample(10))

st.subheader("Atmospheric temperature")

df["size"] = 10
fig = px.scatter_mapbox(
    df,
    lat="LAT",
    lon="LON",
    color="ATMP",
    opacity=0.5,
    color_continuous_scale="viridis",
    size="size",
    zoom=1,
)

fig.update_layout(mapbox_style="carto-darkmatter", mapbox_center_lon=0)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

st.plotly_chart(fig)

df_zscore = load_ascore_atmp_data(conn)
df_zscore["ATMP"] = df_zscore["ATMP"].round(2)
print(df_zscore)

df_zscore["size"] = 10
fig = px.scatter_mapbox(
    df_zscore,
    lat="lat",
    lon="lon",
    color="ATMP",
    opacity=0.5,
    color_continuous_scale="viridis",
    size="size",
    zoom=1,
)

fig.update_layout(mapbox_style="carto-darkmatter", mapbox_center_lon=0)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

st.plotly_chart(fig)
