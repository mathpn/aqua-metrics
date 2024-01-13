import sqlite3

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

st.title("Buoy data dashboard")


def load_data():
    conn = sqlite3.connect("file:data/database.db?mode=ro", uri=True)
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


df = load_data()

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
