import sqlite3

import streamlit as st
import pandas as pd
import numpy as np

st.title("Buoy data dashboard")


def load_data():
    conn = sqlite3.connect("file:data/database.db?mode=ro", uri=True)
    cur = conn.execute(
        "SELECT ATMP FROM realtime_data WHERE ingestion_ts = (SELECT MAX(ingestion_ts) FROM realtime_data)"
    )
    data = (x[0] for x in cur)
    return [x for x in data if x is not None]


data = load_data()

st.subheader("Distribution of atmospheric temperature")

hist_values = np.histogram(data, bins=20)[0]

st.bar_chart(hist_values)
