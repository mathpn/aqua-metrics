# %%

import sqlite3
from datetime import timedelta

import numpy as np
import pandas as pd
from plotnine import *
from statsmodels.tsa.seasonal import seasonal_decompose

# %%

conn = sqlite3.connect("./data/database.db")
# %%

df = pd.read_sql_query(
    "SELECT * FROM latest_history",
    conn,
)

df["timestamp"] = pd.to_datetime(df["timestamp"])
# %%

(
    ggplot(df)
    + aes(x="timestamp", y="ATMP", colour="station_code")
    + geom_line()
)
# %%

# %%

df_seasonal = df[df["station_code"] == '13001']
df_seasonal = df_seasonal[["timestamp", "ATMP"]]
df_seasonal = df_seasonal.set_index("timestamp")
df_seasonal = df_seasonal.resample("H").ffill().sort_index()

# %%
decomp_res = seasonal_decompose(df_seasonal)


decomp_res.plot()

# %%
