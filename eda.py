# %%

import sqlite3
from datetime import timedelta

import numpy as np
import pandas as pd
from plotnine import *
from scipy.stats import zscore
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.seasonal import STL, seasonal_decompose

# %%

conn = sqlite3.connect("./data/database.db")
# %%

df = pd.read_sql_query(
    "SELECT * FROM latest_history",
    conn,
)

df["timestamp"] = pd.to_datetime(df["timestamp"])
# %%

(ggplot(df) + aes(x="timestamp", y="ATMP", colour="station_code") + geom_line())
# %%

# %%

df_seasonal = df[df["station_code"] == "13008"]
df_seasonal = df_seasonal[["timestamp", "ATMP"]]
df_seasonal = df_seasonal.set_index("timestamp")
df_seasonal = df_seasonal.resample("H").interpolate().sort_index()

# %%
decomp_res = seasonal_decompose(df_seasonal)


decomp_res.plot()

# %%

stl = STL(df_seasonal, robust=True, period=24)
# %%

result = stl.fit()
# %%

result.plot()
# %%

df_deseasonalized = df_seasonal.join(result.seasonal)
df_deseasonalized = df_deseasonalized.join(result.trend)
df_deseasonalized["deseasonalized"] = (
    df_deseasonalized["ATMP"] - df_deseasonalized["season"]
)
df_deseasonalized["detrended"] = (
    df_deseasonalized["deseasonalized"] - df_deseasonalized["trend"]
)
df_deseasonalized["detrended_z"] = zscore(df_deseasonalized["detrended"])
# %%

df_deseasonalized.head()
# %%

(
    ggplot(df_deseasonalized.reset_index())
    # + aes(x="timestamp", y="detrended")
    + aes(x="timestamp", y="detrended_z")
    + geom_line()
)
# %%

current = df_deseasonalized.iloc[-1]["detrended_z"]
# %%
