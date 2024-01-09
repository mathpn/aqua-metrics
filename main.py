# %%

import re
from sqlalchemy import (
    create_engine,
    text,
    Table,
    Float,
    String,
    MetaData,
    Column,
    Integer,
    ForeignKey,
)
from io import StringIO

import numpy as np
import polars as pl

# %%

with open("./data/31005.txt", "r") as f:
    data = f.read()
    data = "\n".join(re.sub(r"\s+", ";", line) for line in data.splitlines())
    buffer = StringIO(data)

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
    "DPD": pl.Int64,
    "APD": pl.Float64,
    "MWD": pl.Int64,
    "PRES": pl.Float64,
    "ATMP": pl.Float64,
    "WTMP": pl.Float64,
    "DEWP": pl.Float64,
    "VIS": pl.Float64,
    "PTDY": pl.Float64,
    "TIDE": pl.Float64,
}

df = pl.read_csv(
    buffer,
    separator=";",
    skip_rows_after_header=1,
    null_values=["MM"],
    schema=schema,
)

df = df.with_columns([pl.lit("31005").alias("station_code")])

with pl.Config(tbl_cols=df.width):
    print(df)

# %%

df_stations = pl.DataFrame({"station_code": ["31005"], "name": ["ESPIRITO SANTO"]})

print(df_stations)

# %%

URI = "sqlite:///./data/database.db"

engine = create_engine(URI, echo=True)

metadata_obj = MetaData()

stations_table = Table(
    "stations",
    metadata_obj,
    Column("station_code", String(10), primary_key=True),
    Column("name", String(100)),
)

historic_data_table = Table(
    "historic_data",
    metadata_obj,
    Column("station_code", ForeignKey("stations.station_code"), nullable=False),
    Column("timestamp", String),  # XXX no datetime on SQLite
    Column("WDIR", Integer, nullable=True),
    Column("WSPD", Float, nullable=True),
    Column("GST", Float, nullable=True),
    Column("WVHT", Float, nullable=True),
    Column("DPD", Integer, nullable=True),
    Column("APD", Float, nullable=True),
    Column("MWD", Integer, nullable=True),
    Column("PRES", Float, nullable=True),
    Column("ATMP", Float, nullable=True),
    Column("WTMP", Float, nullable=True),
    Column("DEWP", Float, nullable=True),
    Column("VIS", Float, nullable=True),
    Column("PTDY", Float, nullable=True),
    Column("TIDE", Float, nullable=True),
)

metadata_obj.create_all(engine)

# %%

with engine.begin() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

# %%
df = df.with_columns(
    [pl.datetime("YY", "MM", "DD", "hh", "mm", 0).alias("timestamp")]
).drop(["YY", "MM", "DD", "hh", "mm"])

print(df)

# %%

df_stations.write_database(
    table_name="stations", connection=URI, if_table_exists="replace"
)

df.write_database(table_name="historic_data", connection=URI, if_table_exists="append")

# %%
