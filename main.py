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

with open("./data/31005.txt", "r") as f:
    data = f.read()
    data = "\n".join(re.sub(r"\s+", ";", line) for line in data.splitlines())
    buffer = StringIO(data)

df = pl.read_csv(buffer, separator=";", skip_rows=1, null_values=["MM"])

with pl.Config(tbl_cols=df.width):
    print(df)


engine = create_engine("sqlite:///./data/database.db", echo=True)

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
    Column("year", String(4)),
    Column("month", String(2)),
    Column("day", String(2)),
    Column("hour", Integer),
    Column("minute", Integer),
    Column("WDIR", Float),
    Column("WSPD", Float),
    Column("GST", Float),
    Column("WVHT", Float),
    Column("DPD", Float),
    Column("APD", Float),
    Column("MWD", Float),
    Column("PRES", Float),
    Column("ATMP", Float),
    Column("WTMP", Float),
    Column("DEWP", Float),
    Column("VIS", Float),
    Column("PTDY", Float),
    Column("TIDE", Float),
)

metadata_obj.create_all(engine)

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())
