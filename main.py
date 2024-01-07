import re
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
