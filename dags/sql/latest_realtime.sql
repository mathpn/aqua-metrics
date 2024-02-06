PRAGMA foreign_keys=ON;

INSERT INTO realtime_data (
    station_code,
    "WDIR",
    "WSPD",
    "GST",
    "WVHT",
    "DPD",
    "APD",
    "MWD",
    "PRES",
    "PTDY",
    "ATMP",
    "WTMP",
    "DEWP",
    "VIS",
    "TIDE",
    timestamp,
    ingestion_ts
) SELECT
    "STN",
    "WDIR",
    "WSPD",
    "GST",
    "WVHT",
    "DPD",
    "APD",
    "MWD",
    "PRES",
    "PTDY",
    "ATMP",
    "WTMP",
    "DEWP",
    "VIS",
    "TIDE",
    timestamp,
    ingestion_ts
FROM temp_realtime_data;