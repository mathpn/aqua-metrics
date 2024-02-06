CREATE TABLE IF NOT EXISTS stations (
    station_code TEXT PRIMARY KEY,
    name TEXT,
    lat FLOAT,
    lon FLOAT
);

CREATE TABLE IF NOT EXISTS z_scores (
    station_code TEXT PRIMARY KEY,
    "WSPD" FLOAT,
    "ATMP" FLOAT,
    "PRES" FLOAT,
    "WVHT" FLOAT
);

CREATE TABLE realtime_data (
    station_code TEXT,
    "WDIR" BIGINT,
    "WSPD" FLOAT,
    "GST" FLOAT,
    "WVHT" FLOAT,
    "DPD" FLOAT,
    "APD" FLOAT,
    "MWD" FLOAT,
    "PRES" FLOAT,
    "PTDY" FLOAT,
    "ATMP" FLOAT,
    "WTMP" FLOAT,
    "DEWP" FLOAT,
    "VIS" FLOAT,
    "TIDE" FLOAT,
    timestamp DATETIME,
    ingestion_ts DATETIME,
    FOREIGN KEY(station_code) REFERENCES stations(station_code)
);
