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

CREATE TABLE IF NOT EXISTS realtime_data (
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
    timestamp timestamp,
    ingestion_ts timestamp,
    CONSTRAINT fk_station FOREIGN KEY(station_code) REFERENCES stations(station_code)
);

CREATE TABLE IF NOT EXISTS latest_history (
    station_code TEXT,
    "WDIR" BIGINT,
    "WSPD" FLOAT,
    "GST" FLOAT,
    "WVHT" FLOAT,
    "DPD" FLOAT,
    "APD" FLOAT,
    "MWD" FLOAT,
    "PRES" FLOAT,
    "ATMP" FLOAT,
    "WTMP" FLOAT,
    "DEWP" FLOAT,
    "VIS" FLOAT,
    "PTDY" FLOAT,
    "TIDE" FLOAT,
    timestamp TEXT,
    ingestion_ts timestamp,
    CONSTRAINT fk_station FOREIGN KEY(station_code) REFERENCES stations(station_code)
);
