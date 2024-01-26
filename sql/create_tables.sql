CREATE TABLE IF NOT EXISTS stations (
    station_code TEXT PRIMARY KEY,
    name TEXT,
    lat FLOAT,
    lon FLOAT
);

CREATE TABLE IF NOT EXISTS z_scores (
    station_code TEXT PRIMARY KEY,
    "ATMP" FLOAT
);
