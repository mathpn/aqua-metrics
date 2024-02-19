DELETE FROM latest_history;

INSERT INTO latest_history (
    station_code,
    "WDIR",
    "WSPD",
    "GST",
    "WVHT",
    "DPD",
    "APD",
    "MWD",
    "PRES",
    "ATMP",
    "WTMP",
    "DEWP",
    "VIS",
    "PTDY",
    "TIDE",
    timestamp,
    ingestion_ts
) SELECT
    t1.station_code,
    t1."WDIR",
    t1."WSPD",
    t1."GST",
    t1."WVHT",
    t1."DPD",
    t1."APD",
    t1."MWD",
    t1."PRES",
    t1."ATMP",
    t1."WTMP",
    t1."DEWP",
    t1."VIS",
    t1."PTDY",
    t1."TIDE",
    t1.timestamp,
    t1.ingestion_ts
FROM history_data AS t1
JOIN (
    SELECT station_code, MAX(ingestion_ts) AS ingestion_ts
    FROM history_data
    GROUP BY 1
) AS t2
ON t1.station_code = t2.station_code
AND t1.ingestion_ts = t2.ingestion_ts;
