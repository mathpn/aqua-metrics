INSERT OR IGNORE INTO stations (
    station_code,
    name,
    lat,
    lon
) SELECT
    "STN",
    'station_' || "STN",
    "LAT",
    "LON"
FROM realtime_data
WHERE ingestion_ts = (SELECT MAX(ingestion_ts) FROM realtime_data);
