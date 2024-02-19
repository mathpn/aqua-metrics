INSERT INTO stations (
    station_code,
    name,
    lat,
    lon
) SELECT
    "STN",
    'station_' || "STN",
    "LAT",
    "LON"
FROM temp_realtime_data
ON CONFLICT DO NOTHING;
