DROP TABLE IF EXISTS latest_history;

CREATE TABLE latest_history AS
SELECT
    *
FROM history_data AS t1
JOIN (
    SELECT station_code, MAX(ingestion_ts) AS ingestion_ts
    FROM history_data
    GROUP BY 1
) AS t2
ON t1.station_code = t2.station_code
AND t1.ingestion_ts = t2.ingestion_ts;
