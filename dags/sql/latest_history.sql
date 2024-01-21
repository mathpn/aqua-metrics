DROP TABLE IF EXISTS latest_history;

CREATE TABLE latest_history AS
SELECT
    *
FROM history_data
WHERE ingestion_ts = (
    SELECT MAX(ingestion_ts) FROM history_data
);
