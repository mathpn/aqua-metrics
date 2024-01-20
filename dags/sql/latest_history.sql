-- TODO: create table
INSERT INTO latest_history
SELECT
    *
FROM history_data
WHERE ingestion_ts = (
    SELECT MAX(ingestion_ts) FROM history_data
);
