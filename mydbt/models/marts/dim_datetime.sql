{{ config(materialized = 'table') }}
WITH date_series AS (
    SELECT
        TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 hour' * (ROW_NUMBER() OVER ()) AS date
    FROM
        (SELECT 1 FROM STL_SCAN LIMIT 7440) AS t
)
SELECT
    EXTRACT(EPOCH FROM date) AS dateKey,
    date,
    EXTRACT(DAYOFWEEK FROM date) AS dayOfWeek,
    EXTRACT(DAY FROM date) AS dayOfMonth,
    EXTRACT(WEEK FROM date) AS weekOfYear,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN True ELSE False END AS weekendFlag
FROM date_series
