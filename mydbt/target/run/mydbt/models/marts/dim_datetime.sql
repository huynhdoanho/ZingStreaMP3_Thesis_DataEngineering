
  
    

  create  table
    "dev"."marts"."dim_datetime__dbt_tmp"
    
    
    
  as (
    
WITH date_series AS (
    SELECT
        TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 hour' * num AS date
    FROM
         "dev"."staging"."num"
)
SELECT
    EXTRACT(EPOCH FROM date) AS dateKey,
    date,
    EXTRACT(DAYOFWEEK FROM date) AS dayOfWeek,
    EXTRACT(DAY FROM date) AS dayOfMonth,
    EXTRACT(WEEK FROM date) AS weekOfYear,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (2, 6) THEN True ELSE False END AS weekendFlag
FROM date_series
  );
  