CREATE EXTERNAL TABLE spectrum_schema.status_change_events (
    artist VARCHAR(200),
    auth VARCHAR(200),
    city VARCHAR(200),
    duration DOUBLE PRECISION,
    firstName VARCHAR(200),
    gender VARCHAR(200),
    itemInSession INT,
    lastName VARCHAR(200),
    lat DOUBLE PRECISION,
    level VARCHAR(200),
    lon DOUBLE PRECISION,
    method VARCHAR(200),
    page VARCHAR(200),
    registration BIGINT,
    sessionId INT,
    song VARCHAR(200),
    state VARCHAR(200),
    status INT,
    success BOOLEAN,
    ts TIMESTAMP,
    userAgent VARCHAR(200),
    userId BIGINT,
    zip VARCHAR(200),
    year INT,
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION 's3://zingstreamp3/data/status_change_events/';
