CREATE EXTERNAL TABLE spectrum_schema.listen_events (
    artist VARCHAR(200),
    song VARCHAR(200),
    duration DOUBLE PRECISION,
    ts TIMESTAMP,
    sessionId INT,
    auth VARCHAR(200),
    level VARCHAR(200),
    itemInSession INT,
    city VARCHAR(200),
    zip VARCHAR(200),
    state VARCHAR(200),
    userAgent VARCHAR(200),
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    userId BIGINT,
    lastName VARCHAR(200),
    firstName VARCHAR(200),
    gender VARCHAR(200),
    registration BIGINT,
    year INT,
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION 's3://zingstreamp3/data/listen_events/';