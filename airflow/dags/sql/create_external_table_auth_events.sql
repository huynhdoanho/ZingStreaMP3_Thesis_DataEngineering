-- tạo bảng

CREATE EXTERNAL TABLE spectrum_schema.auth_events (
    city VARCHAR(200),
    firstName VARCHAR(200),
    gender VARCHAR(200),
    itemInSession INT,
    lastName VARCHAR(200),
    lat DOUBLE PRECISION,
    level VARCHAR(200),
    lon DOUBLE PRECISION,
    registration BIGINT,
    sessionId INT,
    state VARCHAR(200),
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
LOCATION 's3://zingstreamp3/data/auth_events/';