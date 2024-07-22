-- tạo spectrum schema:
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG DATABASE 'zingstreamp3'
IAM_ROLE 'arn:aws:iam::891377228605:role/MySpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

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


CREATE EXTERNAL TABLE spectrum_schema.page_view_events (
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
LOCATION 's3://zingstreamp3/data/page_view_events/';



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
