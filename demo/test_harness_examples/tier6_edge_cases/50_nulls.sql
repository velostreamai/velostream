-- Tier 6: Null Handling
-- Tests: NULL values, COALESCE, IS NULL, IS NOT NULL
-- Expected: Correct null propagation and handling

-- Application metadata
-- @name nulls_demo
-- @description Null value handling patterns

-- Source definition with nullable fields
CREATE SOURCE sensor_readings (
    sensor_id STRING,
    location STRING,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(7,2),
    battery_level INTEGER,
    signal_strength INTEGER,
    status STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor_readings_nulls',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Handle nulls with COALESCE and defaults
CREATE STREAM processed_readings AS
SELECT
    sensor_id,
    location,
    -- Use COALESCE to provide defaults for null values
    COALESCE(temperature, 0.0) AS temperature,
    COALESCE(humidity, 50.0) AS humidity,
    COALESCE(pressure, 1013.25) AS pressure,
    COALESCE(battery_level, -1) AS battery_level,
    COALESCE(signal_strength, 0) AS signal_strength,
    COALESCE(status, 'unknown') AS status,
    -- Null indicator flags
    CASE WHEN temperature IS NULL THEN 1 ELSE 0 END AS temp_missing,
    CASE WHEN humidity IS NULL THEN 1 ELSE 0 END AS humidity_missing,
    CASE WHEN pressure IS NULL THEN 1 ELSE 0 END AS pressure_missing,
    event_time
FROM sensor_readings;

-- Sink definition
CREATE SINK processed_readings_sink FOR processed_readings WITH (
    'connector' = 'kafka',
    'topic' = 'processed_readings',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
