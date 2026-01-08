-- SQL Application: nulls_demo
-- Version: 1.0.0
-- Description: Null value handling patterns
-- =============================================================================
-- Tier 6: Null Handling
-- =============================================================================
--
-- Tests: NULL values, COALESCE, IS NULL, IS NOT NULL
-- Expected: Correct null propagation and handling
--
-- =============================================================================

-- @app: nulls_demo
-- @description: Null value handling patterns

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
FROM sensor_readings
EMIT CHANGES
WITH (
    'sensor_readings.type' = 'kafka_source',
    'sensor_readings.topic.name' = 'test_sensor_readings',
    'sensor_readings.config_file' = '../configs/sensor_readings_source.yaml',

    'processed_readings.type' = 'kafka_sink',
    'processed_readings.topic.name' = 'test_processed_readings',
    'processed_readings.config_file' = '../configs/output_stream_sink.yaml'
);
