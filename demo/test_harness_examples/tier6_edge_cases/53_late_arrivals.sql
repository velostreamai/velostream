-- SQL Application: late_arrivals_demo
-- Version: 1.0.0
-- Description: Late and out-of-order event handling
-- =============================================================================
-- Tier 6: Late Arrival Handling
-- =============================================================================
--
-- Tests: Out-of-order events, watermark behavior
-- Expected: Correct handling of late data
--
-- =============================================================================

-- @app: late_arrivals_demo
-- @description: Late and out-of-order event handling

-- Windowed aggregation with late event tolerance (uses CREATE TABLE for GROUP BY)
CREATE TABLE sensor_aggregates AS
SELECT
    sensor_id PRIMARY KEY,
    COUNT(*) AS reading_count,
    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,
    SUM(CASE WHEN signal_strength < -70 THEN 1 ELSE 0 END) AS weak_signal_count,
    _window_start AS window_start,
    _window_end AS window_end
FROM sensor_events
GROUP BY sensor_id
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'sensor_events.type' = 'kafka_source',
    'sensor_events.topic.name' = 'test_sensor_events',
    'sensor_events.config_file' = '../configs/sensor_readings_source.yaml',

    'sensor_aggregates.type' = 'kafka_sink',
    'sensor_aggregates.topic.name' = 'test_sensor_aggregates',
    'sensor_aggregates.config_file' = '../configs/aggregates_sink.yaml',

    -- Watermark configuration for late data handling
    'event.time.field' = 'event_time',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s'
);

-- Passthrough with lateness detection based on event ordering
CREATE STREAM lateness_tracking AS
SELECT
    sensor_id,
    location,
    temperature,
    humidity,
    signal_strength,
    status,
    event_time
FROM sensor_events
EMIT CHANGES
WITH (
    'sensor_events.type' = 'kafka_source',
    'sensor_events.topic.name' = 'test_sensor_events',
    'sensor_events.config_file' = '../configs/sensor_readings_source.yaml',

    'lateness_tracking.type' = 'kafka_sink',
    'lateness_tracking.topic.name' = 'test_lateness_tracking',
    'lateness_tracking.config_file' = '../configs/output_stream_sink.yaml'
);
