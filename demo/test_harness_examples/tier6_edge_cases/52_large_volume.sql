-- Tier 6: Large Volume Processing
-- Tests: 100k+ records, performance under load
-- Expected: Correct processing at scale

-- Application metadata
-- @name large_volume_demo
-- @description High volume data processing test

-- Simple transformation at scale
CREATE STREAM enriched_events AS
SELECT
    event_id,
    user_id,
    event_type,
    category,
    value,
    region,
    value * 1.1 AS adjusted_value,
    event_time
FROM events
WHERE value > 0
EMIT CHANGES
WITH (
    'events.type' = 'kafka_source',
    'events.topic.name' = 'test_events',
    'events.config_file' = 'configs/events_source.yaml',

    'enriched_events.type' = 'kafka_sink',
    'enriched_events.topic.name' = 'test_enriched_events',
    'enriched_events.config_file' = 'configs/output_stream_sink.yaml'
);

-- Aggregation at scale
CREATE STREAM regional_stats AS
SELECT
    region,
    event_type,
    COUNT(*) AS event_count,
    SUM(value) AS total_value,
    AVG(value) AS avg_value,
    _window_start AS window_start,
    _window_end AS window_end
FROM events
GROUP BY region, event_type
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'events.type' = 'kafka_source',
    'events.topic.name' = 'test_events',
    'events.config_file' = 'configs/events_source.yaml',

    'regional_stats.type' = 'kafka_sink',
    'regional_stats.topic.name' = 'test_regional_stats',
    'regional_stats.config_file' = 'configs/aggregates_sink.yaml'
);
