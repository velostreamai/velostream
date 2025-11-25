-- Tier 6: Large Volume Processing
-- Tests: 100k+ records, performance under load
-- Expected: Correct processing at scale

-- Application metadata
-- @name large_volume_demo
-- @description High volume data processing test

-- Source definition
CREATE SOURCE events (
    event_id STRING,
    user_id INTEGER,
    event_type STRING,
    category STRING,
    value DECIMAL(10,2),
    region STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'events_large_volume',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

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
WHERE value > 0;

-- Aggregation at scale
CREATE STREAM regional_stats AS
SELECT
    region,
    event_type,
    COUNT(*) AS event_count,
    SUM(value) AS total_value,
    AVG(value) AS avg_value
FROM events
GROUP BY region, event_type
WINDOW TUMBLING(INTERVAL '1' MINUTE);

-- Sink definitions
CREATE SINK enriched_events_sink FOR enriched_events WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

CREATE SINK regional_stats_sink FOR regional_stats WITH (
    'connector' = 'kafka',
    'topic' = 'regional_stats',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
