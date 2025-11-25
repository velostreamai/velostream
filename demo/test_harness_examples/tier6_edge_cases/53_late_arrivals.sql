-- Tier 6: Late Arrival Handling
-- Tests: Out-of-order events, watermark behavior
-- Expected: Correct handling of late data

-- Application metadata
-- @name late_arrivals_demo
-- @description Late and out-of-order event handling

-- Source definition with watermark
CREATE SOURCE sensor_events (
    sensor_id STRING,
    measurement_id STRING,
    value DECIMAL(10,4),
    quality INTEGER,
    event_time TIMESTAMP,
    processing_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor_events_late',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092',
    'watermark.column' = 'event_time',
    'watermark.delay' = '30 seconds'
);

-- Windowed aggregation with late event tolerance
CREATE STREAM sensor_aggregates AS
SELECT
    sensor_id,
    COUNT(*) AS reading_count,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    SUM(CASE WHEN quality < 50 THEN 1 ELSE 0 END) AS low_quality_count
FROM sensor_events
GROUP BY sensor_id
WINDOW TUMBLING(INTERVAL '1' MINUTE)
WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND;

-- Track event lateness
CREATE STREAM lateness_tracking AS
SELECT
    sensor_id,
    measurement_id,
    value,
    event_time,
    processing_time,
    -- Calculate lateness (processing_time - event_time)
    CASE
        WHEN processing_time > event_time THEN 'late'
        ELSE 'on_time'
    END AS arrival_status
FROM sensor_events;

-- Sink definitions
CREATE SINK sensor_aggregates_sink FOR sensor_aggregates WITH (
    'connector' = 'kafka',
    'topic' = 'sensor_aggregates',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

CREATE SINK lateness_tracking_sink FOR lateness_tracking WITH (
    'connector' = 'kafka',
    'topic' = 'lateness_tracking',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
