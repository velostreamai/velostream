-- Tier 2: Count Aggregation
-- Tests: COUNT(*), COUNT(field), GROUP BY
-- Expected: Correct counts per group

-- Application metadata
-- @name count_demo
-- @description Count aggregation with GROUP BY

-- Source definition
CREATE SOURCE user_events (
    user_id INTEGER,
    session_id STRING,
    action STRING,
    page STRING,
    device STRING,
    browser STRING,
    duration_ms INTEGER,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Count actions per user
CREATE STREAM action_counts AS
SELECT
    user_id,
    action,
    COUNT(*) AS total_count,
    COUNT(duration_ms) AS timed_count
FROM user_events
GROUP BY user_id, action
WINDOW TUMBLING (INTERVAL '1' MINUTE);

-- Sink definition
CREATE SINK action_counts_sink FOR action_counts WITH (
    'connector' = 'kafka',
    'topic' = 'action_counts',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
