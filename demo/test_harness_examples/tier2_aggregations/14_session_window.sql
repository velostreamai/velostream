-- Tier 2: Session Window
-- Tests: WINDOW SESSION with gap timeout
-- Expected: Activity-based session grouping

-- Application metadata
-- @name session_window_demo
-- @description Session window for user activity tracking

-- Source definition
CREATE SOURCE user_clicks (
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
    'topic' = 'user_clicks',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Session-based user activity (30-minute session gap)
CREATE STREAM user_sessions AS
SELECT
    user_id,
    COUNT(*) AS action_count,
    COUNT(DISTINCT page) AS pages_visited,
    SUM(duration_ms) AS total_duration_ms,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end
FROM user_clicks
GROUP BY user_id
WINDOW SESSION (INTERVAL '30' MINUTE)
EMIT FINAL;

-- Sink definition
CREATE SINK user_sessions_sink FOR user_sessions WITH (
    'connector' = 'kafka',
    'topic' = 'user_sessions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
