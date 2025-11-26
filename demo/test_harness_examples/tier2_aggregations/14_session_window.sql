-- Tier 2: Session Window Aggregation
-- Tests: WINDOW SESSION with gap timeout
-- Expected: Activity-based session grouping

-- Application metadata
-- @name session_window_demo
-- @description Session window aggregation

CREATE STREAM session_output AS
SELECT
    user_id,
    COUNT(*) AS actions_in_session,
    SUM(duration_ms) AS total_duration,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end
FROM user_activity
GROUP BY user_id
WINDOW SESSION(30s)
EMIT CHANGES
WITH (
    'user_activity.type' = 'kafka_source',
    'user_activity.topic.name' = 'test_user_activity',
    'user_activity.config_file' = 'configs/user_activity_source.yaml',

    'session_output.type' = 'kafka_sink',
    'session_output.topic.name' = 'test_session_output',
    'session_output.config_file' = 'configs/aggregates_sink.yaml'
);
