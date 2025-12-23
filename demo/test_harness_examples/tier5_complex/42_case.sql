-- Tier 5: CASE Expressions
-- Tests: CASE WHEN ... THEN ... ELSE ... END
-- Expected: Conditional logic in queries

-- Application metadata
-- @name case_demo
-- @description CASE expressions for conditional transformations

-- Categorize user behavior using CASE
CREATE STREAM categorized_activity AS
SELECT
    user_id,
    action,
    page,
    device,
    duration_ms,
    -- Categorize engagement level
    CASE
        WHEN action = 'purchase' THEN 'converter'
        WHEN action IN ('add_to_cart', 'search') THEN 'engaged'
        WHEN action = 'page_view' AND duration_ms > 30000 THEN 'interested'
        ELSE 'browser'
    END AS engagement_level,
    -- Categorize session duration
    CASE
        WHEN duration_ms IS NULL THEN 'unknown'
        WHEN duration_ms < 5000 THEN 'bounce'
        WHEN duration_ms < 30000 THEN 'short'
        WHEN duration_ms < 120000 THEN 'medium'
        ELSE 'long'
    END AS duration_category,
    -- Device category
    CASE
        WHEN device = 'mobile' THEN 'M'
        WHEN device = 'tablet' THEN 'T'
        WHEN device = 'desktop' THEN 'D'
        ELSE 'X'
    END AS device_code,
    event_time
FROM user_activity
EMIT CHANGES
WITH (
    'user_activity.type' = 'kafka_source',
    'user_activity.topic.name' = 'test_user_activity',
    'user_activity.config_file' = '../configs/user_activity_source.yaml',

    'categorized_activity.type' = 'kafka_sink',
    'categorized_activity.topic.name' = 'test_categorized_activity',
    'categorized_activity.config_file' = '../configs/output_stream_sink.yaml'
);
