-- Tier 5: CASE Expressions
-- Tests: CASE WHEN ... THEN ... ELSE ... END
-- Expected: Conditional logic in queries

-- Application metadata
-- @name case_demo
-- @description CASE expressions for conditional transformations

-- Source definition
CREATE SOURCE user_activity (
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
    'topic' = 'user_activity_case',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

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
    CASE device
        WHEN 'mobile' THEN 'M'
        WHEN 'tablet' THEN 'T'
        WHEN 'desktop' THEN 'D'
        ELSE 'X'
    END AS device_code,
    event_time
FROM user_activity;

-- Sink definition
CREATE SINK categorized_activity_sink FOR categorized_activity WITH (
    'connector' = 'kafka',
    'topic' = 'categorized_activity',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
