# User Behavior Analytics

Copy-paste SQL queries for analyzing user behavior patterns, engagement metrics, and customer journey analytics.

## User Engagement Analysis

### Session Analytics

```sql
-- Real-time session analysis
SELECT
    user_id,
    session_id,
    MIN(event_timestamp) as session_start,
    MAX(event_timestamp) as session_end,
    COUNT(*) as total_events,
    COUNT(DISTINCT page_url) as pages_visited,
    COUNT(DISTINCT event_type) as event_types,
    DATEDIFF('minutes', MIN(event_timestamp), MAX(event_timestamp)) as session_duration_min,
    -- Engagement metrics
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) as clicks,
    COUNT(CASE WHEN event_type = 'scroll' THEN 1 END) as scrolls,
    COUNT(CASE WHEN event_type = 'form_submit' THEN 1 END) as form_submissions,
    -- Session quality score
    (COUNT(*) * 0.3 +
     COUNT(DISTINCT page_url) * 0.4 +
     DATEDIFF('minutes', MIN(event_timestamp), MAX(event_timestamp)) * 0.3) as engagement_score,
    -- Session classification
    CASE
        WHEN DATEDIFF('minutes', MIN(event_timestamp), MAX(event_timestamp)) > 30 THEN 'LONG_SESSION'
        WHEN COUNT(DISTINCT page_url) > 10 THEN 'HIGH_ENGAGEMENT'
        WHEN COUNT(*) > 50 THEN 'ACTIVE_SESSION'
        ELSE 'STANDARD_SESSION'
    END as session_type
FROM user_events
WHERE event_timestamp > NOW() - INTERVAL '4' HOURS
GROUP BY user_id, session_id
ORDER BY engagement_score DESC;
```

### Page Performance and User Flow

```sql
-- Page-level user behavior analysis
SELECT
    page_url,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    COUNT(*) as total_views,
    -- Time on page analysis
    AVG(CASE
        WHEN LEAD(event_timestamp) OVER (
            PARTITION BY user_id, session_id
            ORDER BY event_timestamp
        ) IS NOT NULL
        THEN DATEDIFF('seconds', event_timestamp,
            LEAD(event_timestamp) OVER (
                PARTITION BY user_id, session_id
                ORDER BY event_timestamp
            )
        )
    END) as avg_time_on_page_sec,
    -- Bounce analysis
    COUNT(CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) = 1
         AND LEAD(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) IS NULL
        THEN 1
    END) as bounces,
    COUNT(CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) = 1
        THEN 1
    END) as entrances,
    -- Bounce rate calculation
    COUNT(CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) = 1
         AND LEAD(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) IS NULL
        THEN 1
    END) * 100.0 / NULLIF(COUNT(CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) = 1
        THEN 1
    END), 0) as bounce_rate_pct
FROM user_events
WHERE event_type = 'page_view'
  AND event_timestamp > NOW() - INTERVAL '24' HOURS
GROUP BY page_url
ORDER BY unique_users DESC;
```

## Customer Journey Analytics

### Funnel Analysis

```sql
-- E-commerce conversion funnel
WITH funnel_events AS (
    SELECT
        user_id,
        session_id,
        event_timestamp,
        event_type,
        page_url,
        -- Define funnel stages
        CASE
            WHEN page_url LIKE '%/home%' OR page_url = '/' THEN 1
            WHEN page_url LIKE '%/product%' THEN 2
            WHEN page_url LIKE '%/cart%' THEN 3
            WHEN event_type = 'purchase' THEN 4
            ELSE 0
        END as funnel_stage,
        CASE
            WHEN page_url LIKE '%/home%' OR page_url = '/' THEN 'Homepage'
            WHEN page_url LIKE '%/product%' THEN 'Product_View'
            WHEN page_url LIKE '%/cart%' THEN 'Cart'
            WHEN event_type = 'purchase' THEN 'Purchase'
            ELSE 'Other'
        END as stage_name
    FROM user_events
    WHERE event_timestamp > NOW() - INTERVAL '7' DAYS
)
SELECT
    stage_name,
    funnel_stage,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    -- Conversion rates
    COUNT(DISTINCT user_id) * 100.0 /
    FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_stage) as conversion_rate_from_start,
    COUNT(DISTINCT user_id) * 100.0 /
    LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_stage) as step_conversion_rate,
    -- Drop-off analysis
    LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_stage) -
    COUNT(DISTINCT user_id) as users_dropped,
    (LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_stage) -
     COUNT(DISTINCT user_id)) * 100.0 /
    LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY funnel_stage) as drop_off_rate_pct
FROM funnel_events
WHERE funnel_stage > 0
GROUP BY stage_name, funnel_stage
ORDER BY funnel_stage;
```

### User Journey Mapping

```sql
-- Detailed user journey analysis
SELECT
    user_id,
    session_id,
    event_timestamp,
    page_url,
    event_type,
    -- Journey progression
    ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) as step_number,
    LAG(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) as previous_page,
    LEAD(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp) as next_page,
    -- Time between steps
    DATEDIFF('seconds',
        LAG(event_timestamp) OVER (PARTITION BY user_id, session_id ORDER BY event_timestamp),
        event_timestamp
    ) as time_from_previous_sec,
    -- Journey patterns
    STRING_AGG(
        CASE
            WHEN page_url LIKE '%/product%' THEN 'P'
            WHEN page_url LIKE '%/category%' THEN 'C'
            WHEN page_url LIKE '%/cart%' THEN 'Cart'
            WHEN page_url LIKE '%/checkout%' THEN 'Check'
            ELSE 'Other'
        END,
        ' -> '
    ) OVER (
        PARTITION BY user_id, session_id
        ORDER BY event_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as journey_path
FROM user_events
WHERE event_timestamp > NOW() - INTERVAL '24' HOURS
  AND event_type IN ('page_view', 'click', 'purchase')
ORDER BY user_id, session_id, event_timestamp;
```

## Behavioral Segmentation

### User Activity Patterns

```sql
-- Segment users by behavior patterns
SELECT
    user_id,
    -- Activity metrics
    COUNT(DISTINCT DATE(event_timestamp)) as active_days,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(*) as total_events,
    AVG(session_length.duration_min) as avg_session_duration,
    -- Engagement patterns
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as cart_additions,
    COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) as product_views,
    COUNT(DISTINCT page_category) as categories_explored,
    -- User classification
    CASE
        WHEN COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 5 THEN 'FREQUENT_BUYER'
        WHEN COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0 THEN 'BUYER'
        WHEN COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) > 0 THEN 'CART_USER'
        WHEN COUNT(*) > 50 THEN 'BROWSER'
        ELSE 'CASUAL_VISITOR'
    END as user_segment,
    -- Recency analysis
    DATEDIFF('days', MAX(event_timestamp), NOW()) as days_since_last_activity,
    CASE
        WHEN DATEDIFF('days', MAX(event_timestamp), NOW()) <= 1 THEN 'VERY_RECENT'
        WHEN DATEDIFF('days', MAX(event_timestamp), NOW()) <= 7 THEN 'RECENT'
        WHEN DATEDIFF('days', MAX(event_timestamp), NOW()) <= 30 THEN 'MODERATE'
        ELSE 'INACTIVE'
    END as recency_segment
FROM user_events e
LEFT JOIN (
    SELECT
        user_id,
        session_id,
        DATEDIFF('minutes', MIN(event_timestamp), MAX(event_timestamp)) as duration_min
    FROM user_events
    WHERE event_timestamp > NOW() - INTERVAL '30' DAYS
    GROUP BY user_id, session_id
) session_length ON e.user_id = session_length.user_id
WHERE e.event_timestamp > NOW() - INTERVAL '30' DAYS
GROUP BY user_id
ORDER BY total_events DESC;
```

### Content Preferences Analysis

```sql
-- Analyze user content preferences and interests
SELECT
    user_id,
    -- Content engagement
    COUNT(DISTINCT content_category) as categories_engaged,
    COUNT(DISTINCT product_id) as unique_products_viewed,
    MAX(content_category) as most_viewed_category,
    -- Time spent analysis
    SUM(CASE WHEN event_type = 'page_view' THEN
        COALESCE(DATEDIFF('seconds', event_timestamp,
            LEAD(event_timestamp) OVER (
                PARTITION BY user_id, session_id
                ORDER BY event_timestamp
            )
        ), 30) -- Default 30 seconds for last page view
    END) as total_time_spent_sec,
    -- Engagement depth
    AVG(scroll_depth) as avg_scroll_depth,
    COUNT(CASE WHEN event_type = 'share' THEN 1 END) as shares,
    COUNT(CASE WHEN event_type = 'comment' THEN 1 END) as comments,
    COUNT(CASE WHEN event_type = 'like' THEN 1 END) as likes,
    -- Interest scoring
    (COUNT(CASE WHEN event_type = 'like' THEN 1 END) * 1.0 +
     COUNT(CASE WHEN event_type = 'share' THEN 1 END) * 2.0 +
     COUNT(CASE WHEN event_type = 'comment' THEN 1 END) * 3.0) as engagement_score,
    -- Content preference patterns
    CASE
        WHEN COUNT(CASE WHEN content_type = 'video' THEN 1 END) >
             COUNT(CASE WHEN content_type = 'article' THEN 1 END) THEN 'VIDEO_PREFERRED'
        WHEN COUNT(CASE WHEN content_type = 'article' THEN 1 END) >
             COUNT(CASE WHEN content_type = 'video' THEN 1 END) THEN 'ARTICLE_PREFERRED'
        ELSE 'MIXED_CONTENT'
    END as content_preference
FROM user_events
WHERE event_timestamp > NOW() - INTERVAL '30' DAYS
GROUP BY user_id
HAVING COUNT(*) > 10  -- Filter for users with meaningful activity
ORDER BY engagement_score DESC;
```

## Retention Analysis

### User Retention Cohorts

```sql
-- Monthly user retention cohort analysis
WITH user_cohorts AS (
    SELECT
        user_id,
        DATE_FORMAT(MIN(event_timestamp), '%Y-%m') as cohort_month,
        MIN(event_timestamp) as first_activity
    FROM user_events
    GROUP BY user_id
),
cohort_activity AS (
    SELECT
        uc.user_id,
        uc.cohort_month,
        DATE_FORMAT(e.event_timestamp, '%Y-%m') as activity_month,
        DATEDIFF('month', uc.first_activity, e.event_timestamp) as period_number
    FROM user_cohorts uc
    JOIN user_events e ON uc.user_id = e.user_id
    WHERE e.event_timestamp >= uc.first_activity
)
SELECT
    cohort_month,
    COUNT(DISTINCT CASE WHEN period_number = 0 THEN user_id END) as cohort_size,
    COUNT(DISTINCT CASE WHEN period_number = 1 THEN user_id END) as month_1_retained,
    COUNT(DISTINCT CASE WHEN period_number = 2 THEN user_id END) as month_2_retained,
    COUNT(DISTINCT CASE WHEN period_number = 3 THEN user_id END) as month_3_retained,
    COUNT(DISTINCT CASE WHEN period_number = 6 THEN user_id END) as month_6_retained,
    -- Retention rates
    COUNT(DISTINCT CASE WHEN period_number = 1 THEN user_id END) * 100.0 /
    COUNT(DISTINCT CASE WHEN period_number = 0 THEN user_id END) as month_1_retention_pct,
    COUNT(DISTINCT CASE WHEN period_number = 2 THEN user_id END) * 100.0 /
    COUNT(DISTINCT CASE WHEN period_number = 0 THEN user_id END) as month_2_retention_pct,
    COUNT(DISTINCT CASE WHEN period_number = 3 THEN user_id END) * 100.0 /
    COUNT(DISTINCT CASE WHEN period_number = 0 THEN user_id END) as month_3_retention_pct
FROM cohort_activity
GROUP BY cohort_month
ORDER BY cohort_month;
```

### Churn Prediction Analysis

```sql
-- Identify users at risk of churning
SELECT
    user_id,
    last_activity,
    DATEDIFF('days', last_activity, NOW()) as days_since_last_activity,
    total_sessions_last_30d,
    avg_session_duration_min,
    total_purchases,
    days_since_last_purchase,
    -- Churn risk factors
    CASE
        WHEN DATEDIFF('days', last_activity, NOW()) > 30 THEN 'HIGH_RISK'
        WHEN DATEDIFF('days', last_activity, NOW()) > 14 THEN 'MEDIUM_RISK'
        WHEN DATEDIFF('days', last_activity, NOW()) > 7 THEN 'LOW_RISK'
        ELSE 'ACTIVE'
    END as churn_risk_level,
    -- Engagement trend
    CASE
        WHEN recent_sessions < historical_avg_sessions * 0.5 THEN 'DECLINING_ENGAGEMENT'
        WHEN recent_sessions > historical_avg_sessions * 1.5 THEN 'INCREASING_ENGAGEMENT'
        ELSE 'STABLE_ENGAGEMENT'
    END as engagement_trend,
    -- Churn probability score
    (CASE WHEN DATEDIFF('days', last_activity, NOW()) > 30 THEN 40 ELSE 0 END +
     CASE WHEN total_sessions_last_30d < 2 THEN 30 ELSE 0 END +
     CASE WHEN total_purchases = 0 THEN 20 ELSE 0 END +
     CASE WHEN avg_session_duration_min < 2 THEN 10 ELSE 0 END) as churn_probability_score
FROM (
    SELECT
        user_id,
        MAX(event_timestamp) as last_activity,
        COUNT(DISTINCT session_id) as total_sessions_last_30d,
        AVG(DATEDIFF('minutes',
            MIN(event_timestamp),
            MAX(event_timestamp)
        )) as avg_session_duration_min,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as total_purchases,
        COALESCE(DATEDIFF('days', MAX(CASE WHEN event_type = 'purchase' THEN event_timestamp END), NOW()), 999) as days_since_last_purchase,
        -- Recent vs historical comparison
        COUNT(DISTINCT CASE WHEN event_timestamp > NOW() - INTERVAL '7' DAYS THEN session_id END) as recent_sessions,
        COUNT(DISTINCT CASE WHEN event_timestamp BETWEEN NOW() - INTERVAL '30' DAYS AND NOW() - INTERVAL '7' DAYS THEN session_id END) / 3.0 as historical_avg_sessions
    FROM user_events
    WHERE event_timestamp > NOW() - INTERVAL '30' DAYS
    GROUP BY user_id
) user_metrics
ORDER BY churn_probability_score DESC;
```

## A/B Testing Analysis

### Experiment Performance Comparison

```sql
-- A/B test results analysis
SELECT
    experiment_id,
    variant,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    -- Conversion metrics
    COUNT(CASE WHEN event_type = 'conversion' THEN 1 END) as conversions,
    COUNT(CASE WHEN event_type = 'conversion' THEN 1 END) * 100.0 /
    COUNT(DISTINCT user_id) as conversion_rate_pct,
    -- Engagement metrics
    AVG(CASE WHEN event_type = 'page_view' THEN
        DATEDIFF('seconds', event_timestamp,
            LEAD(event_timestamp) OVER (
                PARTITION BY user_id, session_id
                ORDER BY event_timestamp
            )
        )
    END) as avg_page_time_sec,
    AVG(session_events.event_count) as avg_events_per_session,
    -- Revenue metrics (if applicable)
    SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) as total_revenue,
    AVG(CASE WHEN event_type = 'purchase' THEN revenue END) as avg_revenue_per_purchase,
    SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) /
    COUNT(DISTINCT user_id) as revenue_per_user
FROM user_events e
JOIN (
    SELECT
        user_id,
        session_id,
        COUNT(*) as event_count
    FROM user_events
    WHERE event_timestamp > NOW() - INTERVAL '30' DAYS
    GROUP BY user_id, session_id
) session_events ON e.user_id = session_events.user_id AND e.session_id = session_events.session_id
WHERE e.event_timestamp > NOW() - INTERVAL '30' DAYS
  AND experiment_id IS NOT NULL
GROUP BY experiment_id, variant
ORDER BY experiment_id, variant;
```

## User Experience Metrics

### Performance Impact on User Behavior

```sql
-- Analyze how page performance affects user behavior
SELECT
    page_url,
    -- Performance metrics
    CASE
        WHEN load_time_ms < 1000 THEN 'FAST'
        WHEN load_time_ms < 3000 THEN 'MODERATE'
        ELSE 'SLOW'
    END as performance_tier,
    AVG(load_time_ms) as avg_load_time_ms,
    -- User behavior metrics
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_page_views,
    -- Bounce analysis by performance
    COUNT(CASE
        WHEN next_page_in_session IS NULL THEN 1
    END) as bounces,
    COUNT(CASE
        WHEN next_page_in_session IS NULL THEN 1
    END) * 100.0 / COUNT(*) as bounce_rate_pct,
    -- Time on page by performance
    AVG(CASE WHEN time_on_page_sec BETWEEN 1 AND 600 THEN time_on_page_sec END) as avg_time_on_page_sec,
    -- Conversion impact
    COUNT(CASE WHEN converted_in_session = true THEN 1 END) as conversions,
    COUNT(CASE WHEN converted_in_session = true THEN 1 END) * 100.0 / COUNT(*) as conversion_rate_pct
FROM (
    SELECT
        e.page_url,
        e.user_id,
        e.session_id,
        e.event_timestamp,
        e.load_time_ms,
        LEAD(e.page_url) OVER (
            PARTITION BY e.user_id, e.session_id
            ORDER BY e.event_timestamp
        ) as next_page_in_session,
        DATEDIFF('seconds', e.event_timestamp,
            LEAD(e.event_timestamp) OVER (
                PARTITION BY e.user_id, e.session_id
                ORDER BY e.event_timestamp
            )
        ) as time_on_page_sec,
        CASE WHEN EXISTS(
            SELECT 1 FROM user_events e2
            WHERE e2.user_id = e.user_id
              AND e2.session_id = e.session_id
              AND e2.event_type = 'conversion'
              AND e2.event_timestamp > e.event_timestamp
        ) THEN true ELSE false END as converted_in_session
    FROM user_events e
    WHERE e.event_type = 'page_view'
      AND e.event_timestamp > NOW() - INTERVAL '7' DAYS
      AND e.load_time_ms IS NOT NULL
) page_sessions
GROUP BY page_url, performance_tier
ORDER BY avg_load_time_ms, unique_users DESC;
```

## Search and Discovery Analytics

### Search Behavior Analysis

```sql
-- Analyze user search patterns and success rates
SELECT
    search_query,
    COUNT(*) as search_count,
    COUNT(DISTINCT user_id) as unique_searchers,
    COUNT(DISTINCT session_id) as unique_sessions,
    -- Search success metrics
    COUNT(CASE WHEN result_clicked = true THEN 1 END) as searches_with_clicks,
    COUNT(CASE WHEN result_clicked = true THEN 1 END) * 100.0 / COUNT(*) as click_through_rate_pct,
    COUNT(CASE WHEN converted_after_search = true THEN 1 END) as searches_with_conversion,
    COUNT(CASE WHEN converted_after_search = true THEN 1 END) * 100.0 / COUNT(*) as search_conversion_rate_pct,
    -- Results quality
    AVG(num_results) as avg_results_returned,
    AVG(CASE WHEN result_clicked = true THEN click_position END) as avg_click_position,
    -- Search refinement patterns
    COUNT(CASE WHEN refined_search = true THEN 1 END) as refined_searches,
    COUNT(CASE WHEN refined_search = true THEN 1 END) * 100.0 / COUNT(*) as refinement_rate_pct
FROM (
    SELECT
        s.user_id,
        s.session_id,
        s.search_query,
        s.event_timestamp,
        s.num_results,
        CASE WHEN EXISTS(
            SELECT 1 FROM user_events e
            WHERE e.user_id = s.user_id
              AND e.session_id = s.session_id
              AND e.event_type = 'result_click'
              AND e.event_timestamp BETWEEN s.event_timestamp AND s.event_timestamp + INTERVAL '5' MINUTES
        ) THEN true ELSE false END as result_clicked,
        CASE WHEN EXISTS(
            SELECT 1 FROM user_events e
            WHERE e.user_id = s.user_id
              AND e.session_id = s.session_id
              AND e.event_type = 'conversion'
              AND e.event_timestamp > s.event_timestamp
        ) THEN true ELSE false END as converted_after_search,
        CASE WHEN EXISTS(
            SELECT 1 FROM user_events e
            WHERE e.user_id = s.user_id
              AND e.session_id = s.session_id
              AND e.event_type = 'search'
              AND e.event_timestamp > s.event_timestamp
              AND e.event_timestamp < s.event_timestamp + INTERVAL '10' MINUTES
        ) THEN true ELSE false END as refined_search,
        (SELECT MIN(rc.click_position) FROM user_events rc
         WHERE rc.user_id = s.user_id
           AND rc.session_id = s.session_id
           AND rc.event_type = 'result_click'
           AND rc.event_timestamp BETWEEN s.event_timestamp AND s.event_timestamp + INTERVAL '5' MINUTES
        ) as click_position
    FROM user_events s
    WHERE s.event_type = 'search'
      AND s.event_timestamp > NOW() - INTERVAL '7' DAYS
) search_analysis
GROUP BY search_query
HAVING search_count > 5  -- Focus on popular searches
ORDER BY search_count DESC;
```

## Performance Tips

1. **Partition by time** for efficient time-range queries
2. **Index user_id and session_id** for user journey analysis
3. **Use window functions** for sequence analysis
4. **Consider data sampling** for large datasets
5. **Cache aggregated metrics** for real-time dashboards

## Common Behavioral Patterns

```sql
-- Pattern: User Engagement Level
CASE
    WHEN total_events > 100 AND sessions > 10 THEN 'HIGHLY_ENGAGED'
    WHEN total_events > 50 AND sessions > 5 THEN 'MODERATELY_ENGAGED'
    WHEN total_events > 10 THEN 'LIGHTLY_ENGAGED'
    ELSE 'MINIMAL_ENGAGEMENT'
END as engagement_level;

-- Pattern: Session Quality
CASE
    WHEN session_duration > 30 AND pages_visited > 5 THEN 'HIGH_QUALITY'
    WHEN session_duration > 10 AND pages_visited > 2 THEN 'MEDIUM_QUALITY'
    ELSE 'LOW_QUALITY'
END as session_quality;

-- Pattern: User Lifecycle Stage
CASE
    WHEN days_since_signup <= 7 THEN 'NEW_USER'
    WHEN days_since_signup <= 30 THEN 'EARLY_ADOPTER'
    WHEN last_activity > NOW() - INTERVAL '7' DAYS THEN 'ACTIVE_USER'
    WHEN last_activity > NOW() - INTERVAL '30' DAYS THEN 'AT_RISK'
    ELSE 'CHURNED'
END as lifecycle_stage;
```