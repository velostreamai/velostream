-- SQL Application: Social Media Analytics Platform (Phase 1B-4 Features)
-- Version: 2.0.0
-- Description: Real-time social media monitoring showcasing advanced FerrisStreams capabilities
-- Author: Social Analytics Team
-- Features: Event-time processing, Circuit breakers, Advanced window functions, Observability
-- Dependencies: Configuration files in configs/ directory using extends pattern
-- Tag: sentiment:enabled
-- Tag: languages:multi
-- Tag: features:watermarks,circuit-breakers,advanced-sql,observability

-- ====================================================================================
-- PHASE 1B: EVENT-TIME PROCESSING - Social Posts Stream
-- ====================================================================================
-- Process social media posts with proper event-time handling for viral content detection

CREATE STREAM social_posts_with_event_time AS
SELECT 
    post_id,
    user_id,
    content,
    platform,
    engagement_score,
    post_timestamp,
    -- Extract event-time from when post was actually created
    TIMESTAMP(post_timestamp) as event_time,
    follower_count,
    hashtags,
    mentions
FROM social_posts_raw
WITH (
    -- Phase 1B: Event-time configuration for social media bursts
    'event.time.field' = 'post_timestamp',
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s',  -- Social media can have delays
    'late.data.strategy' = 'include_in_next_window', -- Include delayed viral posts
    
    'config_file' = 'examples/configs/social_posts_topic.yaml'
);

-- ====================================================================================
-- PHASE 3: ADVANCED WINDOW FUNCTIONS - Viral Content Detection
-- ====================================================================================
-- Sophisticated viral content detection using advanced SQL features

CREATE STREAM viral_content_detection AS
SELECT 
    post_id,
    user_id,
    content,
    platform,
    engagement_score,
    event_time,
    follower_count,
    
    -- Phase 3: Advanced window functions for viral detection
    RANK() OVER (
        PARTITION BY platform 
        ORDER BY engagement_score DESC
    ) as engagement_rank,
    
    DENSE_RANK() OVER (
        PARTITION BY DATE_TRUNC('hour', event_time), platform
        ORDER BY engagement_score DESC  
    ) as hourly_engagement_rank,
    
    PERCENT_RANK() OVER (
        PARTITION BY platform
        ORDER BY engagement_score
    ) as engagement_percentile,
    
    -- Engagement velocity analysis
    LAG(engagement_score, 1) OVER (
        PARTITION BY user_id 
        ORDER BY event_time
    ) as prev_engagement,
    
    LEAD(engagement_score, 1) OVER (
        PARTITION BY user_id
        ORDER BY event_time  
    ) as next_engagement,
    
    -- Statistical analysis over sliding window
    AVG(engagement_score) OVER (
        PARTITION BY platform
        ORDER BY event_time
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) as avg_engagement_100,
    
    STDDEV(engagement_score) OVER (
        PARTITION BY platform
        ORDER BY event_time
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) as engagement_volatility,
    
    -- Z-score for anomaly detection
    (engagement_score - AVG(engagement_score) OVER (
        PARTITION BY platform
        ORDER BY event_time
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    )) / NULLIF(STDDEV(engagement_score) OVER (
        PARTITION BY platform
        ORDER BY event_time
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ), 0) as engagement_z_score,
    
    -- Viral classification using complex CASE
    CASE
        WHEN engagement_score > AVG(engagement_score) OVER (
            PARTITION BY platform
            ORDER BY event_time
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) * 10 THEN 'MEGA_VIRAL'
        WHEN engagement_score > AVG(engagement_score) OVER (
            PARTITION BY platform
            ORDER BY event_time
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ) * 5 THEN 'VIRAL'
        WHEN ABS((engagement_score - AVG(engagement_score) OVER (
            PARTITION BY platform
            ORDER BY event_time
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(engagement_score) OVER (
            PARTITION BY platform
            ORDER BY event_time
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        ), 0)) > 3.0 THEN 'TRENDING'
        ELSE 'NORMAL'
    END as viral_status,
    
    NOW() as detection_time
FROM social_posts_with_event_time
-- Phase 1B: Event-time tumbling windows (5-minute analysis windows)
WINDOW TUMBLING (event_time, INTERVAL '5' MINUTE)
-- Phase 3: Complex HAVING with subqueries
HAVING engagement_score > (
    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY engagement_score)
    FROM social_posts_with_event_time s2
    WHERE s2.platform = social_posts_with_event_time.platform
    AND s2.event_time >= social_posts_with_event_time.event_time - INTERVAL '1' HOUR
)
AND COUNT(*) >= 3  -- At least 3 posts in window
INTO viral_content_alerts
WITH (
    -- Phase 2: Resource management for high-volume social data
    'max.memory.mb' = '4096',
    'max.groups' = '500000',  -- Many users and hashtags
    'spill.to.disk' = 'true',
    'memory.pressure.threshold' = '0.8',
    
    -- Phase 2: Circuit breaker for external API calls
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '5',
    'circuit.breaker.timeout' = '90s',
    'circuit.breaker.slow.call.threshold' = '15s',
    
    -- Phase 2: Retry configuration for social media APIs
    'retry.max.attempts' = '3',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '200ms',
    'retry.max.delay' = '30s',
    
    -- Phase 4: Comprehensive observability
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'viral_content_detection',
    'prometheus.histogram.buckets' = '0.1,0.5,1.0,5.0,15.0,30.0,60.0',
    
    'config_file' = 'examples/configs/viral_content_alerts_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B+3: HASHTAG TRENDING WITH SESSION WINDOWS
-- ====================================================================================
-- Advanced hashtag analysis using session-based windows and joins

CREATE STREAM hashtag_trending_analysis AS
SELECT 
    hashtag,
    COUNT(DISTINCT post_id) as post_count,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(engagement_score) as total_engagement,
    AVG(engagement_score) as avg_engagement,
    
    -- Phase 3: Advanced statistical measures
    STDDEV(engagement_score) as engagement_stddev,
    VARIANCE(engagement_score) as engagement_variance,
    MIN(engagement_score) as min_engagement,
    MAX(engagement_score) as max_engagement,
    
    -- Trending velocity calculations
    COUNT(*) / EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) * 3600 as posts_per_hour,
    
    -- Session time analysis
    SESSION_START() as trend_start,
    SESSION_END() as trend_end,
    EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 60 as trend_duration_minutes,
    
    -- Platform distribution analysis
    LISTAGG(DISTINCT platform, ', ') as platforms,
    COUNT(DISTINCT platform) as platform_count,
    
    -- Influence analysis using window functions
    AVG(follower_count) as avg_influencer_reach,
    MAX(follower_count) as top_influencer_reach,
    
    -- Complex trend classification
    CASE
        WHEN COUNT(DISTINCT post_id) > 1000 
         AND COUNT(DISTINCT user_id) > 500
         AND EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 60 < 60 
         THEN 'EXPLOSIVE_TREND'
        WHEN COUNT(DISTINCT post_id) > 100 
         AND AVG(engagement_score) > 1000 
         THEN 'HIGH_ENGAGEMENT_TREND'
        WHEN COUNT(DISTINCT platform) >= 3 
         AND COUNT(DISTINCT user_id) > 100 
         THEN 'CROSS_PLATFORM_TREND'
        WHEN EXTRACT(EPOCH FROM (SESSION_END() - SESSION_START())) / 3600 > 2
         AND COUNT(DISTINCT post_id) > 50
         THEN 'SUSTAINED_TREND' 
        ELSE 'EMERGING_TREND'
    END as trend_classification,
    
    NOW() as analysis_time
FROM (
    SELECT 
        UNNEST(STRING_TO_ARRAY(REGEXP_REPLACE(content, '[^#\w\s]', '', 'g'), ' ')) as hashtag,
        post_id,
        user_id, 
        engagement_score,
        event_time,
        platform,
        follower_count
    FROM social_posts_with_event_time
    WHERE content LIKE '%#%'
) hashtag_posts
WHERE hashtag LIKE '#%'  -- Only actual hashtags
-- Phase 1B: Session windows based on hashtag activity (30-minute inactivity gap)
GROUP BY hashtag, SESSION(event_time, INTERVAL '30' MINUTE)
-- Phase 3: Complex HAVING with multiple aggregation conditions
HAVING COUNT(DISTINCT post_id) >= 10
   AND COUNT(DISTINCT user_id) >= 5  
   AND SUM(engagement_score) > 5000
   AND EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) >= 300  -- At least 5 minutes
INTO trending_hashtags
WITH (
    -- Phase 2: Full resource and fault tolerance configuration
    'max.memory.mb' = '8192',  -- Large memory for hashtag processing
    'max.groups' = '1000000',  -- Many possible hashtags
    'max.session.windows' = '100000',
    'spill.to.disk' = 'true',
    'session.timeout' = '3600s',  -- 1 hour max session
    
    -- Circuit breaker for trending analysis
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '3',
    'circuit.breaker.success.threshold' = '7',
    'circuit.breaker.timeout' = '120s',
    
    -- Retry for critical trending data
    'retry.max.attempts' = '5',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '100ms',
    'retry.max.delay' = '60s',
    
    -- Dead letter queue for complex hashtag analysis failures
    'dead.letter.queue.enabled' = 'true',
    'dead.letter.queue.topic' = 'hashtag-analysis-failures',
    
    -- Phase 4: Advanced observability for trending analysis
    'observability.metrics.enabled' = 'true',
    'observability.tracing.enabled' = 'true',
    'observability.profiling.enabled' = 'true',
    'observability.span.name' = 'hashtag_trending_analysis',
    'observability.alerts.enabled' = 'true',
    'prometheus.histogram.buckets' = '1,5,10,30,60,300,1800,3600',
    
    'config_file' = 'examples/configs/trending_hashtags_sink.yaml'
);

-- ====================================================================================
-- PHASE 1B-4 SOCIAL MEDIA FEATURE SHOWCASE SUMMARY
-- ====================================================================================
-- This social media analytics demo demonstrates:

-- PHASE 1B: Watermarks & Time Semantics
-- ✓ Event-time extraction from post_timestamp for viral detection
-- ✓ BoundedOutOfOrderness strategy with social media delay tolerance
-- ✓ Session windows for hashtag trending analysis
-- ✓ Late data inclusion for delayed viral posts

-- PHASE 2: Resource Management & Circuit Breakers
-- ✓ High-memory configuration for hashtag processing
-- ✓ Circuit breakers for external social media API calls
-- ✓ Retry logic for API rate limiting scenarios  
-- ✓ Dead letter queue for complex analysis failures

-- PHASE 3: Advanced Query Features  
-- ✓ Window functions: RANK, DENSE_RANK, PERCENT_RANK for viral ranking
-- ✓ Statistical functions: STDDEV, VARIANCE for engagement analysis
-- ✓ Session windows with SESSION_START/SESSION_END
-- ✓ Complex subqueries in HAVING clauses
-- ✓ String functions: UNNEST, STRING_TO_ARRAY, REGEXP_REPLACE
-- ✓ Advanced CASE expressions for trend classification

-- PHASE 4: Observability Integration
-- ✓ Distributed tracing for viral detection pipeline
-- ✓ Prometheus metrics with custom buckets for social engagement
-- ✓ Performance profiling for hashtag processing bottlenecks
-- ✓ Alert integration for explosive trend detection

-- Use cases demonstrated:
-- - Real-time viral content detection with statistical anomaly analysis
-- - Hashtag trending analysis with session-based windows
-- - Cross-platform social media monitoring
-- - Influencer impact analysis with engagement velocity
-- - Robust handling of high-volume, bursty social media data