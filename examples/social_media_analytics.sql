-- SQL Application: Social Media Analytics Platform
-- Version: 1.5.2
-- Description: Real-time social media monitoring and sentiment analysis system
-- Author: Social Analytics Team
-- Dependencies: kafka-tweets, kafka-posts, kafka-comments
-- Tag: sentiment:enabled
-- Tag: languages:multi

-- Name: Trending Hashtag Monitor
-- Property: min_mentions=100
-- Property: trending_window=1h
START JOB trending_hashtags AS
SELECT 
    SUBSTRING(content, POSITION('#' IN content), POSITION(' ' IN content FROM POSITION('#' IN content)) - POSITION('#' IN content)) as hashtag,
    COUNT(*) as mention_count,
    COUNT(DISTINCT user_id) as unique_users,
    AVG(engagement_score) as avg_engagement,
    timestamp() as trending_time
FROM social_posts
WHERE content LIKE '%#%'
    AND timestamp >= timestamp() - INTERVAL '1' HOUR
GROUP BY SUBSTRING(content, POSITION('#' IN content), POSITION(' ' IN content FROM POSITION('#' IN content)) - POSITION('#' IN content))
HAVING COUNT(*) > 100
WINDOW TUMBLING(1h)
WITH ('output.topic' = 'trending_hashtags');

-- Name: Viral Content Detector
-- Property: viral_threshold=10000
-- Property: engagement_multiplier=5
START JOB viral_content_detection AS
SELECT 
    post_id,
    user_id,
    content,
    likes_count,
    shares_count,
    comments_count,
    (likes_count + shares_count * 2 + comments_count * 3) as engagement_score,
    views_count,
    (likes_count + shares_count * 2 + comments_count * 3) / GREATEST(views_count, 1) as engagement_rate,
    timestamp() as viral_detected_time
FROM social_posts
WHERE (likes_count + shares_count * 2 + comments_count * 3) > 10000
    AND (likes_count + shares_count * 2 + comments_count * 3) / GREATEST(views_count, 1) > 0.1
WITH ('output.topic' = 'viral_content_alerts');

-- Name: Sentiment Analysis Engine
-- Property: sentiment_threshold=0.8
-- Property: batch_size=1000
START JOB sentiment_analysis AS
SELECT 
    post_id,
    user_id,
    content,
    CASE 
        WHEN content LIKE '%love%' OR content LIKE '%great%' OR content LIKE '%amazing%' THEN 'POSITIVE'
        WHEN content LIKE '%hate%' OR content LIKE '%terrible%' OR content LIKE '%awful%' THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END as sentiment,
    CASE 
        WHEN content LIKE '%!%' OR content LIKE '%!!!%' THEN 'HIGH'
        WHEN content LIKE '%?%' THEN 'MEDIUM'
        ELSE 'LOW'
    END as emotion_intensity,
    LENGTH(content) as content_length,
    timestamp() as analyzed_at
FROM social_posts
WHERE content IS NOT NULL AND LENGTH(content) > 10
WITH ('output.topic' = 'sentiment_analysis');

-- Name: Influencer Activity Monitor
-- Property: min_followers=10000
-- Property: activity_threshold=5
START JOB influencer_monitoring AS
SELECT 
    user_id,
    username,
    follower_count,
    COUNT(*) as post_count_1h,
    SUM(likes_count + shares_count + comments_count) as total_engagement_1h,
    AVG(likes_count + shares_count + comments_count) as avg_engagement_per_post,
    MAX(likes_count + shares_count + comments_count) as max_engagement,
    timestamp() as monitoring_time
FROM social_posts
WHERE follower_count > 10000
    AND timestamp >= timestamp() - INTERVAL '1' HOUR
GROUP BY user_id, username, follower_count
HAVING COUNT(*) > 5
WINDOW TUMBLING(1h)
WITH ('output.topic' = 'influencer_activity');

-- Name: Crisis Detection System
-- Property: crisis_keywords=disaster,emergency,breaking,urgent
-- Property: alert_threshold=50
START JOB crisis_detection AS
SELECT 
    'CRISIS_ALERT' as alert_type,
    CASE 
        WHEN content LIKE '%disaster%' OR content LIKE '%emergency%' THEN 'DISASTER'
        WHEN content LIKE '%breaking%' OR content LIKE '%urgent%' THEN 'BREAKING_NEWS'
        WHEN content LIKE '%fire%' OR content LIKE '%earthquake%' THEN 'NATURAL_DISASTER'
        ELSE 'GENERAL_CRISIS'
    END as crisis_category,
    COUNT(*) as mention_count,
    COUNT(DISTINCT user_id) as unique_reporters,
    LISTAGG(DISTINCT location, ', ') as affected_locations,
    MIN(timestamp) as first_mention,
    MAX(timestamp) as latest_mention,
    timestamp() as detection_time
FROM social_posts
WHERE (content LIKE '%disaster%' OR content LIKE '%emergency%' OR content LIKE '%breaking%' 
       OR content LIKE '%urgent%' OR content LIKE '%fire%' OR content LIKE '%earthquake%')
    AND timestamp >= timestamp() - INTERVAL '10' MINUTE
GROUP BY CASE 
    WHEN content LIKE '%disaster%' OR content LIKE '%emergency%' THEN 'DISASTER'
    WHEN content LIKE '%breaking%' OR content LIKE '%urgent%' THEN 'BREAKING_NEWS'
    WHEN content LIKE '%fire%' OR content LIKE '%earthquake%' THEN 'NATURAL_DISASTER'
    ELSE 'GENERAL_CRISIS'
END
HAVING COUNT(*) > 50
WITH ('output.topic' = 'crisis_alerts');