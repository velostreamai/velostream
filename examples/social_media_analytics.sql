-- SQL Application: Social Media Analytics Platform
-- Version: 1.6.0
-- Description: Real-time social media monitoring and sentiment analysis system
-- Author: Social Analytics Team
-- Dependencies: Configuration files in configs/ directory using extends pattern
-- Tag: sentiment:enabled
-- Tag: languages:multi

-- Trending Hashtag Monitor
-- Windowed analysis of popular hashtags
CREATE STREAM trending_hashtags AS
SELECT
    SUBSTRING(content, 1, 20) as hashtag,
    COUNT(*) as mention_count,
    COUNT(user_id) as unique_users,
    AVG(engagement_score) as avg_engagement,
    NOW() as trending_time
FROM social_posts
WHERE content LIKE '%#%'
    AND timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY SUBSTRING(content, 1, 20)
HAVING COUNT(*) > 100
WINDOW TUMBLING(1h);

-- Viral Content Detector
-- Identifies high-engagement content for viral analysis
CREATE STREAM viral_content_alerts AS
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
    NOW() as viral_detected_time
FROM social_posts
WHERE (likes_count + shares_count * 2 + comments_count * 3) > 10000
    AND (likes_count + shares_count * 2 + comments_count * 3) / GREATEST(views_count, 1) > 0.1;

-- Sentiment Analysis Engine
-- Basic sentiment classification of social media content
CREATE STREAM sentiment_analysis AS
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
    NOW() as analyzed_at
FROM social_posts
WHERE content IS NOT NULL AND LENGTH(content) > 10;

-- Influencer Activity Monitor
-- Windowed monitoring of high-follower user activity
CREATE STREAM influencer_activity AS
SELECT
    user_id,
    username,
    follower_count,
    COUNT(*) as post_count_1h,
    SUM(likes_count + shares_count + comments_count) as total_engagement_1h,
    AVG(likes_count + shares_count + comments_count) as avg_engagement_per_post,
    MAX(likes_count + shares_count + comments_count) as max_engagement,
    COUNT(merchant_category) as categories_visited,
    NOW() as monitoring_time
FROM social_posts
WHERE follower_count > 10000
    AND timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY user_id, username, follower_count
HAVING COUNT(*) > 5
WINDOW TUMBLING(1h);

-- Crisis Detection System
-- Emergency and disaster keyword monitoring
CREATE STREAM crisis_alerts AS
SELECT
    'CRISIS_ALERT' as alert_type,
    CASE
        WHEN content LIKE '%disaster%' OR content LIKE '%emergency%' THEN 'DISASTER'
        WHEN content LIKE '%breaking%' OR content LIKE '%urgent%' THEN 'BREAKING_NEWS'
        WHEN content LIKE '%fire%' OR content LIKE '%earthquake%' THEN 'NATURAL_DISASTER'
        ELSE 'GENERAL_CRISIS'
    END as crisis_category,
    COUNT(*) as mention_count,
    COUNT(user_id) as unique_reporters,
    LISTAGG(location, ', ') as affected_locations,
    MIN(timestamp) as first_mention,
    MAX(timestamp) as latest_mention,
    NOW() as detection_time
FROM social_posts
WHERE (content LIKE '%disaster%' OR content LIKE '%emergency%' OR content LIKE '%breaking%'
       OR content LIKE '%urgent%' OR content LIKE '%fire%' OR content LIKE '%earthquake%')
    AND timestamp >= NOW() - INTERVAL '10' MINUTE
GROUP BY CASE
    WHEN content LIKE '%disaster%' OR content LIKE '%emergency%' THEN 'DISASTER'
    WHEN content LIKE '%breaking%' OR content LIKE '%urgent%' THEN 'BREAKING_NEWS'
    WHEN content LIKE '%fire%' OR content LIKE '%earthquake%' THEN 'NATURAL_DISASTER'
    ELSE 'GENERAL_CRISIS'
END
HAVING COUNT(*) > 50;
