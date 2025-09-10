-- SQL Application: Social Media Analytics Platform
-- Version: 1.6.0
-- Description: Real-time social media monitoring and sentiment analysis system
-- Author: Social Analytics Team
-- Dependencies: Configuration files in configs/ directory using extends pattern
-- Tag: sentiment:enabled
-- Tag: languages:multi

-- Configure social media streams using extends-based configuration
CREATE STREAM social_posts WITH (
    config_file = 'examples/configs/social_posts_topic.yaml'
);

CREATE STREAM trending_hashtags WITH (
    config_file = 'examples/configs/trending_hashtags_sink.yaml'
);

-- Trending Hashtag Monitor
-- Windowed analysis of popular hashtags
INSERT INTO trending_hashtags
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
WINDOW TUMBLING(1h);

-- Viral Content Detector
-- Identifies high-engagement content for viral analysis
CREATE STREAM viral_content_alerts WITH (
    config_file = 'examples/configs/viral_content_alerts_sink.yaml'
);

INSERT INTO viral_content_alerts
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
    AND (likes_count + shares_count * 2 + comments_count * 3) / GREATEST(views_count, 1) > 0.1;

-- Sentiment Analysis Engine
-- Basic sentiment classification of social media content
CREATE STREAM sentiment_analysis WITH (
    config_file = 'examples/configs/sentiment_analysis_sink.yaml'
);

INSERT INTO sentiment_analysis
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
WHERE content IS NOT NULL AND LENGTH(content) > 10;

-- Influencer Activity Monitor
-- Windowed monitoring of high-follower user activity
CREATE STREAM influencer_activity WITH (
    config_file = 'examples/configs/influencer_activity_sink.yaml'
);

INSERT INTO influencer_activity
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
WINDOW TUMBLING(1h);

-- Crisis Detection System
-- Emergency and disaster keyword monitoring
CREATE STREAM crisis_alerts WITH (
    config_file = 'examples/configs/crisis_alerts_sink.yaml'
);

INSERT INTO crisis_alerts
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
HAVING COUNT(*) > 50;