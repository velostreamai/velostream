-- =============================================================================
-- APP: Price Analytics
-- =============================================================================
-- @app price_analytics
-- @description Price movement detection with window functions and alerts
-- @version 1.0.0
-- @depends_on app_market_data (market_data_ts)
--
-- Pipeline Flow:
--   market_data_ts → [price_movement_alerts]
--                  → [price_movement_debug]
--                  → [price_stats]
--
-- External Dependencies:
--   - market_data_ts: From app_market_data pipeline
--
-- Output Topics:
--   - price_alerts: Price movement alerts with severity
--   - price_movement_debug: Debug stream for filter visibility
--   - price_stats: Simple 1-minute price statistics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Stage 1: Advanced Price Movement Alerts
-- -----------------------------------------------------------------------------
-- Uses LAG, LEAD, RANK, and other window functions to detect price movements.
-- Classifies movements as SIGNIFICANT, MODERATE, or NORMAL.

CREATE STREAM price_movement_alerts AS
SELECT
    symbol PRIMARY KEY,
    price,
    volume,
    event_time,

    -- Previous and next prices
    LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY event_time
    ) as prev_price,
    LEAD(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY event_time
    ) as next_price,

    -- Price change percentage
    (price - LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY event_time
    )) /
     LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY event_time
    ) * 100 as price_change_pct,

    -- Ranking functions
    RANK() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY price DESC
    ) as price_rank,
    DENSE_RANK() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY volume DESC
    ) as volume_rank,
    PERCENT_RANK() OVER (
        ROWS WINDOW
            BUFFER 1000 ROWS
            PARTITION BY symbol
            ORDER BY price
    ) as price_percentile,

    -- Volatility measure
    STDDEV(price) OVER (
        ROWS WINDOW BUFFER 10 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) as price_volatility_10_periods,

    -- Movement severity classification
    CASE
        WHEN ABS((price - LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY event_time
                 )) /
                 LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY event_time
                 )) * 100 > 5.0 THEN 'SIGNIFICANT'
        WHEN ABS((price - LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY event_time
                 )) /
                 LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY event_time
                 )) * 100 > 2.0 THEN 'MODERATE'
        ELSE 'NORMAL'
    END as movement_severity,

    NOW() as detection_time
FROM market_data_ts

WITH (
    -- Source configuration (external dependency)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'price_movement_alerts.type' = 'kafka_sink',
    'price_movement_alerts.topic.name' = 'price_alerts',
    'price_movement_alerts.config_file' = '../configs/kafka_sink.yaml',

    -- Circuit breaker
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '5',
    'circuit.breaker.timeout' = '60s'
);

-- -----------------------------------------------------------------------------
-- Stage 2: Price Movement Debug Stream
-- -----------------------------------------------------------------------------
-- Diagnostic stream showing filter condition visibility.
-- Useful for debugging why events pass or fail filter conditions.

CREATE STREAM price_movement_debug AS
SELECT
    symbol PRIMARY KEY,
    COUNT(*) as record_count,
    AVG(price) as avg_price,
    STDDEV(price) as stddev_price,
    MAX(volume) as max_volume,
    AVG(volume) as avg_volume,

    -- Filter condition visibility
    COUNT(*) > 1 as passes_count_filter,
    STDDEV(price) > AVG(price) * 0.0001 as passes_volatility_filter,
    AVG(price) * 0.0001 as volatility_threshold,
    MAX(volume) > AVG(volume) * 1.1 as passes_volume_filter,
    AVG(volume) * 1.1 as volume_threshold,

    -- Combined filter result
    CASE
        WHEN COUNT(*) > 1
            AND STDDEV(price) > AVG(price) * 0.0001
            AND MAX(volume) > AVG(volume) * 1.1
        THEN 'WILL_EMIT'
        ELSE 'FILTERED_OUT'
    END as filter_result,

    _window_start AS window_start,
    _window_end AS window_end,
    NOW() AS debug_timestamp

FROM market_data_ts
GROUP BY symbol
  WINDOW TUMBLING(event_time, INTERVAL '1' MINUTE)
  HAVING COUNT(*) > 0
  EMIT CHANGES
WITH (
    -- Source configuration
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'price_movement_debug.type' = 'kafka_sink',
    'price_movement_debug.topic.name' = 'price_movement_debug',
    'price_movement_debug.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- Stage 3: Simple Price Statistics
-- -----------------------------------------------------------------------------
-- Basic 1-minute price statistics per symbol.
-- Good for testing GROUP BY + WINDOW + EMIT CHANGES.

CREATE STREAM price_stats AS
SELECT
    symbol PRIMARY KEY,
    COUNT(*) as record_count,
    AVG(price) as avg_price,
    _window_start AS window_start,
    _window_end AS window_end,
    NOW() AS stats_timestamp

FROM market_data_ts
GROUP BY symbol
  WINDOW TUMBLING(1m)
  EMIT CHANGES
WITH (
    -- Source configuration
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'price_stats.type' = 'kafka_sink',
    'price_stats.topic.name' = 'price_stats',
    'price_stats.config_file' = '../configs/kafka_sink.yaml'
);
