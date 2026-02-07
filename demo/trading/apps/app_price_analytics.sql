-- =============================================================================
-- SQL Application: price_analytics
-- =============================================================================
-- @app: price_analytics
-- @version: 1.0.0
-- @description: Price movement detection with window functions and alerts
-- @phase: production
-- @depends_on: app_market_data

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:price_analytics-1}
-- @deployment.node_name: Price Analytics Pipeline
-- @deployment.region: ${AWS_REGION:us-east-1}

--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true

--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: adaptive
-- @batch_size: 1000
-- @num_partitions: 8
-- @partitioning_strategy: hash

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 100ms
-- @sla.availability: 99.9%
-- @data_retention: 7d
-- @compliance: []

--
-- PIPELINE FLOW
-- =============================================================================
-- External Dependencies:
--   - market_data_ts: From app_market_data pipeline
--
-- Output Topics:
--   - price_alerts: Price movement alerts with severity
--   - price_movement_debug: Debug stream for filter visibility
--   - price_stats: Simple 1-minute price statistics

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: price_movement_alerts
-- @description: Detects price movements using LAG/LEAD/RANK window functions
-- -----------------------------------------------------------------------------
-- @metric: velo_price_movement_alerts_total
-- @metric_type: counter
-- @metric_help: "Total price movement alerts generated"
-- @metric_labels: symbol, movement_severity
--
-- @metric: velo_price_change_pct
-- @metric_type: gauge
-- @metric_help: "Price change percentage"
-- @metric_labels: symbol
-- @metric_field: price_change_pct
--
-- @metric: velo_price_change_distribution
-- @metric_type: histogram
-- @metric_help: "Distribution of price change percentages"
-- @metric_labels: symbol
-- @metric_field: price_change_pct
-- @metric_buckets: 0.1, 0.5, 1.0, 2.0, 5.0, 10.0
--
-- @metric: velo_price_volatility
-- @metric_type: gauge
-- @metric_help: "10-period rolling price volatility (stddev)"
-- @metric_labels: symbol
-- @metric_field: price_volatility_10_periods
--
-- @metric: velo_price_percentile
-- @metric_type: gauge
-- @metric_help: "Price percentile rank within symbol window"
-- @metric_labels: symbol
-- @metric_field: price_percentile

CREATE STREAM price_movement_alerts AS
SELECT
    symbol PRIMARY KEY,
    price,
    volume,
    _event_time,

    -- Previous and next prices
    LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY _event_time
    ) as prev_price,
    LEAD(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY _event_time
    ) as next_price,

    -- Price change percentage
    (price - LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY _event_time
    )) /
     LAG(price, 1) OVER (
        ROWS WINDOW
            BUFFER 100 ROWS
            PARTITION BY symbol
            ORDER BY _event_time
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
        ORDER BY _event_time
    ) as price_volatility_10_periods,

    -- Movement severity classification
    CASE
        WHEN ABS((price - LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY _event_time
                 )) /
                 LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY _event_time
                 )) * 100 > 5.0 THEN 'SIGNIFICANT'
        WHEN ABS((price - LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY _event_time
                 )) /
                 LAG(price, 1) OVER (
                    ROWS WINDOW
                        BUFFER 100 ROWS
                        PARTITION BY symbol
                        ORDER BY _event_time
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
    'market_data_ts.auto.offset.reset' = 'earliest',

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
-- @name: price_stats
-- @description: 1-minute price statistics per symbol with OHLC and spread
-- -----------------------------------------------------------------------------
-- @metric: velo_price_stats_total
-- @metric_type: counter
-- @metric_help: "Total 1-minute price stat windows emitted"
-- @metric_labels: symbol
--
-- @metric: velo_price_stats_avg
-- @metric_type: gauge
-- @metric_help: "Average price per symbol per minute"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
-- @metric: velo_price_stats_spread
-- @metric_type: gauge
-- @metric_help: "Price range (high - low) per minute window"
-- @metric_labels: symbol
-- @metric_field: price_range
--
-- @metric: velo_price_stats_stddev
-- @metric_type: gauge
-- @metric_help: "Price standard deviation per minute window"
-- @metric_labels: symbol
-- @metric_field: price_stddev

CREATE STREAM price_stats AS
SELECT
    symbol PRIMARY KEY,
    COUNT(*) as record_count,
    AVG(price) as avg_price,
    MIN(price) as low_price,
    MAX(price) as high_price,
    MAX(price) - MIN(price) as price_range,
    STDDEV(price) as price_stddev,
    FIRST_VALUE(price) as open_price,
    LAST_VALUE(price) as close_price,
    SUM(volume) as total_volume,
    _window_start AS window_start,
    _window_end AS window_end

FROM market_data_ts
GROUP BY symbol
  WINDOW TUMBLING(1m)
  EMIT CHANGES
WITH (
    -- Source configuration
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',
    'market_data_ts.auto.offset.reset' = 'earliest',

    -- Sink configuration
    'price_stats.type' = 'kafka_sink',
    'price_stats.topic.name' = 'price_stats',
    'price_stats.config_file' = '../configs/kafka_sink.yaml'
);
