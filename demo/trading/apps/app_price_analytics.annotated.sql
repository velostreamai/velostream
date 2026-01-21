-- =============================================================================
-- APPLICATION: app_price_analytics
-- =============================================================================
-- @app: app_price_analytics  # Application identifier
-- @version: 1.0.0  # Semantic version
-- @description: TODO - Describe your application  # Human-readable description
-- @phase: development  # Options: development, staging, production

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:app_price_analytics-1}  # Unique node identifier (supports env vars)
-- @deployment.node_name: app_price_analytics Platform  # Human-readable node name
-- @deployment.region: ${AWS_REGION:us-east-1}  # Deployment region

--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: true  # Enable Prometheus metrics collection
-- @observability.tracing.enabled: true  # Enable distributed tracing (OpenTelemetry)
-- @observability.profiling.enabled: prod  # Options: off, dev (8-10% overhead), prod (2-3% overhead)
-- @observability.error_reporting.enabled: true  # Enable structured error reporting

--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: adaptive  # Options: simple (low latency), transactional (exactly-once), adaptive (parallel)
-- @batch_size: 1000  # Records per batch (higher = throughput, lower = latency)
-- @num_partitions: 8  # Parallel partitions for adaptive mode (default: CPU cores)
-- @partitioning_strategy: hash  # Options: sticky, hash, smart, roundrobin, fanin

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 100ms  # Expected P99 latency target
-- @sla.availability: 99.9%  # Availability target
-- @data_retention: 7d  # Data retention policy
-- @compliance: []  # Compliance requirements (e.g., SOX, GDPR, PCI-DSS)

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- =============================================================================
-- APPLICATION: price_analytics
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

-- -----------------------------------------------------------------------------
-- METRICS for price_movement_alerts
-- -----------------------------------------------------------------------------
-- @metric: velo_app_price_analytics_price_movement_alerts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by price_movement_alerts"
--
-- @metric: velo_app_price_analytics_current_price
-- @metric_type: gauge
-- @metric_help: "Current price value"
-- @metric_field: price
--
-- @metric: velo_app_price_analytics_current_prev_price
-- @metric_type: gauge
-- @metric_help: "Current prev_price value"
-- @metric_field: prev_price
--
-- @metric: velo_app_price_analytics_current_next_price
-- @metric_type: gauge
-- @metric_help: "Current next_price value"
-- @metric_field: next_price
--
-- @metric: velo_app_price_analytics_current_price_change_pct
-- @metric_type: gauge
-- @metric_help: "Current price_change_pct value"
-- @metric_field: price_change_pct
--
-- @metric: velo_app_price_analytics_current_price_rank
-- @metric_type: gauge
-- @metric_help: "Current price_rank value"
-- @metric_field: price_rank
--
-- @metric: velo_app_price_analytics_current_price_percentile
-- @metric_type: gauge
-- @metric_help: "Current price_percentile value"
-- @metric_field: price_percentile
--
-- @metric: velo_app_price_analytics_current_price_volatility_10_periods
-- @metric_type: gauge
-- @metric_help: "Current price_volatility_10_periods value"
-- @metric_field: price_volatility_10_periods
--
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
-- @name: price_movement_debug
-- @description: Diagnostic stream showing filter condition visibility
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- METRICS for price_movement_debug
-- -----------------------------------------------------------------------------
-- @metric: velo_app_price_analytics_price_movement_debug_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by price_movement_debug"
-- @metric_labels: symbol
--
-- @metric: velo_app_price_analytics_price_movement_debug_count
-- @metric_type: counter
-- @metric_help: "Count aggregation from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: count
--
-- @metric: velo_app_price_analytics_price_movement_debug_record_count
-- @metric_type: gauge
-- @metric_help: "Current record_count from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: record_count
--
-- @metric: velo_app_price_analytics_price_movement_debug_avg_price
-- @metric_type: gauge
-- @metric_help: "Current avg_price from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
-- @metric: velo_app_price_analytics_price_movement_debug_stddev_price
-- @metric_type: gauge
-- @metric_help: "Current stddev_price from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: stddev_price
--
-- @metric: velo_app_price_analytics_price_movement_debug_max_volume
-- @metric_type: gauge
-- @metric_help: "Current max_volume from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: max_volume
--
-- @metric: velo_app_price_analytics_price_movement_debug_avg_volume
-- @metric_type: gauge
-- @metric_help: "Current avg_volume from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: avg_volume
--
-- @metric: velo_app_price_analytics_price_movement_debug_passes_count_filter
-- @metric_type: gauge
-- @metric_help: "Current passes_count_filter from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: passes_count_filter
--
-- @metric: velo_app_price_analytics_price_movement_debug_passes_volume_filter
-- @metric_type: gauge
-- @metric_help: "Current passes_volume_filter from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: passes_volume_filter
--
-- @metric: velo_app_price_analytics_price_movement_debug_volume_threshold
-- @metric_type: gauge
-- @metric_help: "Current volume_threshold from price_movement_debug"
-- @metric_labels: symbol
-- @metric_field: volume_threshold
--
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
  WINDOW TUMBLING(_event_time, INTERVAL '1' MINUTE)
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
-- @name: price_stats
-- @description: Basic 1-minute price statistics per symbol
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- METRICS for price_stats
-- -----------------------------------------------------------------------------
-- @metric: velo_app_price_analytics_price_stats_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by price_stats"
-- @metric_labels: symbol
--
-- @metric: velo_app_price_analytics_price_stats_count
-- @metric_type: counter
-- @metric_help: "Count aggregation from price_stats"
-- @metric_labels: symbol
-- @metric_field: count
--
-- @metric: velo_app_price_analytics_price_stats_record_count
-- @metric_type: gauge
-- @metric_help: "Current record_count from price_stats"
-- @metric_labels: symbol
-- @metric_field: record_count
--
-- @metric: velo_app_price_analytics_price_stats_avg_price
-- @metric_type: gauge
-- @metric_help: "Current avg_price from price_stats"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
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
