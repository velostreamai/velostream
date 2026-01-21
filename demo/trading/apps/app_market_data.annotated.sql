-- =============================================================================
-- APPLICATION: app_market_data
-- =============================================================================
-- @app: app_market_data  # Application identifier
-- @version: 1.0.0  # Semantic version
-- @description: TODO - Describe your application  # Human-readable description
-- @phase: development  # Options: development, staging, production

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:app_market_data-1}  # Unique node identifier (supports env vars)
-- @deployment.node_name: app_market_data Platform  # Human-readable node name
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
-- APPLICATION: market_data_pipeline
-- =============================================================================
-- @app: market_data_pipeline
-- @version: 1.0.0
-- @description: Core market data processing - ingestion, OHLCV candles, and enrichment
-- @phase: production

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:market_data-1}
-- @deployment.node_name: Market Data Pipeline
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
-- @job_mode: simple
-- @batch_size: 5000
-- @num_partitions: 16
-- @partitioning_strategy: hash

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 50ms
-- @sla.availability: 99.99%
-- @data_retention: 30d
-- @compliance: [MiFID-II, SEC-Rule-613]

--
-- PIPELINE FLOW
-- =============================================================================
-- Input Topics:
--   - in_market_data: Raw market data feed
--
-- Output Topics:
--   - market_data_ts: Event-time processed market data
--   - tick_buckets: 1-second OHLCV candles
--   - enriched_market_data: Market data with instrument metadata
--
-- Reference Tables:
--   - instrument_reference: Instrument metadata for enrichment

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: market_data_ts
-- @description: Ingests raw market data with event-time watermarks
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- METRICS for market_data_ts
-- -----------------------------------------------------------------------------
-- @metric: velo_app_market_data_market_data_ts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by market_data_ts"
--
-- @metric: velo_app_market_data_current_price
-- @metric_type: gauge
-- @metric_help: "Current price value"
-- @metric_field: price
--
-- @metric: velo_app_market_data_current_bid_price
-- @metric_type: gauge
-- @metric_help: "Current bid_price value"
-- @metric_field: bid_price
--
-- @metric: velo_app_market_data_current_ask_price
-- @metric_type: gauge
-- @metric_help: "Current ask_price value"
-- @metric_field: ask_price
--
CREATE STREAM market_data_ts AS
SELECT
    symbol PRIMARY KEY,
    exchange,
    timestamp,
    timestamp as _event_time,
    price,
    bid_price,
    ask_price,
    bid_size,
    ask_size,
    volume,
    vwap,
    market_cap
FROM in_market_data_stream
EMIT CHANGES
WITH (
    -- Event-time processing configuration
    'event.time.field' = 'timestamp',
    'event.time.format' = 'epoch_millis',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s',
    'late.data.strategy' = 'dead_letter',

    -- Source configuration
    'in_market_data_stream.type' = 'kafka_source',
    'in_market_data_stream.topic.name' = 'in_market_data',
    'in_market_data_stream.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: tick_buckets
-- @description: Aggregates market data into 1-second OHLCV candles
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- METRICS for tick_buckets
-- -----------------------------------------------------------------------------
-- @metric: velo_app_market_data_tick_buckets_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by tick_buckets"
-- @metric_labels: symbol
--
-- @metric: velo_app_market_data_tick_buckets_count
-- @metric_type: counter
-- @metric_help: "Count aggregation from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: count
--
-- @metric: velo_app_market_data_tick_buckets_avg_price
-- @metric_type: gauge
-- @metric_help: "Current avg_price from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
-- @metric: velo_app_market_data_tick_buckets_min_price
-- @metric_type: gauge
-- @metric_help: "Current min_price from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: min_price
--
-- @metric: velo_app_market_data_tick_buckets_max_price
-- @metric_type: gauge
-- @metric_help: "Current max_price from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: max_price
--
-- @metric: velo_app_market_data_tick_buckets_total_volume
-- @metric_type: gauge
-- @metric_help: "Current total_volume from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: total_volume
--
-- @metric: velo_app_market_data_tick_buckets_trade_count
-- @metric_type: gauge
-- @metric_help: "Current trade_count from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: trade_count
--
-- @metric: velo_app_market_data_tick_buckets_open_price
-- @metric_type: gauge
-- @metric_help: "Current open_price from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: open_price
--
-- @metric: velo_app_market_data_tick_buckets_close_price
-- @metric_type: gauge
-- @metric_help: "Current close_price from tick_buckets"
-- @metric_labels: symbol
-- @metric_field: close_price
--
CREATE STREAM tick_buckets AS
SELECT
    symbol PRIMARY KEY,
    TUMBLE_START(_event_time, INTERVAL '1' SECOND) as bucket_start,
    TUMBLE_END(_event_time, INTERVAL '1' SECOND) as bucket_end,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(volume) as total_volume,
    COUNT(*) as trade_count,
    FIRST_VALUE(price) as open_price,
    LAST_VALUE(price) as close_price
FROM market_data_ts
WINDOW TUMBLING(_event_time, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES
WITH (
    -- Source configuration (from Stage 1)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.topic.name' = 'tick_buckets',
    'tick_buckets.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: enriched_market_data
-- @description: Enriches market data with instrument metadata via stream-table join
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- METRICS for enriched_market_data
-- -----------------------------------------------------------------------------
-- @metric: velo_app_market_data_enriched_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by enriched_market_data"
--
-- @metric: velo_app_market_data_current_m_price
-- @metric_type: gauge
-- @metric_help: "Current m.price value"
-- @metric_field: m.price
--
-- @metric: velo_app_market_data_current_m_bid_price
-- @metric_type: gauge
-- @metric_help: "Current m.bid_price value"
-- @metric_field: m.bid_price
--
-- @metric: velo_app_market_data_current_m_ask_price
-- @metric_type: gauge
-- @metric_help: "Current m.ask_price value"
-- @metric_field: m.ask_price
--
-- @metric: velo_app_market_data_current_normalized_price
-- @metric_type: gauge
-- @metric_help: "Current normalized_price value"
-- @metric_field: normalized_price
--
CREATE STREAM enriched_market_data AS
SELECT
    m.symbol PRIMARY KEY,
    m.exchange,
    m.price,
    m.bid_price,
    m.ask_price,
    m.volume,
    m._event_time,
    m.timestamp,

    -- Enrichment from reference table
    r.instrument_name,
    r.isin_code,
    r.tick_size,
    r.lot_size,
    r.trading_venue as instrument_venue,
    r.settlement_currency,
    r.margin_requirement,
    r.position_limit,
    r.restricted_trading_hours,

    -- Calculated fields using reference data
    FLOOR(m.price / r.tick_size) * r.tick_size as normalized_price,
    m.volume / r.lot_size as lot_count,

    NOW() as enrichment_time,
    EXTRACT(EPOCH FROM (NOW() - m._event_time)) as enrichment_latency_seconds

FROM market_data_ts m
LEFT JOIN instrument_reference r ON m.symbol = r.symbol

EMIT CHANGES
WITH (
    -- Source configuration (from Stage 1)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Reference table
    'instrument_reference.type' = 'file_source',
    'instrument_reference.config_file' = '../configs/instrument_reference_table.yaml',

    -- Sink configuration
    'enriched_market_data.type' = 'kafka_sink',
    'enriched_market_data.topic.name' = 'enriched_market_data',
    'enriched_market_data.config_file' = '../configs/kafka_sink.yaml',

    -- Join optimization
    'join.timeout' = '30s',
    'cache.enabled' = 'true',
    'cache.ttl_seconds' = '3600'
);
