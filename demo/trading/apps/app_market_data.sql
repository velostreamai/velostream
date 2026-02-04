-- =============================================================================
-- SQL Application: market_data_pipeline
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
-- DATA GENERATION HINTS for in_market_data_stream
-- =============================================================================
-- These hints guide test data generation when no schema YAML exists.
-- If schemas/in_market_data_stream.schema.yaml exists, it takes precedence.
--
-- @data.source: in_market_data_stream
-- @data.record_count: 1000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"
--
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT", "AMZN", "META"], weights: [0.25, 0.25, 0.2, 0.15, 0.15]
--
-- @data.exchange.type: string
-- @data.exchange: enum ["NASDAQ", "NYSE"], weights: [0.6, 0.4]
--
-- @data.timestamp.type: integer
-- @data.timestamp: timestamp, sequential: true
--
-- @data.price.type: decimal(4)
-- @data.price: range [150, 400], distribution: random_walk, volatility: 0.02, drift: 0.0001, group_by: symbol
--
-- @data.bid_price.type: decimal(4)
-- @data.bid_price.derived: "price * random(0.998, 0.9999)"
--
-- @data.ask_price.type: decimal(4)
-- @data.ask_price.derived: "price * random(1.0001, 1.002)"
--
-- @data.bid_size.type: integer
-- @data.bid_size: range [100, 10000]
--
-- @data.ask_size.type: integer
-- @data.ask_size: range [100, 10000]
--
-- @data.volume.type: integer
-- @data.volume: range [1000, 500000], distribution: log_normal
--
-- @data.vwap.type: decimal(4)
-- @data.vwap.derived: "price * random(0.995, 1.005)"
--
-- @data.market_cap.type: integer
-- @data.market_cap: range [100000000000, 3000000000000]

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: market_data_ts
-- @description: Ingests raw market data with event-time watermarks
-- -----------------------------------------------------------------------------
-- @metric: velo_market_data_ts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by market_data_ts"
--
-- @metric: velo_market_data_current_price
-- @metric_type: gauge
-- @metric_help: "Current price value"
-- @metric_field: price
-- @metric_labels: symbol

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
    'in_market_data_stream.topic.name' = 'in_market_data_stream',
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
-- @metric: velo_tick_buckets_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by tick_buckets"
-- @metric_labels: symbol
--
-- @metric: velo_tick_buckets_avg_price
-- @metric_type: gauge
-- @metric_help: "Average price in bucket"
-- @metric_labels: symbol
-- @metric_field: avg_price
--
-- @metric: velo_tick_buckets_trade_count
-- @metric_type: gauge
-- @metric_help: "Trade count in bucket"
-- @metric_labels: symbol
-- @metric_field: trade_count

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
    'market_data_ts.auto.offset.reset' = 'earliest',

    -- Sink configuration
    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.topic.name' = 'tick_buckets',
    'tick_buckets.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: enriched_market_data
-- @description: Enriches market data with instrument metadata via stream-table join
-- -----------------------------------------------------------------------------
-- @metric: velo_enriched_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by enriched_market_data"
--
-- @metric: velo_enriched_market_data_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Enrichment latency in seconds"
-- @metric_field: enrichment_latency_seconds

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
    -- Direct integer arithmetic: both NOW() and _event_time are epoch_millis
    -- Use COALESCE to handle potential NULL or missing values gracefully
    COALESCE((NOW() - m._event_time) / 1000.0, 0.0) as enrichment_latency_seconds

FROM market_data_ts m
LEFT JOIN instrument_reference r ON m.symbol = r.symbol

EMIT CHANGES
WITH (
    -- Source configuration (from Stage 1)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',
    'market_data_ts.auto.offset.reset' = 'earliest',

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
