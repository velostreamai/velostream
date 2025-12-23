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
-- METRICS
-- =============================================================================
-- @metric: velo_trading_market_data_total
-- @metric_type: counter
-- @metric_help: "Total market data records processed"
-- @metric_labels: symbol, exchange
--
-- @metric: velo_trading_current_price
-- @metric_type: gauge
-- @metric_help: "Current price by symbol"
-- @metric_labels: symbol
-- @metric_field: price
--
-- @metric: velo_trading_tick_buckets_total
-- @metric_type: counter
-- @metric_help: "Total OHLCV tick buckets generated"
-- @metric_labels: symbol

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

CREATE STREAM enriched_market_data AS
SELECT
    m.symbol PRIMARY KEY,
    m.exchange,
    m.price,
    m.bid_price,
    m.ask_price,
    m.volume,
    m.event_time,
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
    EXTRACT(EPOCH FROM (NOW() - m.event_time)) as enrichment_latency_seconds

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
