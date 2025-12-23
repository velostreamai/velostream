-- =============================================================================
-- APP: Market Data Pipeline
-- =============================================================================
-- @app market_data_pipeline
-- @description Core market data processing: ingestion → OHLCV → enrichment
-- @version 1.0.0
-- @job_mode: simple
--
-- Pipeline Flow:
--   in_market_data → [market_data_ts] → [tick_buckets]
--                                     → [enriched_market_data]
--
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

-- -----------------------------------------------------------------------------
-- Stage 1: Market Data with Event Time
-- -----------------------------------------------------------------------------
-- Ingests raw market data and adds event-time processing with watermarks.
-- This is the foundation stream that all other market data apps depend on.

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
-- Stage 2: Tick Buckets (1-second OHLCV)
-- -----------------------------------------------------------------------------
-- Aggregates market data into 1-second OHLCV candles.
-- Useful for charting and downstream analytics.

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
-- Stage 3: Enriched Market Data (Stream-Table Join)
-- -----------------------------------------------------------------------------
-- Enriches market data with instrument metadata from reference table.
-- Demonstrates the classic lookup pattern for real-time data enrichment.

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
