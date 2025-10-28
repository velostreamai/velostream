-- SQL Application: Real-Time Trading Analytics (FR-058 Phase 1B-4 Features)
-- @application: real_time_trading_analytics
-- @version: 5.0.0
-- @phase: 1B-4
-- @description: Real-Time Trading Analytics Demo showcasing Phase 1B-4 features
-- @author: Quantitative Trading Team
-- @sla.latency.p99: 5ms
-- @sla.availability: 99.9%
-- @data_retention: 24h
-- @compliance: SEC_FINRA_CFTC
-- @tags: trading, risk-management, market-data, real-time
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true
-- @deployment.node_id: prod-trading-cluster-${TRADING_POD_ID:1}
-- @deployment.node_name: Production Trading Analytics Platform
-- @deployment.region: ${AWS_REGION:us-east-1}

-- ====================================================================================
-- PHASE 1B: EVENT-TIME WATERMARK PROCESSING - Market Data Stream
-- ====================================================================================
-- Showcases event-time processing with watermarks for handling out-of-order market data
-- Demonstrates late data detection and proper windowing based on trade execution time



-- FR-073 SQL-Native Observability: Tick Data Processing Rate
-- @metric: velo_trading_tick_buckets_total
-- @metric_type: counter
-- @metric_help: "Tick buckets created per symbol"
-- @metric_labels: symbol

-- FR-073 SQL-Native Observability: Trade Count per Bucket
-- @metric: velo_trading_trades_per_bucket
-- @metric_type: gauge
-- @metric_help: "Number of trades in each 1-second bucket"
-- @metric_field: trade_count
-- @metric_labels: symbol

-- FR-073 SQL-Native Observability: Volume Distribution
-- @metric: velo_trading_tick_volume_distribution
-- @metric_type: histogram
-- @metric_help: "Distribution of trading volume per tick"
-- @metric_field: total_volume
-- @metric_labels: symbol
-- @metric_buckets: 100, 500, 1000, 5000, 10000, 50000, 100000
-- @job_name: tick_buckets_streams
CREATE STREAM tick_buckets AS
SELECT
    symbol,
--     TUMBLE_START(event_time, INTERVAL '1' SECOND) as bucket_start,
--     TUMBLE_END(event_time, INTERVAL '1' SECOND) as bucket_end,
--     AVG(price) as avg_price,
--     MIN(price) as min_price,
--     MAX(price) as max_price,
--     SUM(volume) as total_volume,
--     COUNT(*) as trade_count,
--     FIRST_VALUE(price) as open_price,
    LAST_VALUE(price) as close_price
FROM market_data_ts
    WINDOW TUMBLING(_event_time, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.config_file' = 'configs/market_data_ts_source.yaml',

    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.config_file' = 'configs/tick_buckets_sink.yaml'
    );


-- FR-073 SQL-Native Observability: Market Data Throughput Counter
-- @metric: velo_trading_market_data_total
-- @metric_type: counter
-- @metric_help: "Total market data records processed"
-- @metric_labels: symbol, exchange

-- FR-073 SQL-Native Observability: Current Price Gauge
-- @metric: velo_trading_current_price
-- @metric_type: gauge
-- @metric_help: "Current market price per symbol"
-- @metric_field: price
-- @metric_labels: symbol, exchange
-- @job_name: market-data-event-time-1
CREATE STREAM market_data_ts AS
SELECT
    symbol,
    exchange,
    timestamp,
    timestamp as event_timestamp,
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
    -- Phase 1B: Configure event-time processing
    'event.time.field' = 'timestamp',
    'event.time.format' = 'epoch_millis',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s',  -- 5s tolerance for market data
    'late.data.strategy' = 'dead_letter',     -- Route late trades to DLQ

    'in_market_data_stream.type' = 'kafka_source',
    'in_market_data_stream.config_file' = 'configs/market_data_source.yaml',

    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.config_file' = 'configs/market_data_ts_sink.yaml'
);
