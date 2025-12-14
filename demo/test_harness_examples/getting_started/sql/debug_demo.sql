-- Debug Demo: Multi-stage pipeline
-- A simple 3-stage pipeline perfect for demonstrating step-by-step debugging
--
-- @name debug_demo
-- @description Multi-stage pipeline for debugging demonstration
-- @job_mode: simple

-- Stage 1: Filter high-value trades
CREATE STREAM high_value_trades AS
SELECT
    symbol,
    price,
    volume,
    price * volume AS trade_value
FROM market_data
WHERE price > 50
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data',
    'market_data.config_file' = '../../configs/common_kafka_source.yaml',
    'market_data.datasource.schema.key.field' = 'symbol',
    'market_data.datasource.schema.value.schema.file' = 'schemas/market_data.schema.yaml',

    'high_value_trades.type' = 'kafka_sink',
    'high_value_trades.topic' = 'high_value_trades',
    'high_value_trades.key_field' = 'symbol',
    'high_value_trades.config_file' = '../../configs/common_kafka_sink.yaml'
);

-- Stage 2: Aggregate by symbol into 10-second windows
-- @job_mode: simple
CREATE TABLE symbol_aggregates AS
SELECT
    symbol,
    COUNT(*) AS trade_count,
    SUM(trade_value) AS total_value,
    AVG(price) AS avg_price,
    _window_start AS window_start,
    _window_end AS window_end
FROM high_value_trades
GROUP BY symbol
WINDOW TUMBLING(10s)
EMIT CHANGES
WITH (
    'high_value_trades.type' = 'kafka_source',
    'high_value_trades.topic' = 'high_value_trades',
    'high_value_trades.config_file' = '../../configs/common_kafka_source.yaml',

    'symbol_aggregates.type' = 'kafka_sink',
    'symbol_aggregates.topic' = 'symbol_aggregates',
    'symbol_aggregates.key_field' = 'symbol',
    'symbol_aggregates.config_file' = '../../configs/common_kafka_sink.yaml'
);

-- Stage 3: Flag symbols with high activity
-- @job_mode: transactional
CREATE STREAM flagged_symbols AS
SELECT
    symbol,
    trade_count,
    total_value,
    avg_price,
    window_start,
    window_end,
    CASE
        WHEN trade_count > 10 THEN 'HIGH_ACTIVITY'
        WHEN trade_count > 5 THEN 'MEDIUM_ACTIVITY'
        ELSE 'LOW_ACTIVITY'
    END AS activity_level
FROM symbol_aggregates
EMIT CHANGES
WITH (
    'symbol_aggregates.type' = 'kafka_source',
    'symbol_aggregates.topic' = 'symbol_aggregates',
    'symbol_aggregates.config_file' = '../../configs/common_kafka_source.yaml',

    'flagged_symbols.type' = 'kafka_sink',
    'flagged_symbols.topic' = 'flagged_symbols_output',
    'flagged_symbols.config_file' = '../../configs/common_kafka_sink.yaml'
);
