-- SQL Application: compound_keys_demo
-- Version: 1.0.0
-- Description: Demonstrates compound key configuration with PRIMARY KEY annotation
-- =============================================================================
-- Tier 2: Compound Keys Demo
-- =============================================================================
--
-- Tests: Multiple PRIMARY KEY fields, compound GROUP BY, key format
-- Expected: Pipe-delimited keys like "US|Widget"
--
-- =============================================================================

-- @app: compound_keys_demo
-- @description: Demonstrates compound key configuration with PRIMARY KEY annotation

-- Example 1: Compound PRIMARY KEY (explicit key declaration)
-- Kafka key will be: "US|Electronics" (pipe-delimited)
CREATE TABLE regional_category_stats AS
SELECT
    region PRIMARY KEY,
    category PRIMARY KEY,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    _window_start AS window_start,
    _window_end AS window_end
FROM orders
GROUP BY region, category
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'regional_category_stats.type' = 'kafka_sink',
    'regional_category_stats.topic.name' = 'test_regional_category_stats',
    'regional_category_stats.config_file' = '../configs/aggregates_sink.yaml'
);

-- Example 2: Compound GROUP BY without PRIMARY KEY (implicit key)
-- Kafka key will also be: "T1|AAPL" (pipe-delimited, auto-generated)
CREATE TABLE trader_symbol_stats AS
SELECT
    trader_id,
    symbol,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_quantity,
    AVG(price) AS avg_price,
    _window_start AS window_start,
    _window_end AS window_end
FROM trades
GROUP BY trader_id, symbol
WINDOW TUMBLING(5m)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.topic.name' = 'test_trades',
    'trades.config_file' = '../configs/trades_source.yaml',

    'trader_symbol_stats.type' = 'kafka_sink',
    'trader_symbol_stats.topic.name' = 'test_trader_symbol_stats',
    'trader_symbol_stats.config_file' = '../configs/aggregates_sink.yaml'
);

-- Example 3: Single PRIMARY KEY with compound GROUP BY
-- PRIMARY KEY takes precedence - Kafka key will be just: "AAPL" (not compound)
-- Note: Uses trades_3 as input to avoid topic conflicts with trader_symbol_stats
CREATE TABLE symbol_only_key AS
SELECT
    symbol PRIMARY KEY,
    exchange,
    COUNT(*) AS trade_count,
    SUM(quantity * price) AS total_value,
    _window_start AS window_start,
    _window_end AS window_end
FROM trades_3
GROUP BY symbol, exchange
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'trades_3.type' = 'kafka_source',
    'trades_3.topic.name' = 'test_trades_3',
    'trades_3.config_file' = '../configs/trades_source.yaml',

    'symbol_only_key.type' = 'kafka_sink',
    'symbol_only_key.topic.name' = 'test_symbol_only_key',
    'symbol_only_key.config_file' = '../configs/aggregates_sink.yaml'
);
