-- SQL Application: json_format_demo
-- Version: 1.0.0
-- Description: Demonstrate JSON serialization
-- =============================================================================
-- Tier 7: JSON Serialization (Default)
-- =============================================================================
--
-- WHAT IS JSON SERIALIZATION?
-- ---------------------------
-- JSON (JavaScript Object Notation) is the default and most common format:
--   - Human-readable text format
--   - No schema required (schema-less)
--   - Universal support across languages
--   - Easy debugging and inspection
--
-- TRADE-OFFS:
--   + Easy to read and debug
--   + No schema setup required
--   + Universal compatibility
--   - Larger message size (2-5x vs binary formats)
--   - No type enforcement
--   - No schema evolution guarantees
--
-- BEST FOR:
--   - Development and testing
--   - Debugging production issues
--   - Interoperability with legacy systems
--   - Low-volume streams where size doesn't matter
--
-- =============================================================================

-- @app: json_format_demo
-- @description: Demonstrate JSON serialization

CREATE STREAM trades_json AS
SELECT
    trade_id,
    symbol,
    quantity,
    price,
    side,
    quantity * price AS notional_value,
    event_time
FROM raw_trades
WHERE quantity > 0
EMIT CHANGES
WITH (
    'raw_trades.type' = 'kafka_source',
    'raw_trades.topic.name' = 'test_raw_trades',
    'raw_trades.config_file' = '../configs/trades_source.yaml',
    -- JSON is the default, but shown explicitly for clarity
    'raw_trades.value.serializer' = 'json',

    'trades_json.type' = 'kafka_sink',
    'trades_json.topic.name' = 'test_trades_json',
    'trades_json.config_file' = '../configs/output_stream_sink.yaml',
    'trades_json.value.serializer' = 'json'
);
