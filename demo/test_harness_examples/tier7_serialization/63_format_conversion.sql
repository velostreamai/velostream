-- SQL Application: format_conversion_demo
-- Version: 1.0.0
-- Description: Convert JSON input to Avro output
-- =============================================================================
-- Tier 7: Format Conversion (JSON → Avro)
-- =============================================================================
--
-- FORMAT CONVERSION USE CASES:
-- ----------------------------
--   1. Legacy Migration: Convert JSON producers to Avro consumers
--   2. Data Lake Ingestion: JSON APIs → Avro/Parquet for analytics
--   3. Performance Optimization: Move from JSON to binary in hot paths
--   4. Interoperability: Bridge systems using different formats
--
-- HOW IT WORKS:
--   - Velostream deserializes input using source format
--   - Internal processing uses typed FieldValue representation
--   - Output serializes using sink format
--   - Schema mapping handled automatically (matching field names)
--
-- CONSIDERATIONS:
--   - Field names must match between formats
--   - Type compatibility required (e.g., JSON number → Avro long)
--   - Avro/Protobuf require output schema
--   - NULL handling differs between formats
--
-- =============================================================================

-- @app: format_conversion_demo
-- @description: Convert JSON input to Avro output

CREATE STREAM trades_converted AS
SELECT
    trade_id,
    symbol,
    quantity,
    price,
    side,
    quantity * price AS notional_value,
    event_time
FROM json_trades
WHERE quantity > 0
EMIT CHANGES
WITH (
    -- Input: JSON format (legacy producer)
    'json_trades.type' = 'kafka_source',
    'json_trades.topic.name' = 'test_json_trades',
    'json_trades.config_file' = '../configs/trades_source.yaml',
    'json_trades.value.serializer' = 'json',

    -- Output: Avro format (modern consumer)
    'trades_converted.type' = 'kafka_sink',
    'trades_converted.topic.name' = 'test_trades_converted',
    'trades_converted.config_file' = '../configs/output_stream_sink.yaml',
    'trades_converted.value.serializer' = 'avro',
    'trades_converted.avro.schema.file' = 'tier7_serialization/schemas/trade_record.avsc'
);
