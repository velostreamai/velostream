-- SQL Application: avro_format_demo
-- Version: 1.0.0
-- Description: Demonstrate Avro serialization with schema
-- =============================================================================
-- Tier 7: Avro Serialization
-- =============================================================================
--
-- WHAT IS AVRO SERIALIZATION?
-- ---------------------------
-- Apache Avro is a row-oriented binary format:
--   - Compact binary encoding (50-80% smaller than JSON)
--   - Schema required for encoding/decoding
--   - Schema evolution support (add fields, deprecate fields)
--   - Rich type system (primitives, complex types, logical types)
--
-- SCHEMA FEATURES:
--   - Primitive types: null, boolean, int, long, float, double, bytes, string
--   - Complex types: records, enums, arrays, maps, unions, fixed
--   - Logical types: date, time-millis, timestamp-millis, decimal
--
-- TRADE-OFFS:
--   + Compact binary format
--   + Schema evolution (backward/forward compatible)
--   + Type safety and validation
--   + Schema Registry integration
--   - Requires schema definition
--   - Not human-readable
--   - More complex setup
--
-- BEST FOR:
--   - Production streaming pipelines
--   - High-volume data streams
--   - Schema Registry environments
--   - Data lake ingestion
--
-- =============================================================================

-- @app: avro_format_demo
-- @description: Demonstrate Avro serialization with schema

CREATE STREAM trades_avro AS
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

    'trades_avro.type' = 'kafka_sink',
    'trades_avro.topic.name' = 'test_trades_avro',
    'trades_avro.config_file' = '../configs/output_stream_sink.yaml',

    -- Avro serialization configuration
    'trades_avro.value.serializer' = 'avro',
    'trades_avro.avro.schema.file' = 'schemas/trade_record.avsc'
);
