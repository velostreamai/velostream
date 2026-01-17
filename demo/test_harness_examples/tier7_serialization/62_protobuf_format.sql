-- SQL Application: protobuf_format_demo
-- Version: 1.0.0
-- Description: Demonstrate Protobuf serialization
-- =============================================================================
-- Tier 7: Protobuf Serialization
-- =============================================================================
--
-- WHAT IS PROTOBUF SERIALIZATION?
-- --------------------------------
-- Protocol Buffers (Protobuf) is Google's binary serialization format:
--   - Extremely compact encoding (smallest of all formats)
--   - Fastest serialization/deserialization
--   - Strong cross-language support (Java, Python, Go, C++, Rust, etc.)
--   - Backward/forward compatibility by design
--
-- SCHEMA FEATURES:
--   - Numbered fields (field numbers, not names, in wire format)
--   - Scalar types: int32, int64, uint32, float, double, bool, string, bytes
--   - Complex types: messages (nested), enums, repeated fields, maps
--   - Optional fields (proto3: all fields optional by default)
--
-- TRADE-OFFS:
--   + Smallest message size
--   + Fastest serialization
--   + Excellent cross-language support
--   + Strong backward/forward compatibility
--   - Requires .proto schema files
--   - Not human-readable
--   - Field numbers must be managed carefully
--
-- BEST FOR:
--   - High-frequency trading systems
--   - Cross-language microservices
--   - Bandwidth-constrained environments
--   - gRPC integration
--
-- =============================================================================

-- @app: protobuf_format_demo
-- @description: Demonstrate Protobuf serialization

CREATE STREAM trades_protobuf AS
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

    'trades_protobuf.type' = 'kafka_sink',
    'trades_protobuf.topic.name' = 'test_trades_protobuf',
    'trades_protobuf.config_file' = '../configs/output_stream_sink.yaml',

    -- Protobuf serialization configuration
    'trades_protobuf.value.serializer' = 'protobuf',
    'trades_protobuf.protobuf.schema.file' = 'tier7_serialization/schemas/trade_record.proto'
);
