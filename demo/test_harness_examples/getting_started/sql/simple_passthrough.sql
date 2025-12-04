-- Simple Passthrough Test
-- Passes market data through without aggregation for pipeline testing
--
-- @name simple_passthrough
-- @description Simple passthrough to verify test harness pipeline works
-- @job_mode: simple
CREATE STREAM market_output AS
SELECT
    symbol,
    price,
    volume
FROM market_data
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data',
    'market_data.config_file' = '../../configs/common_kafka_source.yaml',
    'market_data.datasource.schema.key.field' = 'symbol',
    'market_data.datasource.schema.value.schema.file' = 'schemas/market_data.schema.yaml',

    'market_output.type' = 'kafka_sink',
    'market_output.topic' = 'market_output',
    'market_output.config_file' = '../../configs/common_kafka_sink.yaml'
);
