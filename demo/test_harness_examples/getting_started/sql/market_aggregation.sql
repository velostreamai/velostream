-- Market Data Aggregation
-- Aggregates trading data into 5-minute buckets per symbol
--
-- This is a demo SQL application for the FR-084 Test Harness Getting Started guide.
-- See: docs/feature/FR-084-app-test-harness/GETTING_STARTED.md

-- Application metadata
-- @name market_aggregation
-- @description Market data aggregation into 5-minute OHLCV bars
-- @job_mode: simple
CREATE STREAM market_aggregates AS
SELECT
    symbol,
    COUNT(*) AS trade_count,
    SUM(volume) AS total_volume,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    _window_start AS window_start,
    _window_end AS window_end
FROM market_data
GROUP BY symbol
  WINDOW TUMBLING(10s)
  EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data',
    'market_data.config_file' = '../configs/common_kafka_source.yaml',
    'market_data.datasource.schema.key.field' = 'symbol',
    'market_data.datasource.schema.value.schema.file' = 'schemas/market_data.schema.yaml',

    'market_aggregates.type' = 'kafka_sink',
    'market_aggregates.topic' = 'market_aggregates_output',
    'market_aggregates.config_file' = '../configs/common_kafka_sink.yaml'
);
