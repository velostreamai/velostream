-- =============================================================================
-- APPLICATION: trading_signals
-- =============================================================================
-- @app: trading_signals
-- @version: 1.0.0
-- @description: Volume analytics, order flow, and arbitrage detection
-- @phase: production
-- @depends_on: app_market_data

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:trading_signals-1}
-- @deployment.node_name: Trading Signals Pipeline
-- @deployment.region: ${AWS_REGION:us-east-1}

--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true

--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: adaptive
-- @batch_size: 2000
-- @num_partitions: 16
-- @partitioning_strategy: hash

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 5ms
-- @sla.availability: 99.99%
-- @data_retention: 7d
-- @compliance: [MiFID-II, Reg-NMS]

--
-- PIPELINE FLOW
-- =============================================================================
-- Input Topics:
--   - in_order_book: Order book events
--   - market_data_exchange_a: Exchange A market data
--   - market_data_exchange_b: Exchange B market data
--
-- External Dependencies:
--   - market_data_ts: From app_market_data pipeline
--
-- Output Topics:
--   - volume_spikes: Volume anomaly alerts
--   - order_imbalance: Order flow imbalance signals
--   - arbitrage_opportunities: Cross-exchange arbitrage opportunities

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: volume_spike_analysis
-- @description: Detects volume anomalies using statistical methods
-- -----------------------------------------------------------------------------

CREATE STREAM volume_spike_analysis AS
SELECT
    symbol PRIMARY KEY,
    _window_start AS window_start,
    _window_end AS window_end,

    -- Basic aggregations
    COUNT(*) AS trade_count,
    AVG(volume) AS avg_volume,
    STDDEV_POP(volume) AS stddev_volume,
    MAX(volume) AS max_volume,
    MIN(volume) AS min_volume,

    -- Rolling metrics (last 20 trades)
    AVG(volume) OVER (
        ROWS WINDOW BUFFER 20 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS rolling_avg_20,

    STDDEV_POP(volume) OVER (
        ROWS WINDOW BUFFER 20 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS rolling_stddev_20,

    -- Volume percentile
    PERCENT_RANK() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY volume
    ) AS volume_percentile,

    -- Spike classification
    CASE
        WHEN AVG(volume) > 0 AND MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
        WHEN AVG(volume) > 0 AND MAX(volume) > 3 * AVG(volume) THEN 'HIGH_SPIKE'
        WHEN STDDEV_POP(volume) > 0
            AND ABS((MAX(volume) - AVG(volume)) / STDDEV_POP(volume)) > 2.0
            THEN 'STATISTICAL_ANOMALY'
        ELSE 'NORMAL'
    END AS spike_classification,

    -- Circuit breaker state
    CASE
        WHEN AVG(volume) > 0 AND MAX(volume) > 10 * AVG(volume) THEN 'TRIGGER_BREAKER'
        WHEN spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
            AND STDDEV_POP(volume) > 3 THEN 'PAUSE_FEED'
        WHEN spike_classification = 'HIGH_SPIKE'
            AND STDDEV_POP(volume) > 2 THEN 'SLOW_MODE'
        ELSE 'ALLOW'
    END AS circuit_state,

    NOW() AS detection_time

FROM market_data_ts
GROUP BY symbol
WINDOW SLIDING(event_time, 5m, 1m)
EMIT CHANGES
WITH (
    -- Source configuration (external dependency)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Resource management
    'max.memory.mb' = '1024',
    'max.groups' = '50000',
    'spill.to.disk' = 'true',

    -- Circuit breaker
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '3',
    'circuit.breaker.timeout' = '120s',

    -- Retry with exponential backoff
    'retry.max.attempts' = '5',
    'retry.backoff.strategy' = 'exponential',
    'retry.initial.delay' = '100ms',
    'retry.max.delay' = '30s',

    -- Sink configuration
    'volume_spike_analysis.type' = 'kafka_sink',
    'volume_spike_analysis.topic.name' = 'volume_spikes',
    'volume_spike_analysis.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: order_flow_imbalance
-- @description: Detects institutional trading patterns from order book data
-- -----------------------------------------------------------------------------

CREATE STREAM order_flow_imbalance AS
SELECT
    symbol PRIMARY KEY,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) AS buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) AS sell_volume,
    SUM(quantity) AS total_volume,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) AS buy_ratio,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) / SUM(quantity) AS sell_ratio,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS analysis_time
FROM in_order_book_stream
GROUP BY symbol
WINDOW TUMBLING(event_time, INTERVAL '1' MINUTE)
HAVING
    SUM(quantity) > 10000
    AND (
        SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7
        OR SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7
    )
EMIT CHANGES
WITH (
    -- Source configuration
    'in_order_book_stream.type' = 'kafka_source',
    'in_order_book_stream.topic.name' = 'order_book',
    'in_order_book_stream.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'order_flow_imbalance.type' = 'kafka_sink',
    'order_flow_imbalance.topic.name' = 'order_imbalance',
    'order_flow_imbalance.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: arbitrage_detection
-- @description: Cross-exchange price discrepancy detection
-- -----------------------------------------------------------------------------

CREATE STREAM arbitrage_detection AS
SELECT
    a.symbol PRIMARY KEY,
    a.exchange as exchange_a,
    b.exchange as exchange_b,
    a.bid_price as bid_a,
    b.ask_price as ask_b,
    (a.bid_price - b.ask_price) as spread,
    (a.bid_price - b.ask_price) / b.ask_price * 10000 as spread_bps,
    LEAST(a.bid_size, b.ask_size) as available_volume,
    (a.bid_price - b.ask_price) * LEAST(a.bid_size, b.ask_size) as potential_profit,
    NOW() as opportunity_time
FROM in_market_data_stream_a a
JOIN in_market_data_stream_b b ON a.symbol = b.symbol
WHERE a.bid_price > b.ask_price
    AND (a.bid_price - b.ask_price) / b.ask_price * 10000 > 10
    AND LEAST(a.bid_size, b.ask_size) > 50000
EMIT CHANGES
WITH (
    -- Source configuration - Exchange A
    'in_market_data_stream_a.type' = 'kafka_source',
    'in_market_data_stream_a.topic.name' = 'market_data_exchange_a',
    'in_market_data_stream_a.config_file' = '../configs/kafka_source.yaml',

    -- Source configuration - Exchange B
    'in_market_data_stream_b.type' = 'kafka_source',
    'in_market_data_stream_b.topic.name' = 'market_data_exchange_b',
    'in_market_data_stream_b.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'arbitrage_detection.type' = 'kafka_sink',
    'arbitrage_detection.topic.name' = 'arbitrage_opportunities',
    'arbitrage_detection.config_file' = '../configs/kafka_sink.yaml'
);
