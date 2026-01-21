-- =============================================================================
-- APPLICATION: compliance
-- =============================================================================
-- @app: compliance
-- @version: 1.0.0
-- @description: Regulatory compliance filtering and market hours enforcement
-- @phase: production
-- @depends_on: app_market_data

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:compliance-1}
-- @deployment.node_name: Compliance Pipeline
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
-- @job_mode: transactional
-- @batch_size: 100
-- @num_partitions: 2
-- @partitioning_strategy: hash

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 10ms
-- @sla.availability: 99.999%
-- @data_retention: 365d
-- @compliance: [OFAC, MiFID-II, SEC-Rule-613, Reg-NMS]

--
-- PIPELINE FLOW
-- =============================================================================
-- External Dependencies:
--   - market_data_ts: From app_market_data pipeline
--
-- Reference Tables:
--   - regulatory_watchlist: Blocked/suspended instruments and traders
--   - instrument_schedules: Market session schedules
--   - trading_halts: Current trading halts
--
-- Output Topics:
--   - compliant_market_data: Trades passing compliance checks
--   - active_hours_market_data: Trades during active market hours

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: compliant_market_data
-- @description: Filters out trades involving restricted traders or blocked instruments
-- -----------------------------------------------------------------------------

CREATE STREAM compliant_market_data AS
SELECT
    m.symbol PRIMARY KEY,
    m.exchange,
    m.price,
    m.bid_price,
    m.ask_price,
    m.volume,
    m._event_time,
    m.timestamp,
    'COMPLIANT' as compliance_status
FROM market_data_ts m
WHERE NOT EXISTS (
    SELECT 1 FROM regulatory_watchlist w
    WHERE (w.symbol = m.symbol OR w.trader_id IS NOT NULL)
      AND w.restriction_type IN ('BLOCKED', 'SUSPENDED')
      AND w.effective_date <= m._event_time
      AND (w.expiry_date IS NULL OR w.expiry_date > m._event_time)
)
EMIT CHANGES
WITH (
    -- Source configuration (external dependency)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Watchlist table
    'regulatory_watchlist.type' = 'file_source',
    'regulatory_watchlist.config_file' = '../configs/regulatory_watchlist_table.yaml',

    -- Sink configuration
    'compliant_market_data.type' = 'kafka_sink',
    'compliant_market_data.topic.name' = 'compliant_market_data',
    'compliant_market_data.config_file' = '../configs/kafka_sink.yaml',

    -- Compliance-critical settings
    'delivery.guarantee' = 'exactly_once',
    'circuit.breaker.failure.threshold' = '1'
);

-- -----------------------------------------------------------------------------
-- @name: active_hours_market_data
-- @description: Filters market data to only include actively trading instruments
-- -----------------------------------------------------------------------------

CREATE STREAM active_hours_market_data AS
SELECT
    m.symbol PRIMARY KEY,
    m.exchange,
    m.price,
    m.bid_price,
    m.ask_price,
    m.volume,
    m._event_time,
    m.timestamp,
    'ACTIVE_TRADING' as market_session,
    CASE
        WHEN h.halt_start_time IS NOT NULL THEN 'HALTED'
        WHEN i.session_type = 'REGULAR' THEN 'REGULAR_HOURS'
        WHEN i.session_type = 'PRE_MARKET' THEN 'PRE_MARKET'
        WHEN i.session_type = 'POST_MARKET' THEN 'POST_MARKET'
        ELSE 'UNKNOWN'
    END as current_session
FROM market_data_ts m
WHERE m.symbol IN (
    SELECT symbol FROM instrument_schedules i
    WHERE market_status = 'OPEN'
      AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')
)
AND m.symbol NOT IN (
    SELECT symbol FROM trading_halts h
    WHERE halt_status = 'HALTED'
      AND halt_start_time <= m._event_time
      AND (halt_end_time IS NULL OR halt_end_time > m._event_time)
)
EMIT CHANGES
WITH (
    -- Source configuration (external dependency)
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Reference tables
    'instrument_schedules.type' = 'file_source',
    'instrument_schedules.config_file' = '../configs/instrument_schedules_table.yaml',

    'trading_halts.type' = 'file_source',
    'trading_halts.config_file' = '../configs/trading_halts_table.yaml',

    -- Sink configuration
    'active_hours_market_data.type' = 'kafka_sink',
    'active_hours_market_data.topic.name' = 'active_hours_market_data',
    'active_hours_market_data.config_file' = '../configs/kafka_sink.yaml',

    -- Low latency for market hours
    'linger.ms' = '0',
    'performance_profile' = 'ultra_low_latency'
);
