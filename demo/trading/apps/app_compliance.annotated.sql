-- =============================================================================
-- APPLICATION: app_compliance
-- =============================================================================
-- @app: app_compliance  # Application identifier
-- @version: 1.0.0  # Semantic version
-- @description: TODO - Describe your application  # Human-readable description
-- @phase: development  # Options: development, staging, production

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:app_compliance-1}  # Unique node identifier (supports env vars)
-- @deployment.node_name: app_compliance Platform  # Human-readable node name
-- @deployment.region: ${AWS_REGION:us-east-1}  # Deployment region

--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: true  # Enable Prometheus metrics collection
-- @observability.tracing.enabled: false  # Enable distributed tracing (OpenTelemetry)
-- @observability.profiling.enabled: off  # Options: off, dev (8-10% overhead), prod (2-3% overhead)
-- @observability.error_reporting.enabled: true  # Enable structured error reporting

--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: simple  # Options: simple (low latency), transactional (exactly-once), adaptive (parallel)
-- @batch_size: 100  # Records per batch (higher = throughput, lower = latency)
-- @num_partitions: 8  # Parallel partitions for adaptive mode (default: CPU cores)
-- @partitioning_strategy: sticky  # Options: sticky, hash, smart, roundrobin, fanin

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 50ms  # Expected P99 latency target
-- @sla.availability: 99.9%  # Availability target
-- @data_retention: 7d  # Data retention policy
-- @compliance: []  # Compliance requirements (e.g., SOX, GDPR, PCI-DSS)

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

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

-- -----------------------------------------------------------------------------
-- METRICS for compliant_market_data
-- -----------------------------------------------------------------------------
-- @metric: velo_app_compliance_compliant_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by compliant_market_data"
--
-- @metric: velo_app_compliance_current_m_price
-- @metric_type: gauge
-- @metric_help: "Current m.price value"
-- @metric_field: m.price
--
-- @metric: velo_app_compliance_current_m_bid_price
-- @metric_type: gauge
-- @metric_help: "Current m.bid_price value"
-- @metric_field: m.bid_price
--
-- @metric: velo_app_compliance_current_m_ask_price
-- @metric_type: gauge
-- @metric_help: "Current m.ask_price value"
-- @metric_field: m.ask_price
--
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

-- -----------------------------------------------------------------------------
-- METRICS for active_hours_market_data
-- -----------------------------------------------------------------------------
-- @metric: velo_app_compliance_active_hours_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by active_hours_market_data"
--
-- @metric: velo_app_compliance_current_m_price
-- @metric_type: gauge
-- @metric_help: "Current m.price value"
-- @metric_field: m.price
--
-- @metric: velo_app_compliance_current_m_bid_price
-- @metric_type: gauge
-- @metric_help: "Current m.bid_price value"
-- @metric_field: m.bid_price
--
-- @metric: velo_app_compliance_current_m_ask_price
-- @metric_type: gauge
-- @metric_help: "Current m.ask_price value"
-- @metric_field: m.ask_price
--
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
