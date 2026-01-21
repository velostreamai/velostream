-- =============================================================================
-- APPLICATION: risk_monitoring
-- =============================================================================
-- @app: risk_monitoring
-- @version: 1.0.0
-- @description: Real-time risk management with position tracking and limits
-- @phase: production
-- @depends_on: app_market_data

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:risk_monitoring-1}
-- @deployment.node_name: Risk Monitoring Pipeline
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
-- @job_mode: simple
-- @batch_size: 500
-- @num_partitions: 4
-- @partitioning_strategy: hash

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 25ms
-- @sla.availability: 99.999%
-- @data_retention: 90d
-- @compliance: [SOX, Basel-III, Dodd-Frank]

--
-- PIPELINE FLOW
-- =============================================================================
-- Input Topics:
--   - trading_positions: Raw position updates
--
-- External Dependencies:
--   - market_data_ts: From app_market_data pipeline (for price joins)
--
-- Reference Tables:
--   - firm_limits: Firm-wide risk limits
--   - desk_limits: Desk-level limits
--   - trader_limits: Individual trader limits
--
-- Output Topics:
--   - trading_positions_ts: Event-time processed positions
--   - risk_alerts: Real-time risk alerts
--   - risk_hierarchy_validation: Multi-tier limit validation

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- @name: trading_positions_ts
-- @description: Ingests raw position updates with event-time processing
-- -----------------------------------------------------------------------------
-- @metric: velo_trading_positions_total
-- @metric_type: counter
-- @metric_help: "Total position updates processed"
-- @metric_labels: trader_id, symbol
--
-- @metric: velo_position_size
-- @metric_type: gauge
-- @metric_help: "Current position size"
-- @metric_labels: trader_id, symbol
-- @metric_field: position_size

CREATE STREAM trading_positions_ts AS
SELECT
    trader_id PRIMARY KEY,
    symbol PRIMARY KEY,
    position_size,
    current_pnl,
    timestamp,
    timestamp as _event_time
FROM in_trading_positions_stream
EMIT CHANGES
WITH (
    -- Event-time processing
    'event.time.field' = 'timestamp',
    'event.time.format' = 'epoch_millis',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '2s',
    'late.data.strategy' = 'update_previous',

    -- Source configuration
    'in_trading_positions_stream.type' = 'kafka_source',
    'in_trading_positions_stream.topic.name' = 'trading_positions',
    'in_trading_positions_stream.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'trading_positions_ts.type' = 'kafka_sink',
    'trading_positions_ts.topic.name' = 'trading_positions_ts',
    'trading_positions_ts.config_file' = '../configs/kafka_sink.yaml'
);

-- -----------------------------------------------------------------------------
-- @name: comprehensive_risk_monitor
-- @description: Joins positions with market data for real-time risk calculations
-- -----------------------------------------------------------------------------

CREATE STREAM comprehensive_risk_monitor AS
SELECT
    p.trader_id PRIMARY KEY,
    p.symbol PRIMARY KEY,
    p.position_size,
    p.current_pnl,
    p._event_time AS position_time,
    m._event_time AS market_time,
    m.price AS current_price,

    -- Rolling cumulative P&L
    SUM(p.current_pnl) OVER (
        ROWS WINDOW BUFFER 10000 ROWS
        PARTITION BY p.trader_id
        ORDER BY p._event_time
    ) AS cumulative_pnl,

    -- Trade count
    COUNT(*) OVER (
        ROWS WINDOW BUFFER 10000 ROWS
        PARTITION BY p.trader_id
        ORDER BY p._event_time
    ) AS trades_today,

    -- P&L volatility
    STDDEV(p.current_pnl) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY p.trader_id
        ORDER BY p._event_time
    ) AS pnl_volatility,

    -- Position value
    ABS(p.position_size * COALESCE(m.price, 0)) AS position_value,

    -- Total exposure
    SUM(ABS(p.position_size * COALESCE(m.price, 0))) OVER (
        ROWS WINDOW BUFFER 10000 ROWS
        PARTITION BY p.trader_id
        ORDER BY p._event_time
    ) AS total_exposure,

    -- Risk classification
    CASE
        WHEN ABS(p.position_size * COALESCE(m.price, 0)) > 1000000 THEN 'POSITION_LIMIT_EXCEEDED'
        WHEN SUM(p.current_pnl) OVER (
            ROWS WINDOW BUFFER 10000 ROWS
            PARTITION BY p.trader_id
            ORDER BY p._event_time
        ) < -100000 THEN 'DAILY_LOSS_LIMIT_EXCEEDED'
        WHEN ABS(p.position_size * COALESCE(m.price, 0)) > 500000 THEN 'POSITION_WARNING'
        WHEN STDDEV(p.current_pnl) OVER (
            ROWS WINDOW BUFFER 100 ROWS
            PARTITION BY p.trader_id
            ORDER BY p._event_time
        ) > 25000 THEN 'HIGH_RISK_PROFILE'
        ELSE 'WITHIN_LIMITS'
    END AS risk_classification,

    EXTRACT(EPOCH FROM (m._event_time - p._event_time)) AS time_lag_seconds,
    NOW() AS risk_check_time

FROM trading_positions_ts p
LEFT JOIN market_data_ts m
    ON p.symbol = m.symbol
    AND m._event_time BETWEEN p._event_time - INTERVAL '30' SECOND
                         AND p._event_time + INTERVAL '30' SECOND

WHERE ABS(p.position_size * COALESCE(m.price, 0)) > 100000
   OR p.current_pnl < -10000

EMIT CHANGES
WITH (
    -- Source configurations
    'trading_positions_ts.type' = 'kafka_source',
    'trading_positions_ts.topic.name' = 'trading_positions_ts',
    'trading_positions_ts.config_file' = '../configs/kafka_source.yaml',

    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'market_data_ts.config_file' = '../configs/kafka_source.yaml',

    -- Sink configuration
    'comprehensive_risk_monitor.type' = 'kafka_sink',
    'comprehensive_risk_monitor.topic.name' = 'risk_alerts',
    'comprehensive_risk_monitor.config_file' = '../configs/kafka_sink.yaml',

    -- Resource management
    'max.memory.mb' = '2048',
    'max.groups' = '100000',
    'spill.to.disk' = 'true',
    'join.timeout' = '60s',

    -- Circuit breaker
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '2',
    'circuit.breaker.timeout' = '30s',

    -- Dead letter queue
    'dead.letter.queue.enabled' = 'true',
    'dead.letter.queue.topic' = 'risk-calculation-failures'
);

-- -----------------------------------------------------------------------------
-- @name: risk_hierarchy_validation
-- @description: Multi-tier hierarchical risk limit validation (firm → desk → trader)
-- -----------------------------------------------------------------------------

CREATE STREAM risk_hierarchy_validation AS
SELECT
    p.trader_id PRIMARY KEY,
    p.desk_id,
    p.position_id,
    p.symbol,
    p.quantity,
    p.entry_price,
    p.current_price,
    p.notional_exposure,
    p.position_type,
    p._event_time,
    p.timestamp,

    -- Hard limits validation (AND logic - all must pass)
    CASE
        WHEN p.notional_exposure <= f.firm_notional_limit
            AND p.notional_exposure / f.firm_total_exposure <= f.max_concentration_ratio
        THEN 'PASSED'
        ELSE 'BREACH'
    END as hierarchy_validation_result,

    -- Warning thresholds (OR logic - any triggers warning)
    CASE
        WHEN p.notional_exposure > f.firm_notional_limit * 0.85
            OR p.notional_exposure / f.firm_total_exposure > f.max_concentration_ratio * 0.9
        THEN 'WARNING'
        ELSE 'SAFE'
    END as escalation_status,

    -- Breach classification
    CASE
        WHEN p.notional_exposure > f.firm_notional_limit THEN 'FIRM_NOTIONAL_BREACH'
        WHEN p.notional_exposure / f.firm_total_exposure > f.max_concentration_ratio THEN 'CONCENTRATION_BREACH'
        ELSE 'NO_BREACH'
    END as breach_type,

    -- Breach severity
    CASE
        WHEN (p.notional_exposure / f.firm_notional_limit) > 1.1 THEN 'CRITICAL'
        WHEN (p.notional_exposure / f.firm_notional_limit) > 1.0 THEN 'SEVERE'
        WHEN (p.notional_exposure / f.firm_notional_limit) > 0.9 THEN 'HIGH'
        WHEN (p.notional_exposure / f.firm_notional_limit) > 0.8 THEN 'MEDIUM'
        ELSE 'LOW'
    END as breach_severity,

    NOW() as validation_time,
    f.firm_name,
    d.desk_name,
    tl.role_name

FROM trading_positions_ts p
LEFT JOIN firm_limits f ON true
LEFT JOIN desk_limits d ON p.desk_id = d.desk_id
LEFT JOIN trader_limits tl ON p.trader_id = tl.trader_id
EMIT CHANGES
WITH (
    -- Source configuration
    'trading_positions_ts.type' = 'kafka_source',
    'trading_positions_ts.topic.name' = 'trading_positions_ts',
    'trading_positions_ts.config_file' = '../configs/kafka_source.yaml',

    -- Reference tables
    'firm_limits.type' = 'file_source',
    'firm_limits.config_file' = '../configs/firm_limits_table.yaml',

    'desk_limits.type' = 'file_source',
    'desk_limits.config_file' = '../configs/desk_limits_table.yaml',

    'trader_limits.type' = 'file_source',
    'trader_limits.config_file' = '../configs/trader_limits_table.yaml',

    -- Sink configuration
    'risk_hierarchy_validation.type' = 'kafka_sink',
    'risk_hierarchy_validation.topic.name' = 'risk_hierarchy_validation',
    'risk_hierarchy_validation.config_file' = '../configs/kafka_sink.yaml',

    -- Resource configuration
    'max.memory.mb' = '1024',
    'join.timeout' = '45s',
    'cache.enabled' = 'true',
    'cache.ttl_seconds' = '300'
);
