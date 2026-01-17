-- =============================================================================
-- APPLICATION: 10_count
-- =============================================================================
-- @app: 10_count  # Application identifier
-- @version: 1.0.0  # Semantic version
-- @description: TODO - Describe your application  # Human-readable description
-- @phase: development  # Options: development, staging, production

--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${POD_NAME:10_count-1}  # Unique node identifier (supports env vars)
-- @deployment.node_name: 10_count Platform  # Human-readable node name
-- @deployment.region: ${AWS_REGION:us-east-1}  # Deployment region

--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: true  # Enable Prometheus metrics collection
-- @observability.tracing.enabled: false  # Enable distributed tracing (OpenTelemetry)
-- @observability.profiling.enabled: prod  # Options: off, dev (8-10% overhead), prod (2-3% overhead)
-- @observability.error_reporting.enabled: true  # Enable structured error reporting

--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: adaptive  # Options: simple (low latency), transactional (exactly-once), adaptive (parallel)
-- @batch_size: 1000  # Records per batch (higher = throughput, lower = latency)
-- @num_partitions: 8  # Parallel partitions for adaptive mode (default: CPU cores)
-- @partitioning_strategy: hash  # Options: sticky, hash, smart, roundrobin, fanin

--
-- METRICS (SQL-Native Prometheus Integration)
-- =============================================================================
-- @metric: velo_10_count_count_output_records_total
-- @metric_type: counter  # Options: counter, gauge, histogram
-- @metric_help: "Total records processed by count_output"
-- @metric_labels: category  # Fields used as Prometheus labels
--
-- @metric: velo_10_count_count_output_count
-- @metric_type: counter  # Options: counter, gauge, histogram
-- @metric_help: "Count aggregation from count_output"
-- @metric_labels: category  # Fields used as Prometheus labels
-- @metric_field: count  # Field to measure (required for gauge/histogram)
--
-- @metric: velo_10_count_current_value_count
-- @metric_type: gauge  # Options: counter, gauge, histogram
-- @metric_help: "Current value_count value"
-- @metric_labels: category  # Fields used as Prometheus labels
-- @metric_field: value_count  # Field to measure (required for gauge/histogram)

--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: 100ms  # Expected P99 latency target
-- @sla.availability: 99.9%  # Availability target
-- @data_retention: 7d  # Data retention policy
-- @compliance: []  # Compliance requirements (e.g., SOX, GDPR, PCI-DSS)

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

-- Tier 2: COUNT Aggregation
-- Tests: COUNT(*) and COUNT(column)
-- Expected: Correct row counting

-- Application metadata
-- @name count_demo
-- @description COUNT aggregation patterns

CREATE TABLE count_output AS
SELECT
    category PRIMARY KEY,
    COUNT(*) AS total_count,
    COUNT(value) AS value_count
FROM input_stream
GROUP BY category
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'test_input_stream',
    'input_stream.config_file' = '../configs/input_stream_source.yaml',

    'count_output.type' = 'kafka_sink',
    'count_output.topic.name' = 'test_count_output',
    'count_output.config_file' = '../configs/aggregates_sink.yaml'
);
