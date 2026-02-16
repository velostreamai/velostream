# SQL Application Annotations Reference

## Overview

SQL Application annotations are special comments that configure application-level metadata and behavior. They use the `-- @` prefix to distinguish them from regular comments and are parsed during application initialization.

## Application-Level Annotations

Application-level annotations appear at the top of the SQL file and apply to the entire application.

### Format

```sql
-- SQL Application: Application Name
-- @annotation.name: value
-- @annotation.category.setting: value
```

### Available Annotations

#### 1. Observability Annotations

##### `@observability.metrics.enabled`

**Purpose**: Enable/disable Prometheus-compatible metrics collection for all jobs

**Values**: `true` | `false`

**Default**: Not set (no app-level default)

**Scope**: Application-wide (applies to all jobs unless overridden)

```sql
-- SQL Application: Trading Platform
-- @observability.metrics.enabled: true
```

**Effect**: Collects metrics for:
- Query execution times
- Record processing rates
- Error counts
- Throughput statistics

**Per-Job Override**:
```sql
-- @name: low-overhead-analysis-1
-- Name: Low-Overhead Analysis
-- WITH (observability.metrics.enabled = false)
CREATE STREAM low_overhead AS
SELECT * FROM stream
EMIT CHANGES;
```

##### `@observability.tracing.enabled`

**Purpose**: Enable/disable distributed tracing for all jobs

**Values**: `true` | `false`

**Default**: Not set (no app-level default)

**Scope**: Application-wide (applies to all jobs unless overridden)

```sql
-- SQL Application: Analytics Suite
-- @observability.tracing.enabled: true
```

**Effect**: Enables:
- End-to-end query execution traces
- Span hierarchy and timing
- Latency analysis
- OpenTelemetry integration

**Per-Job Override**:
```sql
-- @name: high-frequency-stream-1
-- Name: High-Frequency Stream
-- WITH (observability.tracing.enabled = false)
CREATE STREAM hf_stream AS
SELECT * FROM fast_stream
EMIT CHANGES;
```

##### `@observability.profiling.enabled`

**Purpose**: Enable/disable or select profiling mode for all jobs

**Values**:
- `off` - Profiling disabled (zero overhead)
- `dev` - Development mode (detailed: 1000 Hz sampling, flame graphs, early alerts)
- `prod` - Production mode (optimized: 50 Hz sampling, minimal overhead, critical alerts)
- `true` - Equivalent to `prod` (backward compatibility)
- `false` - Equivalent to `off` (backward compatibility)

**Default**: Not set (no app-level default)

**Scope**: Application-wide (applies to all jobs unless overridden)

### Three Profiling Modes

**Mode: `dev` (Development)**
```
Sample Rate:    1000 Hz (every 1ms)
Memory Prof:    Enabled
Flame Graphs:   Generated
CPU Alert:      @ 70%
Memory Alert:   @ 75%
Retention:      3 days
Overhead:       ~8-10%
Use for:        Local debugging, optimization, performance analysis
```

**Mode: `prod` (Production)**
```
Sample Rate:    50 Hz (every 20ms)
Memory Prof:    Disabled
Flame Graphs:   Disabled
CPU Alert:      @ 90%
Memory Alert:   @ 95%
Retention:      30 days
Overhead:       ~2-3%
Use for:        Trading systems, real-time analytics, high-frequency streams
```

**Mode: `off` (Disabled)**
```
Profiling:      Completely disabled
Overhead:       None (0%)
Use for:        Ultra-low-latency requirements, cost-sensitive deployments
```

### Production Example (Minimal Overhead)

```sql
-- SQL Application: Production Trading Platform
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true
```

**Effect with `prod` mode**:
- Minimal CPU/memory overhead (~2-3%)
- Alerts only on critical issues (>90% CPU)
- Lower sampling rate for reduced noise
- 30-day retention for post-incident investigation

### Development Example (Maximum Visibility)

```sql
-- SQL Application: Development Analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: dev
-- @observability.error_reporting.enabled: true
```

**Effect with `dev` mode**:
- Detailed sampling (1000 Hz = 20x more data points)
- Flame graphs automatically generated
- Early alerts at 70-75% thresholds
- Perfect for finding bottlenecks before production

### Per-Job Override (Mix Modes)

```sql
-- App-level: production mode (minimal overhead)
-- @observability.profiling.enabled: prod

-- @name: standard-stream
-- Standard stream uses app-level prod mode
CREATE STREAM standard AS SELECT * FROM stream1 EMIT CHANGES;

-- @name: debug-stream
-- Debug stream switches to dev mode for detailed analysis
-- WITH (observability.profiling.enabled = 'dev')
CREATE STREAM debug AS SELECT * FROM stream2 EMIT CHANGES;

-- @name: critical-stream
-- Critical stream disables profiling for zero overhead
-- WITH (observability.profiling.enabled = 'off')
CREATE STREAM critical AS SELECT * FROM stream3 EMIT CHANGES;
```

**Performance Impact**:
- `off`: 0% overhead
- `prod`: ~2-3% overhead
- `dev`: ~8-10% overhead
- `true`: Equivalent to `prod` (~2-3%)
- `false`: Equivalent to `off` (0%)

##### `@observability.error_reporting.enabled`

**Purpose**: Enable/disable distributed error capture and reporting for all jobs

**Values**: `true` | `false`

**Default**: Not set (no app-level default)

**Scope**: Application-wide (applies to all jobs unless overridden)

**Requires**: `@observability.metrics.enabled: true` (error reporting flows through metrics)

```sql
-- SQL Application: Production Platform
-- @observability.metrics.enabled: true
-- @observability.error_reporting.enabled: true
```

**Effect**: Enables:
- Distributed error capture from all jobs
- Error categorization (SQL, serialization, Kafka, execution)
- Error metrics by type and job
- Error rate tracking and alerting
- Root cause analysis with full error context

**Error Metrics Collected**:
- `velo_errors_total`: Total error count
- `velo_error_rate`: Errors per minute
- `velo_error_by_type`: Errors categorized by type
- `velo_error_by_job`: Errors per job
- `velo_serialization_errors_total`: Serialization failures
- `velo_sql_parsing_errors_total`: SQL parsing failures
- `velo_kafka_errors_total`: Kafka connectivity failures

**Per-Job Override**:
```sql
-- @name: critical-stream-1
-- Name: Critical Stream
-- WITH (observability.error_reporting.enabled = true)
CREATE STREAM critical_stream AS
SELECT * FROM critical
EMIT CHANGES;
```

#### 2. Metrics Annotation

##### `@metric`

**Purpose**: Define custom business metrics to track at the job level

**Values**: Metric name and optional configuration

**Scope**: Can be per-job or application-wide (depends on usage)

**Format**:
```sql
-- @metric: metric_name
-- @metric: metric_name(dimension1, dimension2)
```

**Examples**:

```sql
-- SQL Application: Business Analytics
-- @metric: order_value
-- @metric: transaction_count
-- @metric: customer_segments(segment_type, region)

-- @name: order-processing-1
-- Name: Order Processing
CREATE STREAM order_processor AS
SELECT order_id, total_value FROM orders
EMIT CHANGES;
```

**Metrics Available**:
- Simple counter metrics (auto-incremented)
- Dimensional metrics (grouped by dimensions)
- Custom business KPIs
- Domain-specific measurements

**Use Cases**:
- Track business KPIs (revenue, transaction count, user engagement)
- Monitor domain-specific metrics (order values, conversion rates)
- Measure custom application behavior
- Integrate with business intelligence systems

**Grafana Integration**:
Custom metrics automatically appear in Grafana dashboards:
- Available as query dimensions
- Filterable and aggregatable
- Historical trending
- Alert-ready

### Configuration Examples

#### Example 1: Full Production Observability with Error Reporting

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0
-- Description: Real-time financial trading analytics
-- Author: Finance Team
-- Dependencies: market_data, positions, orders
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true
-- @metric: trade_volume
-- @metric: price_movements
-- @metric: portfolio_value

-- All jobs inherit full observability plus error reporting and custom metrics
-- @name: market-analysis-1
CREATE STREAM market_analysis AS
SELECT * FROM market_data
EMIT CHANGES;

-- @name: risk-monitor-1
CREATE STREAM risk_monitor AS
SELECT * FROM positions
EMIT CHANGES;
```

#### Example 2: Development Environment

```sql
-- SQL Application: Development Analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: dev

-- All jobs get full observability for debugging
-- @name: job-1
CREATE STREAM job1 AS
SELECT * FROM stream1
EMIT CHANGES;

-- @name: job-2
CREATE STREAM job2 AS
SELECT * FROM stream2
EMIT CHANGES;
```

#### Example 3: Mixed Configuration

```sql
-- SQL Application: Hybrid Analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod

-- @name: standard-job-1
-- Name: Standard Job (inherits metrics, tracing, and prod profiling)
CREATE STREAM standard AS
SELECT * FROM stream1
EMIT CHANGES;

-- @name: lightweight-job-1
-- Name: Low-Overhead Job (overrides tracing and profiling)
-- WITH (observability.tracing.enabled = false, observability.profiling.enabled = off)
CREATE STREAM lightweight AS
SELECT * FROM stream2
EMIT CHANGES;

-- @name: intensive-job-1
-- Name: High-Observability Job (upgrades to dev profiling)
-- WITH (observability.profiling.enabled = dev)
CREATE STREAM intensive AS
SELECT * FROM stream3
EMIT CHANGES;
```

## Inheritance and Override Rules

### Basic Inheritance

1. **No explicit per-job configuration**: Inherits app-level setting
2. **Explicit per-job configuration**: Uses per-job setting (overrides app-level)
3. **App-level not set**: No app-level default applied

### Decision Tree

```
For each observability dimension (metrics, tracing, profiling):

1. Is there an explicit per-job setting?
   YES → Use per-job setting
   NO  → 2

2. Is there an app-level setting?
   YES → Use app-level setting
   NO  → 3

3. Use system default (depends on environment)
```

### Examples

```sql
-- SQL Application: Config Demo
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod

-- @name: job1
-- Job 1: Inherits app-level settings (metrics=true, tracing=true, profiling=prod)
CREATE STREAM job1 AS SELECT * FROM stream1 EMIT CHANGES;

-- @name: job2
-- Job 2: Overrides metrics only (metrics=false, tracing=true, profiling=prod)
-- WITH (observability.metrics.enabled = false)
CREATE STREAM job2 AS SELECT * FROM stream2 EMIT CHANGES;

-- @name: job3
-- Job 3: Overrides both metrics and tracing (metrics=false, tracing=false, profiling=prod)
-- WITH (observability.metrics.enabled = false, observability.tracing.enabled = false)
CREATE STREAM job3 AS SELECT * FROM stream3 EMIT CHANGES;

-- @name: job4
-- Job 4: Upgrades profiling to dev while keeping inherited metrics/tracing
-- WITH (observability.profiling.enabled = dev)
CREATE STREAM job4 AS SELECT * FROM stream4 EMIT CHANGES;
```

## Implementation Details

### Parsing

**File**: `src/velostream/sql/app_parser.rs`

**Function**: `extract_metadata()` (lines 159-264)

Annotations are parsed from comment lines at the beginning of the SQL file:

```rust
if line.starts_with("-- @observability.metrics.enabled:") {
    let val = line.replace("-- @observability.metrics.enabled:", "").trim().to_lowercase();
    observability_metrics_enabled = Some(val == "true");
}
```

### Storage

**File**: `src/velostream/sql/app_parser.rs`

**Struct**: `ApplicationMetadata` (lines 73-93)

```rust
pub struct ApplicationMetadata {
    // ... other fields
    pub observability_metrics_enabled: Option<bool>,
    pub observability_tracing_enabled: Option<bool>,
    pub observability_profiling_enabled: Option<bool>,
}
```

### Merging

**File**: `src/velostream/server/stream_job_server.rs`

**Function**: `deploy_sql_application_with_filename()` (lines 1163-1183)

App-level observability settings are intelligently merged with per-job settings:

```rust
// Only inject app-level setting if not explicitly set in job SQL
if let Some(true) = app.metadata.observability_metrics_enabled {
    if !merged_sql.contains("'observability.metrics.enabled'") {
        merged_sql.push_str("\n-- App-level observability injection\n");
        merged_sql.push_str("-- @observability.metrics.enabled: true");
    }
}
```

## Best Practices

### 1. Set App-Level for Consistency

Use app-level settings for observability dimensions that should apply uniformly across all jobs:

```sql
-- ✅ Good: Consistent metrics for all jobs
-- @observability.metrics.enabled: true
```

### 2. Override Only When Necessary

Override at per-job level only for specific jobs with different requirements:

```sql
-- ✅ Good: Most jobs inherit, one job overrides
-- @observability.tracing.enabled: true

-- @name: hf-stream-1
-- Name: High-Frequency Stream
-- WITH (observability.tracing.enabled = false)
CREATE STREAM hf_stream AS
SELECT * FROM fast_stream
EMIT CHANGES;
```

### 3. Document Observability Choices

Add comments explaining why certain observability settings are configured:

```sql
-- SQL Application: Analytics Platform
-- @observability.metrics.enabled: true  -- Monitor all query performance
-- @observability.tracing.enabled: true  -- Track cross-job dependencies
-- @observability.profiling.enabled: prod  -- Balanced overhead for production

-- @name: critical-analysis-1
-- Name: Critical Analysis
-- WITH (observability.profiling.enabled = dev)  -- Debug CPU bottlenecks
CREATE STREAM critical_analysis AS
SELECT * FROM critical_stream
EMIT CHANGES;
```

### 4. Environment-Specific Configurations

Maintain separate SQL files for different environments:

**File**: `financial_trading_dev.sql`
```sql
-- Development environment - maximum observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: dev
```

**File**: `financial_trading_prod.sql`
```sql
-- Production environment - balanced overhead
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
```

### 5. Performance Considerations

Balance observability with performance requirements:

```sql
-- ✅ Recommended for production
-- @observability.metrics.enabled: true     -- ~2% overhead
-- @observability.tracing.enabled: true     -- ~3% overhead
-- @observability.profiling.enabled: prod   -- 2-3% overhead (avoid dev at 8-10%)
```

## Troubleshooting

### Issue: Observability Settings Not Applied

**Symptoms**: App-level observability settings appear to be ignored

**Causes**:
1. Per-job SQL already contains explicit observability configuration
2. Annotation syntax incorrect (check spelling and format)
3. Annotation placed after job definitions

**Solution**:
```sql
-- ✅ Correct: Annotations at top
-- SQL Application: My App
-- @observability.metrics.enabled: true

-- @name: job1
CREATE STREAM job1 AS SELECT * FROM stream1 EMIT CHANGES;

-- ❌ Wrong: Annotation after job
-- @observability.metrics.enabled: true
```

### Issue: Inconsistent Observability Across Jobs

**Symptoms**: Some jobs have observability, others don't

**Causes**:
1. Per-job settings override app-level inconsistently
2. Multiple per-job WITH clauses with conflicting settings

**Solution**: Audit app-level and per-job configurations:
```bash
# Check app-level settings
grep "@observability" your_app.sql

# Check per-job settings
grep "observability\." your_app.sql
```

## Reference

### Annotation Syntax

```
-- @<category>.<subcategory>.<setting>: <value>
```

### Full Annotation List

| Annotation | Type | Values | Default | Notes |
|-----------|------|--------|---------|-------|
| `@observability.metrics.enabled` | Boolean | `true`, `false` | Not set | Enables Prometheus metrics collection (~2% overhead) |
| `@observability.tracing.enabled` | Boolean | `true`, `false` | Not set | Enables distributed tracing (~3% overhead) |
| `@observability.profiling.enabled` | String | `off`, `dev`, `prod`, `true`, `false` | Not set | Profiling mode: off (0%), prod (2-3%), dev (8-10%) |
| `@observability.error_reporting.enabled` | Boolean | `true`, `false` | Not set | Enables error capture & reporting (requires metrics) |
| `@metric` | String | `metric_name` or `metric_name(dim1, dim2)` | N/A | Define custom business metrics |

### Related Configuration

Per-job observability configuration uses WITH clause:

```sql
-- @name: my-job-1
CREATE STREAM my_job AS
SELECT * FROM stream
WITH (
    observability.metrics.enabled = true,
    observability.tracing.enabled = false,
    observability.profiling.enabled = dev
)
EMIT CHANGES;
```

## See Also

- [SQL Application Deployment Guide](./sql-application-guide.md)
- [Observability Guide](../../ops/observability.md)
- [Advanced Configuration Guide](../../advanced-configuration-guide.md)
