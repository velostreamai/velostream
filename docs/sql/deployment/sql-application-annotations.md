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
-- Name: Low-Overhead Analysis
-- WITH (observability.metrics.enabled = false)
START JOB low_overhead AS SELECT * FROM stream;
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
-- Name: High-Frequency Stream
-- WITH (observability.tracing.enabled = false)
START JOB hf_stream AS SELECT * FROM fast_stream;
```

##### `@observability.profiling.enabled`

**Purpose**: Enable/disable performance profiling for all jobs

**Values**: `true` | `false`

**Default**: Not set (no app-level default)

**Scope**: Application-wide (applies to all jobs unless overridden)

```sql
-- SQL Application: Production Platform
-- @observability.profiling.enabled: true
```

**Effect**: Enables:
- CPU usage profiling
- Memory usage tracking
- Automatic bottleneck detection
- Performance recommendations

**Performance Impact**: ~5-10% overhead when enabled

**Per-Job Override**:
```sql
-- Name: Critical Processor
-- WITH (observability.profiling.enabled = true)
START JOB critical AS SELECT * FROM critical_stream;
```

### Configuration Examples

#### Example 1: Full Production Observability

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0
-- Description: Real-time financial trading analytics
-- Author: Finance Team
-- Dependencies: market_data, positions, orders
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false

-- All jobs inherit full observability except profiling
START JOB market_analysis AS SELECT * FROM market_data;
START JOB risk_monitor AS SELECT * FROM positions;
```

#### Example 2: Development Environment

```sql
-- SQL Application: Development Analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: true

-- All jobs get full observability for debugging
START JOB job1 AS SELECT * FROM stream1;
START JOB job2 AS SELECT * FROM stream2;
```

#### Example 3: Mixed Configuration

```sql
-- SQL Application: Hybrid Analytics
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- Name: Standard Job (inherits both metrics and tracing)
START JOB standard AS SELECT * FROM stream1;

-- Name: Low-Overhead Job (overrides tracing)
-- WITH (observability.tracing.enabled = false)
START JOB lightweight AS SELECT * FROM stream2;

-- Name: High-Observability Job (adds profiling)
-- WITH (observability.profiling.enabled = true)
START JOB intensive AS SELECT * FROM stream3;
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

-- Job 1: Inherits app-level settings (metrics=true, tracing=true)
START JOB job1 AS SELECT * FROM stream1;

-- Job 2: Overrides metrics only (metrics=false, tracing=true)
-- WITH (observability.metrics.enabled = false)
START JOB job2 AS SELECT * FROM stream2;

-- Job 3: Overrides both (metrics=false, tracing=false)
-- WITH (observability.metrics.enabled = false, observability.tracing.enabled = false)
START JOB job3 AS SELECT * FROM stream3;

-- Job 4: Adds profiling while keeping inherited settings
-- WITH (observability.profiling.enabled = true)
START JOB job4 AS SELECT * FROM stream4;
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

-- Name: High-Frequency Stream
-- WITH (observability.tracing.enabled = false)
START JOB hf_stream AS SELECT * FROM fast_stream;
```

### 3. Document Observability Choices

Add comments explaining why certain observability settings are configured:

```sql
-- SQL Application: Analytics Platform
-- @observability.metrics.enabled: true  -- Monitor all query performance
-- @observability.tracing.enabled: true  -- Track cross-job dependencies
-- @observability.profiling.enabled: false  -- Reduce overhead in production

-- Name: Critical Analysis
-- WITH (observability.profiling.enabled = true)  -- Debug CPU bottlenecks
START JOB critical_analysis AS SELECT * FROM critical_stream;
```

### 4. Environment-Specific Configurations

Maintain separate SQL files for different environments:

**File**: `financial_trading_dev.sql`
```sql
-- Development environment - maximum observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: true
```

**File**: `financial_trading_prod.sql`
```sql
-- Production environment - balanced overhead
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false
```

### 5. Performance Considerations

Balance observability with performance requirements:

```sql
-- ✅ Recommended for production
-- @observability.metrics.enabled: true     -- ~2% overhead
-- @observability.tracing.enabled: true     -- ~3% overhead
-- @observability.profiling.enabled: false  -- Avoid 5-10% overhead
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

-- ❌ Wrong: Annotation after job
START JOB job1 AS SELECT * FROM stream1;
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

| Annotation | Type | Values | Default |
|-----------|------|--------|---------|
| `@observability.metrics.enabled` | Boolean | `true`, `false` | Not set |
| `@observability.tracing.enabled` | Boolean | `true`, `false` | Not set |
| `@observability.profiling.enabled` | Boolean | `true`, `false` | Not set |

### Related Configuration

Per-job observability configuration uses WITH clause:

```sql
START JOB my_job AS
SELECT * FROM stream
WITH (
    observability.metrics.enabled = true,
    observability.tracing.enabled = false,
    observability.profiling.enabled = true
);
```

## See Also

- [SQL Application Deployment Guide](./sql-application-guide.md)
- [Observability Guide](../../ops/observability.md)
- [Advanced Configuration Guide](../../advanced-configuration-guide.md)
