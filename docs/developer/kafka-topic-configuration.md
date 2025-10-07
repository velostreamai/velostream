# Kafka Topic Configuration for Sources and Sinks

**Version**: 5.1.0
**Status**: ✅ Implemented
**Date**: October 7, 2025

## Overview

This document describes how Velostream configures Kafka topics for data sources (consumers) and data sinks (producers), including the validation rules that prevent silent data loss from misconfigured topics.

## Problem Statement

**Previous Behavior (Before v5.1.0)**:
- Topic names were hardcoded to "default" in some code paths
- Configuration files were not being properly read
- Records were processed successfully but written to wrong topics
- Only warnings were logged - no FAIL FAST behavior
- Silent data loss occurred with no error indication

**Example of Silent Failure**:
```sql
CREATE STREAM market_data_ts AS
SELECT * FROM market_data_stream
EMIT CHANGES
WITH (
    'market_data_ts.config_file' = 'configs/market_data_ts_sink.yaml'
);
```

- **Expected**: Data written to topic `market_data_ts`
- **Actual**: Data written to topic `default` (which may not exist)
- **Result**: Silent data loss with warning: "SUSPICIOUS TOPIC NAME: default"

## Solution

### Topic Name Resolution Priority

#### For Data Sources (Consumers)

Topic name is resolved in the following priority order:

1. **`topic` property** (highest priority)
   ```yaml
   # YAML config
   topic: "my_actual_topic"
   ```

2. **`source.topic` property**
   ```yaml
   # YAML config with prefix
   source.topic: "my_actual_topic"
   ```

3. **`datasource.topic.name` property**
   ```yaml
   # Datasource-scoped config
   datasource:
     topic.name: "my_actual_topic"
   ```

4. **Named source** (default fallback)
   ```sql
   -- Source name used as topic name
   FROM market_data_stream  -- topic = "market_data_stream"
   ```

#### For Data Sinks (Producers)

Topic name is resolved in the following priority order:

1. **`topic` property** (highest priority)
   ```yaml
   # YAML config
   topic: "my_actual_topic"
   ```

2. **`topic.name` property**
   ```yaml
   # Alternative YAML config
   topic.name: "my_actual_topic"
   ```

3. **Named sink** (default fallback)
   ```sql
   -- Sink name used as topic name
   EMIT CHANGES INTO market_data_ts  -- topic = "market_data_ts"
   ```

### FAIL FAST Validation

**All topic names are validated** to prevent misconfiguration:

#### Empty Topic Names
```
❌ CONFIGURATION ERROR: Kafka [source|sink] topic name is empty.
```
- **Result**: Immediate error, job deployment fails
- **Prevents**: Reading from/writing to undefined topics

#### Suspicious Topic Names
The following names are **rejected as invalid**:
- `default`
- `test`
- `temp`
- `placeholder`
- `undefined`
- `null`
- `none`
- `example`
- `my-topic`
- `topic-name`

```
❌ CONFIGURATION ERROR: Kafka [source|sink] configured with
   suspicious topic name 'default'.

This is a common placeholder/fallback value that indicates
configuration was not properly loaded.
```

- **Result**: Immediate error, job deployment fails
- **Prevents**: Silent data loss from placeholder topics

## Configuration Examples

### Example 1: Using Named Sources/Sinks (Recommended)

```sql
-- ✅ BEST PRACTICE: Use meaningful names that match your topics
CREATE STREAM real_time_analytics AS
SELECT
    symbol,
    AVG(price) as avg_price,
    COUNT(*) as trade_count
FROM trades_stream              -- Reads from topic "trades_stream"
GROUP BY symbol
EMIT CHANGES
INTO analytics_output;          -- Writes to topic "analytics_output"
```

**Result**:
- Source topic: `trades_stream` (from source name)
- Sink topic: `analytics_output` (from sink name)
- ✅ Clear, explicit, no configuration needed

### Example 2: Overriding with YAML Configuration

```yaml
# configs/custom_source.yaml
topic: "production_trades_v2"   # Override default topic name
bootstrap.servers: "broker:9092"
group_id: "analytics_consumer"
```

```yaml
# configs/custom_sink.yaml
topic: "production_analytics_v2"  # Override default topic name
bootstrap.servers: "broker:9092"
```

```sql
CREATE STREAM real_time_analytics AS
SELECT * FROM trades_stream
WITH ('trades_stream.config_file' = 'configs/custom_source.yaml')
EMIT CHANGES
INTO analytics_output
WITH ('analytics_output.config_file' = 'configs/custom_sink.yaml');
```

**Result**:
- Source topic: `production_trades_v2` (from YAML, overrides `trades_stream`)
- Sink topic: `production_analytics_v2` (from YAML, overrides `analytics_output`)
- ✅ Flexible per-environment configuration

### Example 3: SQL Properties Override

```sql
CREATE STREAM real_time_analytics AS
SELECT * FROM trades_stream
WITH (
    'trades_stream.topic' = 'production_trades_v2',
    'trades_stream.bootstrap.servers' = 'broker:9092'
)
EMIT CHANGES
INTO analytics_output
WITH (
    'analytics_output.topic' = 'production_analytics_v2',
    'analytics_output.bootstrap.servers' = 'broker:9092'
);
```

**Result**:
- Source topic: `production_trades_v2` (from SQL properties)
- Sink topic: `production_analytics_v2` (from SQL properties)
- ✅ Quick inline configuration without YAML files

## Error Scenarios and Solutions

### Scenario 1: Configuration File Not Found

**SQL**:
```sql
FROM market_data
WITH ('market_data.config_file' = 'configs/missing.yaml')
```

**Error**:
```
❌ CONFIGURATION ERROR: Kafka source configured with suspicious topic name 'default'.
```

**Root Cause**: YAML file not found, topic falls back to "default" (suspicious name)

**Solution**:
1. Check file path is correct
2. Ensure file exists: `ls -la configs/missing.yaml`
3. Fix path or create file

### Scenario 2: Missing Topic in YAML

**YAML (`configs/source.yaml`)**:
```yaml
# Missing 'topic' or 'topic.name' field!
bootstrap.servers: "broker:9092"
group_id: "my_group"
```

**Error**:
```
❌ CONFIGURATION ERROR: Kafka source configured with suspicious topic name 'default'.
```

**Root Cause**: YAML loaded but no topic specified, falls back to source name

**Solution**: If source name is suspicious (like "default"), add explicit topic:
```yaml
topic: "actual_topic_name"
bootstrap.servers: "broker:9092"
```

### Scenario 3: Using Placeholder Names

**SQL**:
```sql
-- ❌ BAD: Generic placeholder names
FROM test
EMIT CHANGES INTO temp;
```

**Error**:
```
❌ CONFIGURATION ERROR: Kafka source configured with suspicious topic name 'test'.
❌ CONFIGURATION ERROR: Kafka sink configured with suspicious topic name 'temp'.
```

**Solution**: Use meaningful, descriptive names:
```sql
-- ✅ GOOD: Descriptive, specific names
FROM market_data_stream
EMIT CHANGES INTO real_time_analytics;
```

## Implementation Details

### Code Locations

#### Sink Topic Configuration
- **File**: `src/velostream/server/processors/common.rs`
- **Function**: `create_kafka_writer()`
- **Lines**: 592-631
- **Logic**:
  ```rust
  let topic = props
      .get("topic")
      .or_else(|| props.get("topic.name"))
      .map(|s| s.to_string())
      .unwrap_or_else(|| sink_name.to_string());
  ```

#### Source Topic Configuration
- **File**: `src/velostream/server/processors/common.rs`
- **Function**: `create_kafka_reader()`
- **Lines**: 495-530
- **Logic**:
  ```rust
  let topic = props
      .get("topic")
      .or_else(|| props.get("source.topic"))
      .or_else(|| props.get("datasource.topic.name"))
      .map(|s| s.to_string())
      .unwrap_or_else(|| source_name.to_string());
  ```

#### Validation
- **Sink**: `src/velostream/datasource/kafka/writer.rs:209-268`
- **Source**: `src/velostream/datasource/kafka/data_source.rs:81-130`

### Validation Flow

```
┌─────────────────────────────────────┐
│  Topic Name Resolution              │
│  (properties → named source/sink)   │
└──────────────┬──────────────────────┘
               │
               v
┌─────────────────────────────────────┐
│  validate_topic_name()              │
│  ├─ Check: empty?                   │
│  ├─ Check: suspicious name?         │
│  └─ Log: validation passed          │
└──────────────┬──────────────────────┘
               │
               ├─ ✅ Valid → Continue
               │
               └─ ❌ Invalid → FAIL FAST
                  (throw error, stop deployment)
```

## Testing

### Unit Tests

**File**: `tests/unit/datasource/kafka_sink_config_test.rs`

```bash
# Run Kafka topic configuration tests
cargo test kafka_sink_config --no-default-features -- --nocapture
```

**Test Coverage**:
- ✅ `test_sink_name_used_as_topic_when_not_specified` - Default behavior
- ✅ `test_topic_from_properties_overrides_sink_name` - Property override
- ✅ `test_default_topic_name_fails_fast` - "default" rejected
- ✅ `test_other_suspicious_topic_names_fail_fast` - All suspicious names rejected
- ✅ `test_valid_topic_names_pass_validation` - Valid names pass
- ✅ `test_empty_topic_name_fails_fast` - Empty name rejected
- ✅ `test_topic_extraction_precedence` - Priority order verified

### Integration Testing

```bash
# Start trading demo to verify real-world behavior
cd demo/trading
./start-demo.sh 1

# Check topic assignment logs
tail -f /tmp/velo_deployment.log | grep "Creating Kafka"

# Expected output:
# Creating Kafka reader for source 'market_data_stream' with topic 'market_data_stream'
# Creating Kafka writer for sink 'market_data_ts' with topic 'market_data_ts'
```

## Migration Guide

### Upgrading from v5.0.x to v5.1.0

#### If You Used Default Topic Names

**Before (v5.0.x - May have worked silently)**:
```sql
FROM my_stream  -- Might have used "default" topic
```

**After (v5.1.0 - Uses source name)**:
```sql
FROM my_stream  -- Uses topic "my_stream" (explicit)
```

**Action Required**: None if source name matches your topic name

#### If You Used Hardcoded "default" Topics

**Before (v5.0.x - Logged warning)**:
```yaml
topic: "default"  # Warning logged, continued processing
```

**After (v5.1.0 - FAILS FAST)**:
```
❌ ERROR: Kafka configured with suspicious topic name 'default'
```

**Action Required**: Update YAML to use actual topic name:
```yaml
topic: "my_actual_topic"  # Replace "default" with real topic
```

#### If Configuration Files Were Missing

**Before (v5.0.x - Warning only)**:
```
WARN: SUSPICIOUS TOPIC NAME: default
[Data written to "default" topic - silent loss]
```

**After (v5.1.0 - Deployment fails)**:
```
❌ ERROR: Kafka configured with suspicious topic name 'default'
[Job deployment fails - prevents data loss]
```

**Action Required**:
1. Fix file paths in SQL `config_file` properties
2. Ensure all referenced YAML files exist
3. Add missing `topic` fields to YAML configs

## Best Practices

### ✅ DO

1. **Use descriptive, meaningful names** that match your Kafka topics:
   ```sql
   FROM production_orders     -- Clear, specific
   EMIT CHANGES INTO customer_analytics  -- Descriptive purpose
   ```

2. **Explicitly configure topics in YAML** for production:
   ```yaml
   topic: "prod_customer_orders_v2"
   ```

3. **Use environment-specific YAML files**:
   ```
   configs/
     dev_orders_source.yaml    (topic: "dev_orders")
     prod_orders_source.yaml   (topic: "prod_orders_v2")
   ```

4. **Verify configuration after deployment**:
   ```bash
   tail -f /tmp/velo_deployment.log | grep "Creating Kafka"
   ```

### ❌ DON'T

1. **Don't use placeholder names**:
   ```sql
   FROM test           -- ❌ Fails validation
   FROM temp           -- ❌ Fails validation
   FROM placeholder    -- ❌ Fails validation
   ```

2. **Don't rely on implicit "default" topic**:
   ```yaml
   # ❌ Missing topic field - may fail if source name is suspicious
   bootstrap.servers: "broker:9092"
   ```

3. **Don't ignore configuration errors**:
   ```
   ❌ ERROR: Kafka configured with suspicious topic name 'default'
   [Don't try to work around - fix the configuration!]
   ```

## Troubleshooting

### Q: Why does my job fail with "suspicious topic name"?

**A**: Your configuration is not being loaded correctly. Check:
1. File path in `config_file` property
2. File exists and is readable
3. YAML contains `topic` or `topic.name` field
4. Source/sink names are not in the suspicious list

### Q: Can I use "test" as a topic name?

**A**: No. "test" is a placeholder name that suggests misconfiguration. Use a descriptive name like `test_customer_orders` or `integration_test_data`.

### Q: How do I override the topic for a specific environment?

**A**: Use environment-specific YAML files:
```bash
# Development
WITH ('orders.config_file' = 'configs/dev_orders.yaml')

# Production
WITH ('orders.config_file' = 'configs/prod_orders.yaml')
```

### Q: What if my topic really is named "default"?

**A**: Rename your Kafka topic. "default" is a poor topic name that suggests it's a fallback value. Use a descriptive name that indicates the data's purpose.

## Performance Impact

- **Validation overhead**: < 0.1ms per source/sink creation
- **Runtime impact**: Zero (validation only at job deployment)
- **Memory impact**: Negligible (validation in initialization path)

## Security Considerations

- **Prevents data exfiltration**: Misconfigured topics can't silently send data to wrong destinations
- **Audit trail**: All topic assignments logged at INFO level
- **Configuration validation**: Early detection of misconfigurations before data flows

## Related Documentation

- [Kafka Configuration Guide](kafka-configuration.md)
- [Data Source Configuration](../sql/integration/kafka-configuration.md)
- [Compression Independence](compression-independence.md)
- [Event-Time Extraction](../sql/watermarks-time-semantics.md)

## Multi-Source Processor Data Loss Fix (v5.2.0)

**Critical Bug Fixed**: Multi-source processors were processing records through SQL engine but never writing output to sinks, causing silent data loss and violating transactional guarantees.

### Problem (Before v5.2.0)
Both `SimpleJobProcessor` and `TransactionalJobProcessor` had critical bugs in multi-source batch processing:

1. **No Sink Writes**: Used `execute_with_record()` which doesn't return SQL output
   - Records were processed successfully
   - SQL transformations were applied
   - **But output was never written to Kafka sinks**
   - Result: 100% data loss for multi-source jobs

2. **Infinite Loops**: Processors never checked if sources finished
   - Loop only exited on shutdown signal
   - Required manual termination even when all data processed
   - Result: Hung processes in production

### Solution (v5.2.0)

**Files Changed**:
- `src/velostream/server/processors/simple.rs`
- `src/velostream/server/processors/transactional.rs`

**Key Changes**:
```rust
// OLD (BROKEN) - Never captures output
for record in batch {
    engine_lock.execute_with_record(query, record).await?;
}
// Output records lost!

// NEW (FIXED) - Captures and writes output
let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
all_output_records.extend(batch_result.output_records);

// Write to all sinks
for sink_name in &sink_names {
    context.write_batch_to(sink_name, all_output_records.clone()).await?;
}
```

**Exit Condition Fix**:
```rust
// Check if all sources have finished processing
let sources_finished = {
    let source_names = context.list_sources();
    let mut all_finished = true;
    for source_name in source_names {
        match context.has_more_data(&source_name).await {
            Ok(has_more) => {
                if has_more {
                    all_finished = false;
                    break;
                }
            }
            // ... error handling
        }
    }
    all_finished
};

if sources_finished {
    info!("All sources have finished - no more data to process");
    break;
}
```

### Verification

**Demo Test**: Trading demo (`demo/trading/`)
```bash
./start-demo.sh 1

# Check sink topics have data
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market_data_ts \
  --from-beginning --max-messages 5
```

**Result**: ✅ Sinks now receive data correctly

**Unit Tests**:
- `tests/unit/server/processors/multi_source_sink_write_test.rs` - Simple processor
- `tests/unit/server/processors/transactional_multi_source_sink_write_test.rs` - Transactional processor

### Impact

- **Severity**: Critical - 100% data loss in multi-source jobs
- **Affected Versions**: v5.0.0 - v5.1.0
- **Fixed In**: v5.2.0
- **Processors Affected**: Both Simple and Transactional
- **Transactional Violation**: Transactions committed but output discarded

## Changelog

### v5.2.0 (October 7, 2025)
- 🚨 **CRITICAL FIX**: Multi-source processors now write output to sinks
- ✅ Fixed silent data loss in `SimpleJobProcessor::process_multi_source_batch()`
- ✅ Fixed silent data loss in `TransactionalJobProcessor::process_multi_source_transactional_batch()`
- ✅ Added source completion checks - processors exit cleanly when sources finish
- ✅ Comprehensive unit tests for sink write verification
- ✅ Support for `schema.key.field` in nested YAML configurations

### v5.1.0 (October 7, 2025)
- ✅ Use named source/sink as default topic name
- ✅ FAIL FAST validation for suspicious topic names
- ✅ Property extraction priority documented
- ✅ Comprehensive error messages with solutions
- ✅ Test coverage for all scenarios

### v5.0.x (Before October 7, 2025)
- ⚠️ Hardcoded "default" topic in some paths
- ⚠️ Warning-only for suspicious names
- ⚠️ Silent data loss possible
- ⚠️ Multi-source processors didn't write to sinks (critical bug)
