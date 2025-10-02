# Kafka and File Source Limitations

## Current Status (September 28, 2025)

This document outlines the current limitations and behaviors of Kafka and File sources in Velostream's CREATE TABLE implementation.

## 📊 Supported Data Sources

| Source Type | Status | Notes |
|------------|--------|-------|
| **Kafka** | ✅ Fully Supported | Streaming and batch modes |
| **Files** | ✅ Supported | JSON, CSV, Parquet formats |
| **PostgreSQL** | ❌ Not Implemented | Planned for future |
| **MySQL** | ❌ Not Implemented | Planned for future |
| **S3** | ❌ Not Implemented | Planned for future |
| **ClickHouse** | ❌ Not Implemented | Planned for future |
| **Iceberg** | ❌ Not Implemented | Planned for future |

## 🚨 Critical Limitations

### Kafka Source Limitations

#### 1. Non-existent Topics
**Current Behavior**: Immediate failure
```rust
// What happens internally:
consumer.subscribe(&[&topic])?  // Throws error immediately
// Error: "Unknown topic or partition"
```

**User Experience**:
```sql
CREATE TABLE my_table AS
SELECT * FROM non_existent_topic;
-- Error: Unknown topic or partition 'non_existent_topic'
```

**Workaround**:
```bash
# Create topic first
kafka-topics --create --topic non_existent_topic --partitions 3 --replication-factor 1
```

#### 2. No Retry Logic
- **No wait**: Fails immediately, no retry attempts
- **No auto-creation**: Won't create topic even with permissions
- **Manual intervention**: User must create topic manually

#### 3. Connection Failures
- **Broker unavailable**: Immediate failure
- **Network issues**: No automatic reconnection during table creation
- **Authentication errors**: Fail without retry

### File Source Limitations

#### 1. Missing Files
**Current Behavior**: Immediate failure
```rust
// What happens internally:
File::open(path)?  // Throws error immediately
// Error: "No such file or directory"
```

**User Experience**:
```sql
CREATE TABLE data AS
SELECT * FROM file:///missing/file.json;
-- Error: No such file or directory: /missing/file.json
```

#### 2. Pattern Matching Limitations
- **Empty matches**: Fails if pattern matches no files
- **No wait for matches**: Doesn't wait for files to appear
- **Watch mode**: Only monitors after initial successful load

#### 3. File Format Issues
- **Malformed files**: Immediate failure, no partial processing
- **Schema mismatches**: Fails without recovery
- **Encoding errors**: No fallback encoding attempts

## 🔄 Update Behavior

### Kafka Tables
| Feature | Current Status | Details |
|---------|---------------|---------|
| **Update Frequency** | ✅ Real-time | Continuous consumption |
| **Incremental Updates** | ✅ Yes | Via Kafka offsets |
| **Resume After Restart** | ✅ Yes | Consumer group tracking |
| **Backpressure Handling** | ✅ Yes | Built into Kafka consumer |
| **AUTO_OFFSET Support** | ✅ Yes | `earliest` or `latest` |

### File Tables
| Feature | Current Status | Details |
|---------|---------------|---------|
| **Update Frequency** | ⚠️ Static/Watch | One-time or watch mode |
| **Incremental Updates** | ❌ No | Full file reprocessing |
| **Resume After Restart** | ❌ No | Restarts from beginning |
| **Pattern Monitoring** | ⚠️ Partial | Only with watch mode |
| **New File Detection** | ✅ Yes | With watch mode enabled |

## 🎯 Configuration Options

### Currently Supported

#### Kafka Properties
```sql
CREATE TABLE kafka_table AS
SELECT * FROM kafka_topic
WITH (
    "auto.offset.reset" = "latest",  -- or "earliest" (default)
    "group.id" = "custom-group",
    "batch.size" = "1000"
);
```

#### File Properties
```sql
CREATE TABLE file_table AS
SELECT * FROM file:///path/to/file.json
WITH (
    "format" = "json",
    "watch" = "true",  -- Monitor for new files
    "recursive" = "true"  -- Scan subdirectories
);
```

### NOT Currently Supported (Planned)

#### Kafka Retry Properties
```sql
-- PLANNED BUT NOT IMPLEMENTED:
WITH (
    "topic.wait.timeout" = "60s",     -- Wait for topic
    "topic.retry.interval" = "5s",     -- Retry interval
    "topic.create.if.missing" = "true" -- Auto-create topic
);
```

#### File Retry Properties
```sql
-- PLANNED BUT NOT IMPLEMENTED:
WITH (
    "file.wait.timeout" = "300s",    -- Wait for file
    "file.retry.interval" = "10s",    -- Retry interval
    "file.create.empty" = "true"      -- Create if missing
);
```

## 📝 Error Messages

### Current Error Messages (Limited)
```
Error: Unknown topic or partition 'my_topic'
Error: No such file or directory: /path/to/file.json
```

### Planned Enhanced Error Messages
```
Kafka topic 'my_topic' does not exist. Options:
1. Create the topic: kafka-topics --create --topic my_topic
2. Add retry configuration: WITH ("topic.wait.timeout" = "30s")
3. Check topic name spelling

File '/path/to/file.json' does not exist. Options:
1. Check the file path
2. Add wait configuration: WITH ("file.wait.timeout" = "30s")
3. For patterns, ensure matching files will arrive
```

## 🚀 Planned Improvements

### Phase 5 Implementation (September 28-30, 2025)

1. **Kafka Topic Retry Logic**
   - Configurable wait timeout
   - Retry intervals with backoff
   - Optional auto-creation
   - Connection retry logic

2. **File Source Retry Logic**
   - Wait for file existence
   - Pattern matching with wait
   - Watch mode improvements
   - Better error recovery

3. **Enhanced Error Handling**
   - Actionable error messages
   - Configuration suggestions
   - Command examples
   - Recovery guidance

## 📊 Comparison with Competitors

| Feature | Velostream (Current) | Kafka Streams | Flink | Spark Streaming |
|---------|---------------------|--------------|-------|-----------------|
| **Missing Topic** | ❌ Fails | ⚠️ Waits | ✅ Configurable | ✅ Configurable |
| **Missing File** | ❌ Fails | N/A | ✅ Configurable | ✅ Configurable |
| **AUTO_OFFSET** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Retry Logic** | ❌ No | ⚠️ Limited | ✅ Yes | ✅ Yes |

## 🎯 Best Practices (Current State)

### For Kafka Sources
1. **Always create topics first**:
   ```bash
   kafka-topics --create --topic my_topic --partitions 3
   ```

2. **Verify broker connectivity**:
   ```bash
   kafka-topics --list --bootstrap-server localhost:9092
   ```

3. **Use consumer groups wisely**:
   ```sql
   WITH ("group.id" = "unique-consumer-group")
   ```

### For File Sources
1. **Ensure files exist before CREATE TABLE**:
   ```bash
   ls -la /path/to/file.json
   ```

2. **Use watch mode for dynamic files**:
   ```sql
   WITH ("watch" = "true")
   ```

3. **Test patterns before using**:
   ```bash
   ls /path/to/*.json
   ```

## 📋 Migration Guide

### When Phase 5 is Implemented

**Before (Current)**:
```bash
# Must create topic first
kafka-topics --create --topic events

# Then create table
CREATE TABLE events_table AS
SELECT * FROM events;
```

**After (Phase 5)**:
```sql
-- Single command with retry
CREATE TABLE events_table AS
SELECT * FROM events
WITH (
    "topic.wait.timeout" = "60s",
    "topic.create.if.missing" = "true"
);
```

## Summary

While Velostream provides robust streaming capabilities for existing Kafka topics and files, it currently lacks retry and wait mechanisms for missing sources. Phase 5 implementation will address these limitations, bringing Velostream to parity with other streaming platforms in terms of source resilience and user experience.