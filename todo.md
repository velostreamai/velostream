# Velostream Active Development TODO

**Last Updated**: October 6, 2025
**Status**: ✅ **MAJOR MILESTONE** - HAVING Clause Enhancement Complete
**Current Priority**: **🎯 READY: Financial Trading Demo Production-Ready**

**Related Files**:
- 📋 **Archive**: [todo-consolidated.md](todo-consolidated.md) - Full historical TODO with completed work
- ✅ **Completed**: [todo-complete.md](todo-complete.md) - Successfully completed features

---

## 🎯 **CURRENT STATUS & NEXT PRIORITIES**

### **✅ Recent Completions - October 6, 2025**
- ✅ **HAVING Clause Enhancement Complete**: Phases 1-4 implemented (11,859 errors → 0)
  - ✅ Phase 1: BinaryOp support (arithmetic operations in HAVING)
  - ✅ Phase 2: Column alias support (reference SELECT aliases)
  - ✅ Phase 3: CASE expression support (conditional logic)
  - ✅ Phase 4: Enhanced args_match (complex expression matching)
  - ✅ Added 12 comprehensive unit tests (all passing)
  - ✅ ~350 lines production code + extensive test coverage
- ✅ **Demo Resilience**: Automated startup and health checking scripts
- ✅ **SQL Validation**: Financial trading demo validates successfully
- ✅ **100% Query Success**: All 8 trading queries execute without errors

### **Previous Completions - September 27, 2024**
- ✅ **Test Failures Resolved**: Both `test_optimized_aggregates` and `test_error_handling` fixed
- ✅ **OptimizedTableImpl Complete**: Production-ready with enterprise performance (1.85M+ lookups/sec)
- ✅ **Phase 2 CTAS**: All 65 CTAS tests passing with comprehensive validation
- ✅ **Reserved Keywords Fixed**: STATUS, METRICS, PROPERTIES now usable as field names

*Full details moved to [todo-complete.md](todo-complete.md)*

---

---

## ✅ **RESOLVED: HAVING Clause Enhancement**

**Status**: ✅ **COMPLETED** October 6, 2025
**Issue**: GitHub #75
**Solution**: Phases 1-4 implementation (11,859 errors → 0)

See "Recent Completions" section above for full details.

---

## 🚨 **CRITICAL GAP: Event-Time Extraction Not Implemented (ALL Data Sources)**

**Identified**: October 7, 2025
**Priority**: **CRITICAL** - Phase 1B watermarks feature incomplete across ALL data sources
**Status**: ❌ **NOT IMPLEMENTED** - Configuration accepted but no extraction logic
**Risk Level**: 🔴 **HIGH** - Documented feature not working, demo using processing-time instead of event-time
**Impact**: Financial trading demo and all event-time applications using wrong timestamps
**Scope**: **ALL DATA SOURCES** - Kafka, File, HTTP, SQL, S3, etc.

### **Problem Statement**

The Phase 1B watermarks feature is **partially implemented**:
- ✅ Watermark infrastructure exists and works correctly
- ✅ Configuration properties (`event.time.field`, `event.time.format`) are accepted
- ✅ WatermarkManager checks `record.event_time` field
- ❌ **NO DATA SOURCE extracts event-time from record fields** (all hardcoded to `None`)
- ❌ **Field extraction logic not implemented anywhere**
- ❌ **Timestamp format parsing not implemented**
- ❌ **Generic extraction trait missing** (should work for Kafka, File, HTTP, etc.)

**Current Behavior**:
```rust
// src/velostream/datasource/kafka/reader.rs:660-669
Ok(StreamRecord {
    fields,
    timestamp: message.timestamp()...,
    offset: message.offset(),
    partition: message.partition(),
    headers: message.take_headers().into_map(),
    event_time: None,  // ← ALWAYS None! Configuration ignored
})
```

**Expected Behavior**:
```rust
// Should extract from fields based on config
let event_time = if let Some(field_name) = config.get("event.time.field") {
    extract_and_parse_timestamp(&fields, field_name, config.get("event.time.format"))
} else {
    None
};

Ok(StreamRecord {
    fields,
    timestamp: message.timestamp()...,
    event_time,  // ← Populated from message data
    // ...
})
```

### **Impact Analysis**

**Production Impact**:
```
DOCUMENTED (docs/sql/watermarks-time-semantics.md):
  'event.time.field' = 'timestamp',
  'event.time.format' = 'epoch_millis'

ACTUAL BEHAVIOR:
  → Config accepted (no error)
  → Watermarks enabled
  → BUT uses processing-time instead of event-time!
  → Late data detection WRONG
  → Window emissions based on WRONG timestamps
```

**Financial Trading Demo**:
- Demo configured with `'event.time.field' = 'timestamp'`
- Demo expects event-time windowing
- **Actually using processing-time** (data arrival time, not trade execution time)
- Market data analysis timestamps are INCORRECT
- Risk calculations based on WRONG timing

**User Trust Issue**:
- Feature is fully documented as working
- Configuration is silently accepted
- No error/warning that it's not implemented
- Users believe event-time is working when it's not

### **Gap Analysis**

#### **Code References**

**Hardcoded None Assignment (ALL Data Sources)**:
- `src/velostream/datasource/kafka/reader.rs:668` - Kafka: `event_time: None` (ALWAYS)
- File readers: Also hardcoded to `None` (same pattern)
- All data sources share this problem - **NONE extract event-time**
- No extraction logic exists in ANY data source

**Configuration Detection (Works)**:
- `src/velostream/server/stream_job_server.rs:1117` - Checks for `event.time.field` config
- Enables watermarks correctly

**Watermark Infrastructure (Works)**:
- `src/velostream/sql/execution/watermarks.rs:252` - Checks `record.event_time`
- `src/velostream/sql/execution/watermarks.rs:290` - Falls back to processing-time
- Infrastructure correctly handles event_time when present

**Missing Implementation (Generic Across All Sources)**:
- ❌ **Generic timestamp extraction trait/module**
- ❌ Field extraction from `record.fields` HashMap
- ❌ Timestamp format parsing (`epoch_millis`, `ISO8601`, custom formats)
- ❌ Error handling for missing/invalid timestamps
- ❌ Configuration validation
- ❌ Integration in Kafka, File, HTTP, SQL data sources

#### **Documentation Status**

**Documented as Working**:
- ✅ `docs/sql/watermarks-time-semantics.md` - Full guide with examples (354 lines)
- ✅ Lines 130-132: SQL configuration example
- ✅ Lines 168-170: WITH clause example
- ✅ Complete API documentation for event-time semantics

**Documentation Gap**:
- ❌ No mention that extraction is NOT implemented
- ❌ No warning about processing-time fallback
- ❌ Examples show configurations that don't work

**Demo Files Using It**:
- ✅ `demo/trading/sql/financial_trading.sql` - 3 queries configured (lines 31-32, 286-287)
- ❌ Demo believes it's using event-time but uses processing-time

#### **Testing Gaps**

**Tests That Exist** ✅:
- `tests/unit/sql/execution/phase_1b_watermarks_test.rs` - 10+ watermark tests
- Tests manually create records with `event_time: Some(...)`
- Tests verify watermark logic works when event_time is present
- **BUT no tests for extraction from fields**

**Tests That Don't Exist** ❌:
- ❌ Kafka reader event-time extraction from message fields
- ❌ Timestamp format parsing (`epoch_millis`, `ISO8601`, etc.)
- ❌ Configuration-driven field extraction
- ❌ Error handling for invalid timestamps
- ❌ Integration test: Kafka message → event_time populated
- ❌ End-to-end test: event.time.field config → watermarks use event-time

**Test File Locations**:
- Unit tests needed in: `tests/unit/datasource/kafka/event_time_extraction_test.rs` (NEW)
- Integration tests needed in: `tests/integration/kafka/event_time_integration_test.rs` (NEW)

### **Implementation Requirements**

#### **Phase 1: Generic Event-Time Extraction Infrastructure** (Estimated: 4 days)

**New Module**: `src/velostream/datasource/event_time.rs` (Generic for ALL sources)

```rust
/// Generic event-time extraction module
/// Used by Kafka, File, HTTP, SQL, and all other data sources

/// Timestamp format enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampFormat {
    /// Unix epoch milliseconds (1696723200000)
    EpochMillis,
    /// Unix epoch seconds (1696723200)
    EpochSeconds,
    /// ISO 8601 format (2023-10-08T00:00:00Z)
    ISO8601,
    /// Custom chrono format string
    Custom(String),
}

impl TimestampFormat {
    pub fn parse(s: &str) -> Result<Self, EventTimeError> {
        match s {
            "epoch_millis" => Ok(TimestampFormat::EpochMillis),
            "epoch_seconds" | "epoch" => Ok(TimestampFormat::EpochSeconds),
            "iso8601" | "ISO8601" => Ok(TimestampFormat::ISO8601),
            custom => Ok(TimestampFormat::Custom(custom.to_string())),
        }
    }
}

/// Configuration for event-time extraction
#[derive(Debug, Clone)]
pub struct EventTimeConfig {
    /// Field name to extract timestamp from
    pub field_name: String,
    /// Format of the timestamp
    pub format: Option<TimestampFormat>,
}

impl EventTimeConfig {
    pub fn from_properties(properties: &HashMap<String, String>) -> Option<Self> {
        let field_name = properties.get("event.time.field")?.clone();
        let format = properties.get("event.time.format")
            .map(|s| TimestampFormat::parse(s))
            .transpose()
            .ok()?;

        Some(EventTimeConfig { field_name, format })
    }
}

/// Generic event-time extraction from StreamRecord fields
/// Works for ANY data source (Kafka, File, HTTP, SQL, etc.)
pub fn extract_event_time(
    fields: &HashMap<String, FieldValue>,
    config: &EventTimeConfig,
) -> Result<DateTime<Utc>, EventTimeError> {
    let field_value = fields.get(&config.field_name)
        .ok_or_else(|| EventTimeError::MissingField {
            field: config.field_name.clone(),
            available_fields: fields.keys().cloned().collect(),
        })?;

    let datetime = match &config.format {
        Some(TimestampFormat::EpochMillis) => {
            let millis = field_value.as_integer()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "Integer (epoch millis)",
                    actual: field_value.type_name(),
                })?;
            DateTime::from_timestamp_millis(millis)
                .ok_or(EventTimeError::InvalidTimestamp {
                    value: format!("{}", millis),
                    format: "epoch_millis",
                })?
        }
        Some(TimestampFormat::EpochSeconds) => {
            let secs = field_value.as_integer()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "Integer (epoch seconds)",
                    actual: field_value.type_name(),
                })?;
            DateTime::from_timestamp(secs, 0)
                .ok_or(EventTimeError::InvalidTimestamp {
                    value: format!("{}", secs),
                    format: "epoch_seconds",
                })?
        }
        Some(TimestampFormat::ISO8601) => {
            let s = field_value.as_string()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: "String (ISO 8601)",
                    actual: field_value.type_name(),
                })?;
            DateTime::parse_from_rfc3339(s)
                .map_err(|e| EventTimeError::ParseError {
                    value: s.clone(),
                    format: "ISO8601",
                    error: e.to_string(),
                })?
                .with_timezone(&Utc)
        }
        Some(TimestampFormat::Custom(fmt)) => {
            let s = field_value.as_string()
                .map_err(|_| EventTimeError::TypeMismatch {
                    field: config.field_name.clone(),
                    expected: format!("String ({})", fmt),
                    actual: field_value.type_name(),
                })?;
            NaiveDateTime::parse_from_str(s, fmt)
                .map_err(|e| EventTimeError::ParseError {
                    value: s.clone(),
                    format: fmt.clone(),
                    error: e.to_string(),
                })?
                .and_local_timezone(Utc)
                .single()
                .ok_or(EventTimeError::AmbiguousTimezone {
                    value: s.clone(),
                })?
        }
        None => {
            // Auto-detect: try integer (epoch millis), then ISO 8601
            auto_detect_timestamp(field_value, &config.field_name)?
        }
    };

    Ok(datetime)
}

/// Auto-detect timestamp format from field value
fn auto_detect_timestamp(
    field_value: &FieldValue,
    field_name: &str,
) -> Result<DateTime<Utc>, EventTimeError> {
    // Try integer (epoch millis) first
    if let Ok(millis) = field_value.as_integer() {
        if let Some(dt) = DateTime::from_timestamp_millis(millis) {
            return Ok(dt);
        }
    }

    // Try string (ISO 8601)
    if let Ok(s) = field_value.as_string() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    Err(EventTimeError::AutoDetectFailed {
        field: field_name.to_string(),
        value_type: field_value.type_name(),
    })
}

/// Event-time extraction errors
#[derive(Debug, thiserror::Error)]
pub enum EventTimeError {
    #[error("Field '{field}' not found in record. Available fields: {}", available_fields.join(", "))]
    MissingField {
        field: String,
        available_fields: Vec<String>,
    },

    #[error("Type mismatch for field '{field}': expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("Invalid timestamp value '{value}' for format '{format}'")]
    InvalidTimestamp {
        value: String,
        format: &'static str,
    },

    #[error("Failed to parse '{value}' as {format}: {error}")]
    ParseError {
        value: String,
        format: String,
        error: String,
    },

    #[error("Ambiguous timezone for value '{value}'")]
    AmbiguousTimezone {
        value: String,
    },

    #[error("Auto-detect failed for field '{field}' with type {value_type}")]
    AutoDetectFailed {
        field: String,
        value_type: &'static str,
    },
}
```

**Then integrate in ALL data sources:**

```rust
// Kafka: src/velostream/datasource/kafka/reader.rs
fn deserialize_with_format(&self, ...) -> Result<StreamRecord, KafkaError> {
    // ... existing deserialization code ...

    let event_time = if let Some(ref config) = self.event_time_config {
        Some(extract_event_time(&fields, config)?)
    } else {
        None
    };

    Ok(StreamRecord {
        fields,
        timestamp: message.timestamp()...,
        event_time,  // ← Now populated!
        // ...
    })
}

// File: src/velostream/datasource/file/reader.rs
fn read_record(&self, line: &str) -> Result<StreamRecord, FileError> {
    // ... parse line into fields ...

    let event_time = if let Some(ref config) = self.event_time_config {
        Some(extract_event_time(&fields, config)?)
    } else {
        None
    };

    Ok(StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        event_time,  // ← Now populated!
        // ...
    })
}

// Same pattern for HTTP, SQL, S3, etc.
```

// Add extraction method
fn extract_event_time(
    &self,
    fields: &HashMap<String, FieldValue>
) -> Result<Option<DateTime<Utc>>, KafkaError> {
    let Some(field_name) = &self.event_time_field else {
        return Ok(None);  // Not configured, use processing-time
    };

    let Some(field_value) = fields.get(field_name) else {
        return Err(KafkaError::MissingEventTimeField {
            field: field_name.clone(),
        });
    };

    let datetime = match &self.event_time_format {
        Some(TimestampFormat::EpochMillis) => {
            let millis = field_value.as_integer()?;
            DateTime::from_timestamp_millis(millis)
        }
        Some(TimestampFormat::EpochSeconds) => {
            let secs = field_value.as_integer()?;
            DateTime::from_timestamp(secs, 0)
        }
        Some(TimestampFormat::ISO8601) => {
            let s = field_value.as_string()?;
            DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc)
        }
        Some(TimestampFormat::Custom(fmt)) => {
            let s = field_value.as_string()?;
            NaiveDateTime::parse_from_str(s, fmt)?
                .and_local_timezone(Utc)
                .single()
        }
        None => {
            // Auto-detect: try integer first, then string
            self.auto_detect_timestamp(field_value)?
        }
    };

    Ok(Some(datetime.ok_or(KafkaError::InvalidTimestamp)?))
}
```

**Deliverables**:
- [ ] `TimestampFormat` enum with 4 formats
- [ ] `extract_event_time()` method in KafkaDataSource
- [ ] Configuration parsing in `new()` / `from_properties()`
- [ ] Error types for missing/invalid timestamps
- [ ] Auto-detection fallback logic

#### **Phase 2: Configuration Integration** (Estimated: 2 days)

**File**: `src/velostream/datasource/kafka/data_source.rs`

```rust
impl KafkaDataSource {
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, KafkaError> {
        // ... existing code ...

        let event_time_field = properties.get("event.time.field").cloned();
        let event_time_format = properties.get("event.time.format")
            .map(|s| TimestampFormat::parse(s))
            .transpose()?;

        Ok(Self {
            // ... existing fields ...
            event_time_field,
            event_time_format,
        })
    }
}

impl TimestampFormat {
    pub fn parse(s: &str) -> Result<Self, KafkaError> {
        match s {
            "epoch_millis" => Ok(TimestampFormat::EpochMillis),
            "epoch_seconds" | "epoch" => Ok(TimestampFormat::EpochSeconds),
            "iso8601" | "ISO8601" => Ok(TimestampFormat::ISO8601),
            custom => Ok(TimestampFormat::Custom(custom.to_string())),
        }
    }
}
```

**Deliverables**:
- [ ] Configuration property extraction
- [ ] Format string parsing
- [ ] Backward compatibility (None = processing-time)
- [ ] Configuration validation

#### **Phase 3: Testing** (Estimated: 3 days)

**New Test Files**:
1. `tests/unit/datasource/kafka/event_time_extraction_test.rs`
2. `tests/integration/kafka/event_time_watermarks_test.rs`

**Test Cases**:
```rust
#[test]
fn test_extract_event_time_epoch_millis() {
    // Given: Kafka message with timestamp field (epoch millis)
    // When: event.time.field = 'timestamp', format = 'epoch_millis'
    // Then: StreamRecord.event_time populated correctly
}

#[test]
fn test_extract_event_time_iso8601() {
    // Given: Kafka message with ISO 8601 timestamp string
    // When: event.time.field = 'event_timestamp', format = 'iso8601'
    // Then: StreamRecord.event_time parsed correctly
}

#[test]
fn test_extract_event_time_custom_format() {
    // Given: Kafka message with custom timestamp format
    // When: event.time.field = 'ts', format = '%Y-%m-%d %H:%M:%S'
    // Then: StreamRecord.event_time parsed using custom format
}

#[test]
fn test_missing_event_time_field_error() {
    // Given: event.time.field configured but field missing in message
    // When: Processing message
    // Then: Clear error with field name
}

#[test]
fn test_invalid_timestamp_format_error() {
    // Given: Field value doesn't match expected format
    // When: Parsing timestamp
    // Then: Clear error with format mismatch details
}

#[test]
fn test_no_config_uses_processing_time() {
    // Given: No event.time.field configured
    // When: Processing message
    // Then: StreamRecord.event_time = None (fallback to processing-time)
}

#[tokio::test]
async fn test_integration_watermarks_use_event_time() {
    // Given: Kafka messages with out-of-order event-times
    // When: Watermarks enabled with event.time.field
    // Then: Watermarks advance based on event-time, not processing-time
}
```

**Deliverables**:
- [ ] 15+ unit tests covering all formats
- [ ] 5+ integration tests for watermark interaction
- [ ] Error handling test coverage
- [ ] Performance benchmark (extraction overhead)

#### **Phase 4: Documentation Update** (Estimated: 1 day)

**Files to Update**:
- `docs/sql/watermarks-time-semantics.md` - Add implementation status
- `docs/sql/integration/kafka-configuration.md` - Event-time field extraction guide
- `CHANGELOG.md` - Document feature completion

**New Documentation**:
```markdown
## Event-Time Field Extraction

### Supported Timestamp Formats

| Format | Config Value | Example |
|--------|-------------|---------|
| **Unix Epoch (milliseconds)** | `epoch_millis` | `1696723200000` |
| **Unix Epoch (seconds)** | `epoch_seconds` or `epoch` | `1696723200` |
| **ISO 8601** | `iso8601` or `ISO8601` | `2023-10-08T00:00:00Z` |
| **Custom Format** | Any chrono format string | `%Y-%m-%d %H:%M:%S` |

### Configuration Examples

```sql
-- Extract from epoch milliseconds field
CREATE STREAM trades AS
SELECT * FROM market_data_stream
WITH (
    'event.time.field' = 'timestamp',
    'event.time.format' = 'epoch_millis'
);

-- Extract from ISO 8601 string field
CREATE STREAM events AS
SELECT * FROM event_stream
WITH (
    'event.time.field' = 'event_timestamp',
    'event.time.format' = 'iso8601'
);

-- Extract with custom format
CREATE STREAM logs AS
SELECT * FROM log_stream
WITH (
    'event.time.field' = 'log_time',
    'event.time.format' = '%Y-%m-%d %H:%M:%S%.3f'
);
```
```

**Deliverables**:
- [ ] Updated watermarks guide
- [ ] Kafka configuration guide
- [ ] Migration guide for existing demos
- [ ] Performance characteristics documentation

### **Success Metrics**

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| **Event-Time Extraction** | 0% | 100% | All configured messages extract event-time |
| **Timestamp Format Support** | 0 | 4 | epoch_millis, epoch_seconds, ISO8601, custom |
| **Test Coverage** | 0 tests | 20+ tests | Unit + integration coverage |
| **Documentation Accuracy** | 0% | 100% | Docs match implementation |
| **Demo Correctness** | Processing-time | Event-time | Trading demo uses trade execution time |
| **Error Handling** | Silent failure | Clear errors | Missing/invalid timestamps reported |

### **Timeline Estimate**

**Total Duration**: 2 weeks (9 working days)
- Phase 1 (Core Extraction): 3 days
- Phase 2 (Configuration): 2 days
- Phase 3 (Testing): 3 days
- Phase 4 (Documentation): 1 day

**Priority**: **CRITICAL** - This blocks proper event-time processing for all applications

### **Risk Assessment**

🔴 **High Risk**:
- Users may have deployed systems believing event-time works
- Financial applications using wrong timestamps for compliance
- Silent failures are production incidents waiting to happen

**Mitigation**:
- Add validation that errors if event.time.field configured but not implemented
- Update demo to show warning if using processing-time
- Document current status immediately

---

## 🚀 **NEW ARCHITECTURE: Generic Table Loading System**

**Identified**: September 29, 2024
**Priority**: **HIGH** - Performance & scalability enhancement
**Status**: 📋 **DESIGNED** - Ready for implementation
**Impact**: **🎯 MAJOR** - Unified loading for all data source types

### **Architecture Overview**

Replace source-specific loading with generic **Bulk + Incremental Loading** pattern that works across all data sources (Kafka, File, SQL, HTTP, S3).

#### **Two-Phase Loading Pattern**
```rust
trait TableDataSource {
    /// Phase 1: Initial bulk load of existing data
    async fn bulk_load(&self) -> Result<Vec<StreamRecord>, Error>;

    /// Phase 2: Incremental updates for new/changed data
    async fn incremental_load(&self, since: SourceOffset) -> Result<Vec<StreamRecord>, Error>;

    /// Get current position/offset for incremental loading
    async fn get_current_offset(&self) -> Result<SourceOffset, Error>;

    /// Check if incremental loading is supported
    fn supports_incremental(&self) -> bool;
}
```

#### **Loading Strategies by Source Type**
| Data Source | Bulk Load | Incremental Load | Offset Tracking |
|-------------|-----------|------------------|-----------------|
| **Kafka** | ✅ Consume from earliest | ✅ Consumer offset | ✅ Kafka offsets |
| **Files** | ✅ Read full file | ✅ File position/tail | ✅ Byte position |
| **SQL DB** | ✅ Full table scan | ✅ Change tracking | ✅ Timestamp/ID |
| **HTTP API** | ✅ Initial GET request | ✅ Polling/webhooks | ✅ ETag/timestamp |
| **S3** | ✅ List + read objects | ✅ Event notifications | ✅ Last modified |

### **Implementation Tasks**

#### **Phase 1: Core Trait & Interface** (Estimated: 1 week)
- [ ] Define `TableDataSource` trait with bulk/incremental methods
- [ ] Create `SourceOffset` enum for different offset types
- [ ] Implement generic CTAS loading orchestrator
- [ ] Add offset persistence for resume capability

#### **Phase 2: Source Implementations** (Estimated: 2 weeks)
- [ ] **KafkaDataSource**: Implement bulk (earliest→latest) + incremental (offset-based)
- [ ] **FileDataSource**: Implement bulk (full read) + incremental (file position tracking)
- [ ] **SqlDataSource**: Implement bulk (full query) + incremental (timestamp-based)

#### **Phase 3: Advanced Features** (Estimated: 1 week)
- [ ] Configurable incremental loading intervals
- [ ] Error recovery and retry logic
- [ ] Performance monitoring and metrics
- [ ] Health checks for loading status

### **Benefits**
- **🚀 Fast Initial Load**: Bulk load gets tables operational quickly
- **🔄 Real-time Updates**: Incremental load keeps data fresh
- **📊 Consistent Behavior**: Same pattern across all source types
- **⚡ Performance**: Minimal overhead for incremental updates
- **🛡️ Resilience**: Bulk load works even if incremental fails

---

## 🚨 **CRITICAL GAP: Stream-Table Load Coordination**

**Identified**: September 27, 2024
**Priority**: **LOW** - Core features complete, only optimization remaining
**Status**: 🟢 **PHASES 1-3 COMPLETE** - Core synchronization, graceful degradation, and progress monitoring all implemented
**Risk Level**: 🟢 **MINIMAL** - All critical gaps addressed, only optimization features remain

### **Problem Statement**

Streams can start processing before reference tables are fully loaded, causing:
- **Missing enrichment data** in stream-table joins
- **Inconsistent results** during startup phase
- **Silent failures** with no warning about incomplete tables
- **Production incidents** when tables are slow to load

### **Current State Analysis**

#### **What EXISTS** ✅
- `TableRegistry` with basic table management
- Background job tracking via `JoinHandle`
- Table status tracking (`Populating`, `BackgroundJobFinished`)
- Health monitoring for job completion checks
- **Progress monitoring system** - Complete real-time tracking ✅
- **Health dashboard** - Full REST API with Prometheus metrics ✅
- **Progress streaming** - Broadcast channels for real-time updates ✅
- **Circuit breaker pattern** - Production-ready with comprehensive tests ✅

#### **What's REMAINING** ⚠️
- ✅ ~~Synchronization barriers~~ - `wait_for_table_ready()` method **IMPLEMENTED**
- ✅ ~~Startup coordination~~ - Streams wait for table readiness **IMPLEMENTED**
- ✅ ~~Graceful degradation~~ - 5 fallback strategies **IMPLEMENTED**
- ✅ ~~Retry logic~~ - Exponential backoff retry **IMPLEMENTED**
- ✅ ~~Progress monitoring~~ - Complete implementation **COMPLETED**
- ✅ ~~Health dashboard~~ - Full REST API **COMPLETED**
- ❌ **Dependency graph resolution** - Table dependency tracking not implemented
- ❌ **Parallel loading optimization** - Multi-table parallel loading not implemented
- ✅ ~~Async Integration~~ - **VERIFIED WORKING** (225/225 tests passing, no compilation errors)

### **Production Impact**

```
BEFORE (BROKEN):
Stream Start ──────┐
                   ├──> JOIN (Missing Data!) ──> ❌ Incorrect Results
Table Loading ─────┘

NOW (IMPLEMENTED):
Table Loading ──> Ready Signal ──┐
                                  ├──> JOIN ──> ✅ Complete Results
Stream Start ───> Wait for Ready ┘
                      ↓
                Graceful Degradation
                (UseDefaults/Retry/Skip)
```

### **Implementation Plan**

#### **✅ Phase 1: Core Synchronization - COMPLETED September 27, 2024**
**Timeline**: October 1-7, 2024 → **COMPLETED EARLY**
**Goal**: Make table coordination the DEFAULT behavior → **✅ ACHIEVED**

```rust
// 1. Add synchronization as CORE functionality
impl TableRegistry {
    pub async fn wait_for_table_ready(
        &self,
        table_name: &str,
        timeout: Duration
    ) -> Result<TableReadyStatus, SqlError> {
        // Poll status with exponential backoff
        // Return Ready/Timeout/Error
    }
}

// 2. ENFORCE coordination in ALL stream starts
impl StreamJobServer {
    async fn start_job(&self, query: &StreamingQuery) -> Result<(), SqlError> {
        // MANDATORY: Extract and wait for ALL table dependencies
        let required_tables = extract_table_dependencies(query);

        // Block until ALL tables ready (no bypass option)
        for table in required_tables {
            self.table_registry.wait_for_table_ready(
                &table,
                Duration::from_secs(60)
            ).await?;
        }

        // Only NOW start stream processing
        self.execute_streaming_query(query).await
    }
}
```

**✅ DELIVERABLES COMPLETED**:
- ✅ `wait_for_table_ready()` method with exponential backoff
- ✅ `wait_for_tables_ready()` for multiple dependencies
- ✅ MANDATORY coordination in StreamJobServer.deploy_job()
- ✅ Clear timeout errors (60s default)
- ✅ Comprehensive test suite (8 test scenarios)
- ✅ No bypass options - correct behavior enforced
- ✅ Production-ready error messages and logging

**🎯 PRODUCTION IMPACT**: Streams now WAIT for tables, preventing missing enrichment data

#### **🔄 Phase 2: Graceful Degradation - IN PROGRESS September 27, 2024**
**Timeline**: October 8-14, 2024 → **STARTED EARLY**
**Goal**: Handle partial data scenarios gracefully → **⚡ CORE IMPLEMENTATION COMPLETE**

```rust
// 1. Configurable fallback behavior
pub enum TableMissingDataStrategy {
    UseDefaults(HashMap<String, FieldValue>),
    SkipRecord,
    EmitWithNulls,
    WaitAndRetry { max_retries: u32, delay: Duration },
    FailFast,
}

// 2. Implement in join processor
impl StreamTableJoinProcessor {
    fn handle_missing_table_data(
        &self,
        strategy: &TableMissingDataStrategy,
        stream_record: &StreamRecord
    ) -> Result<Option<StreamRecord>, SqlError> {
        match strategy {
            UseDefaults(defaults) => Ok(Some(enrich_with_defaults(stream_record, defaults))),
            SkipRecord => Ok(None),
            EmitWithNulls => Ok(Some(add_null_fields(stream_record))),
            WaitAndRetry { .. } => self.retry_with_backoff(stream_record),
            FailFast => Err(SqlError::TableNotReady),
        }
    }
}
```

**✅ DELIVERABLES - CORE IMPLEMENTATION COMPLETE**:
- ✅ **Graceful Degradation Framework**: Complete `graceful_degradation.rs` module
- ✅ **5 Fallback Strategies**: UseDefaults, SkipRecord, EmitWithNulls, WaitAndRetry, FailFast
- ✅ **StreamRecord Optimization**: Renamed to SimpleStreamRecord (48% memory savings)
- ✅ **StreamTableJoinProcessor Integration**: Graceful degradation in all join methods
- ✅ **Batch Processing Support**: Degradation for both individual and bulk operations
- ✅ **Async Compilation**: **VERIFIED WORKING** - All tests passing (no blocking issues)

**🎯 PRODUCTION IMPACT**: Missing table data now handled gracefully with configurable strategies

#### **✅ Phase 3: Progress Monitoring - COMPLETED October 2024**
**Timeline**: October 15-21, 2024 → **COMPLETED EARLY**
**Goal**: Real-time visibility into table loading → **✅ ACHIEVED**

**Implementation Files**:
- `src/velostream/server/progress_monitoring.rs` (564 lines) - Complete progress tracking system
- `src/velostream/server/health_dashboard.rs` (563 lines) - Full REST API endpoints
- `src/velostream/server/progress_streaming.rs` - Real-time streaming support
- `tests/unit/server/progress_monitoring_integration_test.rs` - Comprehensive test coverage

**Implemented Features**:
```rust
// ✅ Progress tracking with atomic counters
pub struct TableProgressTracker {
    records_loaded: AtomicUsize,
    bytes_processed: AtomicU64,
    loading_rate: f64,      // records/sec
    bytes_per_second: f64,  // bytes/sec
    estimated_completion: Option<DateTime<Utc>>,
    progress_percentage: Option<f64>,
}

// ✅ Health dashboard REST API
GET /health/tables          // Overall health status
GET /health/table/{name}    // Individual table health
GET /health/progress        // Loading progress for all tables
GET /health/metrics         // Comprehensive metrics + Prometheus format
GET /health/connections     // Streaming connection stats
POST /health/table/{name}/wait  // Wait for table with progress

// ✅ Real-time streaming
pub enum ProgressEvent {
    InitialSnapshot, TableUpdate, SummaryUpdate,
    TableCompleted, TableFailed, KeepAlive
}
```

**✅ Deliverables - ALL COMPLETED**:
- ✅ Real-time progress tracking with atomic operations
- ✅ Loading rate calculation (records/sec + bytes/sec)
- ✅ ETA estimation based on current rates
- ✅ Health dashboard integration with REST API
- ✅ Progress streaming with broadcast channels
- ✅ Prometheus metrics export
- ✅ Comprehensive test coverage

#### **🟡 Phase 4: Advanced Coordination - PARTIALLY COMPLETE**
**Timeline**: October 22-28, 2024
**Status**: 🟡 **1 of 3 features complete, 2 remaining**

**✅ COMPLETED: Circuit Breaker Pattern**
- **File**: `src/velostream/sql/execution/circuit_breaker.rs` (674 lines)
- **Features**: Full circuit breaker states (Closed, Open, HalfOpen), configurable thresholds, automatic recovery, failure rate calculation
- **Test Coverage**: 13 comprehensive tests passing

**❌ REMAINING: Dependency Graph Resolution**
```rust
// TODO: Implement table dependency tracking
pub struct TableDependencyGraph {
    nodes: HashMap<String, TableNode>,
    edges: Vec<(String, String)>, // dependencies
}

impl TableDependencyGraph {
    pub fn topological_load_order(&self) -> Result<Vec<String>, CycleError> {
        // Determine optimal table loading order
    }

    pub fn detect_cycles(&self) -> Result<(), CycleError> {
        // Detect circular dependencies
    }
}
```

**❌ REMAINING: Parallel Loading with Dependencies**
```rust
// TODO: Implement parallel loading coordinator
pub async fn load_tables_with_dependencies(
    tables: Vec<TableDefinition>,
    max_parallel: usize
) -> Result<(), SqlError> {
    let graph = build_dependency_graph(&tables);
    let load_order = graph.topological_load_order()?;

    // Load in waves respecting dependencies
    for wave in load_order.chunks(max_parallel) {
        join_all(wave.iter().map(|t| load_table(t))).await?;
    }
}
```

**Deliverables Status**:
- ❌ Dependency graph resolution (NOT STARTED) - **[Implementation Plan Available](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md)**
- ❌ Parallel loading optimization (NOT STARTED) - **[Implementation Plan Available](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md)**
- ✅ Circuit breaker pattern (COMPLETE)
- ✅ Advanced retry strategies (via graceful degradation - COMPLETE)

**📋 Implementation Plan**: See [fr-025-phase-4-parallel-loading-implementation-plan.md](../docs/feature/fr-025-phase-4-parallel-loading-implementation-plan.md) for detailed 2-week implementation guide with code examples, test cases, and integration points.

### **Success Metrics**

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| **Startup Coordination** | 0% | 100% | ALL streams wait for tables |
| **Missing Data Incidents** | Unknown | 0 | Zero incomplete enrichment |
| **Average Wait Time** | N/A | < 30s | Time waiting for tables |
| **Retry Success Rate** | 0% | > 95% | Successful retries after initial failure |
| **Visibility** | None | 100% | Full progress monitoring |

### **Testing Strategy**

1. **Unit Tests**: Synchronization primitives, timeout handling
2. **Integration Tests**: Full startup coordination flow
3. **Chaos Tests**: Slow loading, failures, network issues
4. **Load Tests**: 50K+ record tables, multiple dependencies
5. **Production Simulation**: Real data patterns and volumes

### **Risk Mitigation**

- **Timeout Defaults**: Conservative 60s default, configurable per-table
- **Monitoring**: Comprehensive metrics from day 1
- **Fail-Safe Defaults**: Start with strict coordination, relax as needed
- **Testing Coverage**: Extensive testing before marking feature complete

---

## 🔄 **NEXT DEVELOPMENT PRIORITIES**

### ✅ **PHASE 3: Stream-Table Joins - COMPLETED September 27, 2024**

**Status**: ✅ **COMPLETED** - Moved to [todo-complete.md](todo-complete.md)
**Achievement**: 840x performance improvement with advanced optimization suite
**Production Status**: Enterprise-ready with 98K+ records/sec throughput

---

### ✅ **PHASE 4: Enhanced CREATE TABLE Features - COMPLETED September 28, 2024**

**Status**: ✅ **COMPLETED**
**Timeline**: Completed in 1 day
**Achievement**: Full AUTO_OFFSET support and comprehensive documentation

#### **Feature 1: Wildcard Field Discovery**
**Status**: ✅ **VERIFIED SUPPORTED**
- Parser fully supports `SelectField::Wildcard`
- `CREATE TABLE AS SELECT *` works in production
- Documentation created at `docs/sql/create-table-wildcard.md`

#### **Feature 2: AUTO_OFFSET Configuration for TABLEs**
**Status**: ✅ **IMPLEMENTED**
- Added `new_with_properties()` method to Table
- Updated CTAS processor to pass properties
- Full test coverage added
- Backward compatible (defaults to `earliest`)

**Completed Implementation**:
```sql
-- Use latest offset (now working!)
CREATE TABLE real_time_data AS
SELECT * FROM kafka_stream
WITH ("auto.offset.reset" = "latest");

-- Use earliest offset (default)
CREATE TABLE historical_data AS
SELECT * FROM kafka_stream
WITH ("auto.offset.reset" = "earliest");
```

---

### ✅ **PHASE 5: Missing Source Handling - COMPLETED September 28, 2024**

**Status**: ✅ **CORE FUNCTIONALITY COMPLETED**
**Timeline**: Completed in 1 day
**Achievement**: Robust Kafka retry logic with configurable timeouts

#### **✅ Completed Features**

##### **✅ Task 1: Kafka Topic Wait/Retry**
- ✅ Added `topic.wait.timeout` property support
- ✅ Added `topic.retry.interval` configuration
- ✅ Implemented retry loop with logging
- ✅ Backward compatible (no wait by default)

```sql
-- NOW WORKING:
CREATE TABLE events AS
SELECT * FROM kafka_topic
WITH (
    "topic.wait.timeout" = "60s",
    "topic.retry.interval" = "5s"
);
```

##### **✅ Task 2: Utility Functions**
- ✅ Duration parsing utility (`parse_duration`)
- ✅ Topic missing error detection (`is_topic_missing_error`)
- ✅ Enhanced error message formatting
- ✅ Comprehensive test coverage

##### **✅ Task 3: Integration**
- ✅ Updated `Table::new_with_properties` with retry logic
- ✅ All CTAS operations now support retry
- ✅ Full test suite added
- ✅ Documentation updated

#### **✅ Fully Completed**
- ✅ **File Source Retry**: Complete implementation with comprehensive test suite ✅ **COMPLETED September 28, 2024**

#### **Success Metrics**
- [x] Zero manual intervention for transient missing Kafka topics
- [x] Zero manual intervention for transient missing file sources ✅ **NEW**
- [x] Clear error messages with solutions
- [x] Configurable retry behavior
- [x] Backward compatible (no retry by default)
- [x] Production-ready timeout handling for Kafka and file sources ✅ **EXPANDED**

**Key Benefits**:
- **No more immediate failures** for missing Kafka topics or file sources
- **Configurable wait times** up to any duration for both Kafka and file sources
- **Intelligent retry intervals** with comprehensive logging
- **100% backward compatible** - existing code unchanged
- **Pattern matching support** - wait for glob patterns like `*.json` to appear
- **File watching integration** - seamlessly works with existing file watching features

---

### 🟡 **PRIORITY 2: Advanced Window Functions**
**Timeline**: 4 weeks
**Dependencies**: ✅ Prerequisites met (Phase 2 complete)
**Status**: 🔄 **READY TO START**

### 🟡 **PRIORITY 3: Enhanced JOIN Operations**
**Timeline**: 8 weeks
**Dependencies**: Stream-Table joins completion
**Status**: ❌ **PENDING** (depends on Priority 1)

### 🟡 **PRIORITY 4: Comprehensive Aggregation Functions**
**Timeline**: 5 weeks
**Dependencies**: ✅ Prerequisites met (OptimizedTableImpl complete)
**Status**: 🔄 **READY TO START**

### 🟡 **PRIORITY 5: Advanced SQL Features**
**Timeline**: 12 weeks
**Dependencies**: Stream-Table joins completion
**Status**: ❌ **PENDING** (depends on Priority 1)

---

## 📊 **Overall Progress Summary**

| Phase | Status | Completion | Timeline | Dates |
|-------|--------|------------|----------|-------|
| **Phase 1**: SQL Subquery Foundation | ✅ **COMPLETED** | 100% | Weeks 1-3 | Aug 1-21, 2024 ✅ |
| **Phase 2**: OptimizedTableImpl & CTAS | ✅ **COMPLETED** | 100% | Weeks 4-8 | Aug 22 - Sep 26, 2024 ✅ |
| **Phase 3**: Stream-Table Joins | ✅ **COMPLETED** | 100% | Week 9 | Sep 27, 2024 ✅ |
| **Phase 4**: Advanced Streaming Features | 🔄 **READY TO START** | 0% | Weeks 10-17 | Sep 28 - Dec 21, 2024 |

### **Key Achievements**
- ✅ **OptimizedTableImpl**: 90% code reduction with 1.85M+ lookups/sec performance
- ✅ **Stream-Table Joins**: 40,404 trades/sec with real-time enrichment capability
- ✅ **Enhanced SQL Validator**: Intelligent JOIN performance analysis (Stream-Table vs Stream-Stream)
- ✅ **SQL Aggregation**: COUNT and SUM operations with proper type handling
- ✅ **Reserved Keywords**: STATUS, METRICS, PROPERTIES fixed for production use
- ✅ **Test Coverage**: 222 unit + 1513+ comprehensive + 56 doc tests all passing
- ✅ **Financial Precision**: ScaledInteger for exact arithmetic operations
- ✅ **Multi-Table Joins**: Complete pipeline (user profiles + market data + limits)
- ✅ **Production Ready**: Complete validation with enterprise benchmarks

### **Recent Milestone Achievement**
**🎯 Target**: Complete Phase 3 Stream-Table Joins by October 25, 2024 → **✅ COMPLETED September 27, 2024**
- **Progress**: 100% complete (3 weeks ahead of schedule!)
- **Achievement**: Real-time trade enrichment with KTable joins fully implemented
- **Foundation**: ✅ OptimizedTableImpl provides enterprise performance foundation
- **Results**: 40,404 trades/sec throughput with complete financial enrichment pipeline
- **Quality**: Enhanced SQL validation with intelligent JOIN performance warnings

### **Next Development Priorities**
**📅 Phase 4 (Sep 28 - Dec 21, 2024)**: Advanced Streaming Features (NOW READY TO START)
- Advanced Window Functions with complex aggregations
- Enhanced JOIN Operations across multiple streams
- Comprehensive Aggregation Functions
- Advanced SQL Features and optimization
- Production Deployment Readiness

**🚀 Accelerated Timeline**: Phase 3 completion 3 weeks early opens opportunity for expanded Phase 4 scope

---

*This document focuses on active development priorities. See [todo-consolidated.md](todo-consolidated.md) for comprehensive historical context and [todo-complete.md](todo-complete.md) for completed work archive.*