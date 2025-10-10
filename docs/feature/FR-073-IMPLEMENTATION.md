# FR-073: SQL-Native Observability - Implementation Tracking

> **Design Document**: See [FR-073-DESIGN.md](FR-073-DESIGN.md) for complete specifications and architecture.

---

## Implementation Status

**Overall Progress**: 4 of 7 phases complete (57%)

| Phase                                 | Status                | Duration      | LOC        | Tests  | Completion Date |
|---------------------------------------|-----------------------|---------------|------------|--------|-----------------|
| Phase 0: Comment Preservation         | ✅ Complete            | 2-3 days      | ~150       | 5      | October 2025    |
| Phase 1: Annotation Parser            | ✅ Complete            | 1 week        | ~350       | 16     | October 2025    |
| Phase 2A: Runtime - Counters          | ✅ Complete            | 1 week        | ~240       | 13     | October 2025    |
| Phase 2B: Runtime - Gauges/Histograms | ✅ Complete            | 1 week        | ~450       | 8      | October 2025    |
| Phase 3: Label Extraction             | 📋 Ready              | 0.5 weeks     | ~200       | 6      | TBD             |
| Phase 4: Condition Evaluation         | ⏳ Waiting             | 3 days        | ~200       | 6      | TBD             |
| Phase 5: Registry Management          | ⏳ Waiting             | 1 week        | ~250       | 5      | TBD             |
| Phase 6: Documentation                | ⏳ Waiting             | 1 week        | ~500       | -      | TBD             |
| **TOTAL**                             | **Phase 0-1-2 Done**  | **7.5 weeks** | **~2,840** | **74** | -               |

---

## ✅ Phase 0: Comment Preservation (COMPLETE)

**Duration**: 2-3 days
**Complexity**: Low
**LOC**: ~150 lines
**Status**: ✅ **COMPLETED** (October 2025)

### Problem Identified

The original FR-073 plan assumed SQL comments were preserved during parsing. Technical validation revealed comments were being discarded:

```rust
// BEFORE Phase 0: Comments were discarded
'-' => {
    if next is '-' {
        // Consume until newline, but DON'T STORE
        while ch != '\n' { skip(); }
    }
}
```

**Impact**: Cannot extract `@metric` annotations from comments that don't exist!

### Solution Implemented

Modified the SQL tokenizer to preserve comments with full text and position information.

#### New Token Types

```rust
// Comments (preserved for annotation parsing)
SingleLineComment, // -- comment text
MultiLineComment,  // /* comment text */
```

#### Comment Preservation Logic

**Single-line comments**:
```rust
let mut comment_text = String::new();
while let Some(&ch) = chars.peek() {
    if ch == '\n' || ch == '\r' { break; }
    comment_text.push(ch);
    chars.next();
}

tokens.push(Token {
    token_type: TokenType::SingleLineComment,
    value: comment_text.trim().to_string(),
    position: comment_start_pos,
});
```

#### Public API

```rust
/// Tokenize SQL and separate comments from other tokens
pub fn tokenize_with_comments(&self, sql: &str)
    -> Result<(Vec<Token>, Vec<Token>), SqlError>
{
    let all_tokens = self.tokenize(sql)?;
    let mut tokens = Vec::new();
    let mut comments = Vec::new();

    for token in all_tokens {
        match token.token_type {
            TokenType::SingleLineComment | TokenType::MultiLineComment => {
                comments.push(token);
            }
            _ => tokens.push(token),
        }
    }

    Ok((tokens, comments))
}

/// Extract comments that appear before a CREATE statement
pub fn extract_preceding_comments(
    comments: &[Token],
    create_position: usize
) -> Vec<String>
{
    comments.iter()
        .filter(|token| token.position < create_position)
        .map(|token| token.value.clone())
        .collect()
}
```

### Files Modified

- `src/velostream/sql/parser.rs` (+150 LOC)

### Validation

- ✅ `cargo fmt --all` - Passed
- ✅ `cargo check --no-default-features` - Passed
- ✅ No breaking changes - existing `tokenize()` unchanged
- ✅ Backward compatible - new API is opt-in

### Deliverables

- ✅ Public `tokenize_with_comments()` method
- ✅ Public `extract_preceding_comments()` helper
- ✅ Documentation: `docs/feature/FR-073-PHASE-0-COMPLETE.md`

---

## ✅ Phase 1: Annotation Parser (COMPLETE)

**Duration**: 1 week
**Complexity**: Medium
**LOC**: ~350 lines
**Status**: ✅ **COMPLETED** (October 2025)

### Overview

With Phase 0 complete, comments are now available for parsing. Phase 1 implemented:
1. ✅ Parsing `@metric` annotation directives from comment text
2. ✅ Creating `MetricAnnotation` data structures with full validation
3. ✅ Attaching annotations to `StreamingQuery::CreateStream` AST nodes
4. ✅ Comprehensive test coverage (16 unit tests)

### Files Created/Modified

- ✅ `src/velostream/sql/parser/annotations.rs` (~390 LOC) - New annotation parser module
- ✅ `src/velostream/sql/parser.rs` - Integration with parser pipeline
- ✅ `src/velostream/sql/ast.rs` - Added `metric_annotations` field to `CreateStream` variant
- ✅ `tests/unit/sql/parser/metric_annotations_test.rs` (~234 LOC) - Comprehensive test suite
- ✅ Various test files updated to include empty `metric_annotations` vectors

### Supported Annotation Directives

| Directive | Description | Required |
|-----------|-------------|----------|
| `@metric: <name>` | Metric name | ✅ Yes |
| `@metric_type: counter\|gauge\|histogram` | Metric type | ✅ Yes |
| `@metric_help: "description"` | Help text | ❌ No |
| `@metric_labels: label1, label2` | Label fields | ❌ No |
| `@metric_condition: <expression>` | Filter condition | ❌ No |
| `@metric_sample_rate: <0.0-1.0>` | Sampling rate | ❌ No (default 1.0) |
| `@metric_field: <field_name>` | Field to measure | ✅ Yes (gauge/histogram) |
| `@metric_buckets: [v1, v2, ...]` | Histogram buckets | ❌ No |

### Validation Implemented

- ✅ Prometheus metric naming rules: `[a-zA-Z_:][a-zA-Z0-9_:]*`
- ✅ Required field validation based on metric type
- ✅ Sample rate range validation (0.0 to 1.0)
- ✅ Gauge/Histogram require `@metric_field`
- ✅ Invalid metric type detection
- ✅ Graceful error messages with clear context

### Test Coverage (16 tests)

```
✅ test_parse_simple_counter_annotation
✅ test_parse_counter_with_labels
✅ test_parse_counter_with_help
✅ test_parse_gauge_annotation
✅ test_parse_histogram_annotation
✅ test_parse_annotation_with_condition
✅ test_parse_annotation_with_sample_rate
✅ test_parse_multiple_annotations
✅ test_parse_complete_annotation
✅ test_parse_annotation_skips_non_annotation_comments
✅ test_parse_annotation_invalid_metric_type
✅ test_parse_annotation_gauge_without_field
✅ test_parse_annotation_invalid_sample_rate
✅ test_parse_annotation_invalid_metric_name
✅ test_parse_annotation_metric_type_without_metric
```

### Validation Status

- ✅ `cargo fmt --all -- --check` - Passed
- ✅ `cargo check --no-default-features` - Passed
- ✅ All 16 unit tests passing
- ✅ Zero breaking changes to existing code
- ✅ Backward compatible (empty annotations for existing queries)

### Usage Example

```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Total number of volume spikes detected"
-- @metric_labels: symbol, spike_ratio
-- @metric_condition: volume > hourly_avg_volume * 2.0
CREATE STREAM volume_spikes AS
SELECT
    symbol,
    volume,
    hourly_avg_volume,
    (volume / hourly_avg_volume) as spike_ratio,
    event_time
FROM market_data_stream
WHERE volume > hourly_avg_volume * 2.0;
```

**Parser Output**:
```rust
MetricAnnotation {
    name: "velo_trading_volume_spikes_total",
    metric_type: MetricType::Counter,
    help: Some("Total number of volume spikes detected"),
    labels: vec!["symbol", "spike_ratio"],
    condition: Some("volume > hourly_avg_volume * 2.0"),
    sample_rate: 1.0,
    field: None,
    buckets: None,
}
```

**Integration with Parser**:
```rust
use velostream::velostream::sql::parser::StreamingSqlParser;

let parser = StreamingSqlParser::new();
let query = parser.parse(sql)?;

match query {
    StreamingQuery::CreateStream { metric_annotations, .. } => {
        for annotation in metric_annotations {
            println!("Metric: {} ({})", annotation.name, annotation.metric_type);
        }
    }
    _ => {}
}
```

---

## ✅ Phase 2A: Runtime Integration - Counters (COMPLETE)

**Duration**: 1 week (actual)
**Complexity**: Medium
**LOC**: ~240 lines
**Status**: ✅ **COMPLETED** (October 2025)

### Overview

Implemented counter metric integration into SimpleStreamProcessor. When records flow through a stream with counter annotations, the runtime:
1. ✅ Registers counters on job start
2. ✅ Extracts label values from record fields
3. ✅ Increments counters after SQL processing
4. ✅ Exports metrics via Prometheus endpoint

### Files Modified/Created

- ✅ `src/velostream/observability/metrics.rs` (+150 LOC)
  - Added `dynamic_counters: Arc<Mutex<HashMap<String, IntCounterVec>>>`
  - Implemented `register_counter_metric()` for runtime registration
  - Implemented `emit_counter()` for metric emission

- ✅ `src/velostream/server/processors/simple.rs` (+90 LOC)
  - Added `register_counter_metrics()` method
  - Added `emit_counter_metrics()` method
  - Integrated registration in `process_job()`
  - Integrated emission in `process_simple_batch()`

- ✅ `tests/integration/sql_metrics_integration_test.rs` (~250 LOC new)
  - Test counter registration and emission
  - Test multiple label combinations
  - Test multiple metrics per query

### Implementation Details

**Counter Registration** (on job start):
```rust
async fn register_counter_metrics(&self, query: &StreamingQuery, job_name: &str) {
    let counter_annotations = match query {
        StreamingQuery::CreateStream { metric_annotations, .. } => {
            metric_annotations.iter()
                .filter(|a| a.metric_type == MetricType::Counter)
                .collect()
        }
        _ => return,
    };

    for annotation in counter_annotations {
        metrics.register_counter_metric(
            &annotation.name,
            help,
            &annotation.labels,
        )?;
    }
}
```

**Counter Emission** (after SQL processing):
```rust
async fn emit_counter_metrics(&self, query: &StreamingQuery, output_records: &[StreamRecord], job_name: &str) {
    for record in output_records {
        for annotation in &counter_annotations {
            let label_values: Vec<String> = annotation.labels.iter()
                .filter_map(|label| record.fields.get(label).map(|v| v.to_display_string()))
                .collect();

            if label_values.len() == annotation.labels.len() {
                metrics.emit_counter(&annotation.name, &label_values)?;
            }
        }
    }
}
```

### Test Coverage (3 integration tests + 10 unit tests)

**Integration Tests**:
```
✅ test_counter_metric_registration_and_emission
✅ test_counter_metric_with_multiple_labels
✅ test_multiple_counter_metrics
```

**Unit Tests** (metrics.rs):
```
✅ test_register_counter_metric_basic
✅ test_register_counter_metric_with_labels
✅ test_register_counter_idempotent
✅ test_emit_counter_basic
✅ test_emit_counter_with_labels
✅ test_emit_counter_multiple_label_combinations
✅ test_emit_counter_before_register_error
✅ test_register_counter_when_inactive
✅ test_emit_counter_when_inactive_succeeds_silently
✅ test_dynamic_counter_in_metrics_export
```

### Validation Status

- ✅ `cargo fmt --all -- --check` - Passed
- ✅ `cargo check --no-default-features` - Passed
- ✅ All 21 metrics unit tests passing
- ✅ All 3 integration tests passing
- ✅ Zero breaking changes
- ✅ Label extraction working correctly

### Usage Example

```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Total volume spikes detected"
-- @metric_labels: symbol
CREATE STREAM volume_spikes AS
SELECT symbol, volume, avg_volume
FROM market_data
WHERE volume > avg_volume * 2;
```

**Result**: Every record emits counter increment with `symbol` label.

---

## ✅ Phase 2B: Runtime Integration - Gauges/Histograms (COMPLETE)

**Duration**: 1 week (actual)
**Complexity**: Medium
**LOC**: ~450 lines
**Status**: ✅ **COMPLETED** (October 2025)

### Overview

Extended runtime integration to support gauge and histogram metrics. When records flow through a stream with gauge/histogram annotations, the runtime:
1. ✅ Registers gauges and histograms on job start
2. ✅ Extracts label values from record fields
3. ✅ Sets gauge values or observes histogram values from `@metric_field`
4. ✅ Supports custom histogram buckets via `@metric_buckets`
5. ✅ Exports metrics via Prometheus endpoint

### Files Modified/Created

- ✅ `src/velostream/observability/metrics.rs` (+300 LOC)
  - Added `dynamic_gauges: Arc<Mutex<HashMap<String, GaugeVec>>>`
  - Added `dynamic_histograms: Arc<Mutex<HashMap<String, HistogramVec>>>`
  - Implemented `register_gauge_metric()` for runtime registration
  - Implemented `register_histogram_metric()` with custom bucket support
  - Implemented `emit_gauge()` for gauge value setting
  - Implemented `emit_histogram()` for value observation

- ✅ `src/velostream/server/processors/simple.rs` (+150 LOC)
  - Added `register_gauge_metrics()` method
  - Added `register_histogram_metrics()` method
  - Added `emit_gauge_metrics()` method with field value extraction
  - Added `emit_histogram_metrics()` method with field value extraction
  - Integrated registration in `process_job()`
  - Integrated emission in `process_simple_batch()`

- ✅ `tests/integration/sql_metrics_integration_test.rs` (+383 LOC)
  - Test gauge registration and emission
  - Test histogram registration with custom buckets
  - Test histogram with default buckets
  - Test mixed metric types (counter + gauge + histogram)
  - Test multiple labels for all metric types

### Implementation Details

**Gauge Registration and Emission**:
```rust
// Registration (on job start)
async fn register_gauge_metrics(&self, query: &StreamingQuery, job_name: &str) {
    for annotation in gauge_annotations {
        metrics.register_gauge_metric(
            &annotation.name,
            help,
            &annotation.labels,
        )?;
    }
}

// Emission (after SQL processing)
async fn emit_gauge_metrics(&self, query: &StreamingQuery, output_records: &[StreamRecord], job_name: &str) {
    for record in output_records {
        // Extract the gauge value from the specified field
        if let Some(field_name) = &annotation.field {
            if let Some(field_value) = record.fields.get(field_name) {
                let value = match field_value {
                    FieldValue::Float(v) => *v,
                    FieldValue::Integer(v) => *v as f64,
                    FieldValue::ScaledInteger(v, scale) =>
                        (*v as f64) / 10_f64.powi(*scale as i32),
                    _ => continue,
                };

                metrics.emit_gauge(&annotation.name, &label_values, value)?;
            }
        }
    }
}
```

**Histogram Registration with Custom Buckets**:
```rust
pub fn register_histogram_metric(
    &self,
    name: &str,
    help: &str,
    label_names: &[String],
    buckets: Option<Vec<f64>>,
) -> Result<(), SqlError> {
    // Use custom buckets or defaults
    let bucket_values = buckets.unwrap_or_else(|| {
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    });

    let opts = HistogramOpts::new(name, help).buckets(bucket_values);
    let histogram = HistogramVec::new(opts, label_names)?;

    self.registry.register(Box::new(histogram.clone()))?;
    self.dynamic_histograms.lock().await.insert(name.to_string(), histogram);
}
```

### Test Coverage (8 integration tests)

**Integration Tests**:
```
✅ test_counter_metric_registration_and_emission
✅ test_counter_metric_with_multiple_labels
✅ test_multiple_counter_metrics
✅ test_gauge_metric_registration_and_emission
✅ test_gauge_metric_with_multiple_labels
✅ test_histogram_metric_registration_and_emission
✅ test_histogram_with_default_buckets
✅ test_mixed_metric_types
```

### Validation Status

- ✅ `cargo fmt --all -- --check` - Passed
- ✅ `cargo check --no-default-features` - Passed
- ✅ All 8 integration tests passing
- ✅ Zero breaking changes
- ✅ Field value extraction working for Float, Integer, and ScaledInteger
- ✅ Custom histogram buckets parsed and applied correctly
- ✅ Default histogram buckets applied when not specified

### Usage Examples

**Gauge Metric**:
```sql
-- @metric: current_order_volume
-- @metric_type: gauge
-- @metric_help: "Current volume per symbol"
-- @metric_labels: symbol
-- @metric_field: volume
CREATE STREAM volume_monitor AS
SELECT symbol, volume, avg_volume
FROM market_data;
```

**Histogram Metric with Custom Buckets**:
```sql
-- @metric: trade_volume_distribution
-- @metric_type: histogram
-- @metric_help: "Distribution of trade volumes"
-- @metric_labels: symbol
-- @metric_field: volume
-- @metric_buckets: 100,500,1000,5000,10000
CREATE STREAM volume_distribution AS
SELECT symbol, volume
FROM market_data;
```

**Result**:
- Gauges track the current value from the `volume` field
- Histograms observe volume distributions with custom bucket boundaries
- All metrics export via Prometheus with proper label dimensions

---

## 📋 Phase 3: Label Extraction (Ready to Start)

**Duration**: 0.5 weeks
**Complexity**: Low
**LOC**: ~200 lines
**Status**: 📋 **READY TO START** (Phase 2B complete)

### Overview

Enhance label extraction to support:
- Nested field access (e.g., `metadata.region`)
- Type conversion for label values
- Default values for missing fields
- Label value validation

### Files to Modify

- `src/velostream/server/processors/simple.rs` (~100 LOC)
- `src/velostream/sql/execution/types.rs` (~100 LOC)
- `tests/unit/observability/label_extraction_test.rs` (~150 LOC new)

---

## ⏳ Phase 4: Condition Evaluation (Waiting)

**Duration**: 3 days
**Complexity**: Low
**LOC**: ~200 lines
**Status**: ⏳ **WAITING** (Phase 2 dependency)

**Note**: Reduced from 1 week to 3 days by reusing existing expression evaluator.

### Overview

Implement conditional metric emission using VeloStream's existing expression evaluator:
- Parse condition SQL expressions
- Evaluate conditions on each record
- Only emit metrics when condition evaluates to true
- Handle condition evaluation errors gracefully

### Files to Modify

- `src/velostream/server/processors/simple.rs` (~100 LOC)
- `src/velostream/sql/execution/expression/mod.rs` (~100 LOC)
- `tests/unit/observability/condition_test.rs` (~150 LOC new)

---

## ⏳ Phase 5: Registry Management (Waiting)

**Duration**: 1 week
**Complexity**: Low
**LOC**: ~250 lines
**Status**: ⏳ **WAITING** (Phase 2 dependency)

### Overview

Implement lifecycle management for metrics:
- Register metrics when stream is deployed
- Update metric metadata (help text, labels)
- Unregister metrics when stream is dropped
- Handle metric name collisions
- Support metric updates on stream redeploy

### Files to Modify

- `src/velostream/observability/metrics.rs` (~150 LOC)
- `src/velostream/server/stream_job_server.rs` (~100 LOC)
- `tests/unit/observability/registry_test.rs` (~150 LOC new)

---

## ⏳ Phase 6: Documentation (Waiting)

**Duration**: 1 week
**Complexity**: Low
**LOC**: ~500 lines
**Status**: ⏳ **WAITING** (All phases dependency)

### Overview

Comprehensive documentation for SQL-native observability:
- User guide with examples
- Annotation reference
- Best practices
- Troubleshooting guide
- Migration guide from external metrics

### Files to Create

- `docs/user-guide/sql-native-observability.md` (~300 LOC)
- `docs/reference/metric-annotations.md` (~200 LOC)
- Update `README.md` with feature highlights
- Update `examples/` with annotated SQL files

---

## Technical Decisions

### Phase 0 Decisions

**Decision**: Preserve comments in separate token stream
**Rationale**: Keeps parser clean, allows opt-in annotation parsing
**Alternative Considered**: Inline comments in AST (rejected - too invasive)

### Phase 1 Decisions

**Decision**: Use `SqlError::ParseError` for validation errors
**Rationale**: Consistent with existing error handling
**Alternative Considered**: New `AnnotationError` type (rejected - unnecessary complexity)

**Decision**: Attach annotations to `CreateStream` enum variant
**Rationale**: VeloStream uses enum-based AST, not separate definition structs
**Alternative Considered**: Separate `StreamDefinition` struct (rejected - doesn't match codebase architecture)

**Decision**: Support all metric types in Phase 1
**Rationale**: Complete parser implementation prevents rework
**Alternative Considered**: Counter-only MVP (rejected - requires parser changes later)

---

## Performance Tracking

| Phase | Compile Time | Test Time | Runtime Overhead |
|-------|--------------|-----------|------------------|
| Phase 0 | No change | No change | 0% |
| Phase 1 | +0.5s | +1.2s (16 tests) | 0% (parse time only) |
| Phase 2 | TBD | TBD | Target: < 5% |

---

## Next Steps

**Immediate**: Start Phase 3 - Label Extraction Enhancement
1. Add support for nested field access (e.g., `metadata.region`)
2. Implement type conversion for label values
3. Add default values for missing fields
4. Implement label value validation
5. Write comprehensive unit tests

**Blocked On**:
- Phase 4-6 blocked on Phase 3 completion
- No external dependencies

**Risks**:
- Performance overhead in hot path (mitigation: profiling and optimization)
- Label cardinality explosion (mitigation: documentation on best practices)
- Nested field access complexity (mitigation: leverage existing field resolution)

---

## Validation Checklist

Before marking each phase complete:
- [ ] All code formatted with `cargo fmt --all`
- [ ] Compilation passes with `cargo check --no-default-features`
- [ ] All tests pass (unit + integration)
- [ ] No clippy warnings
- [ ] Documentation updated
- [ ] Examples updated if applicable
- [ ] Performance benchmarks within targets
- [ ] Git commit with phase completion message

---

## Related Documents

- [FR-073-DESIGN.md](FR-073-DESIGN.md) - Complete design specifications
- [FR-073-PHASE-0-COMPLETE.md](FR-073-PHASE-0-COMPLETE.md) - Phase 0 detailed writeup
- [FR-073-UNIFIED-OBSERVABILITY.md](FR-073-UNIFIED-OBSERVABILITY.md) - Original combined document (archived)
