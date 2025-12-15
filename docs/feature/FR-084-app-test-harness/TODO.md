# FR-084: SQL Application Test Harness - Work Tracking

## Summary

| Phase | Description | LoE | Status | Dependencies |
|-------|-------------|-----|--------|--------------|
| Phase 1 | Foundation | 3-4 days | ✅ COMPLETE | None |
| Phase 2 | Execution Engine | 3-4 days | ✅ COMPLETE | Phase 1 |
| Phase 3 | Assertions | 2-3 days | ✅ COMPLETE | Phase 2 |
| Phase 4 | Reporting | 1-2 days | ✅ COMPLETE | Phase 3 |
| Phase 5 | Advanced Features | 3-4 days | ✅ COMPLETE | Phase 4 |
| Phase 6 | AI-Powered Features | 2-3 days | ✅ COMPLETE | Phase 5 |
| Phase 7 | StreamJobServer Integration | 1 day | ✅ COMPLETE | Phase 2 |
| Phase 8 | Performance Assertions | 0.5 day | ✅ COMPLETE | Phase 3 |
| Phase 9 | Foreign Key Reference Data | 0.5 day | ✅ COMPLETE | Phase 1 |
| Phase 10 | In-Memory Schema Registry | 0.5 day | ✅ COMPLETE | Phase 1 |
| Phase 11 | Advanced Testing (DLQ, Fault Injection, Table State) | 1 day | ✅ COMPLETE | Phase 3 |
| Phase 12 | File-Based Testing Support | 0.5 day | ✅ COMPLETE | Phase 1 |
| Phase 13 | Interactive Debug Mode | 1-2 days | ✅ COMPLETE | Phase 2 |
| Documentation | Developer Guide | 0.5 day | ✅ COMPLETE | All |
| Demo Apps | Test Fixtures | 4 days | ✅ COMPLETE | Parallel with Phase 2-3 |

**Total Estimated LoE: 20-26 days**

**Status: 100% COMPLETE**

## Documentation

- [README.md](./README.md) - Feature overview and architecture
- [DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md) - Detailed usage guide with examples
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Cheat sheet for common operations

---

## Phase 1: Foundation (3-4 days) ✅ COMPLETE

### Tasks

- [x] **1.1 CLI Skeleton** (0.5 day) → [`cli.rs`](../../src/velostream/test_harness/cli.rs), [`velo-test.rs`](../../src/bin/velo-test.rs)
  - [x] Create `src/bin/velo-test.rs` with clap
  - [x] Subcommands: `run`, `validate`, `init`, `infer-schema`, `stress`
  - [x] Common flags: `--spec`, `--schemas`, `--output`, `--query`

- [x] **1.2 Module Structure** (0.5 day) → [`mod.rs`](../../src/velostream/test_harness/mod.rs)
  - [x] Create `src/velostream/test_harness/mod.rs`
  - [x] Create modules: `infra.rs`, `schema.rs`, `generator.rs`, `executor.rs`, `capture.rs`, `assertions.rs`, `report.rs`, `cli.rs`, `ai.rs`, `spec.rs`, `error.rs`, `config_override.rs`

- [x] **1.3 Testcontainers Infrastructure** (1 day) → [`infra.rs`](../../src/velostream/test_harness/infra.rs)
  - [x] Implement `TestHarnessInfra` struct with `with_kafka()` pattern
  - [x] Kafka topic creation/deletion via AdminClient
  - [x] Dynamic port retrieval for bootstrap.servers
  - [x] Temp directory management for file sinks
  - [x] Producer/Consumer factory methods

- [x] **1.4 Schema Parsing** (1 day) → [`schema.rs`](../../src/velostream/test_harness/schema.rs)
  - [x] Define `Schema` struct (fields, constraints, relationships)
  - [x] YAML deserialization with serde
  - [x] Constraint types: enum, min/max, distribution, derived, references
  - [x] Schema validation
  - [x] `SchemaRegistry` for schema collection management

- [x] **1.5 Data Generation** (1 day) → [`generator.rs`](../../src/velostream/test_harness/generator.rs)
  - [x] Implement `SchemaDataGenerator`
  - [x] Enum generation (with weights via WeightedIndex)
  - [x] Range generation (integer, decimal, float)
  - [x] Timestamp generation (relative and absolute ranges)
  - [x] Distribution support (uniform, normal, log_normal, zipf) via Box-Muller
  - [x] Derived field expressions (multiplication)
  - [x] Foreign key relationships (placeholder)

### Phase 1 Success Criteria
- [x] `velo-test --help` shows all subcommands
- [x] `TestHarnessInfra::with_kafka()` connects to external Kafka
- [x] Can parse schema YAML files
- [x] Can generate records matching schema

---

## Phase 2: Execution Engine (3-4 days) ✅ COMPLETE

### Tasks

- [x] **2.1 Test Spec Parsing** (0.5 day) → [`spec.rs`](../../src/velostream/test_harness/spec.rs)
  - [x] Define `TestSpec` struct with `QueryTest`, `InputConfig`, `AssertionConfig`
  - [x] Parse `test_spec.yaml` format via serde_yaml
  - [x] Query definitions with inputs and assertions
  - [x] Validation for duplicate names and from_previous references

- [x] **2.2 Config Override** (1 day) → [`config_override.rs`](../../src/velostream/test_harness/config_override.rs)
  - [x] `ConfigOverrides` struct with builder pattern
  - [x] Override `bootstrap.servers` to testcontainers
  - [x] Override topic names with `test_{run_id}_` prefix
  - [x] Override file paths to temp directory
  - [x] Apply overrides to config maps and SQL properties

- [x] **2.3 Query Execution** (1.5 days) → [`executor.rs`](../../src/velostream/test_harness/executor.rs) ⚠️ **PLACEHOLDER**
  - [x] `QueryExecutor` struct with infrastructure integration
  - [x] Integration with `SqlValidator` for parsing
  - [x] Query name extraction from CREATE STREAM statements
  - [x] Timeout handling per query
  - [x] Error capture and reporting
  - ⚠️ **NOTE**: `execute_query()` at line 167 is a placeholder - does NOT call `StreamJobServer.deploy_job()`

- [x] **2.4 Sink Capture** (1 day) → [`capture.rs`](../../src/velostream/test_harness/capture.rs)
  - [x] `SinkCapture` with Kafka StreamConsumer
  - [x] Wait for messages with timeout and idle detection
  - [x] File sink reading (JSONL format)
  - [x] Record deserialization to `HashMap<String, FieldValue>`
  - [x] Support for configurable min/max records

- [x] **2.5 Input Chaining** (0.5 day) → [`executor.rs`](../../src/velostream/test_harness/executor.rs)
  - [x] Store captured outputs by query name
  - [x] Support `from_previous: "query_name"` in test spec
  - [x] Publish records from previous query output

### Phase 2 Success Criteria
- [x] QueryExecutor publishes generated data to Kafka
- [x] SinkCapture captures output from Kafka topics
- [x] Input chaining via `from_previous` works
- [x] Config overrides applied correctly

---

## Phase 3: Assertions (2-3 days) ✅ COMPLETE

### Tasks

- [x] **3.1 Assertion Framework** (0.5 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] `AssertionRunner` struct with context support
  - [x] `AssertionResult` struct (pass/fail, message, expected, actual, details)
  - [x] `AssertionContext` for template variable context

- [x] **3.2 Basic Assertions** (1 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] `record_count` (equals, between, greater_than, less_than, expression)
  - [x] `schema_contains` (required fields present)
  - [x] `no_nulls` (all fields or specific fields)
  - [x] `field_in_set` (values within allowed set)

- [x] **3.3 Field Value Assertions** (0.5 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] `field_values` with operators (equals, not_equals, gt, lt, gte, lte, contains, starts_with, ends_with, matches)
  - [x] Type-aware comparisons via `field_value_to_f64` and `field_value_to_string`

- [x] **3.4 Aggregate Assertions** (0.5 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] `aggregate_check` (SUM, COUNT, AVG, MIN, MAX)
  - [x] Tolerance support for floating point comparisons
  - [x] Template variable substitution (e.g., `{{inputs.source_name.count}}`)

- [x] **3.5 JOIN Assertions** (0.5 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] `join_coverage` (match rate calculation based on output/input ratio)
  - [x] Input tracking via `AssertionContext.with_input_records()`
  - [x] Diagnostic information for failures (match_rate, record counts)

### Phase 3 Success Criteria
- [x] All assertion types implemented
- [x] Clear failure messages with context
- [x] Template variables implemented ({{inputs.X.count}}, {{outputs.X.count}}, {{variables.X}})

---

## Phase 4: Reporting (1-2 days) ✅ COMPLETE

### Tasks

- [x] **4.1 Text Report** (0.5 day) → [`report.rs`](../../src/velostream/test_harness/report.rs)
  - [x] `ReportGenerator` with `TestReport` struct
  - [x] Summary section (total, passed, failed, skipped, errors)
  - [x] Per-query results with timing
  - [x] Assertion details for failures
  - [x] Record count summary

- [x] **4.2 JSON Output** (0.5 day) → [`report.rs`](../../src/velostream/test_harness/report.rs)
  - [x] `write_json_report()` with serde_json
  - [x] Structured JSON schema for results
  - [x] Machine-readable format for tooling

- [x] **4.3 JUnit XML Output** (0.5 day) → [`report.rs`](../../src/velostream/test_harness/report.rs)
  - [x] `write_junit_report()` with XML escaping
  - [x] JUnit XML testsuites/testsuite/testcase structure
  - [x] Failure and error elements with details

- [x] **4.4 Exit Codes** (0.25 day) → [`cli.rs`](../../src/velostream/test_harness/cli.rs)
  - [x] `OutputFormat` enum (Text, Json, Junit)
  - [x] Exit code logic (implemented in CLI runner)

### Phase 4 Success Criteria
- [x] All three output formats implemented
- [x] JUnit XML with proper escaping
- [x] Exit codes in CLI (implemented: 0=success, 1=failure)

---

## Phase 5: Advanced Features (3-4 days) ✅ COMPLETE

### Tasks

- [x] **5.1 Schema Inference** (1 day) → [`inference.rs`](../../src/velostream/test_harness/inference.rs)
  - [x] Analyze SQL for field types
  - [x] Sample CSV files for value ranges
  - [x] Generate schema.yaml from analysis
  - [x] `velo-test infer-schema` command

- [x] **5.2 Test Spec Generation** (1 day) → [`spec_generator.rs`](../../src/velostream/test_harness/spec_generator.rs)
  - [x] Analyze queries for patterns (aggregates, JOINs, windows)
  - [x] Generate appropriate assertions
  - [x] `velo-test init` command

- [x] **5.3 Derived Field Expressions** (0.5 day) → [`generator.rs`](../../src/velostream/test_harness/generator.rs)
  - [x] Expression parser for derived constraints
  - [x] Support for `random()`, field references, arithmetic (`+`, `-`, `*`, `/`, `%`)
  - [x] Comparison operators (`>`, `<`, `>=`, `<=`, `==`, `!=`)
  - [x] Functions: `abs()`, `min()`, `max()`, `concat()`, `round()`, `floor()`, `ceil()`
  - [x] Comprehensive test coverage (12 new tests)

- [x] **5.4 Template Assertions** (0.5 day) → [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
  - [x] Jinja-style template syntax (`{{ expression }}`)
  - [x] Custom validation logic with comparison/logical/arithmetic operators
  - [x] Loop over output records (`all(record.field > 0 for record in records)`)
  - [x] Aggregate functions: `sum()`, `count()`, `avg()`, `min()`, `max()`, `len()`

- [x] **5.5 Stress Test Mode** (1 day) → [`stress.rs`](../../src/velostream/test_harness/stress.rs)
  - [x] `velo-test stress` command
  - [x] Configurable record count and duration
  - [x] Throughput measurement
  - [x] Memory tracking (macOS + Linux support)
    - [x] `MemoryTracker` with peak/start/final memory tracking
    - [x] Platform-specific APIs (mach for macOS, /proc for Linux)
    - [x] Memory growth calculation and reporting

### Phase 5 Success Criteria
- [x] Can generate schema from SQL + CSV
- [x] Can generate test_spec from SQL
- [x] Stress test produces performance report

---

## Phase 6: AI-Powered Features (2-3 days) ✅ COMPLETE

### Tasks

- [x] **6.1 Claude API Integration** (0.5 day) → [`ai.rs`](../../src/velostream/test_harness/ai.rs)
  - [x] Add reqwest crate for HTTP client
  - [x] `AiAssistant` struct with client
  - [x] API key configuration (env var)
  - [x] Rate limiting and error handling
  - [x] `call_claude()` async API method

- [x] **6.2 AI Schema Inference** (1 day) → [`ai.rs`](../../src/velostream/test_harness/ai.rs)
  - [x] Prompt engineering for schema generation
  - [x] SQL + CSV sample analysis
  - [x] Intelligent constraint suggestions
  - [x] `--ai` flag for `infer-schema` command

- [x] **6.3 AI Failure Analysis** (0.5 day) → [`ai.rs`](../../src/velostream/test_harness/ai.rs)
  - [x] Prompt engineering for failure explanation
  - [x] Context building (query, inputs, outputs, assertion)
  - [x] Actionable fix suggestions
  - [x] `analyze_failure()` method implemented in AiAssistant

- [x] **6.4 AI Test Generation** (0.5 day) → [`ai.rs`](../../src/velostream/test_harness/ai.rs)
  - [x] Prompt engineering for test spec generation
  - [x] Query pattern recognition
  - [x] Intelligent assertion selection
  - [x] `--ai` flag for `init` command

### Phase 6 Success Criteria
- [x] AI schema inference produces valid schemas
- [x] AI failure analysis provides actionable fixes
- [x] AI test generation creates reasonable assertions

---

## Dependencies Graph

```
Phase 1 (Foundation)
    │
    ▼
Phase 2 (Execution Engine)
    │
    ▼
Phase 3 (Assertions)
    │
    ▼
Phase 4 (Reporting)
    │
    ▼
Phase 5 (Advanced Features)
    │
    ▼
Phase 6 (AI-Powered Features)
```

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Testcontainers Docker dependency | HIGH | Support `--mock` mode for unit tests without Docker |
| StreamJobServer integration complexity | MEDIUM | Start with simplified execution, iterate |
| Claude API rate limits | LOW | Implement caching and rate limiting |
| Schema expression parser complexity | MEDIUM | Start with simple expressions, defer complex ones |

---

## Milestones

| Milestone | Phases | Target |
|-----------|--------|--------|
| MVP | 1-4 | Core functionality working |
| Feature Complete | 1-5 | All non-AI features |
| Full Release | 1-6 | Including AI features |

---

---

## Demo SQL Applications

Test fixtures that exercise different Velostream functionality. Located in `demo/test_harness_examples/`.

### Tier 1: Basic Operations (Phase 1-2 validation)

| App | File | Features Tested |
|-----|------|-----------------|
| **Simple Passthrough** | `01_passthrough.sql` | Basic SELECT, Kafka source/sink |
| **Field Projection** | `02_projection.sql` | SELECT specific fields, aliases |
| **Simple Filter** | `03_filter.sql` | WHERE clause, comparisons |
| **Type Casting** | `04_casting.sql` | CAST, type conversions |

### Tier 2: Aggregations (Phase 2-3 validation)

| App | File | Features Tested |
|-----|------|-----------------|
| **Count Aggregation** | `10_count.sql` | COUNT(*), GROUP BY |
| **Sum/Avg Aggregation** | `11_sum_avg.sql` | SUM, AVG, MIN, MAX |
| **Tumbling Window** | `12_tumbling_window.sql` | WINDOW TUMBLING |
| **Sliding Window** | `13_sliding_window.sql` | WINDOW SLIDING |
| **Session Window** | `14_session_window.sql` | WINDOW SESSION |

### Tier 3: Joins (Phase 2-3 validation)

| App | File | Features Tested |
|-----|------|-----------------|
| **Stream-Table JOIN** | `20_stream_table_join.sql` | LEFT JOIN with file source |
| **Stream-Stream JOIN** | `21_stream_stream_join.sql` | JOIN two Kafka sources |
| **Multi-Table JOIN** | `22_multi_join.sql` | Multiple JOINs |

### Tier 4: Window Functions (Phase 3 validation)

| App | File | Features Tested |
|-----|------|-----------------|
| **LAG/LEAD** | `30_lag_lead.sql` | LAG, LEAD functions |
| **ROW_NUMBER** | `31_row_number.sql` | ROW_NUMBER, RANK |
| **Running Aggregates** | `32_running_agg.sql` | Running SUM, AVG |
| **ROWS WINDOW BUFFER** | `33_rows_buffer.sql` | ROWS WINDOW BUFFER N ROWS |

### Tier 5: Complex Patterns (Phase 4-5 validation)

| App | File | Features Tested |
|-----|------|-----------------|
| **Multi-Stage Pipeline** | `40_pipeline.sql` | Multiple CREATE STREAM chained |
| **Subqueries** | `41_subqueries.sql` | IN (SELECT ...), EXISTS |
| **CASE Expressions** | `42_case.sql` | CASE WHEN logic |
| **Complex Filters** | `43_complex_filter.sql` | AND, OR, BETWEEN, IN |

### Tier 6: Edge Cases (Regression testing)

| App | File | Features Tested |
|-----|------|-----------------|
| **Null Handling** | `50_nulls.sql` | NULL values, COALESCE, IS NULL |
| **Empty Input** | `51_empty.sql` | Zero records handling |
| **Large Volume** | `52_large_volume.sql` | 100k+ records |
| **Late Arrivals** | `53_late_arrivals.sql` | Out-of-order events |

### Directory Structure

```
demo/test_harness_examples/
├── CLAUDE.md                    # Context for Claude Code
├── README.md                    # Overview of demo apps
│
├── tier1_basic/
│   ├── 01_passthrough.sql
│   ├── 01_passthrough.test.yaml  # Test spec
│   ├── 02_projection.sql
│   ├── 02_projection.test.yaml
│   └── ...
│
├── tier2_aggregations/
│   ├── 10_count.sql
│   ├── 10_count.test.yaml
│   └── ...
│
├── tier3_joins/
│   ├── 20_stream_table_join.sql
│   ├── 20_stream_table_join.test.yaml
│   ├── data/
│   │   └── reference_table.csv
│   └── ...
│
├── tier4_window_functions/
│   └── ...
│
├── tier5_complex/
│   └── ...
│
├── tier6_edge_cases/
│   └── ...
│
├── schemas/
│   ├── simple_record.schema.yaml
│   ├── market_data.schema.yaml
│   └── order_event.schema.yaml
│
└── configs/
    ├── kafka_source.yaml
    └── kafka_sink.yaml
```

### Demo App Tasks

- [x] **Tier 1: Basic** (1 day) - 4 apps ✅ COMPLETE
- [x] **Tier 2: Aggregations** (1 day) - 5 apps ✅ COMPLETE
- [x] **Tier 3: Joins** (0.5 day) - 3 apps ✅ COMPLETE
- [x] **Tier 4: Window Functions** (0.5 day) - 4 apps ✅ COMPLETE
- [x] **Tier 5: Complex** (0.5 day) - 4 apps ✅ COMPLETE
- [x] **Tier 6: Edge Cases** (0.5 day) - 4 apps ✅ COMPLETE

**Total Demo Apps LoE: 4 days** ✅ COMPLETE

**All 24 demo SQL apps pass validation:**
- Fixed WINDOW syntax (use simple duration: `1m`, `5m`, `30s`)
- Fixed CAST syntax (DECIMAL without precision/scale)
- Fixed CASE syntax (use searched form: `CASE WHEN x = 'value' THEN ...`)
- Created 17 config YAML files for sources/sinks
- All WITH clauses include proper topic.name and config_file references

---

## Notes

- Phase 1-4 = MVP (~10-13 days)
- Phase 5 = Nice-to-have advanced features (~3-4 days)
- Phase 6 = AI enhancement (~2-3 days)
- Demo apps = Test fixtures (~4 days, parallel with Phase 2-3)
- Consider shipping MVP first, then iterate on advanced features

---

## ✅ FR-084 COMPLETE

**All phases completed successfully!**

| Component | Status |
|-----------|--------|
| Phase 1: Foundation | ✅ Complete |
| Phase 2: Execution Engine | ✅ Complete |
| Phase 3: Assertions | ✅ Complete |
| Phase 4: Reporting | ✅ Complete |
| Phase 5: Advanced Features | ✅ Complete |
| Phase 6: AI-Powered Features | ✅ Complete |
| Demo Apps (24 total) | ✅ Complete |

### Key Deliverables

1. **CLI Tool** (`velo-test`): `run`, `validate`, `init`, `infer-schema`, `stress`, `debug` commands
2. **Schema-driven data generation** with distributions, constraints, derived fields
3. **Comprehensive assertion framework** with 10+ assertion types
4. **Multiple output formats**: Text, JSON, JUnit XML
5. **Stress testing** with throughput and memory tracking
6. **AI-powered features**: Schema inference, failure analysis, test generation
7. **24 demo SQL applications** across 6 tiers of complexity
8. **Interactive Debug Mode** with step-by-step execution, breakpoints, and data visibility

### What's Next?

FR-084 is feature-complete. Potential follow-up work:
- [x] Integration tests with real Kafka clusters (testcontainers)
- [x] Documentation and user guides (DEVELOPER_GUIDE.md, QUICK_REFERENCE.md, GETTING_STARTED.md)
- [x] Interactive debug mode with data visibility commands
- [ ] Performance benchmarking of the test harness itself
- [ ] CI/CD integration examples

---

## Future Enhancements

### Docker Distribution

**Status**: Planned
**Priority**: Medium
**Effort**: 1-2 days

Currently, `velo-test` requires building from source with Rust toolchain. A Docker-based distribution would enable:

1. **No Rust Required**: Users can run tests without installing Rust
2. **Consistent Environment**: Same behavior across all platforms
3. **CI/CD Friendly**: Easy integration with containerized pipelines
4. **Bundled Dependencies**: Include all required tools in the image

**Proposed Implementation**:

```dockerfile
# Dockerfile
FROM rust:1.75-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin velo-test

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/velo-test /usr/local/bin/
ENTRYPOINT ["velo-test"]
```

**Usage Pattern**:
```bash
# Validate SQL (no Kafka)
docker run -v $(pwd):/workspace velostream/velo-test \
    validate /workspace/sql/my_app.sql

# Run tests (with Kafka via docker network)
docker run -v $(pwd):/workspace --network kafka-network velostream/velo-test \
    run /workspace/sql/my_app.sql --spec /workspace/test_spec.yaml

# With docker-compose for full environment
docker-compose -f docker-compose.test.yml run velo-test \
    run /workspace/sql/my_app.sql --spec /workspace/test_spec.yaml
```

**Tasks**:
- [ ] Create optimized multi-stage Dockerfile
- [ ] Add docker-compose.yml for full test environment (Kafka + Schema Registry + velo-test)
- [ ] Publish to Docker Hub or GitHub Container Registry
- [ ] Update documentation with Docker usage examples
- [ ] Add CI workflow to build and push Docker images on release

**Tracking**: Update GETTING_STARTED.md "Option B: Docker Image" section when implemented.

### Testcontainers Integration

Added support for testcontainers Kafka with a new feature flag:

```toml
# In Cargo.toml
[features]
test-support = ["testcontainers", "testcontainers-modules"]
```

Usage in integration tests:
```rust
use velostream::velostream::test_harness::TestHarnessInfra;

#[tokio::test]
async fn test_with_kafka() {
    let mut infra = TestHarnessInfra::with_testcontainers().await.unwrap();
    infra.start().await.unwrap();
    // ... run tests
    infra.stop().await.unwrap();
}
```

Run tests with:
```bash
cargo test --tests --features test-support integration::test_harness_integration_test
```

---

## Implementation Reality

> **This section documents what is actually implemented vs what remains as placeholder or gap.**

### ✅ Fully Working Components

| Component | File | What Works |
|-----------|------|------------|
| **CLI** | [`cli.rs`](../../src/velostream/test_harness/cli.rs), [`velo-test.rs`](../../src/bin/velo-test.rs) | All subcommands parse correctly, `validate` works end-to-end |
| **Schema Parsing** | [`schema.rs`](../../src/velostream/test_harness/schema.rs) | YAML parsing, validation, `SchemaRegistry` |
| **Data Generation** | [`generator.rs`](../../src/velostream/test_harness/generator.rs) | Enum, range, timestamp, distributions, derived expressions |
| **Test Spec** | [`spec.rs`](../../src/velostream/test_harness/spec.rs) | Full YAML parsing with validation |
| **Config Override** | [`config_override.rs`](../../src/velostream/test_harness/config_override.rs) | Bootstrap servers, topic prefix, temp paths |
| **Sink Capture** | [`capture.rs`](../../src/velostream/test_harness/capture.rs) | Kafka consumer, file reader, timeout handling |
| **Assertions** | [`assertions.rs`](../../src/velostream/test_harness/assertions.rs) | 10+ assertion types, template assertions |
| **Reports** | [`report.rs`](../../src/velostream/test_harness/report.rs) | Text, JSON, JUnit XML |
| **AI Features** | [`ai.rs`](../../src/velostream/test_harness/ai.rs) | Claude API integration, all AI methods |
| **Schema Inference** | [`inference.rs`](../../src/velostream/test_harness/inference.rs) | SQL analysis, CSV sampling |
| **Spec Generator** | [`spec_generator.rs`](../../src/velostream/test_harness/spec_generator.rs) | Pattern detection, assertion generation |
| **Stress Test** | [`stress.rs`](../../src/velostream/test_harness/stress.rs) | Throughput, memory tracking (macOS/Linux) |
| **Infrastructure** | [`infra.rs`](../../src/velostream/test_harness/infra.rs) | Testcontainers Kafka, topic management |

### ✅ Phase 7: StreamJobServer Integration (Completed)

The `QueryExecutor` now integrates with `StreamJobServer` to execute SQL queries:

| Component | Implementation |
|-----------|----------------|
| **StreamJobServer Field** | `executor.rs` - Added `server: Option<Arc<StreamJobServer>>` field |
| **Initialization** | `with_server()` async method creates and configures server |
| **Query Execution** | `deploy_job()` called for each query with proper config |
| **Job Waiting** | `wait_for_job_completion()` polls job status until done |
| **Cleanup** | `stop_job()` called after capture to stop running jobs |

### ✅ Phase 8: Performance Assertions (Completed)

Added execution time and memory usage assertions:

| Assertion | Fields | Implementation |
|-----------|--------|----------------|
| **`execution_time`** | `max_ms`, `min_ms` | `assertions.rs` - Checks query execution time |
| **`memory_usage`** | `max_bytes`, `max_mb`, `max_growth_bytes` | `assertions.rs` - Checks memory consumption |

`CapturedOutput` extended with:
- `memory_peak_bytes: Option<u64>` - Peak memory during execution
- `memory_growth_bytes: Option<i64>` - Memory growth (can be negative)

Memory tracking integrated into `execute_query()` using `MemoryTracker` from `stress.rs`.

### ✅ Phase 9: Foreign Key Reference Data (Completed)

Full foreign key relationship support in data generation:

| Feature | Method | Description |
|---------|--------|-------------|
| **Direct Loading** | `load_reference_data(table, field, values)` | Load explicit value lists |
| **From Records** | `load_reference_data_from_records(table, field, records)` | Extract from generated records |
| **Sampling** | `generate_reference_value()` | Samples from loaded data |
| **Fallback** | Placeholder values | `REF_{TABLE}_{random}` when no data loaded |

Example usage:
```rust
let mut generator = SchemaDataGenerator::new(Some(42));

// Load reference data for foreign keys
generator.load_reference_data("customers", "id", vec![
    FieldValue::String("CUST001".to_string()),
    FieldValue::String("CUST002".to_string()),
]);

// Or load from previously generated records
let customer_records = generator.generate(&customer_schema, 100).unwrap();
generator.load_reference_data_from_records("customers", "id", &customer_records);
```

### ✅ Phase 11: Advanced Testing Features (Completed)

Added comprehensive testing capabilities for production-grade test scenarios:

| Module | File | Features |
|--------|------|----------|
| **DLQ Capture** | [`dlq.rs`](../../src/velostream/test_harness/dlq.rs) | Dead Letter Queue capture, error classification, statistics |
| **Fault Injection** | [`fault_injection.rs`](../../src/velostream/test_harness/fault_injection.rs) | Chaos testing: malformed records, duplicates, out-of-order events |
| **Table State** | [`table_state.rs`](../../src/velostream/test_harness/table_state.rs) | CTAS materialized table tracking, snapshots, changelog |

**New Assertion Types in `spec.rs`:**

| Assertion | Description |
|-----------|-------------|
| `dlq_count` | Check DLQ error record counts and error types |
| `error_rate` | Check error rate stays within bounds |
| `no_duplicates` | Verify record uniqueness by key fields |
| `ordering` | Verify record ordering (ascending/descending) |
| `completeness` | Verify no data loss compared to input |
| `table_freshness` | Check CTAS table freshness and lag |
| `data_quality` | Comprehensive quality checks (nulls, ranges, patterns) |

**DLQ Capture Features:**
- `DlqConfig`: Enable DLQ capture with custom topic and timeout
- `ErrorType`: Classification (deserialization, schema_validation, type_conversion, etc.)
- `DlqRecord`: Captured error records with metadata
- `DlqStatistics`: Error counts, rates, and samples

**Fault Injection Features:**
- `MalformedRecordConfig`: Inject parse errors, missing fields, wrong types
- `DuplicateConfig`: Inject duplicate records at configurable rate
- `OutOfOrderConfig`: Shuffle records within a window
- `FieldCorruptionConfig`: Corrupt specific field values
- `SlowProcessingConfig`: Simulate slow processing delays

**Table State Features:**
- `TableState`: Track CTAS table records, changelog, statistics
- `TableSnapshot`: Point-in-time snapshots with persistence
- `TableStateManager`: Manage multiple tables with snapshot history
- Key-based upsert, delete, lookup operations

### ⚠️ Remaining Items

| Feature | Description | Status |
|---------|-------------|--------|
| **Schema Registry Container** | Testcontainers only starts Kafka, not Schema Registry | Not implemented |

### Integration Test Results

All 5 integration tests pass with testcontainers:
- `test_testcontainers_kafka_startup` ✅
- `test_topic_lifecycle` ✅
- `test_producer_consumer_creation` ✅
- `test_config_overrides` ✅
- `test_temp_directory` ✅

Run with: `cargo test --tests --features test-support integration::test_harness_integration_test`

### ✅ Phase 12: File-Based Testing Support (Completed)

Added comprehensive file-based input/output support for testing SQL applications without Kafka infrastructure:

| Module | File | Features |
|--------|------|----------|
| **File I/O** | [`file_io.rs`](../../src/velostream/test_harness/file_io.rs) | CSV/JSON file loading and writing for test data |
| **Source/Sink Types** | [`spec.rs`](../../src/velostream/test_harness/spec.rs) | `SourceType` and `SinkType` enums with file support |
| **File Assertions** | [`assertions.rs`](../../src/velostream/test_harness/assertions.rs) | File-specific assertion implementations |

**New Source/Sink Types:**

```yaml
# File-based input source
inputs:
  - source: trades
    source_type:
      type: file
      path: ./input_data.csv
      format: csv       # csv, csv_no_header, json_lines, json
      watch: false      # Watch for file changes

# File-based output sink
output:
  sink_type:
    type: file
    path: ./output.csv
    format: csv
```

**New File Assertions:**

| Assertion | Description |
|-----------|-------------|
| `file_exists` | Verify output file exists with optional size constraints |
| `file_row_count` | Verify number of data rows in output file |
| `file_contains` | Verify file contains specific values in a field |
| `file_matches` | Compare output file to expected file content |

**File Assertion Examples:**

```yaml
assertions:
  # Check file exists and has content
  - type: file_exists
    path: ./output.csv
    min_size_bytes: 100

  # Check row count
  - type: file_row_count
    path: ./output.csv
    format: csv
    equals: 6

  # Check specific values exist
  - type: file_contains
    path: ./output.csv
    format: csv
    field: symbol
    expected_values: [AAPL, GOOGL, MSFT]
    mode: all  # or 'any'

  # Compare to expected file
  - type: file_matches
    actual_path: ./output.csv
    expected_path: ./expected.csv
    format: csv
    ignore_order: true
    numeric_tolerance: 0.01
```

**Supported File Formats:**
- `csv` - CSV with header row
- `csv_no_header` - CSV without header
- `json_lines` - JSON Lines (one JSON object per line)
- `json` - JSON array of objects

**Benefits:**
- **Fast Testing**: No Kafka infrastructure required
- **Deterministic**: File inputs provide repeatable tests
- **Easy Debugging**: Output files can be inspected directly
- **CI/CD Friendly**: Works in environments without Docker

**Demo Example:**
See `demo/test_harness_examples/tier1_basic/file_io/` for a complete file-based testing example

### ✅ Phase 13: Interactive Debug Mode (Completed)

Full-featured interactive debugger for step-by-step SQL execution and data inspection:

| Module | File | Features |
|--------|------|----------|
| **Debug CLI** | [`velo-test.rs`](../../src/bin/velo-test.rs) | `debug` command with REPL interface |
| **Statement Executor** | [`statement_executor.rs`](../../src/velostream/test_harness/statement_executor.rs) | Debug commands, session state, breakpoints |
| **Infrastructure** | [`infra.rs`](../../src/velostream/test_harness/infra.rs) | Topic inspection, message peeking |

**Debug Commands:**

| Category | Commands |
|----------|----------|
| **Execution Control** | `step`, `continue`, `run`, `break <N>`, `unbreak <N>`, `clear` |
| **State Inspection** | `list`, `status`, `inspect <N>`, `inspect-all`, `history` |
| **Infrastructure** | `topics`, `consumers`, `jobs`, `schema <topic>` |
| **Data Visibility** | `messages <topic\|N>`, `head <stmt>`, `tail <stmt>`, `filter <stmt>`, `export <stmt>` |

**Key Features:**

- **Numbered Topic References**: After `topics`, use `messages 1` instead of full topic name
- **Enhanced Message Display**: Shows partition, offset, timestamp, key, headers, and formatted JSON value
- **Filter Expressions**: `filter 1 status=FAILED`, `filter 1 price>100`, `filter 1 message~error`
- **Export**: `export 1 results.json` or `export 1 results.csv`
- **Breakpoints**: Set on statement names or numbers, pause before execution
- **Post-Completion Inspection**: Continue inspecting state after execution finishes

**Example Session:**

```bash
$ velo-test debug app.sql --spec test_spec.yaml --breakpoint 3

🐛 Velostream SQL Debugger
════════════════════════════════════════

📝 SQL Statements:
    [1] create_stream (CREATE STREAM)
  * [3] enriched (SELECT)           <- breakpoint

(debug) step
   ✅ create_stream completed in 234ms

(debug) topics
   📋 Topics (1):
   [1] test_abc_input [test] (100 messages)

(debug) messages 1 --last 2
📨 2 messages:
  [1] P0:offset 98
      key: AAPL
      value: {"symbol": "AAPL", "price": 178.50}

(debug) run
✅ All statements completed

(debug) export 3 results.json
✅ Exported 100 records to results.json (JSON)
```

See [README.md#interactive-debug-mode](./README.md#interactive-debug-mode) for full documentation.
