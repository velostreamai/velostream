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
| Demo Apps | Test Fixtures | 4 days | NOT STARTED | Parallel with Phase 2-3 |

**Total Estimated LoE: 18-24 days**

**MVP (Phase 1-4 + Tier 1-3 Demos): ~12-15 days**

---

## Phase 1: Foundation (3-4 days) ✅ COMPLETE

### Tasks

- [x] **1.1 CLI Skeleton** (0.5 day)
  - [x] Create `src/bin/velo_test.rs` with clap
  - [x] Subcommands: `run`, `validate`, `init`, `infer-schema`, `stress`
  - [x] Common flags: `--spec`, `--schemas`, `--output`, `--query`

- [x] **1.2 Module Structure** (0.5 day)
  - [x] Create `src/velostream/test_harness/mod.rs`
  - [x] Create modules: `infra.rs`, `schema.rs`, `generator.rs`, `executor.rs`, `capture.rs`, `assertions.rs`, `report.rs`, `cli.rs`, `ai.rs`, `spec.rs`, `error.rs`, `config_override.rs`

- [x] **1.3 Testcontainers Infrastructure** (1 day)
  - [x] Implement `TestHarnessInfra` struct with `with_kafka()` pattern
  - [x] Kafka topic creation/deletion via AdminClient
  - [x] Dynamic port retrieval for bootstrap.servers
  - [x] Temp directory management for file sinks
  - [x] Producer/Consumer factory methods

- [x] **1.4 Schema Parsing** (1 day)
  - [x] Define `Schema` struct (fields, constraints, relationships)
  - [x] YAML deserialization with serde
  - [x] Constraint types: enum, min/max, distribution, derived, references
  - [x] Schema validation
  - [x] `SchemaRegistry` for schema collection management

- [x] **1.5 Data Generation** (1 day)
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

- [x] **2.1 Test Spec Parsing** (0.5 day)
  - [x] Define `TestSpec` struct with `QueryTest`, `InputConfig`, `AssertionConfig`
  - [x] Parse `test_spec.yaml` format via serde_yaml
  - [x] Query definitions with inputs and assertions
  - [x] Validation for duplicate names and from_previous references

- [x] **2.2 Config Override** (1 day)
  - [x] `ConfigOverrides` struct with builder pattern
  - [x] Override `bootstrap.servers` to testcontainers
  - [x] Override topic names with `test_{run_id}_` prefix
  - [x] Override file paths to temp directory
  - [x] Apply overrides to config maps and SQL properties

- [x] **2.3 Query Execution** (1.5 days)
  - [x] `QueryExecutor` struct with infrastructure integration
  - [x] Integration with `SqlValidator` for parsing
  - [x] Query name extraction from CREATE STREAM statements
  - [x] Timeout handling per query
  - [x] Error capture and reporting

- [x] **2.4 Sink Capture** (1 day)
  - [x] `SinkCapture` with Kafka StreamConsumer
  - [x] Wait for messages with timeout and idle detection
  - [x] File sink reading (JSONL format)
  - [x] Record deserialization to `HashMap<String, FieldValue>`
  - [x] Support for configurable min/max records

- [x] **2.5 Input Chaining** (0.5 day)
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

- [x] **3.1 Assertion Framework** (0.5 day)
  - [x] `AssertionRunner` struct with context support
  - [x] `AssertionResult` struct (pass/fail, message, expected, actual, details)
  - [x] `AssertionContext` for template variable context

- [x] **3.2 Basic Assertions** (1 day)
  - [x] `record_count` (equals, between, greater_than, less_than, expression)
  - [x] `schema_contains` (required fields present)
  - [x] `no_nulls` (all fields or specific fields)
  - [x] `field_in_set` (values within allowed set)

- [x] **3.3 Field Value Assertions** (0.5 day)
  - [x] `field_values` with operators (equals, not_equals, gt, lt, gte, lte, contains, starts_with, ends_with, matches)
  - [x] Type-aware comparisons via `field_value_to_f64` and `field_value_to_string`

- [x] **3.4 Aggregate Assertions** (0.5 day)
  - [x] `aggregate_check` (SUM, COUNT, AVG, MIN, MAX)
  - [x] Tolerance support for floating point comparisons
  - [ ] Template variable substitution (TODO: Phase 5)

- [x] **3.5 JOIN Assertions** (0.5 day)
  - [x] `join_coverage` (match rate calculation - placeholder)
  - [ ] Key overlap analysis (TODO: requires input tracking)
  - [x] Diagnostic information for failures

### Phase 3 Success Criteria
- [x] All assertion types implemented
- [x] Clear failure messages with context
- [ ] Template variables (deferred to Phase 5)

---

## Phase 4: Reporting (1-2 days) ✅ COMPLETE

### Tasks

- [x] **4.1 Text Report** (0.5 day)
  - [x] `ReportGenerator` with `TestReport` struct
  - [x] Summary section (total, passed, failed, skipped, errors)
  - [x] Per-query results with timing
  - [x] Assertion details for failures
  - [x] Record count summary

- [x] **4.2 JSON Output** (0.5 day)
  - [x] `write_json_report()` with serde_json
  - [x] Structured JSON schema for results
  - [x] Machine-readable format for tooling

- [x] **4.3 JUnit XML Output** (0.5 day)
  - [x] `write_junit_report()` with XML escaping
  - [x] JUnit XML testsuites/testsuite/testcase structure
  - [x] Failure and error elements with details

- [x] **4.4 Exit Codes** (0.25 day)
  - [x] `OutputFormat` enum (Text, Json, Junit)
  - [ ] Exit code logic (implement in CLI runner)

### Phase 4 Success Criteria
- [x] All three output formats implemented
- [x] JUnit XML with proper escaping
- [ ] Exit codes in CLI (implement when integrating)

---

## Phase 5: Advanced Features (3-4 days)

### Tasks

- [x] **5.1 Schema Inference** (1 day) ✅ COMPLETE
  - [x] Analyze SQL for field types
  - [x] Sample CSV files for value ranges
  - [x] Generate schema.yaml from analysis
  - [x] `velo-test infer-schema` command

- [x] **5.2 Test Spec Generation** (1 day) ✅ COMPLETE
  - [x] Analyze queries for patterns (aggregates, JOINs, windows)
  - [x] Generate appropriate assertions
  - [x] `velo-test init` command

- [ ] **5.3 Derived Field Expressions** (0.5 day)
  - [ ] Expression parser for derived constraints
  - [ ] Support for `random()`, field references, arithmetic

- [ ] **5.4 Template Assertions** (0.5 day)
  - [ ] Jinja-style template syntax
  - [ ] Custom validation logic
  - [ ] Loop over output records

- [x] **5.5 Stress Test Mode** (1 day) ✅ COMPLETE
  - [x] `velo-test stress` command
  - [x] Configurable record count and duration
  - [x] Throughput measurement
  - [ ] Memory tracking (future enhancement)

### Phase 5 Success Criteria
- [x] Can generate schema from SQL + CSV
- [x] Can generate test_spec from SQL
- [x] Stress test produces performance report

---

## Phase 6: AI-Powered Features (2-3 days)

### Tasks

- [x] **6.1 Claude API Integration** (0.5 day) ✅ COMPLETE
  - [x] Add reqwest crate for HTTP client
  - [x] `AiAssistant` struct with client
  - [x] API key configuration (env var)
  - [x] Rate limiting and error handling
  - [x] `call_claude()` async API method

- [x] **6.2 AI Schema Inference** (1 day) ✅ COMPLETE
  - [x] Prompt engineering for schema generation
  - [x] SQL + CSV sample analysis
  - [x] Intelligent constraint suggestions
  - [x] `--ai` flag for `infer-schema` command

- [x] **6.3 AI Failure Analysis** (0.5 day) ✅ COMPLETE
  - [x] Prompt engineering for failure explanation
  - [x] Context building (query, inputs, outputs, assertion)
  - [x] Actionable fix suggestions
  - [x] `analyze_failure()` method implemented in AiAssistant

- [x] **6.4 AI Test Generation** (0.5 day) ✅ COMPLETE
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

- [ ] **Tier 1: Basic** (1 day) - 4 apps
- [ ] **Tier 2: Aggregations** (1 day) - 5 apps
- [ ] **Tier 3: Joins** (0.5 day) - 3 apps
- [ ] **Tier 4: Window Functions** (0.5 day) - 4 apps
- [ ] **Tier 5: Complex** (0.5 day) - 4 apps
- [ ] **Tier 6: Edge Cases** (0.5 day) - 4 apps

**Total Demo Apps LoE: 4 days** (can be done in parallel with Phase 2-3)

---

## Notes

- Phase 1-4 = MVP (~10-13 days)
- Phase 5 = Nice-to-have advanced features (~3-4 days)
- Phase 6 = AI enhancement (~2-3 days)
- Demo apps = Test fixtures (~4 days, parallel with Phase 2-3)
- Consider shipping MVP first, then iterate on advanced features
