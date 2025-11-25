# FR-084: SQL Application Test Harness - Work Tracking

## Summary

| Phase | Description | LoE | Status | Dependencies |
|-------|-------------|-----|--------|--------------|
| Phase 1 | Foundation | 3-4 days | NOT STARTED | None |
| Phase 2 | Execution Engine | 3-4 days | NOT STARTED | Phase 1 |
| Phase 3 | Assertions | 2-3 days | NOT STARTED | Phase 2 |
| Phase 4 | Reporting | 1-2 days | NOT STARTED | Phase 3 |
| Phase 5 | Advanced Features | 3-4 days | NOT STARTED | Phase 4 |
| Phase 6 | AI-Powered Features | 2-3 days | NOT STARTED | Phase 5 |

**Total Estimated LoE: 14-20 days**

---

## Phase 1: Foundation (3-4 days)

### Tasks

- [ ] **1.1 CLI Skeleton** (0.5 day)
  - [ ] Create `src/bin/velo_test.rs` with clap
  - [ ] Subcommands: `run`, `validate`, `init`, `infer-schema`, `stress`
  - [ ] Common flags: `--spec`, `--schemas`, `--output`, `--query`

- [ ] **1.2 Module Structure** (0.5 day)
  - [ ] Create `src/test_harness/mod.rs`
  - [ ] Create placeholder modules: `infra.rs`, `schema.rs`, `generator.rs`, `executor.rs`, `capture.rs`, `assertions.rs`, `report.rs`, `cli.rs`

- [ ] **1.3 Testcontainers Infrastructure** (1 day)
  - [ ] Add testcontainers dependencies to Cargo.toml
  - [ ] Implement `TestHarnessInfra` struct
  - [ ] Kafka container startup/shutdown
  - [ ] Dynamic port retrieval for bootstrap.servers
  - [ ] Temp directory management for file sinks

- [ ] **1.4 Schema Parsing** (1 day)
  - [ ] Define `Schema` struct (fields, constraints, relationships)
  - [ ] YAML deserialization with serde
  - [ ] Constraint types: enum, min/max, distribution, derived, references
  - [ ] Schema validation

- [ ] **1.5 Data Generation** (1 day)
  - [ ] Implement `SchemaDataGenerator`
  - [ ] Enum generation (with weights)
  - [ ] Range generation (integer, decimal)
  - [ ] Timestamp generation (relative ranges)
  - [ ] Distribution support (uniform, normal, log_normal)
  - [ ] Derived field expressions (basic)
  - [ ] Foreign key relationships (sample from reference)

### Phase 1 Success Criteria
- [ ] `velo-test --help` shows all subcommands
- [ ] Kafka testcontainer starts and stops cleanly
- [ ] Can parse `market_data.schema.yaml` example
- [ ] Can generate 1000 records matching schema

---

## Phase 2: Execution Engine (3-4 days)

### Tasks

- [ ] **2.1 Test Spec Parsing** (0.5 day)
  - [ ] Define `TestSpec` struct
  - [ ] Parse `test_spec.yaml` format
  - [ ] Query definitions with inputs and assertions

- [ ] **2.2 Config Override** (1 day)
  - [ ] Intercept source/sink configurations
  - [ ] Override `bootstrap.servers` to testcontainers
  - [ ] Override topic names with `test_{run_id}_` prefix
  - [ ] Override file paths to temp directory

- [ ] **2.3 Query Execution** (1.5 days)
  - [ ] Integration with `SqlValidator` for parsing
  - [ ] Integration with `QueryAnalyzer` for source/sink extraction
  - [ ] Integration with `StreamJobServer` for execution
  - [ ] Timeout handling per query
  - [ ] Error capture and reporting

- [ ] **2.4 Sink Capture** (1 day)
  - [ ] Kafka consumer for topic capture
  - [ ] Wait for messages with timeout
  - [ ] File sink reading (JSONL format)
  - [ ] Record deserialization to `HashMap<String, FieldValue>`

- [ ] **2.5 Input Chaining** (0.5 day)
  - [ ] Store captured outputs by sink name
  - [ ] Support `from_previous: true` in test spec
  - [ ] Topological sort for query dependency order

### Phase 2 Success Criteria
- [ ] Can execute single query with testcontainers Kafka
- [ ] Can capture output from Kafka sink
- [ ] Can chain Query 2 input from Query 1 output
- [ ] Config overrides work correctly

---

## Phase 3: Assertions (2-3 days)

### Tasks

- [ ] **3.1 Assertion Framework** (0.5 day)
  - [ ] Define `Assertion` trait
  - [ ] `AssertionResult` struct (pass/fail, message, details)
  - [ ] Assertion registry and factory

- [ ] **3.2 Basic Assertions** (1 day)
  - [ ] `record_count` (equals, between, greater_than, less_than)
  - [ ] `schema_contains` (required fields present)
  - [ ] `no_nulls` / `field_not_null`
  - [ ] `field_in_set` (values within allowed set)

- [ ] **3.3 Field Value Assertions** (0.5 day)
  - [ ] `field_values` with operators
  - [ ] Type-aware comparisons (numeric, string, timestamp)

- [ ] **3.4 Aggregate Assertions** (0.5 day)
  - [ ] `aggregate_check` (SUM, COUNT, AVG, MIN, MAX)
  - [ ] Expression parsing
  - [ ] Template variable substitution (`{{inputs.source.count}}`)

- [ ] **3.5 JOIN Assertions** (0.5 day)
  - [ ] `join_coverage` (match rate calculation)
  - [ ] Key overlap analysis
  - [ ] Diagnostic information for failures

### Phase 3 Success Criteria
- [ ] All assertion types implemented
- [ ] Clear failure messages with context
- [ ] Template variables work in expectations

---

## Phase 4: Reporting (1-2 days)

### Tasks

- [ ] **4.1 Text Report** (0.5 day)
  - [ ] Summary section (total, passed, failed, skipped)
  - [ ] Per-query results with timing
  - [ ] Assertion details for failures
  - [ ] Data generation summary
  - [ ] Performance metrics

- [ ] **4.2 JSON Output** (0.5 day)
  - [ ] Structured JSON schema for results
  - [ ] Machine-readable format for tooling

- [ ] **4.3 JUnit XML Output** (0.5 day)
  - [ ] JUnit XML schema compliance
  - [ ] Test suite and test case mapping
  - [ ] CI/CD integration (GitHub Actions)

- [ ] **4.4 Exit Codes** (0.25 day)
  - [ ] 0 = all passed
  - [ ] 1 = failures
  - [ ] 2 = errors (infrastructure)

### Phase 4 Success Criteria
- [ ] All three output formats working
- [ ] JUnit XML validates against schema
- [ ] Exit codes correct for CI/CD

---

## Phase 5: Advanced Features (3-4 days)

### Tasks

- [ ] **5.1 Schema Inference** (1 day)
  - [ ] Analyze SQL for field types
  - [ ] Sample CSV files for value ranges
  - [ ] Generate schema.yaml from analysis
  - [ ] `velo-test infer-schema` command

- [ ] **5.2 Test Spec Generation** (1 day)
  - [ ] Analyze queries for patterns (aggregates, JOINs, windows)
  - [ ] Generate appropriate assertions
  - [ ] `velo-test init` command

- [ ] **5.3 Derived Field Expressions** (0.5 day)
  - [ ] Expression parser for derived constraints
  - [ ] Support for `random()`, field references, arithmetic

- [ ] **5.4 Template Assertions** (0.5 day)
  - [ ] Jinja-style template syntax
  - [ ] Custom validation logic
  - [ ] Loop over output records

- [ ] **5.5 Stress Test Mode** (1 day)
  - [ ] `velo-test stress` command
  - [ ] Configurable record count and duration
  - [ ] Throughput measurement
  - [ ] Memory tracking

### Phase 5 Success Criteria
- [ ] Can generate schema from SQL + CSV
- [ ] Can generate test_spec from SQL
- [ ] Stress test produces performance report

---

## Phase 6: AI-Powered Features (2-3 days)

### Tasks

- [ ] **6.1 Claude API Integration** (0.5 day)
  - [ ] Add anthropic crate dependency
  - [ ] `AiAssistant` struct with client
  - [ ] API key configuration (env var)
  - [ ] Rate limiting and error handling

- [ ] **6.2 AI Schema Inference** (1 day)
  - [ ] Prompt engineering for schema generation
  - [ ] SQL + CSV sample analysis
  - [ ] Intelligent constraint suggestions
  - [ ] `--ai` flag for `infer-schema` command

- [ ] **6.3 AI Failure Analysis** (0.5 day)
  - [ ] Prompt engineering for failure explanation
  - [ ] Context building (query, inputs, outputs, assertion)
  - [ ] Actionable fix suggestions
  - [ ] Integration with report output

- [ ] **6.4 AI Test Generation** (0.5 day)
  - [ ] Prompt engineering for test spec generation
  - [ ] Query pattern recognition
  - [ ] Intelligent assertion selection
  - [ ] `--ai` flag for `init` command

### Phase 6 Success Criteria
- [ ] AI schema inference produces valid schemas
- [ ] AI failure analysis provides actionable fixes
- [ ] AI test generation creates reasonable assertions

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

## Notes

- Phase 1-4 = MVP (~10-13 days)
- Phase 5 = Nice-to-have advanced features (~3-4 days)
- Phase 6 = AI enhancement (~2-3 days)
- Consider shipping MVP first, then iterate on advanced features
