# Velostream Active Development TODO

**Last Updated**: 2025-10-27
**Current Status**: ✅ Production Ready
**Test Status**: 365/365 tests passing

---

## 📊 Project Status Summary

### ✅ COMPLETED FEATURES

| Feature | Status | Commits | Test Count |
|---------|--------|---------|-----------|
| FR-079: Windowed EMIT CHANGES | ✅ Complete (6 phases) | 04f29be → d8fe6e7 | 12 tests |
| FR-080: System Columns & Headers | ✅ Complete (4 phases) | 2762fa8 → 5d12a0d | 14 tests |
| SQL Parser: Window/GROUP BY Ordering | ✅ Fixed | 317c8e1 | 365 total |
| Core SQL Engine | ✅ Stable | Multiple | 240+ tests |
| Kafka Integration | ✅ Stable | Multiple | 50+ tests |
| Serialization (JSON/Avro/Protobuf) | ✅ Stable | Multiple | 40+ tests |

### 🎯 Current Test Results
```
✅ Unit Tests: 365/365 passing
✅ Code Formatting: Passed
✅ Compilation: Passed
✅ Clippy Linting: Passed
```

---

## 🔄 ACTIVE DEVELOPMENT PRIORITIES

### Phase 1: Performance Profiling & Optimization
**Status**: Not Started
**Priority**: Medium (Enhancement)
**Estimated Effort**: 8-12 hours

#### Async Framework Performance Analysis
- [ ] Add tokio-console instrumentation for task execution visibility
- [ ] Profile Arc<Mutex<>> lock contention in multi-source scenarios
- [ ] Measure cost of .await points in processing chain
- [ ] Analyze channel overhead (mpsc vs broadcast)
- [ ] Profile task spawning and scheduling overhead

#### Benchmarking
- [ ] Real Kafka benchmarks with network I/O
- [ ] Complex SQL queries (aggregations, joins, windows)
- [ ] Multi-source/multi-sink scenarios
- [ ] Memory usage profiling
- [ ] Latency percentiles (p50, p95, p99)

**Rationale**: Understand performance characteristics and identify optimization opportunities for production deployments.

---

### Phase 2: Processor Architecture Refactoring
**Status**: Not Started
**Priority**: Low (Code Quality)
**Estimated Effort**: 4-6 hours

#### Code Cleanup
- [ ] Remove `_output_receiver` parameter from SimpleProcessor and SelectProcessor
- [ ] Update all call sites (tests, job server, stream processor)
- [ ] Add migration note in CHANGELOG
- [ ] Verify backward compatibility

#### Documentation
- [ ] Create comprehensive architecture documentation
- [ ] Diagram execution paths (direct vs interactive)
- [ ] Document state management patterns
- [ ] Add performance characteristics guide
- [ ] Update CLAUDE.md with processor section

**Rationale**: Improve code maintainability and reduce parameter passing complexity.

---

### Phase 3: Generic Table Loading System
**Status**: Design Phase
**Priority**: High (Infrastructure)
**Estimated Effort**: 15-20 hours

#### Architecture
- [ ] Define `TableDataSource` trait with bulk/incremental loading methods
- [ ] Create `SourceOffset` enum for different offset types (Kafka, File position, SQL timestamp)
- [ ] Implement generic CTAS loading orchestrator
- [ ] Add offset persistence for resume capability

#### Implementation
- [ ] **KafkaDataSource**: Bulk (earliest→latest) + Incremental (offset-based)
- [ ] **FileDataSource**: Bulk (full read) + Incremental (file position tracking)
- [ ] **SqlDataSource**: Bulk (full query) + Incremental (timestamp-based)
- [ ] Configurable incremental loading intervals
- [ ] Error recovery and retry logic

**Rationale**: Enable efficient data loading from heterogeneous sources for CTAS operations and streaming joins.

---

### Phase 4: Event-Time Watermarking Enhancements
**Status**: Implementation Complete (Under Documentation)
**Priority**: Medium (Features)
**Estimated Effort**: 3-4 hours

#### Testing & Documentation (Remaining)
- [ ] Dedicated unit tests in `tests/unit/datasource/event_time_test.rs`
- [ ] Integration tests for watermark interaction with processors
- [ ] Performance benchmarks for extraction overhead
- [ ] Error handling test coverage
- [ ] Add event-time extraction examples to watermarks guide
- [ ] Update Kafka configuration documentation
- [ ] Add troubleshooting guide for common timestamp issues

**Note**: Core implementation is complete and working; this is documentation/testing work only.

---

## 📋 RESOLVED ISSUES (Session Completed)

### ✅ Critical Issues - NOW RESOLVED

| Issue | Status | Solution |
|-------|--------|----------|
| Window processor per-record emission | ✅ Fixed | Proper GROUP BY detection and EMIT CHANGES routing |
| System columns test failures | ✅ Fixed | FieldValidator support for system column properties |
| SQL clause ordering (WINDOW before GROUP BY) | ✅ Fixed | Parser validation and test reordering (commit 317c8e1) |

### ✅ Previous Session Completions

- System columns implementation (Phase 1-4): Full ✅
- Header functions with field mutation: Full ✅
- SQLValidator reference output enhancement: Full ✅
- Dynamic expression evaluation in SET_HEADER: Full ✅
- Reserved keyword fixes for STATUS, METRICS, PROPERTIES: Full ✅

---

## 📚 Documentation Archives

For historical context and completed analyses:
- **[todo-consolidated.md](todo-consolidated.md)** - Full historical TODO with completed work
- **[todo-complete.md](todo-complete.md)** - Earlier session completions
- **[FR-079-IMPLEMENTATION-PLAN.md](docs/feature/FR-079-IMPLEMENTATION-PLAN.md)** - Windowed EMIT CHANGES (all 6 phases complete)
- **[FR-080-system-cols.md](docs/feature/FR-080-system-cols.md)** - System columns implementation (all 4 phases complete)

---

## 🚀 Next Actions for New Sessions

1. **Performance Optimization** - Pick from Phase 1 tasks if focusing on speed
2. **New Features** - Start Phase 3 (Generic Table Loading) for extensibility
3. **Code Quality** - Complete Phase 2 (Processor Refactoring) for maintainability
4. **Documentation** - Complete Phase 4 (Event-Time Guide) for user clarity

---

## Testing Commands

```bash
# Run all tests
cargo test --no-default-features

# Run specific test category
cargo test system_columns --no-default-features
cargo test window --no-default-features
cargo test header_functions --no-default-features

# Run with output
cargo test --no-default-features -- --nocapture

# Pre-commit validation
cargo fmt --all -- --check && cargo check && cargo test --lib --no-default-features
```

---

## Git Workflow

```bash
# View recent commits
git log --oneline -10

# Current branch
git status

# Create feature branch
git checkout -b feature/FR-XXX-description

# Commit with proper format
git commit -m "feat: Description

Details about changes.

🤖 Generated with Claude Code

Co-Authored-By: Claude <noreply@anthropic.com>"
```
