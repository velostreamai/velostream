# FR-082 Performance Improvement - Part 2: Job Server V2

## 📖 Overview

This directory contains the **complete architecture and analysis** for Velostream Job Server V2 redesign, focused on achieving ultra-low-latency, high-throughput stream processing with **hash-partitioned pipeline architecture**.

**Key Innovation**: N independent state managers (one per CPU core) using deterministic hash routing for **linear scaling** across cores.

**Target Performance**: 1.5M rec/sec on 8 cores (65x improvement over V1: 23K rec/sec)

---

## 🚀 Quick Start

**New to this project? Start here:**

1. **Architecture Overview**: [FR-082-job-server-v2-PARTITIONED-PIPELINE.md](./FR-082-job-server-v2-PARTITIONED-PIPELINE.md)
   - Read Parts 0-3 for architecture fundamentals
   - Parts 10-12 for query partitionability strategies

2. **Query Analysis**: [FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md](./FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md)
   - Understand which queries can be partitioned (72% of workload)
   - Decision tree for routing strategies

3. **Architecture Decision**: [FR-082-ARCHITECTURE-COMPARISON.md](./FR-082-ARCHITECTURE-COMPARISON.md)
   - Why hash-partitioned > single-actor design

---

## 📂 Document Structure

### ✅ Current Architecture (Use These)

| Document | Purpose | Lines | Status |
|----------|---------|-------|--------|
| **[FR-082-job-server-v2-PARTITIONED-PIPELINE.md](./FR-082-job-server-v2-PARTITIONED-PIPELINE.md)** | 🔵 **Main Architecture Blueprint** | 1,815 | ✅ Active |
| | Hash-partitioned pipeline with N state managers | | |
| | 12 parts covering architecture, performance, implementation | | |
| | **NEW**: Query partitionability (Part 10), Two-phase aggregation (Part 11), Replicated state (Part 12) | | |
| **[FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md](./FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md)** | 📊 Query Analysis | 1,185 | ✅ Active |
| | Analysis of 18 SQL files by partitionability | | |
| | 5 query categories with routing strategies | | |
| | Decision tree + performance expectations | | |
| **[FR-082-ARCHITECTURE-COMPARISON.md](./FR-082-ARCHITECTURE-COMPARISON.md)** | 📋 Architecture Decision | 353 | ✅ Active |
| | Single-actor vs hash-partitioned comparison | | |
| | Performance scaling models | | |
| | Recommendation rationale | | |
| **[FR-082-flink-competitive-analysis.md](./FR-082-flink-competitive-analysis.md)** | 🎯 Competitive Analysis | 1,227 | ✅ Active |
| | Velostream V2 vs Apache Flink feature comparison | | |
| | Performance benchmarks + roadmap | | |

### 📊 Performance Analysis & Baselines

| Document | Purpose | Lines | Status |
|----------|---------|-------|--------|
| **[FR-082-BASELINE-MEASUREMENTS.md](./FR-082-BASELINE-MEASUREMENTS.md)** | 📈 Measured Baselines | 350 | ✅ Active |
| | Comprehensive baseline measurements for all scenarios: | | |
| | - Job Server V1: 22.5-23.6K rec/sec | | |
| | - SQL Engine: 548-790K rec/sec | | |
| | - Job server overhead: ~97% (primary bottleneck) | | |
| | - EMIT CHANGES: Only 4.6% overhead vs standard ✅ | | |
| **[FR-082-SCENARIO-CLARIFICATION.md](./FR-082-SCENARIO-CLARIFICATION.md)** | 🔍 Scenario Analysis | 582 | ✅ Active |
| | Distinguishes 4 SQL scenarios with measured baselines: | | |
| | 1. ROWS WINDOW (no GROUP BY) | | |
| | 2. Pure GROUP BY (no WINDOW): 23.4K rec/sec ✅ | | |
| | 3a. GROUP BY + Window (Standard): 23.6K rec/sec ✅ | | |
| | 3b. GROUP BY + Window + EMIT CHANGES: 22.5K rec/sec ✅ | | |
| | Key finding: EMIT CHANGES only 4.6% overhead! | | |
| **[FR-082-overhead-analysis.md](./FR-082-overhead-analysis.md)** | ⚡ Component Overhead | 180 | Reference |
| | Job server vs pure SQL overhead breakdown | | |
| **[FR-082-locking-strategies-analysis.md](./FR-082-locking-strategies-analysis.md)** | 🔒 Locking Strategies | 393 | Reference |
| | Analysis of 5 locking alternatives | | |
| | Option 5 (Local Merge) principles → V2 design | | |

### ⚠️ Superseded Documents (Historical Reference Only)

| Document | Purpose | Why Superseded | Status |
|----------|---------|----------------|--------|
| **[FR-082-IMPLEMENTATION-SUMMARY.md](./FR-082-IMPLEMENTATION-SUMMARY.md)** | Old Implementation Plan | References single-actor V2 architecture | ⚠️ Superseded |
| | 888 lines covering phases 0-7 | Replaced by hash-partitioned design | Nov 6, 2025 |
| **[FR-082-job-server-v2-blueprint.md](./FR-082-job-server-v2-blueprint.md)** | Original V2 Blueprint | Single StateManagerActor design | ⚠️ Superseded |
| | 3,142 lines, comprehensive but outdated | Fixed at 200K rec/sec (serialization bottleneck) | Nov 6, 2025 |

---

## 🎯 Architecture Summary

### Hash-Partitioned Pipeline Design

```
Source Stream
    │
    ▼
Hash Router (by GROUP BY key)
    │
    ├──────────────┬──────────────┬──────────────┐
    ▼              ▼              ▼              ▼
Partition 0    Partition 1    Partition 2    Partition N
[State Mgr]    [State Mgr]    [State Mgr]    [State Mgr]
[200K r/s]     [200K r/s]     [200K r/s]     [200K r/s]
    │              │              │              │
    └──────────────┴──────────────┴──────────────┘
                          │
                          ▼
                    Output Sink
                 N × 200K rec/sec
```

**Key Properties:**
- **Lock-Free**: Each partition owns its state (no Arc<Mutex>)
- **Linear Scaling**: N cores = N × 200K rec/sec throughput
- **Ultra-Low Latency**: p95 <1ms (vs V1: 50-100ms)
- **CPU Efficient**: Core pinning + cache locality

---

## 📊 Performance Targets

### Single-Source Performance (After Phase 0)

| Scenario | V1 Job Server | SQL Engine | V2 Target (8 cores) | Improvement |
|----------|---------------|------------|---------------------|-------------|
| **Scenario 1: ROWS WINDOW** | TBD | TBD | TBD | TBD |
| **Scenario 2: Pure GROUP BY** | 23.4K rec/sec | 548K rec/sec | 1.5M rec/sec | 64x (vs V1) |
| **Scenario 3a: TUMBLING + GROUP BY (Standard)** | 23.6K rec/sec | 790K rec/sec | 1.5M rec/sec | 64x (vs V1) |
| **Scenario 3b: TUMBLING + GROUP BY (EMIT CHANGES)** | 22.5K rec/sec ✅ | ~790K rec/sec | 1.4M rec/sec | 62x (vs V1) |

**Key Finding**: EMIT CHANGES has only 4.6% overhead vs standard emission - excellent for real-time dashboards!

### Multi-Source Horizontal Scaling

| Configuration | Throughput | Scaling Factor |
|---------------|-----------|----------------|
| **Single partition** | 200K rec/sec | 1x |
| **8 partitions (1 machine)** | 1.5M rec/sec | 7.5x |
| **20 sources × 8 cores** | 30M rec/sec | 150x |
| **10 machines** | 300M rec/sec | 1,500x |

### Latency Targets

| Metric | V1 Current | V2 Target | Improvement |
|--------|-----------|-----------|-------------|
| **p50** | 20-50ms | 200µs | 100-250x |
| **p95** | 50-100ms | 500µs | 100-200x |
| **p99** | 100-200ms | 1ms | 100-200x |

---

## 🔬 Query Partitionability Distribution

Based on analysis of 18 SQL files in the codebase:

**Fully Partitionable (72%)**:
- **GROUP BY (Simple)**: 8 files (44%) - Hash by GROUP BY columns
- **GROUP BY + Windows**: 5 files (28%) - Hash by GROUP BY columns

**Two-Phase Aggregation (11%)**:
- **Global Aggregations**: 2 files (11%) - Local aggregate → merge

**Passthrough (17%)**:
- **Pure SELECT**: 3 files (17%) - Round-robin or single partition

**Key Finding**: 13/18 queries (72%) are fully partitionable with linear scaling across N cores.

---

## 🛠️ Implementation Roadmap

### Phase 0: SQL Engine Optimization (Weeks 1-2) ⚠️ PREREQUISITE

**Goal**: Optimize SQL engine GROUP BY from current baseline → 200K rec/sec

**Week 1: Phase 4B** - Hash Table Optimization
- Replace HashMap with FxHashMap
- Implement GroupKey with pre-computed hash
- Target: 3.58K → 15-20K rec/sec

**Week 2: Phase 4C** - Arc State Sharing
- Wrap group states in Arc<FxHashMap>
- Use Arc<StreamRecord> in accumulators
- String interning + key caching
- Target: 15-20K → 200K rec/sec

**Deliverables**:
- ✅ SQL engine GROUP BY at 200K rec/sec baseline
- ✅ Unblocks V2 implementation

---

### Phase 1: Hash-Partitioned V2 (Weeks 3-8)

**Week 3**: Hash Router + Partition State Manager
- Implement hash-based routing by GROUP BY keys
- Lock-free per-partition state management
- Deliverable: Basic partitioning works

**Week 4**: Partitioned Job Coordinator
- Orchestrate N partition managers
- Source/sink pipeline integration
- Deliverable: End-to-end V2 pipeline operational

**Week 5**: Backpressure + Observability
- Per-partition channel monitoring
- Prometheus metrics exposition
- Deliverable: Production-ready monitoring

**Week 6**: System Fields + Watermarks
- Per-partition watermark management
- System field integration (_PARTITION, _TIMESTAMP, etc.)
- Deliverable: Time-based windowing works

**Week 7-8**: Advanced Features
- ROWS WINDOW support (non-GROUP-BY)
- State TTL for long-running jobs
- Checkpoint/recovery mechanism
- Deliverable: Feature-complete V2

**Total Timeline**: 8 weeks (2 weeks Phase 0 + 6 weeks Phase 1)

---

## 🧪 Testing & Validation

### Phase 0 Validation (SQL Engine)

```bash
# Pure GROUP BY (complex - 5 aggregations)
cargo test --tests --no-default-features --release profile_pure_group_by_complex -- --nocapture

# Target: 200K rec/sec after Phase 4C
```

### Phase 1 Validation (V2 Architecture)

```bash
# Single partition (should match Phase 0 baseline)
cargo test --tests v2::partition_single -- --nocapture

# Multi-partition scaling (2, 4, 8, 16 partitions)
cargo test --tests v2::partition_scaling -- --nocapture

# Backpressure handling
cargo test --tests v2::partition_backpressure -- --nocapture

# System fields integration
cargo test --tests v2::system_fields -- --nocapture
```

---

## 📖 Document Reading Order

### For Implementation Team

1. **Architecture Fundamentals**
   - FR-082-job-server-v2-PARTITIONED-PIPELINE.md (Parts 0-3)
   - FR-082-ARCHITECTURE-COMPARISON.md

2. **Query Strategy**
   - FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md
   - FR-082-SCENARIO-CLARIFICATION.md

3. **Implementation Details**
   - FR-082-job-server-v2-PARTITIONED-PIPELINE.md (Parts 4-12)

### For Stakeholders

1. **Executive Summary**
   - README.md (this document)
   - FR-082-ARCHITECTURE-COMPARISON.md (Executive Summary)

2. **Competitive Positioning**
   - FR-082-flink-competitive-analysis.md

3. **Performance Analysis**
   - FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md (Performance Expectations)

---

## 🤝 Contributing

When adding new documents or updates:

1. **Update this README** with new document links in appropriate section
2. **Mark superseded documents** with ⚠️ warnings at the top
3. **Update performance targets** with actual measurements
4. **Link to relevant test files** and benchmarks
5. **Follow naming convention**: `FR-082-[topic]-[type].md`

---

## 📝 Document Status

| Document | Status | Last Updated | Lines | Purpose |
|----------|--------|--------------|-------|---------|
| **PARTITIONED-PIPELINE.md** | ✅ Active | Nov 6, 2025 | 1,815 | Main architecture |
| **QUERY-PARTITIONABILITY.md** | ✅ Active | Nov 6, 2025 | 1,185 | Query analysis |
| **ARCHITECTURE-COMPARISON.md** | ✅ Active | Nov 6, 2025 | 353 | Architecture decision |
| **BASELINE-MEASUREMENTS.md** | ✅ Active | Nov 6, 2025 | 350 | Measured baselines |
| **SCENARIO-CLARIFICATION.md** | ✅ Active | Nov 6, 2025 | 582 | Scenario types |
| **flink-competitive-analysis.md** | ✅ Active | Oct 2025 | 1,227 | Competitive analysis |
| **overhead-analysis.md** | 📚 Reference | Nov 2025 | 180 | Component overhead |
| **locking-strategies.md** | 📚 Reference | Nov 2025 | 393 | Locking analysis |
| **IMPLEMENTATION-SUMMARY.md** | ⚠️ Superseded | Nov 6, 2025 | 888 | Old implementation plan |
| **job-server-v2-blueprint.md** | ⚠️ Superseded | Nov 6, 2025 | 3,142 | Single-actor V2 |

---

**Project**: Velostream Job Server V2 Redesign
**Feature Request**: FR-082 Performance Improvement - Part 2
**Owner**: Architecture Team
**Status**: ✅ Architecture Complete - Ready for Phase 0 (SQL Engine Optimization)
**Last Updated**: November 6, 2025

---

## 🔗 Related Documentation

- **Parent Feature**: `docs/feature/FR-082-perf-part-1/` (Phases 1-3 analysis)
- **Performance Tests**: `tests/performance/` (Benchmark suite)
- **Source Code**: `src/velostream/server/` (V1 implementation)
- **SQL Engine**: `src/velostream/sql/execution/` (Types, aggregation, windowing)
