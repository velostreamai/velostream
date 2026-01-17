# Velostream Learning Path

A structured progression from beginner to advanced streaming SQL.

## Overview

| Level | Time | Prerequisites | Location |
|-------|------|---------------|----------|
| 1. Quickstart | 5 min | Rust only | `demo/quickstart/` |
| 2. File I/O | 15 min | Level 1 | `demo/test_harness_examples/file_io/` |
| 3. Getting Started | 30 min | Docker | `demo/test_harness_examples/getting_started/` |
| 4. SQL Tiers | 1-2 hr | Level 3 | `demo/test_harness_examples/tier1_basic/` → `tier8_fault_tolerance/` |
| 5. Production Demo | 2+ hr | Docker + Kafka knowledge | `demo/trading/` |

---

## Level 1: Quickstart (5 minutes)

**No Docker. No Kafka. Just streaming SQL.**

```bash
cd demo/quickstart
./velo-test.sh                                      # Run all examples
# Or run individually:
../../target/release/velo-test run hello_world.sql -y
cat output/hello_world_output.csv
```

### What You'll Learn
- CREATE STREAM syntax
- SELECT, WHERE, transformations
- Basic aggregations (COUNT, SUM, AVG)
- Introduction to window functions
- Smart commands for generating artifacts

### SQL Examples
| File | Concept |
|------|---------|
| `hello_world.sql` | Passthrough (SELECT *) |
| `01_filter.sql` | WHERE clause |
| `02_transform.sql` | Column calculations |
| `03_aggregate.sql` | GROUP BY, COUNT, SUM |
| `04_window.sql` | LAG, PARTITION BY |

### Smart Commands (Markdown guides)
| File | Concept |
|------|---------|
| `05_generate_test_spec.md` | `velo-test init`, `infer-schema` |
| `06_generate_runner.md` | `velo-test scaffold` |
| `07_observability.md` | `velo-test annotate` + Prometheus/Grafana |
| `08_deploy.md` | `velo-sql deploy-app`, `velo-cli` |

---

## Level 2: File I/O Patterns (15 minutes)

**Still no Kafka, but more realistic patterns.**

```bash
cd demo/test_harness_examples/file_io
../velo-test.sh .
```

### What You'll Learn
- Complex transformations with CASE
- Trade value calculations
- File-based testing patterns

### Examples
| File | Concept |
|------|---------|
| `passthrough.sql` | Full enrichment pipeline |
| `02_filter.sql` | Volume filtering |
| `03_transform.sql` | Symbol grouping with CASE |

---

## Level 3: Getting Started with Kafka (30 minutes)

**Introduces real streaming with Kafka (via Docker/testcontainers).**

```bash
cd demo/test_harness_examples/getting_started
../velo-test.sh .
```

### What You'll Learn
- Kafka source/sink configuration
- Tumbling windows for time-based aggregation
- Debug mode for step-by-step execution

### Examples
| File | Concept |
|------|---------|
| `simple_passthrough.sql` | Kafka passthrough |
| `market_aggregation.sql` | Tumbling window aggregation |
| `debug_demo.sql` | Multi-stage pipeline |

---

## Level 4: SQL Tiers (1-2 hours)

**Systematic coverage of all SQL features.**

```bash
cd demo/test_harness_examples
./velo-test.sh tier1
./velo-test.sh tier2
# ... through tier8
```

### Tier Progression

| Tier | Focus | Key Concepts |
|------|-------|--------------|
| **Tier 1: Basic** | SELECT, WHERE, CAST | Column selection, filtering, type casting, DISTINCT, ORDER BY, LIMIT |
| **Tier 2: Aggregations** | GROUP BY, windows | COUNT, SUM, AVG, tumbling/sliding/session windows, compound keys |
| **Tier 3: Joins** | All join types | LEFT, RIGHT, FULL OUTER JOIN, stream-table, stream-stream |
| **Tier 4: Window Functions** | LAG, LEAD, ROW_NUMBER | ROWS WINDOW BUFFER, PARTITION BY, running aggregates |
| **Tier 5: Complex Patterns** | Pipelines, CASE, UNION | Multi-stage processing, conditional logic, combining streams |
| **Tier 6: Edge Cases** | Nulls, empty input, large volume | Robustness, error handling, late arrivals |
| **Tier 7: Serialization** | JSON, Avro, Protobuf | Binary formats, schema management, format conversion |
| **Tier 8: Fault Tolerance** | DLQ, fault injection | Error handling, resilience testing, debug mode, stress testing |

---

## Level 5: Production Demo (2+ hours)

**Real-world financial trading system with monitoring.**

```bash
cd demo/trading
./start-demo.sh
```

### What You'll Learn
- Multi-query application architecture
- Prometheus/Grafana monitoring
- Event-time processing with watermarks
- Production deployment patterns

### Applications
| App | Purpose |
|-----|---------|
| `app_market_data.sql` | Market data ingestion with watermarks |
| `app_price_analytics.sql` | Price analysis with LAG/LEAD |
| `app_trading_signals.sql` | Trading signal generation |
| `app_risk.sql` | Risk monitoring |
| `app_compliance.sql` | Compliance tracking |

---

## Choosing Your Path

### "I want to see something work immediately"
→ Start with **Level 1: Quickstart**

### "I'm familiar with SQL but new to streaming"
→ Start with **Level 1**, then jump to **Level 3**

### "I need to understand specific SQL features"
→ Go directly to the relevant **Level 4 tier**

### "I need to work with Avro/Protobuf"
→ **Tier 7: Serialization** - Schema-based binary formats

### "I need to build fault-tolerant pipelines"
→ **Tier 8: Fault Tolerance** - DLQ, error handling, resilience testing

### "I want to see a production-like system"
→ **Level 5: Trading Demo** (but expect a learning curve)

---

## Quick Reference

### Build velo-test (one-time)
```bash
cargo build --release --bin velo-test
```

### Validate SQL (no Kafka needed)
```bash
velo-test validate path/to/query.sql
```

### Run with file I/O (no Kafka needed)
```bash
velo-test run path/to/query.sql -y
```

### Run with Kafka (Docker required)
```bash
cd demo/test_harness_examples
./velo-test.sh tier1
```

---

## Next Steps After This Path

- [CLI Reference](./CLI_REFERENCE.md) - All velo-test commands and options
- [Quick Reference](./QUICK_REFERENCE.md) - Cheat sheet for common patterns
- [SQL Grammar Rules](../sql/PARSER_GRAMMAR.md) - Formal SQL syntax
