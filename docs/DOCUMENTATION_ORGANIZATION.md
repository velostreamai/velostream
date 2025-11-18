# Documentation Organization Guide

## Overview

This document explains how Velostream documentation is organized and where different topics belong.

---

## Folder Structure & Purposes

### `/sql/` - SQL Query Configuration & Execution

**Purpose:** How to write and configure SQL queries, configure job processors via SQL

**Contains:**
- SQL syntax and functions
- Query configuration (annotations, processor modes)
- Error handling strategies (LogAndContinue, FailBatch)
- Dead Letter Queue (from SQL perspective)
- Performance tuning via SQL configuration

**Recent Additions:**
- `job-annotations-guide.md` - SQL annotations (@job_mode, @batch_size, etc.)
- `job-processor-configuration-guide.md` - Processor modes and performance
- `dlq-configuration-guide.md` - **SQL-perspective DLQ** (query configuration impact)
- `logandcontinue-strategy-guide.md` - **SQL failure strategy logging**
- `future-annotations-roadmap.md` - Planned SQL annotations

**When to add docs here:** "How do I configure this via SQL?"

---

### `/data-sources/` - Data Source Infrastructure & Integration

**Purpose:** Kafka, file sources, schema configuration, multi-source integration

**Contains:**
- Kafka configuration and integration
- Data source types and configuration
- Schema registry integration
- Multi-source/sink patterns
- Dead Letter Queue (from infrastructure perspective)

**Current Content:**
- `dlq-and-metrics-guide.md` - **Infrastructure-level DLQ** (implementation details, operational patterns)
- `multi-source-sink-guide.md` - Multiple sources/sinks
- `schema-registry-guide.md` - Avro/Protobuf schemas
- `developer-guide.md` - Building custom sources

**When to add docs here:** "How do I set up this data source?"

---

### `/developer/` - Development & Architecture

**Purpose:** For developers building Velostream or extending it

**Contains:**
- Architecture decisions
- Configuration API reference
- Performance tuning (code-level)
- Development guides

**When to add docs here:** "How does Velostream work internally?"

---

### `/ops/` - Operations & Monitoring

**Purpose:** Running Velostream in production

**Contains:**
- Observability and monitoring
- Health checks
- Deployment guides
- Troubleshooting

**When to add docs here:** "How do I operate this in production?"

---

## Guideline: DLQ Documentation

The DLQ is documented from **two perspectives**:

### 1. Infrastructure Level: `/data-sources/dlq-and-metrics-guide.md`

**Perspective:** Data source operations and infrastructure

**Covers:**
- DLQ implementation details
- Size management and capacity
- Thread-safe operations
- Processor integration matrix
- Monitoring DLQ health
- Recovery patterns

**Audience:** Ops engineers, infrastructure folks, developers building data sources

**Question it answers:** "How does DLQ work at the infrastructure level?"

---

### 2. SQL Configuration Level: `/sql/dlq-configuration-guide.md`

**Perspective:** SQL query configuration and error handling

**Covers:**
- DLQ enabled/disabled by default per processor
- Configuration via SQL annotations (planned)
- Behavior with different failure strategies
- How to access DLQ results
- Use cases and best practices

**Audience:** SQL users, query developers, data engineers writing streaming SQL

**Question it answers:** "How do I configure DLQ for my SQL query?"

---

## Similar Pattern: Failure Strategies

**Failure Strategy Documentation:**

### In `/sql/`:
- `logandcontinue-strategy-guide.md` - **SQL perspective:** What does LogAndContinue log to my application logs?
- `future-annotations-roadmap.md` - **SQL perspective:** How can I configure @failure_strategy in SQL?

### Potential In `/developer/`:
- (Planned) `failure-strategies.md` - **Developer perspective:** How is FailureStrategy implemented?

---

## Organization Principle

**Question-Based Organization:**

| Question | Folder | Document |
|----------|--------|----------|
| How do I write this SQL query? | `/sql/` | `job-annotations-guide.md` |
| How do I configure job mode? | `/sql/` | `job-processor-configuration-guide.md` |
| What gets logged when errors occur? | `/sql/` | `logandcontinue-strategy-guide.md` |
| How is DLQ implemented internally? | `/data-sources/` | `dlq-and-metrics-guide.md` |
| How do I set up Kafka sources? | `/data-sources/` | `multi-source-sink-guide.md` |
| How does Velostream architecture work? | `/developer/` | (Various architecture docs) |
| How do I run Velostream in production? | `/ops/` | (Various ops guides) |

---

## Cross-References

Documents in different folders should cross-reference each other:

### `/sql/dlq-configuration-guide.md`
```markdown
See Also:
- [Infrastructure DLQ Guide](../../data-sources/dlq-and-metrics-guide.md)
  - For implementation details and operational patterns
```

### `/data-sources/dlq-and-metrics-guide.md`
```markdown
See Also:
- [SQL DLQ Configuration](../../sql/dlq-configuration-guide.md)
  - For SQL-level configuration and error handling
```

---

## Current SQL Documentation

### Production-Ready (v0.1)
✅ `job-annotations-guide.md` (861 lines)
- 4 implemented annotations with examples
- Performance impact
- Best practices

✅ `job-processor-configuration-guide.md` (849 lines)
- All 3 processor modes
- Defaults documented
- Performance benchmarks

✅ `dlq-configuration-guide.md` (547 lines)
- SQL perspective on DLQ
- Configuration via code (current)
- Planned: SQL annotations

✅ `logandcontinue-strategy-guide.md` (515 lines)
- Exact logging behavior
- DLQ interaction
- Troubleshooting

✅ `future-annotations-roadmap.md` (579 lines)
- 11+ planned annotations
- Implementation phases
- Priority recommendations

### Supporting
✅ `README.md` (211 lines)
- Index and quick navigation
- References all guides

---

## Data-Sources Documentation

### Current
- `dlq-and-metrics-guide.md` (13,672 bytes)
  - Infrastructure perspective on DLQ
  - Operational patterns
  - Monitoring health

---

## Recommended Organization: NO CHANGES NEEDED ✅

The current organization is **correct**:

1. **SQL documents in `/sql/`** ✅
   - `dlq-configuration-guide.md` - SQL configuration perspective
   - `logandcontinue-strategy-guide.md` - SQL failure strategy logging
   - Audience: SQL developers, data engineers

2. **Infrastructure documents in `/data-sources/`** ✅
   - `dlq-and-metrics-guide.md` - Implementation and operations perspective
   - Audience: Ops engineers, infrastructure developers

3. **Cross-referenced** ✅
   - Both documents exist without duplication
   - Serve different audiences
   - Provide different perspectives on same feature

---

## Future Guidelines

When adding new documentation:

1. **Ask:** "What question does this answer?"
2. **Determine audience:** Developers? Ops? SQL users?
3. **Pick folder:** Matches primary audience and question type
4. **Cross-reference:** Link to related docs in other folders
5. **Avoid duplication:** If topic exists elsewhere, cross-reference instead

---

## Alternative Organization: Consider Moving to `/ops/`

**Note:** The following guides could also belong in `/ops/` if organization by operational concern is preferred:

- `dlq-configuration-guide.md` - Monitoring and error tracking (operational concern)
- `logandcontinue-strategy-guide.md` - Error logging and monitoring (operational concern)

**Pros of `/ops/` location:**
- Groups all error handling/monitoring guides together
- Clear separation: `/sql/` for query writing, `/ops/` for monitoring/troubleshooting
- Aligns with "operational runbook" and "performance-guide" already there

**Pros of `/sql/` location (current):**
- Groups with processor configuration and annotations
- Users writing SQL find everything in one place
- Emphasizes that failure strategy is a query-level configuration

**Recommendation:** Keep in `/sql/` because:
1. Failure strategy is **SQL-configurable** (via @job_mode, @failure_strategy, etc.)
2. DLQ behavior is determined by **SQL query configuration**
3. Users writing SQL are primary audience
4. Following principle: "group by audience and question type"

**If restructuring in future:**
- Could create `/sql/ops/` subdirectory for operational SQL concerns
- Or move all failure strategy + logging to `/ops/` and cross-link extensively

---

## Summary

**Current state: Properly organized** ✅

- `/sql/` documents: SQL configuration and SQL-level behavior
  - `job-annotations-guide.md` - SQL annotations
  - `job-processor-configuration-guide.md` - Processor modes
  - `dlq-configuration-guide.md` - DLQ from SQL perspective
  - `logandcontinue-strategy-guide.md` - Error logging from query perspective

- `/data-sources/` documents: Infrastructure and operations
  - `dlq-and-metrics-guide.md` - DLQ infrastructure perspective

- Both reference DLQ correctly from their perspectives
- No duplication, clear audience separation
- Cross-references enabled for users needing both perspectives
