# SQL Job Command Analysis for velo-test

> **Status**: Partially Implemented
> **Date**: 2025-01-10 (Updated: 2026-01-13)
> **Related**: FR-084 Test Harness

## Executive Summary

This document analyzes the gap between SQL JOB commands (`START JOB`, `STOP JOB`, `DEPLOY JOB`, etc.) and their implementation in `velo-test`. The parser correctly supports these commands, but the test harness bypasses them entirely, using Rust API calls instead.

**Current State**: The test harness uses `QueryExecutor` which calls `StreamJobServer.deploy_job()` directly for all CREATE STREAM/TABLE statements. JOB commands are not specially handled - they fall through to `Other(String)`.

**Recommendation**: Consolidate on a single `StatementType` enum and route JOB commands through `StreamJobServer` methods.

---

## Implementation Status (2026-01-13)

| Recommendation | Status | Notes |
|----------------|--------|-------|
| Contextual keyword parsing for STATUS/METRICS/PROPERTIES | ✅ Done | Correctly uses contextual parsing to avoid field name conflicts |
| Add StopJob/PauseJob/ResumeJob/RollbackJob to app_parser | ❌ Not Done | Only StartJob, DeployJob exist |
| Remove duplicate StatementType enum | ❌ Not Done | Two enums still exist |
| Route JOB commands in StatementExecutor | ❌ Not Done | Falls to `Other(String)` |
| Explicit dependency support | ✅ Done | New `dependencies` field in QueryTest |

---

## 1. Parser TokenType Analysis

### Contextual Keywords (Correct Design)

The following keywords are parsed **contextually** rather than globally reserved. This is **intentional** because they are common field names in data streams:

| Keyword | Context | Why Contextual |
|---------|---------|----------------|
| `STATUS` | `SHOW STATUS` | Common field: `order_status`, `job_status` |
| `METRICS` | `SHOW METRICS` | Common field: `performance_metrics` |
| `PROPERTIES` | `SHOW PROPERTIES` | Common field: `config_properties` |

**Implementation** (parser.rs:3918, 3934, 3950):
```rust
TokenType::Identifier if self.current_token().value.to_uppercase() == "STATUS" => {
    // Only treated as keyword in SHOW context
}
```

**Why NOT to register globally**: Registering these as global keywords would break queries like:
```sql
SELECT status, metrics FROM orders WHERE status = 'active'
```

### TokenTypes Correctly Not Registered (Lexer-Produced)

These are correctly produced by the lexer for special characters/literals:

| Category | TokenTypes |
|----------|------------|
| **Literals** | `Identifier`, `String`, `Number`, `Null` |
| **Punctuation** | `LeftParen`, `RightParen`, `Comma`, `Asterisk`, `Dot`, `Semicolon` |
| **Operators** | `Plus`, `Minus`, `Multiply`, `Divide`, `Concat`, `Equal`, `NotEqual`, `LessThan`, `GreaterThan`, `LessThanOrEqual`, `GreaterThanOrEqual` |
| **Comments** | `SingleLineComment`, `MultiLineComment` |
| **Special** | `Eof` |

---

## 2. SQL Job Commands - Parser Support

The parser **correctly supports** all job lifecycle commands:

| SQL Command | Parser Method | AST Type | Location |
|-------------|---------------|----------|----------|
| `START JOB <name> AS <query>` | `parse_start_job()` | `StreamingQuery::StartJob` | parser.rs:4016 |
| `STOP JOB <name> [FORCE]` | `parse_stop_job()` | `StreamingQuery::StopJob` | parser.rs:4057 |
| `DEPLOY JOB <name> VERSION '<v>' AS <query>` | `parse_deploy_job()` | `StreamingQuery::DeployJob` | parser.rs:4137 |
| `PAUSE JOB <name>` | `parse_pause_job()` | `StreamingQuery::PauseJob` | parser.rs:4100 |
| `RESUME JOB <name>` | `parse_resume_job()` | `StreamingQuery::ResumeJob` | parser.rs:4121 |
| `ROLLBACK JOB <name> [VERSION '<v>']` | `parse_rollback_job()` | `StreamingQuery::RollbackJob` | parser.rs:4221 |

---

## 3. The Dual StatementType Problem

There are **two different `StatementType` enums** in the codebase with different capabilities:

### app_parser::StatementType (Partial)

```rust
// src/velostream/sql/app_parser.rs:254-262
pub enum StatementType {
    CreateStream,
    CreateTable,
    StartJob,      // ✅ Supported
    DeployJob,     // ✅ Supported
    Select,
    Show,
    Other(String),
    // ❌ Missing: StopJob, PauseJob, ResumeJob, RollbackJob
}
```

### statement_executor::StatementType (Incomplete)

```rust
// src/velostream/test_harness/statement_executor.rs:89-95
pub enum StatementType {
    CreateStream,
    CreateTable,
    Select,
    Insert,
    Other(String), // ❌ StartJob/DeployJob fall here
}
```

**Impact**: When `velo-test` executes SQL with `START JOB` or `DEPLOY JOB`, these are classified as `Other("START")` or `Other("DEPLOY")` and are not handled specially.

---

## 4. Current Architecture Flow

### How velo-test Currently Deploys Jobs (Normal Mode)

```
SQL File → QueryExecutor.load_sql_file()
                    │
                    ▼
           parsed_queries HashMap
                    │
                    ▼
           execute_query() for each QueryTest
                    │
                    ▼
           StreamJobServer.deploy_job(name, "1.0.0", sql, topic, ...)
```

**Key observation**: `QueryExecutor` uses `deploy_job()` directly with a hardcoded version `DEFAULT_JOB_VERSION = "1.0.0"`. It does not parse or route JOB commands from SQL.

### How StatementExecutor Works (Step Mode)

```
SQL Statements → StatementExecutor.execute_current()
                          │
                          ▼
                 StatementType::from_sql(sql)
                          │
                          ▼
                 (Falls to Other("START") for job commands)
                          │
                          ▼
                 executor.execute_query(&query)
                          │
                          ▼
                 server.deploy_job(name, version, sql, topic, ...)
```

**Gap**: The `StatementExecutor` doesn't recognize job commands specially - they fall through to `Other(String)`.

### New: Explicit Dependencies (Implemented 2026-01-13)

```yaml
# In test spec YAML
queries:
  - name: vip_orders
    dependencies:
      - vip_customers  # Deployed before vip_orders
    inputs: [...]
```

Dependencies are deployed via `StreamJobServer.deploy_job()` before the main query executes.

---

## 5. Execution Engine Architecture

The execution engine has proper support for job messages:

```rust
// src/velostream/sql/execution/internal.rs:407-418
pub enum ExecutionMessage {
    StartJob {
        job_id: String,
        query: StreamingQuery,
        correlation_id: String,
    },
    StopJob {
        job_id: String,
        correlation_id: String,
    },
    // ...
}
```

```rust
// src/velostream/sql/execution/engine.rs:939-953
match message {
    ExecutionMessage::StartJob { job_id, query, .. } => {
        self.start_query_execution(job_id, query).await?;
    }
    ExecutionMessage::StopJob { job_id, .. } => {
        self.stop_query_execution(&job_id).await?;
    }
    // ...
}
```

### JobProcessor (Stub Implementation)

The `JobProcessor` at `src/velostream/sql/execution/processors/job.rs` exists but is a **stub**:

```rust
// job.rs:29-77
pub fn process_start_job(...) -> Result<Option<StreamRecord>, SqlError> {
    log::info!("Starting job '{}' with properties {:?}", name, properties);

    // In a full implementation, this would:
    // 1. Register the query in the execution engine
    // 2. Start continuous execution
    // 3. Setup monitoring and checkpointing
    // 4. Return success confirmation

    // For now, validate the query and return confirmation
    let mut fields = HashMap::new();
    fields.insert("job_name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("status".to_string(), FieldValue::String("started".to_string()));
    // ... returns status record, doesn't actually start job
}
```

---

## 6. Recommended Solution

### Approach: Consolidate on Single StatementType

**Effort**: ~1 day

| Task | Effort | Description |
|------|--------|-------------|
| Remove duplicate `StatementType` | 30 min | Delete enum from `statement_executor.rs` |
| Import `app_parser::StatementType` | 30 min | Use existing complete enum |
| Add missing types to `app_parser` | 1 hour | Add `StopJob`, `PauseJob`, `ResumeJob`, `RollbackJob` |
| Update `execute_current()` | 2-3 hours | Route JOB commands to `StreamJobServer` |
| Add tests | 1-2 hours | Verify job command execution |

### Why This Is Cleanest

1. **No new architecture** - Uses existing patterns
2. **Single source of truth** - One `StatementType` enum
3. **Follows existing routing** - `deploy_sql_application_with_filename()` already routes by statement type
4. **No confusion** - Developers see one path, not two
5. **Future-proof** - Adding new statement types happens in one place

### What NOT To Do

❌ Add a third execution path in `StatementExecutor`
❌ Make `JobProcessor` call `StreamJobServer` (wrong abstraction level)
❌ Keep two `StatementType` enums with different capabilities
❌ Register STATUS/METRICS/PROPERTIES as global keywords (breaks common field names)

---

## 7. Implementation Plan

### Phase 1: Add Missing StatementTypes to app_parser

```rust
// src/velostream/sql/app_parser.rs
pub enum StatementType {
    CreateStream,
    CreateTable,
    StartJob,
    StopJob,      // ADD
    DeployJob,
    PauseJob,     // ADD
    ResumeJob,    // ADD
    RollbackJob,  // ADD
    Select,
    Show,
    Other(String),
}
```

### Phase 2: Update StatementExecutor

```rust
// src/velostream/test_harness/statement_executor.rs

// Remove local StatementType enum
// Add import:
use crate::velostream::sql::app_parser::StatementType;

// Update execute_current() to handle job commands:
async fn execute_current(&mut self) -> TestHarnessResult<StatementResult> {
    let stmt = &self.statements[self.current_index];

    match stmt.statement_type {
        StatementType::StartJob | StatementType::DeployJob => {
            // Route to StreamJobServer.deploy_job()
        }
        StatementType::StopJob => {
            // Route to StreamJobServer.stop_job()
        }
        StatementType::PauseJob => {
            // Route to StreamJobServer.pause_job()
        }
        StatementType::ResumeJob => {
            // Route to StreamJobServer.resume_job()
        }
        StatementType::CreateStream | StatementType::CreateTable | StatementType::Select => {
            // Existing handling via execute_query()
        }
        // ...
    }
}
```

### ~~Phase 3: Fix Inconsistent TokenType Registration~~ (NO LONGER NEEDED)

**Update 2026-01-13**: This phase has been **removed**. The contextual keyword parsing for STATUS, METRICS, and PROPERTIES is the **correct design** because:

1. These are common field names in data streams
2. Registering them globally breaks queries like `SELECT status FROM orders`
3. The parser correctly handles them contextually in SHOW commands

---

## 8. Files to Modify

| File | Changes |
|------|---------|
| `src/velostream/sql/app_parser.rs` | Add `StopJob`, `PauseJob`, `ResumeJob`, `RollbackJob` to enum |
| `src/velostream/test_harness/statement_executor.rs` | Remove local `StatementType`, import from `app_parser`, update routing |
| `tests/unit/test_harness/statement_executor_test.rs` | Add tests for job command handling |

---

## 9. Verification Checklist

- [x] All job commands parse correctly: `START JOB`, `STOP JOB`, `DEPLOY JOB`, `PAUSE JOB`, `RESUME JOB`, `ROLLBACK JOB`
- [x] `STATUS`, `METRICS`, `PROPERTIES` work as contextual keywords (not globally reserved)
- [x] Explicit dependencies can be declared in test specs
- [ ] `StatementType` correctly identifies all job commands (both enums consolidated)
- [ ] `StatementExecutor` routes job commands to `StreamJobServer`
- [ ] Unit tests cover job command execution
- [ ] Existing tests continue to pass

---

## 10. Related Documentation

- [Parser Grammar](../../../docs/sql/PARSER_GRAMMAR.md) - EBNF grammar for SQL
- [AST Structure](../../../src/velostream/sql/ast.rs) - StreamingQuery variants
- [StreamJobServer](../../../src/velostream/server/stream_job_server.rs) - Job management API
- [StatementExecutor](../../../src/velostream/test_harness/statement_executor.rs) - Interactive execution
- [QueryExecutor](../../../src/velostream/test_harness/executor.rs) - Test harness execution
