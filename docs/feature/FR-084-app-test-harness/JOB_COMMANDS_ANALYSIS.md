# SQL Job Command Analysis for velo-test

> **Status**: Analysis Complete
> **Date**: 2025-01-10
> **Related**: FR-084 Test Harness

## Executive Summary

This document analyzes the gap between SQL JOB commands (`START JOB`, `STOP JOB`, `DEPLOY JOB`, etc.) and their implementation in `velo-test`. The parser correctly supports these commands, but the test harness bypasses them entirely, using Rust API calls instead.

**Recommendation**: Consolidate on a single `StatementType` enum and route JOB commands through `StreamJobServer` methods.

---

## 1. Parser TokenType Analysis

### TokenTypes NOT Registered as Keywords

The following `TokenType` variants are defined in the enum but **not registered** in the keyword HashMap. Instead, they rely on string comparison:

| TokenType | Line in Enum | How Currently Handled |
|-----------|--------------|----------------------|
| `Status` | 200 | `TokenType::Identifier if value == "STATUS"` (parser.rs:3925) |
| `Metrics` | 202 | `TokenType::Identifier if value == "METRICS"` (parser.rs:3941) |
| `Properties` | 183 | `TokenType::Identifier if value == "PROPERTIES"` (parser.rs:3957) |

**Issue**: These have dedicated `TokenType` variants but fall back to identifier matching. This is inconsistent with other keywords.

**Fix**: Add these to the keyword HashMap in `StreamingSqlParser::new()`:

```rust
keywords.insert("STATUS".to_string(), TokenType::Status);
keywords.insert("METRICS".to_string(), TokenType::Metrics);
keywords.insert("PROPERTIES".to_string(), TokenType::Properties);
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

### app_parser::StatementType (Complete)

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

**Impact**: When `velo-test` executes SQL with `START JOB` or `DEPLOY JOB`, these are classified as `Other("START")` or `Other("DEPLOY")` and may not be handled correctly.

---

## 4. Current Architecture Flow

### How velo-test Currently Deploys Jobs

```
SQL File → SqlApplicationParser → SqlApplication
                                      │
                                      ▼
                        StreamJobServer.deploy_sql_application_with_filename()
                                      │
                                      ▼
                        (Iterates statements, calls deploy_job() for each)
```

**Key observation**: `deploy_sql_application_with_filename()` at stream_job_server.rs:1418 already handles `StartJob` and `DeployJob` statement types correctly (lines 1542-1544).

### How StatementExecutor Works

```
SQL Statements → StatementExecutor.execute_current()
                          │
                          ▼
                 executor.execute_query(&query)
                          │
                          ▼
                 server.deploy_job(name, version, sql, topic, ...)
```

**Gap**: The `StatementExecutor` doesn't recognize job commands specially - they fall through to `Other(String)`.

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

### Phase 3: Fix Inconsistent TokenType Registration

```rust
// src/velostream/sql/parser.rs - in StreamingSqlParser::new()

// Add missing keyword registrations:
keywords.insert("STATUS".to_string(), TokenType::Status);
keywords.insert("METRICS".to_string(), TokenType::Metrics);
keywords.insert("PROPERTIES".to_string(), TokenType::Properties);
```

---

## 8. Files to Modify

| File | Changes |
|------|---------|
| `src/velostream/sql/app_parser.rs` | Add `StopJob`, `PauseJob`, `ResumeJob`, `RollbackJob` to enum |
| `src/velostream/sql/parser.rs` | Register `STATUS`, `METRICS`, `PROPERTIES` keywords |
| `src/velostream/test_harness/statement_executor.rs` | Remove local `StatementType`, import from `app_parser`, update routing |
| `tests/unit/test_harness/statement_executor_test.rs` | Add tests for job command handling |

---

## 9. Verification Checklist

- [ ] All job commands parse correctly: `START JOB`, `STOP JOB`, `DEPLOY JOB`, `PAUSE JOB`, `RESUME JOB`, `ROLLBACK JOB`
- [ ] `StatementType` correctly identifies all job commands
- [ ] `StatementExecutor` routes job commands to `StreamJobServer`
- [ ] `STATUS`, `METRICS`, `PROPERTIES` keywords are registered
- [ ] Unit tests cover job command execution
- [ ] Existing tests continue to pass

---

## 10. Related Documentation

- [Parser Grammar](../../../docs/sql/PARSER_GRAMMAR.md) - EBNF grammar for SQL
- [AST Structure](../../../src/velostream/sql/ast.rs) - StreamingQuery variants
- [StreamJobServer](../../../src/velostream/server/stream_job_server.rs) - Job management API
- [StatementExecutor](../../../src/velostream/test_harness/statement_executor.rs) - Interactive execution
