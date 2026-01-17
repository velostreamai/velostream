# Where Parser's AggregateValidator Should Be Called

**Date**: 2025-10-24
**Status**: Analysis - Implementation Ready

---

## Current State

The parser's `AggregateValidator::validate(&query)?` was **commented out** in `src/velostream/sql/parser.rs:460` because it was too strict for parse-time.

## Correct Placement

The parser's `AggregateValidator` should be called in **SqlValidator.validate_query()** as an **error check** (not just warning).

### Location: `src/velostream/sql/validator.rs:632-694`

```rust
pub fn validate_query(
    &self,
    query: &str,
    query_index: usize,
    start_line: usize,
    _full_content: &str,
) -> QueryValidationResult {
    // ... initialization ...

    // Try to parse the SQL statement
    let parsed_query = match self.parser.parse(query) {
        Ok(q) => q,
        Err(e) => {
            // ... error handling ...
            return result;
        }
    };

    // Analyze the query for data sources and sinks
    match self.analyzer.analyze(&parsed_query) {
        Ok(analysis) => {
            // ... analysis ...
        }
        Err(e) => {
            // ... error handling ...
        }
    }

    // Add subquery validation using parsed AST
    self.validate_subquery_patterns_ast(&parsed_query, &mut result);

    result
}
```

### Where to Add the Call

The parser's `AggregateValidator` should be called **after parsing but before returning results**:

```rust
// After line 691, before returning result:

// Validate aggregate function usage semantics
use crate::velostream::sql::parser::validator::AggregateValidator;

match AggregateValidator::validate(&parsed_query) {
    Ok(_) => {
        // Query is valid
    }
    Err(e) => {
        // Add error to result
        result.parsing_errors.push(ValidationError {
            message: format!("Semantic validation error: {}", e),
            line: Some(start_line),
            column: None,
            severity: ErrorSeverity::Error,
        });
        result.is_valid = false;
    }
}
```

---

## Three-Layer Validation Summary

### Layer 1: Parser (`src/velostream/sql/parser.rs`)
**Current**: No semantic validation ✅
**Responsibility**: Syntax validation only

### Layer 2: SqlValidator (`src/velostream/sql/validator.rs`)
**Current**: `validate_aggregation_rules()` - issues warnings only
**Should Also Have**: Parser's `AggregateValidator` - **issue errors**

**Distinction**:
- `validate_aggregation_rules()` → Issues **WARNINGS** for global aggregation
  - `SELECT COUNT(*) FROM orders` → Warning
- `AggregateValidator::validate()` → Issues **ERRORS** for invalid patterns
  - `SELECT id, COUNT(*) FROM orders` (without GROUP BY) → Error

### Layer 3: Phase 7 AggregationValidator (execution layer)
**Current**: Runtime validation at execution time
**Responsibility**: Enforce semantic correctness during query execution

---

## Why This Matters

### Without Parser's AggregateValidator in SqlValidator

```sql
SELECT id, COUNT(*) FROM orders  -- Missing GROUP BY id
```

- ✅ Parses successfully
- ⚠️ Warning from `validate_aggregation_rules()` about global aggregation
- ❌ Error only at execution time (Phase 7)

### With Parser's AggregateValidator in SqlValidator

```sql
SELECT id, COUNT(*) FROM orders  -- Missing GROUP BY id
```

- ✅ Parses successfully
- ❌ **Error at validation time** (SqlValidator catches it early)
- Prevents bad queries from even reaching execution

---

## Implementation Plan

### Step 1: Add Import to SqlValidator
```rust
// src/velostream/sql/validator.rs:6-13
use crate::velostream::sql::parser::validator::AggregateValidator;
```

### Step 2: Call in validate_query()
```rust
// After line 691, before returning result:
match AggregateValidator::validate(&parsed_query) {
    Ok(_) => {},
    Err(e) => {
        result.parsing_errors.push(ValidationError {
            message: format!("Semantic validation: {}", e),
            line: Some(start_line),
            column: None,
            severity: ErrorSeverity::Error,
        });
        result.is_valid = false;
    }
}
```

### Step 3: Update Comment in Parser
```rust
// src/velostream/sql/parser.rs:459
// IMPORTANT: Aggregate validation is NOT done here at parse time.
// It's called later in SqlValidator.validate_query() to provide
// better error context and timing (pre-execution vs parse time).
```

---

## Validation Flow Diagram

```
Query String
    ↓
[PARSER] (Syntax Validation)
    ├─ Tokenize
    ├─ Build AST
    └─ ✅ No semantic checks (removed AggregateValidator)
    ↓
[SQL VALIDATOR] (Pre-Execution Validation) ← WHERE IT SHOULD BE CALLED
    ├─ Parse successful ✓
    ├─ Analyze sources/sinks
    ├─ Call AggregateValidator::validate() ← ADD THIS
    │   ├─ ❌ Reject: SELECT id, COUNT(*) FROM orders (no GROUP BY)
    │   ├─ ❌ Reject: SELECT AVG(price) FROM orders with mixed columns
    │   └─ ✅ Accept: SELECT COUNT(*) FROM orders (single aggregate)
    ├─ validate_aggregation_rules()
    │   └─ ⚠️ Warning: "Creates global aggregation"
    ├─ validate_subquery_patterns_ast()
    └─ Return ValidationResult
    ↓
[EXECUTION] (Runtime Semantics)
    ├─ Phase 7: AggregationValidator (GROUP BY completeness)
    ├─ Phase 6: TypeValidator
    └─ Phase 8: WindowFrameValidator
```

---

## Benefits of This Approach

1. **Early Error Detection**: Catches semantic errors before execution
2. **Better Error Messages**: SqlValidator has more context
3. **Separation of Concerns**: Parser = syntax, Validator = semantics
4. **User-Friendly**: Users get errors during SQL validation, not at runtime
5. **Two-Level Defense**: Both SqlValidator and Phase 7 catch errors

---

## Questions Answered

### "When should AggregateValidator be called?"
**Answer**: In `SqlValidator.validate_query()` - the SQL validation phase, not parsing phase.

### "Where should this be called? In the SQLValidation phase?"
**Answer**: Yes, exactly! In `SqlValidator.validate_query()` at line ~691, after parsing but before returning results.

### "AggregateValidator::validate(&query)?;"
**Answer**: Should be wrapped in error handling to add errors to the `QueryValidationResult` object.

---

## Status

- ✅ Parser change completed (removed from parse-time)
- ⏳ SqlValidator integration ready for implementation
- ⏳ Tests will validate both validator layers

**Ready for next phase**: Adding AggregateValidator call to SqlValidator when approved.
