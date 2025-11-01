# FR-079: Implementation Analysis - STDDEV Function Dispatch

**Date:** October 23, 2025

---

## Executive Summary & Fix Plan

### Problem Identified
Phase 6 implementation is **incomplete**. It threaded the GroupAccumulator through `window.rs:evaluate_aggregate_expression()`, but SELECT field evaluation for GROUP BY queries bypasses this and uses SelectProcessor without accumulator access.

### Root Cause
For the failing test `test_emit_changes_with_tumbling_window_same_window`:
- **Wrong implementation being called:** `functions.rs:stddev_function()` (returns 0.0, no state)
- **Should be called:** `window.rs:evaluate_aggregate_expression()` (uses accumulator with real group data)
- **Why:** SELECT field processing goes through SelectProcessor line 513 → ExpressionEvaluator → functions.rs instead of window.rs

### Root Cause Deep Dive

After analysis, the real problem is **NOT in WindowProcessor** — it's in **SelectProcessor line 931**:

```rust
// SelectProcessor::handle_group_by_record() line 931
// For non-function expressions (like STDDEV(price) > AVG(price) * 0.0001):
let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
// ↑ Evaluates on SINGLE RECORD with NO ACCUMULATOR = STDDEV returns 0.0
```

**The Problem:**
- **Lines 707-921:** Handle simple aggregate functions (COUNT, SUM, AVG, STDDEV directly)
- **Lines 922-933:** Handle non-function expressions (BinaryOp, arithmetic, boolean)
  - These use single-record evaluation
  - **Missing:** Accumulator-aware evaluation for expressions like `STDDEV(price) > AVG(price) * 0.0001`

---

## Phase 7: SelectProcessor Accumulator Integration

### Recommended Fix (Much Simpler Than Options A-C)

**Instead of creating new WindowProcessor evaluator, fix SelectProcessor directly:**

**Location:** `src/velostream/sql/execution/processors/select.rs:922-933`

**Implementation:**
1. Create helper method `evaluate_group_expression_with_accumulator()` in SelectProcessor (~25 lines)
   - Routes aggregate functions through accumulator
   - Falls back to ExpressionEvaluator for non-aggregates

2. Replace line 931 logic to use the helper with accumulator parameter

3. Pass accumulator from line 631 to line 931 evaluation

**LoE:** 3-4 hours total
- Analysis: 15 min
- Create helper method: 30 min
- Modify integration point: 15 min
- Logging & debugging: 15 min
- Tests: 45 min
- Full test suite: 15 min
- Compilation & fixes: 30 min

### Why Phase 7 Instead of Options A-C?

| Approach | LoE | Complexity | Maintenance |
|----------|-----|-----------|------------|
| **Phase 7: Fix SelectProcessor** | 3-4 hrs | Low | Easy - single location |
| **Option A: New WindowProcessor evaluator** | 8-10 hrs | High | Duplication concerns |
| **Option B: Extend ExpressionEvaluator** | 5-6 hrs | Medium | Couples modules |
| **Option C: Interceptor Pattern** | 6-8 hrs | Medium-High | Extra abstraction |

**Phase 7 is RECOMMENDED** — most efficient, lowest risk, single location to fix

---

## Question Asked
> "should it [stddev_function] be called without state? or is the wrong impl being called?"

For test: `test_emit_changes_with_tumbling_window_same_window` which uses:
```sql
SELECT STDDEV(price) > AVG(price) * 0.0001 FROM orders
WINDOW TUMBLING(1m) GROUP BY status EMIT CHANGES
```

---

## The Two STDDEV Implementations

### 1. **Single-Record Fallback** (`src/velostream/sql/execution/expression/functions.rs:1821-1846`)

```rust
fn stddev_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
    // ... validation ...
    let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
    match value {
        FieldValue::Null => Ok(FieldValue::Null),
        FieldValue::Integer(_) | FieldValue::Float(_) => {
            // For streaming, return 0.0 since we only have one value
            // In a real implementation, this would calculate over a window of values
            Ok(FieldValue::Float(0.0))  // ← Always returns 0.0 for single value!
        }
        _ => Err(SqlError::ExecutionError { ... })
    }
}
```

**Purpose:** Single-record evaluation for non-windowed contexts
**State:** None (stateless - operates on one record)
**Return:** Always `0.0` for any numeric value

---

### 2. **Windowed Aggregation Implementation** (`src/velostream/sql/execution/processors/window.rs:1883-1937`)

```rust
fn evaluate_aggregate_expression(
    expr: &Expr,
    records: &[&StreamRecord],
    alias_context: &SelectAliasContext,
    accumulator: Option<&GroupAccumulator>,  // ← Takes accumulator!
) -> Result<FieldValue, SqlError> {
    // ... handles STDDEV, VARIANCE, etc. ...
    match name.to_uppercase().as_str() {
        "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" => {
            // Try to get field name from column
            let field_name = match &args[0] {
                Expr::Column(col_name) => col_name.clone(),
                _ => {
                    // Fallback for non-column expressions
                    return ExpressionEvaluator::evaluate_expression_value(...);
                }
            };

            // Use accumulator if available (Phase 6)
            if let Some(acc) = accumulator {
                if let Some(values) = acc.numeric_values.get(&field_name) {
                    if values.len() < 2 {
                        return Ok(FieldValue::Null);
                    }

                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    let variance = values.iter()
                        .map(|v| (v - mean).powi(2))
                        .sum::<f64>() / (values.len() - 1) as f64;
                    let stddev = variance.sqrt();

                    Ok(FieldValue::Float(stddev))  // ← Real STDDEV from group data
                }
            } else {
                // No accumulator - fallback to single record evaluation
                ExpressionEvaluator::evaluate_expression_value(...)
            }
        }
    }
}
```

**Purpose:** Windowed GROUP BY aggregation
**State:** GroupAccumulator with numeric_values HashMap
**Return:** Real STDDEV computed from actual group values

---

## Implementation Chain for Windowed GROUP BY

The correct execution path for the test query:

```
test SQL:
  SELECT STDDEV(price) > AVG(price) * 0.0001 FROM orders
  WINDOW TUMBLING(1m) GROUP BY status EMIT CHANGES

        ↓

process_windowed_query() [line 123]
  └─ is_emit_changes = true
  └─ group_by_cols = Some(["status"])

        ↓

process_windowed_group_by_emission() [line 894]
  └─ Split buffer into groups by status column
  └─ For each group:
       └─ compute_group_aggregate() [line 732]
            └─ execute_windowed_aggregation_impl() [line 1057]
                 └─ Build accumulator from group records [line 1127]
                      └─ build_accumulator_from_records() [line 1025]
                           └─ Populates accumulator.numeric_values HashMap

                 └─ Process SELECT fields [line 1167]
                      └─ evaluate_aggregate_expression(expr, records, context, Some(&group_accumulator))
                           └─ For STDDEV > AVG * 0.0001 (binary op) [line 2035]
                                ├─ Recursively evaluate left: STDDEV(price)
                                │   └─ Uses accumulator.numeric_values["price"] for real computation
                                ├─ Recursively evaluate right: AVG(price) * 0.0001
                                │   └─ Uses accumulator for AVG, then multiplies
                                └─ Apply > operator to results
```

---

## Answer to the Question

### Should `functions.rs:stddev_function` be called without state?

**NO.** `stddev_function` should **NOT** be called for this test case.

For windowed GROUP BY + EMIT CHANGES queries:
- **Must use:** `window.rs:evaluate_aggregate_expression()` with accumulator
- **Must NOT use:** `functions.rs:stddev_function()` (fallback only)

### Is the wrong implementation being called?

**Unknown.** The code architecture looks correct:

✅ **What's correct:**
- Phase 6 implements accumulator building (line 1127)
- Phase 3 implements binary operator recursion with accumulator (line 2035-2044)
- GroupAccumulator threading is complete through expression evaluation chain
- Test setup includes proper GROUP BY and WINDOW clauses

❌ **What's failing:**
- Test produces 0 results instead of expected 5+ results
- This is a **result collection issue, NOT an expression evaluation issue**

### Root Cause: WRONG STDDEV IMPLEMENTATION PATH

**CRITICAL FINDING:** The query is being processed through **SelectProcessor line 513**, NOT through WindowProcessor's `evaluate_aggregate_expression()`.

**Call Path:**
1. `engine.execute_with_record()` → `execute_internal()` at line 541
2. Detects window → calls `WindowProcessor::process_windowed_query()` at line 575
3. WindowProcessor processes GROUP BY + EMIT CHANGES logic
4. When processing SELECT fields, calls `SelectProcessor` at line 513 via `ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context()`
5. This bypasses WindowProcessor's `evaluate_aggregate_expression()`
6. Routes STDDEV to `functions.rs:stddev_function()` (returns 0.0)
7. Binary operator `>` evaluates as `0.0 > 0.000001` = FALSE
8. Expression evaluates to FALSE instead of real result

**Why getting 0 results:**
- The SELECT field evaluation returns FALSE for `STDDEV(price) > AVG(price) * 0.0001` due to STDDEV returning 0.0
- This causes some filtering/logic to exclude the result
- OR the computed results don't pass through properly

**The STDDEV implementation IS the problem.** Phase 6 threaded the accumulator through window.rs:evaluate_aggregate_expression, but there's a **separate code path through SelectProcessor** that doesn't have accumulator access.

---

## The Core Issue

**Phase 6 implementation is INCOMPLETE.** It threaded the accumulator through `window.rs:evaluate_aggregate_expression()`, but there's a **second code path** that processes SELECT fields through `SelectProcessor`:

```rust
// SelectProcessor line 513 - called for GROUP BY + EMIT CHANGES SELECT fields
let value = ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context(
    expr,
    &joined_record,
    &alias_context,
    &subquery_executor,  // ← SelectProcessor as subquery executor
    context,
)?;
```

This code path:
- Does NOT pass the GroupAccumulator
- Does NOT have access to group state
- Routes aggregate functions through the generic ExpressionEvaluator
- Which dispatches STDDEV to `functions.rs:stddev_function()` (returns 0.0)

---

## Phase 7: SelectProcessor Accumulator Integration - Implementation Details

### Helper Method to Create

**Location:** `src/velostream/sql/execution/processors/select.rs`

**Method signature:**
```rust
fn evaluate_group_expression_with_accumulator(
    expr: &Expr,
    accumulator: &GroupAccumulator,
    record: &StreamRecord,
) -> Result<FieldValue, SqlError> {
    // Detect aggregate functions in expression
    // If aggregate function found: use accumulator data
    // Otherwise: fall back to ExpressionEvaluator
}
```

**Implementation approach:**
1. Pattern match on `expr` type:
   - `Expr::Function { name, args }` → Extract from accumulator
   - `Expr::BinaryOp { left, right, op }` → Recursively evaluate both sides with accumulator
   - Other expressions → Fall back to `ExpressionEvaluator::evaluate_expression_value()`

2. Use similar logic to `window.rs:evaluate_aggregate_expression()` (lines 1735-2140)
   - Handle STDDEV, VARIANCE, COUNT, SUM, AVG, MIN, MAX
   - Recursively handle binary operators

### Integration Point

**Location:** `src/velostream/sql/execution/processors/select.rs:922-933`

**Current code:**
```rust
} else {
    // Handle non-function expressions (e.g., boolean expressions, arithmetic)
    let field_name = if let Some(alias_name) = alias {
        alias_name.clone()
    } else {
        Self::get_expression_name(expr)
    };

    // Evaluate the expression using the original record
    let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
    result_fields.insert(field_name, value);
}
```

**New code:**
```rust
} else {
    // Handle non-function expressions with accumulator support
    let field_name = if let Some(alias_name) = alias {
        alias_name.clone()
    } else {
        Self::get_expression_name(expr)
    };

    // Use accumulator-aware evaluation for GROUP BY context
    let value = Self::evaluate_group_expression_with_accumulator(
        expr,
        accumulator,  // Already available from line 631
        record,
    )?;
    result_fields.insert(field_name, value);
}
```

### Test Cases to Add

Create `tests/unit/sql/execution/processors/select/fr079_group_by_aggregate_expressions_test.rs`:

1. **Basic aggregate expression:**
   ```sql
   SELECT status, COUNT(*) > 1 as has_multiple FROM orders GROUP BY status
   ```

2. **STDDEV comparison:**
   ```sql
   SELECT status, STDDEV(price) > AVG(price) * 0.5 as volatile FROM orders GROUP BY status
   ```

3. **Complex boolean with aggregates:**
   ```sql
   SELECT status, COUNT(*) > 1 AND AVG(price) > 100 as valid FROM orders GROUP BY status
   ```

4. **Binary arithmetic with aggregates:**
   ```sql
   SELECT status, (MAX(price) - MIN(price)) / AVG(price) as volatility FROM orders GROUP BY status
   ```

### Verification

After implementation:
1. `test_emit_changes_with_tumbling_window_same_window` should produce 5+ results
2. Expressions like `STDDEV(price) > AVG(price) * 0.0001` should compute real values
3. All existing GROUP BY tests should continue to pass (no regressions)
4. Non-aggregate expressions should still work correctly

