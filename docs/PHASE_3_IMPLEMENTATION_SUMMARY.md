# Phase 3 Field Validation - Implementation Summary

## Current State
- **SELECT Processor**: ~1900 lines, 10+ main responsibilities
- **Field Validation**: Phase 2 implemented FieldValidator pattern for window processor only
- **Coverage**: GroupBy and PartitionBy validated; WHERE, HAVING, ORDER BY NOT validated

## What Exists (Phase 2)
```rust
// In window.rs:678
FieldValidator::validate_fields_exist(
    record,
    &group_by_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ValidationContext::GroupBy,
)
.map_err(|e| e.to_sql_error())?;
```

## What's Missing (Phase 3 Target)

### 1. WHERE Clause (lines 393-418)
```rust
// BEFORE: No validation
let where_passed = ExpressionEvaluator::evaluate_expression_with_subqueries(...)?;

// AFTER: Add validation first
FieldValidator::validate_expressions(&joined_record, &[where_expr.clone()], ValidationContext::WhereClause)?
    .map_err(|e| e.to_sql_error())?;
```

### 2. GROUP BY Entry Point (lines 456-464)
```rust
// BEFORE: Routes directly without validation
return Self::handle_group_by_record(query, &joined_record, ...);

// AFTER: Validate first
FieldValidator::validate_expressions(&joined_record, group_exprs, ValidationContext::GroupBy)?
    .map_err(|e| e.to_sql_error())?;
// Then route
```

### 3. SELECT Fields (lines 472-530)
```rust
// BEFORE: Processes without validation
SelectField::Expression { expr, alias } => {
    let value = ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context(...)?;
}

// AFTER: Validate first
SelectField::Expression { expr, alias } => {
    FieldValidator::validate_expressions(&joined_record, &[expr.clone()], ValidationContext::SelectClause)?
        .map_err(|e| e.to_sql_error())?;
    let value = ExpressionEvaluator::evaluate_expression_value_with_alias_and_subquery_context(...)?;
}
```

### 4. HAVING Clause (lines 534-563)
```rust
// BEFORE: No validation
if !ExpressionEvaluator::evaluate_expression_with_subqueries(having_expr, ...)?

// AFTER: Validate against combined scope (original + result fields)
let mut having_fields = joined_record.fields.clone();
having_fields.extend(result_fields.clone());
let having_record = StreamRecord { fields: having_fields, ... };
FieldValidator::validate_expressions(&having_record, &[having_expr.clone()], ValidationContext::HavingClause)?
    .map_err(|e| e.to_sql_error())?;
```

### 5. ORDER BY Processing (NOT IMPLEMENTED)
```rust
// NEW: Add after HAVING, before return
if let Some(order_by_exprs) = order_by {
    let order_fields: Vec<Expr> = order_by_exprs.iter().map(|obe| obe.expr.clone()).collect();
    FieldValidator::validate_expressions(&final_record, &order_fields, ValidationContext::OrderByClause)?
        .map_err(|e| e.to_sql_error())?;
    // TODO: Implement sorting (Phase 4)
}
```

## Test Implementation Structure

### File: `tests/unit/sql/execution/processors/select_validation_test.rs`

```rust
#[test]
fn test_select_where_missing_field() {
    // Parse: SELECT * FROM stream WHERE unknown_field = 5
    // Expect: FieldValidationError::FieldNotFound
    // Error message: "Field 'unknown_field' not found in record during WHERE clause"
}

#[test]
fn test_select_group_by_missing_field() {
    // Parse: SELECT status, COUNT(*) FROM stream GROUP BY unknown_field
    // Expect: FieldValidationError::FieldNotFound
}

#[test]
fn test_select_expression_missing_field() {
    // Parse: SELECT unknown_field + 5 FROM stream
    // Expect: FieldValidationError::FieldNotFound
}

#[test]
fn test_select_having_missing_field() {
    // Parse: SELECT status, COUNT(*) FROM stream GROUP BY status HAVING unknown_field > 10
    // Expect: FieldValidationError::FieldNotFound
}

#[test]
fn test_select_order_by_missing_field() {
    // Parse: SELECT id, name FROM stream ORDER BY unknown_field
    // Expect: FieldValidationError::FieldNotFound
}

#[test]
fn test_select_having_combined_scope() {
    // HAVING should recognize both original fields AND computed fields
    // Example: SELECT id, COUNT(*) as cnt FROM stream GROUP BY id HAVING cnt > 5
    // The 'cnt' alias should be recognized in HAVING clause
}

#[test]
fn test_select_system_columns_validation() {
    // System columns should be accessible: _TIMESTAMP, _OFFSET, _PARTITION
    // SELECT _TIMESTAMP, _OFFSET FROM stream WHERE _PARTITION = 0
    // Should NOT fail field validation
}

#[test]
fn test_select_all_clauses_integration() {
    // Complex query exercising all clauses with validation
    let sql = r#"
        SELECT id, price AS p, p * 2 AS doubled
        FROM trades
        WHERE symbol = 'AAPL'
        GROUP BY symbol
        HAVING COUNT(*) > 5
        ORDER BY doubled DESC
    "#;
    // Should validate all fields across all clauses
}
```

## ValidationContext Enum Changes

**File**: `src/velostream/sql/execution/validation/field_validator.rs`

```rust
pub enum ValidationContext {
    // EXISTING (Phase 2)
    GroupBy,
    PartitionBy,
    
    // NEW (Phase 3)
    SelectClause,       // For SELECT field expressions
    WhereClause,        // For WHERE conditions
    JoinCondition,      // For JOIN ON conditions (explicit)
    Aggregation,        // For aggregate functions
    HavingClause,       // For HAVING post-agg filtering
    OrderByClause,      // For ORDER BY sorting
    WindowFrame,        // For window frame definitions
}
```

## Implementation Order

1. Update ValidationContext enum (add SelectClause, OrderByClause, etc.)
2. Add WHERE clause validation (line 393)
3. Add GROUP BY entry validation (line 422)
4. Add SELECT field validation (line 508)
5. Add HAVING clause validation (line 534)
6. Add ORDER BY validation (new, after HAVING)
7. Create select_validation_test.rs
8. Register test in processors/mod.rs
9. Run full test suite

## Code Pattern (Repeat for each clause)

```rust
// Always follows this pattern:
FieldValidator::validate_expressions(
    &record_to_validate,
    &expressions_to_check,
    ValidationContext::SomeClause,
)
.map_err(|e| e.to_sql_error())?;

// Error conversion is built-in to FieldValidationError::to_sql_error()
```

## Expected Error Messages After Phase 3

```
# WHERE clause missing field:
"Field 'symbol' not found in record during WHERE clause"

# GROUP BY missing field:
"Field 'category' not found in record during GROUP BY clause"

# SELECT expression missing field:
"Multiple fields not found during SELECT clause: unknown_field1, unknown_field2"

# HAVING missing field:
"Field 'computed_value' not found in record during HAVING clause"

# ORDER BY missing field:
"Field 'sort_key' not found in record during ORDER BY clause"
```

## Benefits

1. **Consistent**: Same pattern as Phase 2 (window processor)
2. **Fail-Fast**: Errors caught at validation time, not deep in evaluation
3. **Clear Context**: Error messages specify which clause caused the problem
4. **Comprehensive**: Covers all 5 major clauses (WHERE, GROUP BY, SELECT, HAVING, ORDER BY)
5. **Testable**: Each clause independently validatable
6. **Foundation**: Sets up Phase 4 (ORDER BY sorting) and Phase 5 (type validation)

## Files to Modify

1. `src/velostream/sql/execution/validation/field_validator.rs` - Update ValidationContext
2. `src/velostream/sql/execution/processors/select.rs` - Add 5+ validation calls
3. `tests/unit/sql/execution/processors/select_validation_test.rs` - NEW FILE
4. `tests/unit/sql/execution/processors/mod.rs` - Register new test file

## Effort Estimate

- ValidationContext updates: 5 min
- WHERE validation: 10 min
- GROUP BY validation: 10 min
- SELECT validation: 15 min (loop through fields)
- HAVING validation: 15 min (combined scope)
- ORDER BY validation: 10 min
- Test suite: 60 min
- Testing & verification: 20 min
- **Total: ~2-3 hours**
