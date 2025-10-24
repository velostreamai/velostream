# SELECT Processor Pipeline Analysis & Phase 3 Field Validation Plan

## Executive Summary

The SELECT processor currently handles SELECT statement processing but lacks runtime field validation for WHERE, HAVING, and ORDER BY clauses. Phase 2 established the `FieldValidator` pattern for windowed queries. Phase 3 should apply this same validation pattern to the SELECT processor pipeline for comprehensive field validation coverage.

## 1. Current SelectProcessor Structure

### Location
`/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/processors/select.rs`

### Main Methods

```
pub fn process_with_correlation(
    &mut self,
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
    table_ref: &TableReference,
) -> Result<ProcessorResult, SqlError>

pub fn process(
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<ProcessorResult, SqlError>
```

### Current Responsibilities

1. **Windowed Query Routing** (lines 333-370)
   - Detects windowed queries
   - Routes to WindowProcessor
   - No field validation before routing

2. **LIMIT Processing** (lines 372-377)
   - Checks query limits
   - Basic check without field validation

3. **JOIN Processing** (lines 379-385)
   - Processes JOIN clauses
   - No field validation for joined columns

4. **Correlation Context Management** (lines 387-410)
   - Sets/restores correlation context for subqueries
   - Handles context cleanup

5. **WHERE Clause Evaluation** (lines 393-418)
   - Evaluates WHERE expressions
   - **NO field validation** - relies on ExpressionEvaluator
   - Returns early if WHERE fails

6. **GROUP BY Processing** (lines 422-465)
   - Routes to GROUP BY handler
   - Validates EMIT clause semantics
   - **NO field validation** for GROUP BY expressions

7. **SELECT Fields Processing** (lines 468-531)
   - Projects fields from record
   - Handles wildcards, columns, expressions
   - **NO field validation**

8. **HAVING Clause Processing** (lines 534-563)
   - Evaluates HAVING expressions
   - **NO field validation** - assumes expressions are valid

9. **Header Mutations** (lines 566-570)
   - Collects header mutations from fields

### SQL Clause Structure in StreamingQuery

```rust
Select {
    fields: Vec<SelectField>,
    from: StreamSource,
    from_alias: Option<String>,
    joins: Option<Vec<JoinClause>>,
    where_clause: Option<Expr>,           // NOT VALIDATED
    group_by: Option<Vec<Expr>>,           // NOT VALIDATED
    having: Option<Expr>,                  // NOT VALIDATED
    window: Option<WindowSpec>,
    order_by: Option<Vec<OrderByExpr>>,    // NOT PROCESSED AT ALL
    limit: Option<i64>,
    emit_mode: Option<EmitMode>,
    properties: Option<HashMap<String, String>>,
}
```

## 2. Current Validation Coverage

### Phase 2 FieldValidator Pattern

**Location**: `/Users/navery/RustroverProjects/velostream/src/velostream/sql/execution/validation/field_validator.rs`

**Error Type**: `FieldValidationError` with variants:
- `FieldNotFound { field_name, context }`
- `TypeMismatch { field_name, expected_type, actual_type, context }`
- `MultipleFieldsMissing { field_names, context }`

**Validation Context Enum**:
```rust
enum ValidationContext {
    GroupBy,          // Phase 2: Implemented in window.rs
    PartitionBy,      // Phase 2: Implemented in window.rs
    SelectClause,     // NOT YET USED
    WhereClause,      // NOT YET USED
    JoinCondition,    // NOT YET USED
    Aggregation,      // NOT YET USED
    HavingClause,     // NOT YET USED
    WindowFrame,      // NOT YET USED
}
```

**Core FieldValidator Methods**:
```rust
pub fn validate_field_exists(
    record: &StreamRecord,
    field_name: &str,
    context: ValidationContext,
) -> Result<(), FieldValidationError>

pub fn validate_fields_exist(
    record: &StreamRecord,
    field_names: &[&str],
    context: ValidationContext,
) -> Result<(), FieldValidationError>

pub fn validate_field_type<F>(
    field_name: &str,
    value: &FieldValue,
    expected_type: &str,
    context: ValidationContext,
    type_check_fn: F,
) -> Result<(), FieldValidationError>

pub fn validate_expressions(
    record: &StreamRecord,
    expressions: &[Expr],
    context: ValidationContext,
) -> Result<(), FieldValidationError>

pub fn extract_field_names(expr: &Expr) -> HashSet<String>
```

### Phase 2 Implementation in WindowProcessor

**Location**: `window.rs:678-683`

```rust
// Phase 2: Validate that all GROUP BY fields exist in the record
FieldValidator::validate_fields_exist(
    record,
    &group_by_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ValidationContext::GroupBy,
)
.map_err(|e| e.to_sql_error())?;
```

**Pattern Established**:
1. Extract field names from expression
2. Call FieldValidator::validate_field_exists or validate_expressions
3. Convert FieldValidationError to SqlError via .to_sql_error()
4. Fail fast with ? operator

## 3. Gap Analysis: Missing Field Validation in SELECT Processor

### Gap 1: WHERE Clause Fields Not Validated

**Current Code** (lines 393-418):
```rust
let where_passed = if let Some(where_expr) = where_clause {
    let subquery_executor = SelectProcessor;
    let where_result = ExpressionEvaluator::evaluate_expression_with_subqueries(
        where_expr,
        &joined_record,
        &subquery_executor,
        context,
    );
    where_result?
} else {
    true
};
```

**Issue**: 
- WHERE expression fields are not validated before evaluation
- ExpressionEvaluator will handle missing fields via runtime errors
- Better to fail fast with clear validation errors

**What Should Be Added**:
```rust
if let Some(where_expr) = where_clause {
    FieldValidator::validate_expressions(
        &joined_record,
        &[where_expr.clone()],
        ValidationContext::WhereClause,
    )
    .map_err(|e| e.to_sql_error())?;
    
    // Then evaluate...
}
```

### Gap 2: GROUP BY Expression Fields Not Validated

**Current Code** (lines 456-464): Routes to GROUP BY handler WITHOUT field validation

**Issue**: 
- GROUP BY expressions are not validated before processing
- Field extraction happens inside aggregation logic
- Should validate at entry point like window processor does

**What Should Be Added**:
```rust
if let Some(group_exprs) = group_by {
    FieldValidator::validate_expressions(
        &joined_record,
        group_exprs,
        ValidationContext::GroupBy,
    )
    .map_err(|e| e.to_sql_error())?;
    
    // Then route to handler...
}
```

### Gap 3: SELECT Field Expressions Not Validated

**Current Code** (lines 468-531): Processes fields without validation

**Issue**:
- SelectField::Expression entries are not validated
- Fields might reference non-existent columns
- Validation happens during expression evaluation, not upfront

**What Should Be Added**:
```rust
for field in fields {
    match field {
        SelectField::Expression { expr, alias } => {
            FieldValidator::validate_expressions(
                &joined_record,
                &[expr.clone()],
                ValidationContext::SelectClause,
            )
            .map_err(|e| e.to_sql_error())?;
            
            // Then evaluate...
        }
        // ... other cases
    }
}
```

### Gap 4: HAVING Clause Fields Not Validated

**Current Code** (lines 534-563): Evaluates HAVING without field validation

**Issue**:
- HAVING expressions reference both original and computed fields
- No validation that all referenced fields exist
- Mixed field scope (original + result fields)

**What Should Be Added**:
```rust
if let Some(having_expr) = having {
    // Validate against union of original and result fields
    let mut having_fields = joined_record.fields.clone();
    having_fields.extend(result_fields.clone());
    
    let having_record = StreamRecord {
        fields: having_fields,
        // ...
    };
    
    FieldValidator::validate_expressions(
        &having_record,
        &[having_expr.clone()],
        ValidationContext::HavingClause,
    )
    .map_err(|e| e.to_sql_error())?;
    
    // Then evaluate...
}
```

### Gap 5: ORDER BY Not Processed At All

**Current Code**: No ORDER BY processing anywhere in select.rs

**Issue**:
- ORDER BY clause is parsed but never processed
- Query will complete without sorting results
- Field names in ORDER BY not validated

**What Should Be Added**:
```rust
if let Some(order_by_exprs) = order_by {
    // Extract and validate ORDER BY fields
    let order_by_fields: Vec<Expr> = order_by_exprs
        .iter()
        .map(|obe| obe.expr.clone())
        .collect();
    
    FieldValidator::validate_expressions(
        &final_record,
        &order_by_fields,
        ValidationContext::WhereClause,  // TODO: Add OrderByClause context
    )
    .map_err(|e| e.to_sql_error())?;
    
    // Then apply sorting...
}
```

### Gap 6: JOIN ON Condition Fields Not Validated

**Current Code** (lines 379-385): Processes joins without explicit field validation

**Issue**:
- JOIN processor handles validation internally
- But SELECT processor doesn't validate before calling
- Should follow consistent pattern

### Gap 7: System Columns Not Documented

**Current Code** (lines 479-485): 
```rust
let field_value = match name.to_uppercase().as_str() {
    "_TIMESTAMP" => Some(FieldValue::Integer(joined_record.timestamp)),
    "_OFFSET" => Some(FieldValue::Integer(joined_record.offset)),
    "_PARTITION" => Some(FieldValue::Integer(joined_record.partition as i64)),
    _ => joined_record.fields.get(name).cloned(),
};
```

**Issue**:
- System columns bypass field validation
- Not documented in ValidationContext enum
- SELECT can reference these columns without error

## 4. What Phase 3 Should Accomplish

### Objective
Apply the Phase 2 FieldValidator pattern comprehensively to all SELECT processor clauses

### Phase 3 Deliverables

#### 4.1 Add Missing ValidationContext Variants

```rust
pub enum ValidationContext {
    // ... existing variants
    SelectClause,       // Validate SELECT field expressions
    WhereClause,        // Validate WHERE conditions
    HavingClause,       // Validate HAVING conditions  
    OrderByClause,      // NEW: Validate ORDER BY fields
    JoinCondition,      // Validate JOIN ON conditions
}
```

#### 4.2 Refactored SelectProcessor::process() With Validation

**New Implementation Pattern**:

```
1. Route windowed queries (no changes needed - window processor has validation)
2. Check limits (no field validation needed)
3. Process JOINs with validation
   ├─ Validate JOIN ON expression fields
   └─ Process joins
4. Process WHERE clause with validation
   ├─ Validate WHERE expression fields
   └─ Evaluate WHERE condition
5. Process GROUP BY with validation
   ├─ Validate GROUP BY expression fields
   └─ Route to GROUP BY handler
6. Process SELECT fields with validation
   ├─ For each SelectField::Expression:
   │  ├─ Validate expression fields
   │  └─ Evaluate expression
   └─ Apply field projections
7. Process HAVING clause with validation
   ├─ Validate HAVING expression fields (against combined field scope)
   └─ Evaluate HAVING condition
8. Process ORDER BY with validation
   ├─ Validate ORDER BY expression fields
   └─ Apply sorting
9. Return final result record
```

#### 4.3 New ValidationContext Enum Values

Need to add:
- `SelectClause` - for SELECT field expressions
- `OrderByClause` - for ORDER BY expressions
- Update `WhereClause` if currently missing

#### 4.4 Test Coverage for Phase 3

Create `tests/unit/sql/execution/processors/select_validation_test.rs`:

```rust
#[test]
fn test_select_where_field_validation_missing_field()
    // WHERE references non-existent field -> ValidationError

#[test]
fn test_select_group_by_field_validation_missing_field()
    // GROUP BY references non-existent field -> ValidationError

#[test]
fn test_select_fields_validation_missing_field()
    // SELECT expression references non-existent field -> ValidationError

#[test]
fn test_select_having_field_validation_missing_field()
    // HAVING references non-existent field -> ValidationError

#[test]
fn test_select_order_by_field_validation_missing_field()
    // ORDER BY references non-existent field -> ValidationError

#[test]
fn test_select_field_validation_system_columns()
    // System columns (_TIMESTAMP, _OFFSET, _PARTITION) should be recognized

#[test]
fn test_select_having_validation_combined_scope()
    // HAVING should validate against both original + computed fields

#[test]
fn test_select_all_clauses_field_validation()
    // Integration test with all clauses using FieldValidator
```

#### 4.5 ORDER BY Implementation

Since ORDER BY is not implemented at all:

```rust
fn process_order_by(
    record: StreamRecord,
    order_by_exprs: &[OrderByExpr],
) -> Result<StreamRecord, SqlError> {
    // Validate all ORDER BY fields exist
    let order_fields: Vec<Expr> = order_by_exprs
        .iter()
        .map(|obe| obe.expr.clone())
        .collect();
    
    FieldValidator::validate_expressions(
        &record,
        &order_fields,
        ValidationContext::OrderByClause,
    )
    .map_err(|e| e.to_sql_error())?;
    
    // Apply sorting logic...
    Ok(sorted_record)
}
```

#### 4.6 Refactored process() Method Structure

```rust
pub fn process(
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<ProcessorResult, SqlError> {
    if let StreamingQuery::Select {
        fields,
        from,
        from_alias,
        where_clause,
        joins,
        having,
        limit,
        group_by,
        window,
        order_by,
        emit_mode,
        ..
    } = query {
        // 1. Route windowed queries first
        if let Some(_window_spec) = window {
            // ... existing code (window processor handles validation)
        }
        
        // 2. Check limits
        if let Some(limit_value) = limit {
            // ... existing code
        }
        
        // 3. Process JOINs with validation
        let mut joined_record = record.clone();
        if let Some(join_clauses) = joins {
            // TODO: Add field validation for JOIN conditions
            let join_processor = JoinProcessor::new();
            joined_record = join_processor.process_joins(&joined_record, join_clauses, context)?;
        }
        
        // 4. Setup correlation context
        let table_ref = extract_table_reference_from_stream_source(from, from_alias.as_ref());
        let original_context = context.correlation_context.clone();
        context.correlation_context = Some(table_ref);
        
        // 5. Process WHERE clause with validation
        let where_passed = if let Some(where_expr) = where_clause {
            // NEW: Validate WHERE fields
            FieldValidator::validate_expressions(
                &joined_record,
                &[where_expr.clone()],
                ValidationContext::WhereClause,
            )
            .map_err(|e| e.to_sql_error())?;
            
            let subquery_executor = SelectProcessor;
            ExpressionEvaluator::evaluate_expression_with_subqueries(
                where_expr,
                &joined_record,
                &subquery_executor,
                context,
            )?
        } else {
            true
        };
        
        context.correlation_context = original_context;
        
        if !where_passed {
            return Ok(ProcessorResult {
                record: None,
                header_mutations: Vec::new(),
                should_count: false,
            });
        }
        
        // 6. Process GROUP BY with validation
        if let Some(group_exprs) = group_by {
            // NEW: Validate GROUP BY fields
            FieldValidator::validate_expressions(
                &joined_record,
                group_exprs,
                ValidationContext::GroupBy,
            )
            .map_err(|e| e.to_sql_error())?;
            
            // ... existing GROUP BY handling
        }
        
        // 7. Process SELECT fields with validation
        let mut result_fields = HashMap::new();
        for field in fields {
            match field {
                SelectField::Wildcard => {
                    result_fields.extend(joined_record.fields.clone());
                }
                SelectField::Column(name) => {
                    // Handle system columns and regular columns
                    // ... existing code
                }
                SelectField::AliasedColumn { column, alias } => {
                    // ... existing code
                }
                SelectField::Expression { expr, alias } => {
                    // NEW: Validate expression fields
                    FieldValidator::validate_expressions(
                        &joined_record,
                        &[expr.clone()],
                        ValidationContext::SelectClause,
                    )
                    .map_err(|e| e.to_sql_error())?;
                    
                    // ... existing evaluation code
                }
            }
        }
        
        // 8. Process HAVING clause with validation
        if let Some(having_expr) = having {
            let mut having_fields = joined_record.fields.clone();
            having_fields.extend(result_fields.clone());
            
            let having_record = StreamRecord {
                fields: having_fields,
                timestamp: joined_record.timestamp,
                offset: joined_record.offset,
                partition: joined_record.partition,
                headers: joined_record.headers.clone(),
                event_time: None,
            };
            
            // NEW: Validate HAVING fields against combined scope
            FieldValidator::validate_expressions(
                &having_record,
                &[having_expr.clone()],
                ValidationContext::HavingClause,
            )
            .map_err(|e| e.to_sql_error())?;
            
            // ... existing HAVING evaluation code
        }
        
        // 9. NEW: Process ORDER BY with validation
        let final_record = StreamRecord {
            fields: result_fields,
            timestamp: joined_record.timestamp,
            offset: joined_record.offset,
            partition: joined_record.partition,
            headers: joined_record.headers,
            event_time: None,
        };
        
        if let Some(order_by_exprs) = order_by {
            let order_fields: Vec<Expr> = order_by_exprs
                .iter()
                .map(|obe| obe.expr.clone())
                .collect();
            
            // NEW: Validate ORDER BY fields
            FieldValidator::validate_expressions(
                &final_record,
                &order_fields,
                ValidationContext::OrderByClause,
            )
            .map_err(|e| e.to_sql_error())?;
            
            // NEW: Apply sorting (phase 3 only adds validation, sorting implementation in phase 4)
            // TODO: Implement ORDER BY sorting logic
        }
        
        // 10. Collect header mutations and return
        let mut header_mutations = Vec::new();
        Self::collect_header_mutations_from_fields(
            fields,
            &joined_record,
            &mut header_mutations,
        )?;
        
        Ok(ProcessorResult {
            record: Some(final_record),
            header_mutations,
            should_count: true,
        })
    } else {
        Err(SqlError::ExecutionError {
            message: "Invalid query type for SelectProcessor".to_string(),
            query: None,
        })
    }
}
```

## 5. Implementation Checklist for Phase 3

- [ ] Add `SelectClause` to ValidationContext enum
- [ ] Add `OrderByClause` to ValidationContext enum (or reuse WhereClause temporarily)
- [ ] Add WHERE clause field validation in SelectProcessor::process()
- [ ] Add GROUP BY field validation in SelectProcessor::process()
- [ ] Add SELECT field expression validation in SelectProcessor::process()
- [ ] Add HAVING clause field validation in SelectProcessor::process()
- [ ] Add ORDER BY field validation in SelectProcessor::process()
- [ ] Create select_validation_test.rs with comprehensive test coverage
- [ ] Register test file in tests/unit/sql/execution/processors/mod.rs
- [ ] Update ValidationContext Display impl with new variants
- [ ] Verify error messages are clear and actionable
- [ ] Run full test suite: `cargo test --lib --no-default-features`
- [ ] Run pre-commit checks: `cargo fmt --all -- --check && cargo clippy --all-targets --no-default-features`

## 6. Benefits of Phase 3 Implementation

1. **Fail-Fast Validation**: Field errors caught at query execution time, not deep in expression evaluation
2. **Consistent Pattern**: Same validation approach as Phase 2 (window processor)
3. **Clear Error Messages**: ValidationContext provides specific clause context in error messages
4. **Comprehensive Coverage**: All SELECT clauses (WHERE, HAVING, GROUP BY, SELECT, ORDER BY) validated
5. **Foundation for Phase 4**: Clear validation enables ORDER BY implementation
6. **Type Safety**: Integrates with FieldValue type system for future type validation
7. **Maintainability**: Centralized validation logic is easier to test and extend

## 7. Future Phases

- **Phase 4**: Implement ORDER BY sorting logic (foundation laid by validation in Phase 3)
- **Phase 5**: Add type validation (expected numeric vs actual string, etc.) using `validate_field_type()`
- **Phase 6**: Enhance aggregation validation (aggregate functions only in SELECT/HAVING)
- **Phase 7**: Window frame validation for complex RANGE/ROWS specifications

## Summary

The SELECT processor currently lacks field validation for most SQL clauses. Phase 2 established the FieldValidator pattern in window.rs, which should be applied systematically to SELECT processing. Phase 3 should add validation gates at each clause entry point, providing consistent error messages and fail-fast behavior. This sets the foundation for ORDER BY implementation (Phase 4) and type validation (Phase 5).
