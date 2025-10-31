# Comprehensive SQL Function Test Plan

## Executive Summary

- **Total Functions**: 93 built-in SQL functions
- **Existing Tests**: 187 test functions across 14 test files
- **Test Coverage**: ~7,732 lines of test code
- **Status**: Good coverage with some gaps in edge cases and aggregate expression tests

---

## Table of Contents

1. [Test File Organization](#test-file-organization)
2. [Function Categories & Test Status](#function-categories--test-status)
3. [Critical Gap: Aggregates in Expressions](#critical-gap-aggregates-in-expressions)
4. [Existing Test Files Summary](#existing-test-files-summary)
5. [Comprehensive Test Plan by Category](#comprehensive-test-plan-by-category)
6. [Missing Test Coverage](#missing-test-coverage)
7. [Priority Testing](#priority-testing)

---

## Test File Organization

| File | Lines | Focus |
|------|-------|-------|
| `new_functions_test.rs` | 700+ | General function tests |
| `window_functions_test.rs` | 900+ | Window functions (LAG, LEAD, ROW_NUMBER, etc.) |
| `date_functions_test.rs` | 800+ | Date/time functions |
| `statistical_functions_test.rs` | 600+ | STDDEV, VARIANCE, statistical aggregates |
| `advanced_analytics_functions_test.rs` | 800+ | PERCENTILE, CORR, COVAR, REGR functions |
| `advanced_functions_test.rs` | 600+ | Complex function combinations |
| `math_functions_test.rs` | 600+ | ABS, ROUND, CEIL, etc. |
| `string_json_functions_test.rs` | 600+ | String and JSON functions |
| `cast_functions_test.rs` | 500+ | Type casting functions |
| `count_distinct_comprehensive_test.rs` | 600+ | COUNT DISTINCT scenarios |
| `header_functions_test.rs` | 300+ | Kafka header functions |
| `interval_test.rs` | 400+ | INTERVAL type handling |
| Other files | ~600+ | Various other functions |

---

## Function Categories & Test Status

### 1. Aggregate Functions (18 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| COUNT | ✅ Complete | Multiple | COUNT(*), COUNT(col), window context |
| SUM | ✅ Complete | Multiple | Integer, float, ScaledInteger |
| AVG | ✅ Complete | Multiple | Average calculation |
| MIN | ✅ Complete | Multiple | Minimum value |
| MAX | ✅ Complete | Multiple | Maximum value |
| FIRST_VALUE | ⚠️ Partial | Some | Window function support |
| LAST_VALUE | ⚠️ Partial | Some | Window function support |
| MEDIAN | ⚠️ Partial | Few | Limited streaming support |
| STDDEV | ⚠️ Partial | Some | ❌ **MISSING**: Aggregate expression tests |
| STDDEV_POP | ⚠️ Partial | Few | Missing aggregate expression tests |
| VARIANCE | ⚠️ Partial | Some | Missing aggregate expression tests |
| VAR_POP | ⚠️ Partial | Few | Missing aggregate expression tests |
| VAR_SAMP | ⚠️ Partial | Few | Missing aggregate expression tests |
| APPROX_COUNT_DISTINCT | ⚠️ Partial | Few | Basic tests only |
| COUNT_DISTINCT | ✅ Complete | Multiple | Comprehensive coverage |
| LISTAGG | ⚠️ Partial | Few | Array input handling |
| STRING_AGG | ⚠️ Partial | Few | Basic tests only |

### 2. Statistical Functions (8 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| PERCENTILE_CONT | ⚠️ Partial | Few | Boundary validation only |
| PERCENTILE_DISC | ⚠️ Partial | Few | Boundary validation only |
| CORR | ⚠️ Partial | Few | Placeholder implementation |
| COVAR_POP | ⚠️ Partial | Few | Placeholder implementation |
| COVAR_SAMP | ⚠️ Partial | Few | Placeholder implementation |
| REGR_SLOPE | ⚠️ Partial | Few | Placeholder implementation |
| REGR_INTERCEPT | ⚠️ Partial | Few | Placeholder implementation |
| REGR_R2 | ⚠️ Partial | Few | Placeholder implementation |

**Note**: These are partially implemented with placeholder values. Real window aggregation context needed.

### 3. Math Functions (9 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| ABS | ✅ Complete | Multiple | Integer, float |
| ROUND | ✅ Complete | Multiple | Various precision levels |
| CEIL | ✅ Complete | Multiple | Ceiling operations |
| CEILING | ✅ Complete | Multiple | Alias for CEIL |
| FLOOR | ✅ Complete | Multiple | Floor operations |
| SQRT | ✅ Complete | Multiple | Square root, negative handling |
| POWER | ✅ Complete | Multiple | Various exponents |
| POW | ✅ Complete | Multiple | Alias for POWER |
| MOD | ✅ Complete | Multiple | Modulo, division by zero |

### 4. String Functions (14 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| UPPER | ✅ Complete | Multiple | Unicode handling |
| LOWER | ✅ Complete | Multiple | Unicode handling |
| SUBSTRING | ✅ Complete | Multiple | With/without length |
| REPLACE | ✅ Complete | Multiple | Pattern replacement |
| TRIM | ✅ Complete | Multiple | Whitespace handling |
| LTRIM | ✅ Complete | Multiple | Left trim |
| RTRIM | ✅ Complete | Multiple | Right trim |
| LENGTH | ✅ Complete | Multiple | Character counting |
| LEN | ✅ Complete | Multiple | Alias for LENGTH |
| SPLIT | ⚠️ Partial | Few | Delimiter handling |
| JOIN | ⚠️ Partial | Few | Multiple argument joining |
| LEFT | ⚠️ Partial | Few | Substring from left |
| RIGHT | ⚠️ Partial | Few | Substring from right |
| REGEXP | ✅ Complete | Multiple | Regex pattern matching |

### 5. Date/Time Functions (9 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| NOW | ✅ Complete | Multiple | Current timestamp |
| CURRENT_TIMESTAMP | ✅ Complete | Multiple | Alias for NOW |
| TIMESTAMP | ✅ Complete | Multiple | System timestamp |
| TUMBLE_START | ⚠️ Partial | Some | Window boundary extraction |
| TUMBLE_END | ⚠️ Partial | Some | Window boundary extraction |
| DATE_FORMAT | ✅ Complete | Multiple | Format patterns |
| FROM_UNIXTIME | ✅ Complete | Multiple | Unix timestamp conversion |
| UNIX_TIMESTAMP | ✅ Complete | Multiple | Timestamp to Unix |
| EXTRACT | ✅ Complete | Multiple | Date part extraction |
| DATEDIFF | ✅ Complete | Multiple | Date difference calculation |

### 6. Type/Conversion Functions (6 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| CAST | ✅ Complete | Multiple | All type conversions |
| COALESCE | ✅ Complete | Multiple | NULL handling |
| NULLIF | ✅ Complete | Multiple | NULL comparison |
| ARRAY | ⚠️ Partial | Few | Array construction |
| STRUCT | ⚠️ Partial | Few | Struct construction |
| MAP | ⚠️ Partial | Few | Map construction |

### 7. Array/Map Functions (5 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| ARRAY_LENGTH | ⚠️ Partial | Few | Array size |
| ARRAY_CONTAINS | ⚠️ Partial | Few | Element search |
| MAP_KEYS | ⚠️ Partial | Few | Key extraction |
| MAP_VALUES | ⚠️ Partial | Few | Value extraction |
| CONCAT | ✅ Complete | Multiple | String concatenation |

### 8. JSON Functions (2 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| JSON_EXTRACT | ⚠️ Partial | Few | Simplified implementation |
| JSON_VALUE | ⚠️ Partial | Few | Simplified implementation |

### 9. Header Functions (5 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| HEADER | ⚠️ Partial | Some | Kafka header retrieval |
| HEADER_KEYS | ⚠️ Partial | Some | Header key listing |
| HAS_HEADER | ⚠️ Partial | Some | Header existence check |
| SET_HEADER | ⚠️ Partial | Few | Header value setting |
| REMOVE_HEADER | ⚠️ Partial | Few | Header value removal |

### 10. Comparison Functions (2 total)

| Function | Status | Tests | Notes |
|----------|--------|-------|-------|
| LEAST | ✅ Complete | Multiple | Minimum of multiple values |
| GREATEST | ✅ Complete | Multiple | Maximum of multiple values |

---

## Critical Gap: Aggregates in Expressions

### The Problem

**Very few tests exist for aggregate functions used in expressions**:

```sql
-- These patterns are BARELY tested:
SELECT STDDEV(price) > AVG(price) * 0.0001  -- Expression with aggregates
SELECT COUNT(*) > 1 AND SUM(amount) < 1000  -- Logical AND with aggregates
SELECT CASE WHEN AVG(price) > 100 THEN 'expensive' END  -- CASE with aggregates
```

### Where Tests Should Be

The biggest gap is in **`emit_changes_test.rs`** where windowed GROUP BY with aggregates in expressions should be tested:

```rust
#[tokio::test]
async fn test_emit_changes_with_aggregate_expressions() {
    // ❌ Test for: STDDEV(price) > AVG(price) * threshold
    // ❌ Test for: COUNT(*) > 1 AND MAX(volume) < limit
    // ❌ Test for: (SUM(a) + SUM(b)) > total
}
```

---

## Existing Test Files Summary

### 1. **new_functions_test.rs** (700 lines)
- Basic function evaluation
- NULL handling
- Type conversions
- Edge cases for common functions

### 2. **window_functions_test.rs** (900 lines)
- Window frame functions (LAG, LEAD, ROW_NUMBER, RANK)
- Partition and ordering
- Multiple window specifications
- Frame exclusion clauses

### 3. **date_functions_test.rs** (800 lines)
- NOW, CURRENT_TIMESTAMP
- DATE_FORMAT with various patterns
- FROM_UNIXTIME, UNIX_TIMESTAMP
- EXTRACT with all date parts
- DATEDIFF with different units

### 4. **statistical_functions_test.rs** (600 lines)
- STDDEV, VARIANCE calculations
- Boundary conditions (single value, NULL values)
- VAR_POP, STDDEV_POP

### 5. **advanced_analytics_functions_test.rs** (800 lines)
- PERCENTILE_CONT, PERCENTILE_DISC
- CORR, COVAR_POP, COVAR_SAMP
- REGR_SLOPE, REGR_INTERCEPT, REGR_R2
- Boundary validation

### 6. **advanced_functions_test.rs** (600 lines)
- Complex function combinations
- Nested function calls
- Multiple aggregates in one query

### 7. **math_functions_test.rs** (600 lines)
- ABS, ROUND, CEIL, FLOOR
- SQRT with negative numbers
- POWER with various exponents
- MOD with division by zero

### 8. **string_json_functions_test.rs** (600 lines)
- UPPER, LOWER case conversion
- SUBSTRING with length
- REPLACE pattern matching
- JSON_EXTRACT, JSON_VALUE

### 9. **cast_functions_test.rs** (500 lines)
- CAST to all target types
- Type coercion
- Invalid cast handling

### 10. **count_distinct_comprehensive_test.rs** (600 lines)
- COUNT vs COUNT DISTINCT
- Multiple groups
- NULL handling
- Edge cases

### 11. **header_functions_test.rs** (300 lines)
- HEADER retrieval
- HEADER_KEYS listing
- HAS_HEADER checking

### 12. **interval_test.rs** (400 lines)
- INTERVAL type handling
- Window BETWEEN INTERVAL '...' syntax
- Edge cases for intervals

---

## Comprehensive Test Plan by Category

### A. HIGH PRIORITY: Fix Aggregate Expression Testing

#### Phase 1: Basic Aggregate Expressions (CRITICAL)
```rust
#[test]
fn test_count_in_expression() {
    // COUNT(*) > 1, COUNT(col) > 0, etc.
}

#[test]
fn test_stddev_in_expression() {
    // STDDEV(col) > threshold
    // STDDEV(col) > AVG(col) * 0.0001
}

#[test]
fn test_avg_in_expression() {
    // AVG(col) < limit
    // AVG(col) * multiplier
}

#[test]
fn test_sum_in_expression() {
    // SUM(col) > total
    // SUM(a) + SUM(b) > combined
}

#[test]
fn test_min_max_in_expression() {
    // MAX(col) > AVG(col)
    // MIN(col) < percentile
}
```

#### Phase 2: Complex Aggregate Expressions
```rust
#[test]
fn test_aggregate_in_logical_and() {
    // COUNT(*) > 1 AND SUM(amount) < 1000
}

#[test]
fn test_aggregate_in_logical_or() {
    // AVG(price) > 100 OR COUNT(*) < 5
}

#[test]
fn test_aggregate_in_case_expression() {
    // CASE WHEN AVG(price) > 100 THEN 'expensive' END
}

#[test]
fn test_nested_aggregates() {
    // (SUM(a) + SUM(b)) / COUNT(*) > average
}
```

#### Phase 3: Aggregate Expressions with Windows
```rust
#[test]
fn test_emit_changes_with_aggregate_expressions() {
    // STDDEV(price) > AVG(price) * 0.0001 with TUMBLING window
}

#[test]
fn test_aggregate_expressions_sliding_window() {
    // Same tests but with SLIDING window
}

#[test]
fn test_aggregate_expressions_session_window() {
    // Same tests but with SESSION window
}
```

### B. HIGH PRIORITY: Statistical Functions in Expressions

```rust
#[test]
fn test_percentile_in_expression() {
    // PERCENTILE_CONT(0.95) > threshold
}

#[test]
fn test_correlation_in_expression() {
    // CORR(x, y) > 0.5
}

#[test]
fn test_regression_in_expression() {
    // REGR_SLOPE(y, x) > 0 (uptrend)
}

#[test]
fn test_covariance_in_expression() {
    // COVAR_SAMP(x, y) != NULL
}
```

### C. MEDIUM PRIORITY: Edge Cases for All Functions

```rust
#[test]
fn test_all_functions_with_null_input() {
    // Each function tested with NULL input
}

#[test]
fn test_all_functions_with_empty_group() {
    // Aggregate functions with empty groups
}

#[test]
fn test_all_functions_type_mismatches() {
    // Each function with wrong type arguments
}

#[test]
fn test_all_functions_with_scaled_integers() {
    // Financial precision with ScaledInteger
}

#[test]
fn test_all_functions_with_mixed_types() {
    // Integer + Float in same aggregation
}
```

### D. MEDIUM PRIORITY: Window-Specific Aggregate Tests

```rust
#[test]
fn test_aggregates_with_partition_by() {
    // Aggregates in windowed queries with PARTITION BY
}

#[test]
fn test_aggregates_with_order_by_frame() {
    // Aggregates with ORDER BY and frame specifications
}

#[test]
fn test_aggregates_with_emit_changes() {
    // Per-record emission with aggregates
}
```

### E. LOW PRIORITY: Missing Basic Coverage

```rust
#[test]
fn test_split_function() {
    // String splitting with various delimiters
}

#[test]
fn test_join_function() {
    // String joining with multiple arguments
}

#[test]
fn test_array_operations() {
    // ARRAY_CONTAINS, ARRAY_LENGTH edge cases
}

#[test]
fn test_map_operations() {
    // MAP_KEYS, MAP_VALUES with complex maps
}

#[test]
fn test_json_deep_extraction() {
    // JSON_EXTRACT with nested paths
}
```

---

## Missing Test Coverage

### Critical Gaps (Must Fix)

1. **Aggregates in Expressions** (0 tests)
   - Any aggregate function used in comparison/logical expression
   - Example: `STDDEV(price) > AVG(price) * 0.0001`
   - Impact: HIGH - blocks FR-079 completion

2. **Statistical Functions with Real Data** (Placeholder tests only)
   - PERCENTILE_CONT, PERCENTILE_DISC, CORR, COVAR_*, REGR_*
   - Currently return placeholder values in streaming context
   - Need window aggregation context to test properly

3. **Aggregate Expression Evaluation in Windows** (0 tests)
   - Aggregates in SELECT with GROUP BY and WINDOW
   - Aggregates in HAVING clause
   - Aggregates in ORDER BY

### Important Gaps (Should Fix)

4. **Type Mismatch Error Handling**
   - What happens when function receives wrong type?
   - Which functions have proper error messages?

5. **ScaledInteger Support**
   - Do all math functions handle ScaledInteger?
   - Do all aggregates preserve financial precision?

6. **Edge Cases**
   - Empty groups
   - Single value groups (STDDEV should be 0)
   - All NULL values
   - Mixed integer/float inputs

### Nice-to-Have Gaps (Can Fix Later)

7. **Complex JSON Extraction**
   - Nested JSON path extraction
   - Complex selector patterns

8. **Array/Map Operations**
   - Nested arrays
   - Complex map structures

---

## Priority Testing

### IMMEDIATE (Required for FR-079)

1. **Aggregates in Expressions**
   - Time: 4-6 hours
   - Impact: CRITICAL
   - Files: `emit_changes_test.rs`, new test file

2. **STDDEV and Statistical Functions in Expressions**
   - Time: 2-3 hours
   - Impact: CRITICAL
   - Files: `statistical_functions_test.rs`, `emit_changes_test.rs`

### SHORT TERM (Next Sprint)

3. **Aggregate Expression Tests with All Window Types**
   - Time: 3-4 hours
   - Impact: HIGH
   - Files: `window_functions_test.rs`

4. **Error Handling for Type Mismatches**
   - Time: 2-3 hours
   - Impact: HIGH
   - Files: All function test files

### MEDIUM TERM (Next Month)

5. **ScaledInteger Comprehensive Tests**
   - Time: 4-6 hours
   - Impact: MEDIUM
   - Files: All function test files

6. **Complex Edge Cases**
   - Time: 3-4 hours
   - Impact: MEDIUM
   - Files: All function test files

---

## Summary

### Current State
- ✅ 187 test functions across 14 files
- ✅ 7,732 lines of test code
- ✅ Good coverage for basic function operations
- ⚠️ Gaps in aggregate expressions
- ⚠️ Gaps in error handling
- ⚠️ Gaps in edge cases

### What's Needed
1. **Aggregate Expression Tests** (CRITICAL)
   - Currently: 0 tests
   - Needed: 20-30 tests
   - Time: 6-8 hours
   - Files: `emit_changes_test.rs`, new test file

2. **Statistical Function Tests** (HIGH)
   - Currently: Placeholder tests only
   - Needed: Real data tests
   - Time: 3-4 hours
   - Files: `statistical_functions_test.rs`

3. **Edge Case Tests** (MEDIUM)
   - Currently: Scattered coverage
   - Needed: Systematic coverage
   - Time: 4-6 hours
   - Files: All function test files

### Total Gap Analysis
- **Missing Tests**: 40-50 test functions
- **Time to Add**: 15-20 hours
- **Priority**: HIGH (blocks FR-079 completion)

---

## Recommendation

**Implement Approach 1 (Accumulator Threading) FIRST**, then add comprehensive aggregate expression tests to verify the fix works correctly across all scenarios.

The test gaps highlighted here will be automatically resolved once aggregates can access the accumulator context properly.
