# SQL Function Test Coverage Summary

## Quick Overview

- **Total Functions**: 93 built-in SQL functions
- **Test Functions**: 187 existing tests
- **Test Code**: 7,732 lines across 14 files
- **Coverage Quality**: Good for basics, **CRITICAL GAPS for aggregates in expressions**

---

## Coverage by Category

### ✅ EXCELLENT (Comprehensive Tests)

| Category | Functions | Status |
|----------|-----------|--------|
| Math | ABS, ROUND, CEIL, FLOOR, SQRT, POWER, MOD | ✅ Complete |
| String | UPPER, LOWER, SUBSTRING, REPLACE, TRIM, REGEXP | ✅ Complete |
| Comparison | LEAST, GREATEST | ✅ Complete |
| Type Conversion | CAST, COALESCE, NULLIF, CONCAT | ✅ Complete |
| Core Aggregates | COUNT, SUM, AVG, MIN, MAX | ✅ Complete |
| Date/Time | NOW, DATE_FORMAT, EXTRACT, DATEDIFF, UNIX_TIMESTAMP | ✅ Complete |

### ⚠️ GOOD (Partial Tests)

| Category | Functions | Status |
|----------|-----------|--------|
| Advanced Aggregates | STDDEV, VARIANCE, FIRST_VALUE, LAST_VALUE | ⚠️ Basic tests only |
| Statistical | PERCENTILE_CONT, CORR, COVAR, REGR | ⚠️ Boundary tests only |
| Arrays/Maps | ARRAY, MAP, STRUCT, ARRAY_LENGTH, MAP_KEYS | ⚠️ Basic tests only |
| JSON | JSON_EXTRACT, JSON_VALUE | ⚠️ Simplified tests |
| Headers | HEADER, HAS_HEADER, HEADER_KEYS | ⚠️ Basic tests only |

### ❌ CRITICAL GAP (Missing Tests)

| Category | Missing | Priority |
|----------|---------|----------|
| **Aggregates in Expressions** | All 93+ | 🔴 CRITICAL |
| **Window + Aggregate Expressions** | All 93+ | 🔴 CRITICAL |
| **Logical AND/OR with Aggregates** | All 93+ | 🔴 CRITICAL |
| **Type Error Handling** | Most | 🟠 HIGH |
| **ScaledInteger Edge Cases** | Most | 🟠 HIGH |
| **Empty Group Handling** | Aggregates | 🟠 HIGH |

---

## Critical Gap: Aggregates in Expressions

### THE PROBLEM

**Almost NO tests exist for:**
```sql
SELECT STDDEV(price) > AVG(price) * 0.0001     -- ❌ NOT TESTED
SELECT COUNT(*) > 1 AND SUM(amount) < 1000     -- ❌ NOT TESTED
SELECT (SUM(a) + SUM(b)) / COUNT(*) > avg      -- ❌ NOT TESTED
```

### WHY IT MATTERS

These patterns are ESSENTIAL for financial analytics:
- Detecting volatility: `STDDEV(price) > threshold`
- Filtering by aggregates: `COUNT(*) > 1 AND AVG(volume) > min`
- Complex calculations: `(SUM(revenue) - SUM(costs)) / COUNT(*) > margin`

### THE IMPACT

**Blocks FR-079 completion** - Cannot verify aggregate expression fix works correctly without these tests.

---

## Test Files Overview

| File | Lines | Coverage |
|------|-------|----------|
| `new_functions_test.rs` | 700 | General functions |
| `window_functions_test.rs` | 900 | LAG, LEAD, ROW_NUMBER, etc. |
| `date_functions_test.rs` | 800 | Date/time functions |
| `statistical_functions_test.rs` | 600 | STDDEV, VARIANCE, etc. |
| `advanced_analytics_functions_test.rs` | 800 | PERCENTILE, CORR, REGR |
| `advanced_functions_test.rs` | 600 | Complex combinations |
| `math_functions_test.rs` | 600 | Math operations |
| `string_json_functions_test.rs` | 600 | String and JSON |
| `cast_functions_test.rs` | 500 | Type casting |
| `count_distinct_comprehensive_test.rs` | 600 | COUNT DISTINCT |
| `header_functions_test.rs` | 300 | Kafka headers |
| `interval_test.rs` | 400 | INTERVAL handling |
| Others | 600 | Various functions |
| **TOTAL** | **7,732** | |

---

## What Needs to Be Added

### IMMEDIATE (Required for FR-079)

**Aggregate Expression Tests** - 6-8 hours
- Test all aggregates in `expr > literal` patterns
- Test all aggregates in `expr > expr * multiplier` patterns
- Test aggregates in logical AND/OR
- Test aggregates in CASE WHEN
- Test aggregates in window context with EMIT CHANGES

**Example**:
```rust
#[test]
fn test_stddev_in_comparison_expression() {
    // STDDEV(price) > AVG(price) * 0.0001
    // Should use accumulator data, not placeholder 0.0
}

#[tokio::test]
async fn test_aggregate_expressions_with_tumbling_window() {
    // SELECT STDDEV(price) > threshold FROM ...
    // GROUP BY ... WINDOW TUMBLING(1m) EMIT CHANGES
}
```

### SHORT TERM (Next Sprint)

**Edge Case Tests** - 3-4 hours
- NULL value handling across all functions
- Empty group handling for aggregates
- Single value groups (STDDEV = 0)
- Type mismatch error handling

**ScaledInteger Tests** - 4-6 hours
- Financial precision preservation
- Mixed integer/float in aggregations
- Proper rounding and casting

### MEDIUM TERM (Next Month)

**JSON Deep Extraction** - 2-3 hours
- Nested JSON path handling
- Complex selector patterns

**Array/Map Complex Cases** - 2-3 hours
- Nested arrays and maps
- Complex data structures

---

## Quick Stats

### Current Coverage
- ✅ 93 functions defined
- ✅ 187 tests written
- ✅ 7,732 lines of test code
- ✅ All basic operations covered

### Missing Coverage
- ❌ 0 aggregate expression tests (CRITICAL)
- ❌ 0 aggregate + window tests (CRITICAL)
- ⚠️ ~40-50 edge case tests (HIGH)
- ⚠️ ~20-30 error handling tests (MEDIUM)

### Time to Close Gaps
- **Immediate**: 6-8 hours (aggregate expressions)
- **Short term**: 7-10 hours (edge cases + ScaledInteger)
- **Medium term**: 4-6 hours (advanced features)
- **Total**: 17-24 hours

---

## Recommendation

1. **Implement Approach 1** (accumulator threading) - 4 hours
2. **Add aggregate expression tests** - 6-8 hours ← IMMEDIATELY AFTER
3. **Add edge case tests** - 7-10 hours
4. **Add error handling tests** - 3-5 hours

This ensures:
- ✅ Aggregate expressions work correctly
- ✅ All edge cases handled
- ✅ Financial precision maintained
- ✅ FR-079 completion verified

See `docs/SQL_FUNCTION_TEST_PLAN.md` for complete details.
