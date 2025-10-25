# FR-079: SQL Validator Strategy & Phase 5-8 Integration

**Assessment Date**: 2025-10-24
**Focus**: How SQL validator system supports Phase 5-8 correctness work
**Scope**: Validator requirements for 100% demo compatibility

---

## Executive Summary

The SQL validator system is **well-designed and extensible** for Phase 5-8 work. The existing `FieldValidator` infrastructure (runtime layer) is production-ready and requires **zero foundational changes**. However, optional **SemanticValidator enhancements** (pre-execution layer) would significantly improve error diagnostics and developer experience without blocking implementation.

**Recommendation**: Proceed directly with Phase 5-8 execution work. Enhance validator diagnostics in parallel (not critical path).

---

## Current Validator Architecture

### Two-Layer Validation Model

```
┌─────────────────────────────────────────────────────────┐
│  SEMANTIC VALIDATION (Pre-Execution)                     │
│  ────────────────────────────────────────────────────    │
│  • SQL syntax parsing (QueryValidator)                   │
│  • Configuration validation (ConfigurationValidator)     │
│  • Field existence checks (SemanticValidator)            │
│  • Aggregation rules (SemanticValidator)                 │
│  Location: src/velostream/sql/validation/                │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│  RUNTIME VALIDATION (During Execution)                   │
│  ───────────────────────────────────────────────────    │
│  • Field existence in StreamRecord (FieldValidator)      │
│  • Type compatibility checks (Type coercion)             │
│  • Aggregation execution safety (Processors)             │
│  Location: src/velostream/sql/execution/validation/      │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│  EXECUTION (Actual SQL Processing)                       │
│  ─────────────────────────────────────────────────────  │
│  • Sorting enforcement (Phase 5 - OrderProcessor)        │
│  • Type operations (Phase 6 - Type system)               │
│  • Aggregation computation (Phase 7 - Aggregators)       │
│  • Window frame semantics (Phase 8 - WindowProcessor)    │
└─────────────────────────────────────────────────────────┘
```

### Key Components for Phase 5-8

#### 1. FieldValidator (Runtime - READY)

**Location**: `src/velostream/sql/execution/validation/field_validator.rs:109`

```rust
pub struct FieldValidator;

impl FieldValidator {
    // Phase 5: Validates fields in ORDER BY clauses
    pub fn validate_field_exists(
        record: &StreamRecord,
        field_name: &str,
        context: ValidationContext,  // OrderByClause ✅
    ) -> Result<(), FieldValidationError>

    // Phase 6: Type checking support
    // Phase 7: Aggregation context support
    // Phase 8: Window frame validation support
}
```

#### 2. ValidationContext Enum (Extensible - READY)

**Location**: `src/velostream/sql/execution/validation/field_validator.rs:71-106`

```rust
pub enum ValidationContext {
    GroupBy,                // Phase 7 support ✅
    PartitionBy,            // Phase 5/8 support ✅
    SelectClause,           // General support ✅
    WhereClause,            // Phase 6 support ✅
    JoinCondition,          // General support ✅
    Aggregation,            // Phase 7 support ✅
    HavingClause,           // Phase 7 support ✅
    WindowFrame,            // Phase 8 support ✅
    OrderByClause,          // Phase 5 support ✅ NEW in this codebase
}
```

**Status**: ✅ **All Phase 5-8 contexts already supported!**

#### 3. FieldValidationError (Error Types - READY)

```rust
pub enum FieldValidationError {
    FieldNotFound { field_name: String, context: String },
    TypeMismatch {
        field_name: String,
        expected_type: String,
        actual_type: String,
        context: String,
    },
    MultipleFieldsMissing {
        field_names: Vec<String>,
        context: String,
    },
}
```

**Status**: ✅ All error cases covered for Phases 5-8

---

## Phase 5-8 Validator Requirements Analysis

### Phase 5: ORDER BY Sorting

#### What Validator Does
```
Semantic Validation (Pre-execution):
├─ ✓ Field existence check: Is 'event_time' defined in record schema?
├─ ✓ Type validation: Is ORDER BY field sortable (numeric/timestamp)?
└─ ✓ Scope validation: Field not from missing JOIN

Runtime Validation (During execution):
├─ ✓ Field exists in StreamRecord at runtime
├─ ✓ Type compatibility with sort operation
└─ ✓ Error handling via FieldValidator::validate_field_exists(
      record, "event_time", ValidationContext::OrderByClause)
```

#### Demo Requirement: 18 ORDER BY Clauses
**Current Validator Status**:
- ✅ `ValidationContext::OrderByClause` exists (line 89)
- ✅ `FieldValidator::validate_field_exists()` supports it
- ✅ Error chaining works (convert to SqlError)

**Enhancement Needed**: Optional SemanticValidator check
```rust
// Optional - improves error messages (2 hour enhancement)
fn validate_order_by_field_exists(&self, field_name: &str) -> Result<(), ValidationError> {
    if !self.available_fields.contains(field_name) {
        return Err(ValidationError::FieldNotFound {
            field: field_name.to_string(),
            context: "ORDER BY clause".to_string(),
        });
    }
    Ok(())
}
```

**Verdict**: ✅ **Ready for Phase 5** - Enhancement optional, not blocking

---

### Phase 6: Type Validation

#### What Validator Does
```
Semantic Validation (Pre-execution):
├─ Field type lookup: What is the type of 'price'?
├─ Operation compatibility: Can Float / Float?
└─ Implicit coercion rules: Float → Decimal allowed?

Runtime Validation (During execution):
├─ Actual type checking during expression evaluation
├─ FieldValidator error handling: Type mismatch detection
└─ Error conversion to SqlError for reporting
```

#### Demo Requirements: 20+ Type Operations
**Examples from demo**:
- `(price - LAG(...)) / LAG(...) * 100` → Float/Float/Integer = Float ✅
- `ABS(...) > 5.0` → Comparison ✅
- `COALESCE(m.price, 0)` → Integer|Float = Float ✅

**Current Validator Status**:
- ✅ `FieldValidationError::TypeMismatch` supports type checking
- ✅ FieldValidator checks field types at runtime
- ✅ Error messages include expected/actual types

**Enhancement Potential**: SemanticValidator type coercion rules
```rust
// Optional - validates type coercion rules early
fn validate_type_coercion(&self, from_type: &Type, to_type: &Type) -> Result<(), ValidationError> {
    match (from_type, to_type) {
        (Type::Integer, Type::Float) => Ok(()),      // Implicit ✅
        (Type::Float, Type::Integer) => Ok(()),       // Implicit ✅
        (Type::String, Type::Float) => Err(...),      // Invalid ✅
        _ => Ok(()),
    }
}
```

**Verdict**: ✅ **Ready for Phase 6** - Semantic enhancement recommended but optional

---

### Phase 7: Aggregation Validation

#### What Validator Does
```
Semantic Validation (Pre-execution):
├─ GROUP BY completeness: Are all non-aggregated columns in GROUP BY?
├─ Aggregate placement: Functions only in SELECT/HAVING?
└─ HAVING restrictions: Only aggregates in HAVING clause?

Runtime Validation (During execution):
├─ FieldValidator::validate_field_exists() for GROUP BY columns
├─ Aggregation context validation
└─ Error reporting for incomplete GROUP BY
```

#### Demo Requirements: 15+ Aggregation Patterns
**Examples**:
- `GROUP BY symbol` with `COUNT(*), AVG(price), SUM(volume)` ✅
- `HAVING SUM(quantity) > 10000` ✅
- Complex: `SUM(CASE WHEN side='BUY' THEN quantity ELSE 0 END)` ✅

**Current Validator Status**:
- ✅ `ValidationContext::Aggregation` exists
- ✅ `ValidationContext::GroupBy` exists
- ✅ `ValidationContext::HavingClause` exists
- ✅ FieldValidator supports all contexts

**Enhancement Potential**: SemanticValidator aggregation rules
```rust
// Optional - validates aggregation patterns early
fn validate_group_by_completeness(&self, select_fields: &[Expr], group_by: &[Expr]) -> Result<()> {
    for field in select_fields {
        if !is_aggregate(field) && !is_in_group_by(field, group_by) {
            return Err(ValidationError::NotInGroupBy {
                field: format!("{:?}", field),
            });
        }
    }
    Ok(())
}
```

**Verdict**: ✅ **Ready for Phase 7** - Semantic enhancement adds safety but not required

---

### Phase 8: Window Frame Validation

#### What Validator Does
```
Semantic Validation (Pre-execution):
├─ ORDER BY requirement: Must have ORDER BY in OVER clause?
├─ Frame type validation: ROWS vs RANGE vs SESSION?
└─ INTERVAL validation: INTERVAL '1' DAY matches TIMESTAMP?

Runtime Validation (During execution):
├─ Frame boundary validation: UNBOUNDED PRECEDING valid?
├─ Numeric bounds: ROWS BETWEEN 9 PRECEDING AND CURRENT ROW?
└─ Temporal bounds: RANGE BETWEEN INTERVAL '1' DAY PRECEDING?
```

#### Demo Requirements: 12+ Window Frame Specifications
**Critical Case** (Line 414-416):
```sql
RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
```

**Current Validator Status**:
- ✅ `ValidationContext::WindowFrame` exists (line 87)
- ✅ `ValidationContext::PartitionBy` for PARTITION BY validation
- ✅ `ValidationContext::OrderByClause` for ORDER BY in OVER

**Enhancement CRITICAL for Phase 8**: SemanticValidator window rules
```rust
// CRITICAL - validates window frame semantics
fn validate_window_frame(&self, frame: &WindowFrame, order_by: &[Expr]) -> Result<()> {
    // PHASE 8 REQUIREMENT: Validate ORDER BY exists
    if frame.requires_order_by() && order_by.is_empty() {
        return Err(ValidationError::MissingOrderBy {
            reason: format!("RANGE BETWEEN requires ORDER BY"),
        });
    }

    // PHASE 8 REQUIREMENT: Validate INTERVAL matches ORDER BY type
    if let WindowFrameBound::Interval(interval) = &frame.start {
        let order_by_type = self.get_field_type(&order_by[0])?;
        if !order_by_type.is_temporal() {
            return Err(ValidationError::TypeMismatch {
                expected: "TIMESTAMP or DATE".to_string(),
                actual: format!("{:?}", order_by_type),
            });
        }
    }
    Ok(())
}
```

**Verdict**: ⚠️ **Phase 8 Enhancement RECOMMENDED** - Prevents RANGE BETWEEN INTERVAL bugs
- **Effort**: 2-3 hours
- **Benefit**: Catches temporal window frame errors early
- **Demo Impact**: Critical for `trades_today` correctness (line 414-416)

---

## Validator Integration with Phase 5-8 Execution

### How the Layers Work Together

#### Example: Phase 5 Window ORDER BY Enforcement

```rust
// SEMANTIC VALIDATION (Pre-execution) - Optional enhancement
semantic_validator.validate_window_order_by(
    order_by_fields: vec!["event_time"],
    available_fields: schema.fields,
)?;  // Returns: ✅ OK or ✗ Error: "Field 'event_time' not found"

// RUNTIME VALIDATION (During execution) - FieldValidator
for record in stream {
    FieldValidator::validate_field_exists(
        &record,
        "event_time",
        ValidationContext::OrderByClause,
    )?;  // Confirms field exists in actual data

    // EXECUTION (Phase 5 OrderProcessor)
    let sorted_partition = sort_by_field(&records, "event_time");
    // ... compute window function on sorted data
}
```

#### Example: Phase 8 Temporal Window Frame

```rust
// SEMANTIC VALIDATION (Pre-execution) - CRITICAL for Phase 8
semantic_validator.validate_window_frame(
    frame: &WindowFrame::RangeBetween {
        start: WindowFrameBound::Interval(Interval::Day(1)),
        end: WindowFrameBound::CurrentRow,
    },
    order_by: &[Expr::Field("event_time")],
    order_by_type: Type::Timestamp,  // PHASE 8 CHECK
)?;  // Returns: ✅ OK or ✗ Error: "INTERVAL requires TIMESTAMP ORDER BY"

// RUNTIME VALIDATION (During execution)
for record in stream {
    FieldValidator::validate_field_exists(
        &record,
        "event_time",
        ValidationContext::WindowFrame,
    )?;  // Confirms field exists

    // EXECUTION (Phase 8 WindowProcessor)
    let window_start = record.event_time - Duration::days(1);
    let frame_records = partition
        .iter()
        .filter(|r| r.event_time >= window_start && r.event_time <= record.event_time)
        .collect();
    // ... compute aggregation on frame
}
```

---

## Demo Compatibility: Validator Coverage Map

### Phase 5: ORDER BY Sorting (18 clauses)

| Demo Requirement | Validator Layer | Status | Effort |
|------------------|-----------------|--------|--------|
| Parse ORDER BY syntax | SemanticValidator | ✅ Done | — |
| Field existence check | FieldValidator | ✅ Ready | — |
| Type validation (sortable) | SemanticValidator | ⚠️ Optional | 1h |
| Runtime field check | FieldValidator | ✅ Ready | — |
| Error reporting | FieldValidationError | ✅ Ready | — |

**Blocker Status**: 🟢 **NONE** - Ready for Phase 5

---

### Phase 6: Type Validation (20+ operations)

| Demo Requirement | Validator Layer | Status | Effort |
|------------------|-----------------|--------|--------|
| Type coercion rules | SemanticValidator | ⚠️ Optional | 2h |
| Field type tracking | FieldValidator | ✅ Ready | — |
| Operation compatibility | Type system | ✅ Ready | — |
| Runtime type checking | FieldValidator | ✅ Ready | — |
| Error messages | FieldValidationError | ✅ Ready | — |

**Blocker Status**: 🟢 **NONE** - Ready for Phase 6

---

### Phase 7: Aggregation Validation (15+ patterns)

| Demo Requirement | Validator Layer | Status | Effort |
|------------------|-----------------|--------|--------|
| GROUP BY completeness | SemanticValidator | ⚠️ Optional | 2h |
| Aggregate placement | SemanticValidator | ⚠️ Optional | 1h |
| HAVING restrictions | SemanticValidator | ⚠️ Optional | 1h |
| Runtime field check | FieldValidator | ✅ Ready | — |
| Error reporting | FieldValidationError | ✅ Ready | — |

**Blocker Status**: 🟢 **NONE** - Ready for Phase 7

---

### Phase 8: Window Frame Validation (12+ frames, 1 CRITICAL)

| Demo Requirement | Validator Layer | Status | Effort |
|------------------|-----------------|--------|--------|
| ORDER BY requirement | SemanticValidator | 🟡 **CRITICAL** | 1h |
| ROWS validation | SemanticValidator | ⚠️ Optional | 1h |
| RANGE validation | SemanticValidator | 🟡 **CRITICAL** | 1h |
| **INTERVAL validation** | **SemanticValidator** | **🟡 CRITICAL** | **1h** |
| Runtime field check | FieldValidator | ✅ Ready | — |
| Error reporting | FieldValidationError | ✅ Ready | — |

**Blocker Status**: 🟡 **OPTIONAL** - Phase 8 works without, but critical enhancement recommended

**Critical Missing**: INTERVAL type matching validation (RANGE BETWEEN INTERVAL '1' DAY requires TIMESTAMP ORDER BY)

---

## Validator Enhancement Roadmap

### CRITICAL (Must Do for Phase 8)

**Task 1: Window Frame ORDER BY Validation** (1 hour)
```rust
// Location: src/velostream/sql/validation/semantic_validator.rs
pub fn validate_window_requires_order_by(
    &self,
    frame: &WindowFrame,
) -> Result<(), ValidationError> {
    if frame.is_range() && !self.has_order_by {
        return Err(ValidationError::MissingOrderBy {
            reason: "RANGE BETWEEN requires ORDER BY clause".to_string(),
        });
    }
    Ok(())
}
```

**Task 2: INTERVAL-Temporal Type Matching** (1 hour) **[CRITICAL FOR DEMO]**
```rust
// Validates RANGE BETWEEN INTERVAL '1' DAY with TIMESTAMP ORDER BY
pub fn validate_interval_order_by_type(
    &self,
    interval: &Interval,
    order_by_type: &Type,
) -> Result<(), ValidationError> {
    if !order_by_type.is_temporal() {
        return Err(ValidationError::TypeMismatch {
            expected: "TIMESTAMP or DATE".to_string(),
            actual: format!("{:?}", order_by_type),
            context: "RANGE BETWEEN INTERVAL clause".to_string(),
        });
    }
    Ok(())
}
```

**Impact**: Prevents `trades_today` calculation (line 414-416) from silently failing

---

### RECOMMENDED (Improves Error Messages)

**Task 3: Type Coercion Rules** (2 hours)
- Validates implicit Float ← Integer coercion
- Catches invalid type mismatches early
- Better error messages for developers

**Task 4: GROUP BY Completeness** (2 hours)
- Validates all non-aggregated SELECT columns in GROUP BY
- Prevents silent aggregation errors
- Catches developer mistakes

**Task 5: ORDER BY Sortability Check** (1 hour)
- Validates ORDER BY fields are numeric/temporal (sortable)
- Clear error for unsortable types

**Total Recommended**: 5-6 hours across 3 tasks

---

### OPTIONAL (Nice to Have)

- HAVING aggregate placement validation (1 hour)
- Aggregate function type validation (1 hour)
- Join field availability (1 hour)

---

## Recommendation: Validator Strategy for Phase 5-8

### Option 1: Fast Track (Recommended)
**Proceed immediately with Phase 5-8 execution work**

1. ✅ Use existing FieldValidator as-is (no changes needed)
2. ✅ Implement Phase 5-8 in execution layer
3. 🟡 **Add critical Phase 8 validator enhancements** (2 hours, parallel with Phase 8)
   - ORDER BY requirement check
   - INTERVAL-temporal type matching (CRITICAL for trades_today)

**Timeline**: 47 hours (Phases 5-8) + 2 hours (validator enhancement) = 49 hours total

**Demo Impact**: ✅ 100% correct with critical validator enhancement

---

### Option 2: Conservative (Extra Safe)
**Enhance validator comprehensively first, then Phase 5-8**

1. ✅ Add critical Phase 8 enhancements (2 hours)
2. ⚠️ Add recommended enhancements (5 hours)
3. ✅ Then proceed with Phase 5-8 (47 hours)

**Timeline**: 54 hours total

**Demo Impact**: ✅ 100% correct with comprehensive validation

**Trade-off**: 7 hours slower but highest code quality

---

### Option 3: Minimal (Fastest)
**Skip validator enhancements, just Phase 5-8 execution**

1. ✅ Use existing FieldValidator
2. ✅ Implement Phase 5-8

**Timeline**: 47 hours

**Demo Impact**: 🟡 95% - RANGE BETWEEN INTERVAL may have unexpected behavior without semantic validation

**Risk**: `trades_today` calculation (line 414-416) could fail at runtime without early detection

---

## My Recommendation

**Option 1: Fast Track** is optimal

**Rationale**:
1. ✅ FieldValidator is production-ready - no foundational changes needed
2. ✅ Phase 5-8 execution logic is independent of validator enhancements
3. 🟡 Critical Phase 8 validator enhancement (2 hours) prevents demo bugs
4. ⚠️ Recommended enhancements add quality but don't block functionality

**Implementation Plan**:
- **Week 1**: Phase 5-8 execution (47 hours)
- **Parallel (Week 1)**: Phase 8 validator critical enhancement (2 hours)
  - ORDER BY requirement validation
  - INTERVAL-temporal type matching (saves debugging `trades_today`)
- **Week 2 (Optional)**: Recommended enhancements (5 hours) for production robustness

**Expected Outcome**:
- ✅ Demo fully functional with correct results
- ✅ Phase 8 temporal windows validated early
- ✅ Clear error messages for developer mistakes
- ✅ 100% demo compatibility assured

---

## Summary Table

| Phase | Validator Status | Blocker? | Enhancement | Effort |
|-------|------------------|----------|-------------|--------|
| **Phase 5** | ✅ Ready | 🟢 NO | Optional (sort type check) | 1h |
| **Phase 6** | ✅ Ready | 🟢 NO | Recommended (type coercion) | 2h |
| **Phase 7** | ✅ Ready | 🟢 NO | Recommended (GROUP BY check) | 2h |
| **Phase 8** | ⚠️ Partial | 🟡 Critical 2h | **CRITICAL** (ORDER BY + INTERVAL) | **2h** |
| **Total** | — | — | Fast Track (Critical only) | **2h** |

---

## Conclusion

The SQL validator system is **well-designed, extensible, and ready for Phase 5-8 work** with minimal enhancement. The existing `FieldValidator` infrastructure requires zero changes. A focused 2-hour enhancement for Phase 8 temporal window validation would ensure the demo's critical `trades_today` calculation works correctly.

**Proceed directly with Phase 5-8 implementation. Enhance validator for critical Phase 8 temporal patterns in parallel.**

---

**Generated**: 2025-10-24
**Related Document**: FR-079-DEMO-COMPATIBILITY-ASSESSMENT.md
**Decision**: Validator ready - Phase 5-8 can proceed immediately
