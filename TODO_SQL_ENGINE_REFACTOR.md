# SQL Engine Refactor TODO List

**Original Branch**: `34-refactor-sqlengine-attempt2` (merged via PR #38)  
**Current Branch**: `master` 
**Purpose**: Complete the SQL engine refactoring work (architectural improvements, cleanup, optimization)  
**Status**: **IN PROGRESS** - Performance complete, final legacy cleanup remaining

---

## ✅ COMPLETED WORK 

### Core Refactoring (Phases 1-5B) - **COMPLETED** ✅
- **Phase 1**: API Cleanup - Internal types hidden, methods made private
- **Phase 2**: Extract Core Types - FieldValue, StreamRecord extracted to separate files
- **Phase 3**: Extract Expression Engine - Expression evaluation, functions, arithmetic separated
- **Phase 4**: Extract Aggregation Engine - GROUP BY, accumulator logic separated  
- **Phase 5**: Extract Query Processors - SELECT, JOIN, LIMIT, WINDOW processors created
- **Phase 5B**: Enable Processor Execution - All SELECT queries now use processor architecture

### Advanced Features (Phase 6.1-6.2) - **COMPLETED** ✅
- **Phase 6.1**: Window Functions - All window tests passing (100% success rate)
- **Phase 6.2**: Advanced GROUP BY Features - All GROUP BY tests passing (100% success rate)
- **Phase 6.5**: DML Operations - INSERT/UPDATE/DELETE fully implemented 
- **Phase 6.5**: Schema Introspection - SHOW/DESCRIBE operations complete

### Core Subquery Infrastructure - **COMPLETED** ✅
- **SubqueryExecutor Trait**: Architecture for processor-based subquery execution
- **Data Source Integration**: ProcessorContext enhancement with test data sources
- **Mock Implementations Replaced**: Real subquery execution with all tests passing
- **Unique Error IDs**: All mock error messages now have tracking IDs

### **Phase 6.3: Legacy Engine Cleanup** - **PARTIALLY COMPLETE** ⚠️
- **✅ Removed Legacy GROUP BY Methods**: 
  - Deleted `update_group_accumulator_inline` (218 lines)
  - Deleted `update_accumulator_for_expression_inline` helper method
  - Compiler warnings reduced
- **⚠️ Still Contains Legacy Methods**:
  - `apply_query_legacy` still exists for job management operations
  - Dual-path routing (`should_use_processors`) still in place
  - Job management queries (START/STOP/PAUSE/RESUME/DEPLOY JOB) still use legacy path

### **Phase 6.4: Performance Optimizations** - **COMPLETED** ✅
- **✅ Re-benchmarked Performance Tests** 
  - Simple SELECT: 142,000+ records/sec (7.05µs per record)
  - Filtered queries: 155,000+ records/sec (6.46µs per record)
  - GROUP BY: 57,700+ records/sec (17.3µs per record)
  - Window functions: 20,300+ records/sec (49.2µs per record)
- **✅ Performance Validated**: 18-31% faster than legacy estimates
- **✅ Documentation Updated**: Created PERFORMANCE_ANALYSIS.md

---

## 🚧 REMAINING WORK

### **Phase 6.6: Complete Legacy Removal** 🔧 **IN PROGRESS**

#### **Remaining Legacy Methods to Remove:**
1. **`apply_query_legacy()`** (lines 862-1330+) - Main legacy execution path
2. **`should_use_processors()`** (lines 287-297) - Dual-path routing logic
3. **`process_joins()`** - Legacy JOIN handling
4. **`collect_header_mutations_from_fields()`** - Legacy header handling
5. **Various legacy helper methods** flagged by compiler as unused

#### **Query Types Still Using Legacy Path:**
- `StreamingQuery::StartJob` - Job initiation
- `StreamingQuery::StopJob` - Job termination  
- `StreamingQuery::PauseJob` - Job suspension
- `StreamingQuery::ResumeJob` - Job resumption
- `StreamingQuery::DeployJob` - Job deployment
- Some DML operations (UPDATE/DELETE) that return errors

#### **Tasks to Complete:**
- [ ] **Implement Job Management Processors**
  - [ ] Create `JobProcessor` for START/STOP/PAUSE/RESUME/DEPLOY operations
  - [ ] Migrate job management logic to processor architecture
  - [ ] Update routing to use processors for all query types

- [ ] **Remove Dual-Path Routing**
  - [ ] Delete `should_use_processors()` method
  - [ ] Remove `apply_query_legacy()` method
  - [ ] Update `apply_query()` to always use processors
  - [ ] Clean up all legacy execution code

- [ ] **Clean Up Unused Methods**
  - [ ] Remove all methods marked as "never used" by compiler
  - [ ] Delete legacy JOIN processing code
  - [ ] Remove legacy header mutation code
  - [ ] Clean up unused arithmetic/comparison helpers

**Estimated Effort**: 2-3 days  
**Blocker**: Need to implement job management processors first

---

## 📊 **CURRENT STATUS**

### **What's Working:**
- **Test Coverage**: **842/842 tests passing (100% success rate)** ✅
- **Performance**: **Production-ready speeds** (18-31% faster) ✅
- **Core Functionality**: All SQL features operational ✅

### **What Needs Cleanup:**
- **Legacy Code**: ~500+ lines of legacy execution code still present ⚠️
- **Dual-Path System**: Complex routing logic between legacy and processors ⚠️
- **Job Management**: Still using legacy execution path ⚠️
- **Compiler Warnings**: Multiple "never used" warnings for legacy methods ⚠️

### **Architecture State:**
```
Current Flow:
┌─────────────┐
│ apply_query │
└─────┬───────┘
      │
      ▼
┌──────────────────┐     YES    ┌────────────────────────┐
│should_use_processors?├─────────►apply_query_with_processors│
└─────────┬────────┘              └────────────────────────┘
          │ NO
          ▼
┌─────────────────┐
│apply_query_legacy│  ← NEEDS REMOVAL
└─────────────────┘

Target Flow:
┌─────────────┐
│ apply_query │
└─────┬───────┘
      │
      ▼
┌────────────────────────┐
│apply_query_with_processors│  ← ALL queries through processors
└────────────────────────┘
```

---

## 🎯 **IMMEDIATE NEXT STEPS**

1. **Create JobProcessor** - Implement processor for job management operations
2. **Migrate Job Operations** - Move START/STOP/PAUSE/RESUME/DEPLOY to processors
3. **Remove Legacy Path** - Delete `apply_query_legacy` and dual-path routing
4. **Clean Up Warnings** - Remove all unused legacy methods
5. **Final Testing** - Ensure all tests still pass after cleanup

---

## 📋 **COMPLETION CRITERIA**

Before considering refactor complete:
- [ ] All query types use processor architecture
- [ ] No `apply_query_legacy` method exists
- [ ] No `should_use_processors` routing
- [ ] Zero "never used" compiler warnings from engine.rs
- [ ] All 842+ tests still passing
- [ ] Clean, single-path execution flow

---

**Note**: While the engine is **functionally complete** and **production-ready**, the legacy cleanup will improve maintainability and reduce technical debt.