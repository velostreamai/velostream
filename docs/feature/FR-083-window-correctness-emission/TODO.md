# FR-083 Window Correctness & Emission - Outstanding Work

---

## 📊 Summary Table

| Issue | Category | Priority | Status | Affects Trading Demo |
|-------|----------|----------|--------|---------------------|
| Re-emission mechanism | Architecture | CRITICAL | BLOCKED | No |
| 90% data loss with partition batching | Correctness | CRITICAL | BLOCKED | No |
| Validator working directory | Tooling | MEDIUM | PENDING | **YES** |
| WITH RECURSIVE support | Parser | MEDIUM | NOT STARTED | No |
| Standard SQL OVER clause | Parser | LOW | Workaround applied | **YES** (workaround in place) |
| Table alias wildcards (`m.*`) | Parser | LOW | Workaround applied | **YES** (workaround in place) |
| ALL/ANY operators | Parser | LOW | Workaround applied | **YES** (workaround in place) |

---

## 🎯 OUTSTANDING: Re-emission Implementation (BLOCKED)

### Current Status
- ✅ Watermark implementation complete (66x performance improvement)
- ✅ Hanging tests fixed
- ⏳ **Re-emission mechanism**: BLOCKED (requires architectural trait changes)
- ⏳ **Data loss fix**: PENDING (awaiting re-emission implementation)

### Key Metrics

| Metric | Current State | Target |
|--------|---------------|--------|
| Throughput | 71,449 rec/sec ✅ | Maintained |
| Data Loss | 90% (996/10,000) ⏳ | 0% (9,980/10,000) |
| Re-emission | ❌ MISSING | ✅ Required |

---

## 🔧 Architectural Blocker: Re-emission Implementation

**What's blocking 0% data loss?**
- WindowStrategy trait only returns `Result<bool, SqlError>` (should_emit signal)
- Need extended return type to signal: `NoEmit | Emit | EmitWithLateArrival | EmitMultiple`
- Impacts: All 4 window strategy implementations + adapter + emission strategies

---

## 📋 Implementation Phases

### Phase 1 (CRITICAL): TumblingWindowStrategy Re-emission
- [ ] Modify `add_record()` to return EmitSignal enum
- [ ] Implement late arrival detection using watermark
- [ ] Trigger re-emission when late data arrives for closed window
- [ ] Update adapter to handle EmitWithLateArrival signal

### Phase 2 (CRITICAL): SlidingWindowStrategy Re-emission
- [ ] Adapt watermark logic for overlapping windows
- [ ] Track multiple windows affected by late data
- [ ] Emit updated results for ALL affected windows

### Phase 3 (HIGH): SessionWindowStrategy Re-emission
- [ ] Detect when late record should merge sessions
- [ ] Recalculate session boundaries
- [ ] Emit merged session results

### Phase 4 (SKIP): RowsWindowStrategy
- ✅ No changes needed (row-based, not time-based)

---

## ⏳ Pending Tasks

- [ ] **Implement EmitSignal enum** in WindowStrategy trait
- [ ] **Update all strategy implementations** (tumbling, sliding, session)
- [ ] **Update adapter** to handle re-emission signals
- [ ] **Update emission strategies** for late firing
- [ ] **Add integration tests** for partition-batched data with re-emission
- [ ] **Verify 0% data loss** with partition batching test

---

## 🔴 Known Gap: recursive_ctes

- **Status**: NOT YET SUPPORTED
- **Issue**: `WITH RECURSIVE ... UNION ALL` syntax not supported by parser
- **Test File**: `tests/performance/analysis/sql_operations/tier4_specialized/recursive_ctes.rs`
- **Next Step**: Implement WITH RECURSIVE support in parser

---

## 📊 Performance Targets (After Re-emission)

```
CURRENT (with watermark, no re-emission):
├─ Results: 996/10,000 (90% loss)
├─ Throughput: 71,449 rec/sec
└─ Late arrivals: Buffered but not re-emitted

TARGET (with re-emission):
├─ Results: ~9,980/10,000 (0% loss)
├─ Throughput: ~50,000+ rec/sec (with late firing overhead)
├─ Late firings: ~100 per test (expected)
└─ Memory: ~2 MB (bounded by allowed_lateness)
```

---

## 🏦 Trading Demo Issues (`demo/trading/sql/financial_trading.sql`)

### Validator Working Directory Issue (PENDING)

**Issue**: SQL validator reports "File not found" for config files that exist in `demo/trading/configs/`.

**Root Cause**: Validator runs from project root, but SQL uses paths relative to `demo/trading/`.

**Affected Files** (all exist but validator can't find them):
- `configs/market_data_source.yaml`
- `configs/market_data_ts_source.yaml`
- `configs/trading_positions_source.yaml`
- `configs/order_book_source.yaml`
- `configs/market_data_exchange_a_source.yaml`
- `configs/market_data_exchange_b_source.yaml`

**Fix Options**:
1. Run validator from `demo/trading/` directory
2. Update validator to accept base path parameter
3. Use absolute paths in SQL

---

### Parser Enhancements (Workarounds Applied)

These parser limitations were discovered during trading demo validation. **Workarounds have been applied** to `financial_trading.sql`, but native parser support would be beneficial:

| Feature | Priority | Workaround Applied | Native Support |
|---------|----------|-------------------|----------------|
| Standard SQL OVER clause | MEDIUM | ✅ Converted to `ROWS WINDOW BUFFER N ROWS` | Not implemented |
| Table alias wildcards (`m.*`) | LOW | ✅ Expanded to explicit column lists | Not implemented |
| ALL/ANY operators | LOW | ✅ Converted to `AND`/`OR` logic | Not implemented |

**Queries Fixed** (workarounds in place):
- Query #4, #8, #10: Window functions → Velostream syntax
- Query #5, #7: `m.*` → explicit columns
- Query #11: `ALL()`/`ANY()` → `AND`/`OR`

---

## ✅ Recently Completed (Reference)

- [x] SQL syntax fixes in `financial_trading.sql` (6 queries fixed)
- [x] Added `@metric_field` annotation for histogram metric
- [x] TableDataSource config file loading implementation
- [x] 8 table config files with `extends:` pattern
- [x] 8 CSV data files for reference tables
- [x] 8 unit tests for TableDataSource config loading
