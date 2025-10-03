# Complete INTO Clause Removal Plan

## Current Status
- ✅ Validation added: CSAS requires sink configuration
- ✅ Documentation updated: docs/sql/ctas-vs-csas-guide.md uses named-sink pattern
- ✅ Financial trading SQL updated: demo/trading/sql/financial_trading.sql
- ⚠️ INTO syntax still supported in parser (CreateStreamInto/CreateTableInto)

## Remaining Work

### Phase 1: SQL Files (Can do now - 11 files)
- [ ] demo/datasource-demo/file_processing_sql_demo.sql
- [ ] demo/datasource-demo/enhanced_sql_demo.sql
- [ ] demo/datasource-demo/simple_test.sql
- [ ] demo/datasource-demo/test_kafka.sql
- [ ] examples/social_media_analytics.sql
- [ ] examples/iot_monitoring_phase4.sql
- [ ] examples/iot_monitoring.sql
- [ ] examples/social_media_analytics_phase4.sql
- [ ] examples/test_simple_validation.sql
- [ ] examples/ecommerce_analytics_phase4.sql
- [ ] examples/ecommerce_analytics.sql

### Phase 2: Remove AST Variants (Breaking change - needs dedicated session)
**Files to modify:**
- src/velostream/sql/ast.rs - Remove CreateStreamInto/CreateTableInto enum variants
- src/velostream/sql/parser.rs - Remove INTO parsing logic
- src/velostream/sql/execution/engine.rs - Remove CreateStreamInto handling
- src/velostream/sql/query_analyzer.rs - Remove CreateStreamInto analysis
- src/velostream/sql/validator.rs - Remove INTO validation
- src/velostream/server/stream_job_server.rs - Remove INTO job deployment
- src/bin/test_create_stream_into.rs - Delete or convert to named-sink test

**Test files to update (~30-40 files):**
- tests/unit/sql/execution/core/csas_ctas_test.rs
- tests/unit/table/ctas_*.rs files
- tests/unit/sql/query_analyzer_test.rs
- All integration tests using INTO

### Phase 3: Verification
- [ ] Run full test suite
- [ ] Verify all examples work
- [ ] Update any remaining documentation
- [ ] Performance test validation

## Benefits of Complete Removal
✅ Single, clear syntax pattern
✅ No redundant stream naming
✅ Simpler parser (less code to maintain)
✅ Clearer error messages
✅ Consistent with modern streaming SQL patterns

## Migration Strategy
Since there are no external users:
1. Update all SQL files to named-sink pattern
2. Remove AST variants in one commit
3. Fix all compilation errors
4. Validate with full test suite
