// Table Unit Tests
// Fast tests for Table functionality without external dependencies

pub mod compact_table_test;
pub mod ctas_compact_table_test;
mod ctas_simple_integration_test;
mod ctas_table_sharing_test;
pub mod ctas_test;
pub mod optimized_table_test;
pub mod sql_test;
pub mod streaming_test;
pub mod unified_table_test;
pub mod wildcard_standalone_test;
// CTAS with EMIT CHANGES tests
pub mod ctas_emit_changes_test;
mod ctas_named_sources_sinks_test;
// AUTO_OFFSET configuration tests
pub mod ctas_auto_offset_test;
pub mod table_auto_offset_test;
// Retry logic tests
pub mod table_retry_test;
// File retry logic tests
pub mod file_retry_test;
