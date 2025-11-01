//! Query processor tests
//!
//! Tests for specialized query processing including windows, joins, and limits.

pub mod dml;
pub mod graceful_degradation_test;
pub mod join;
pub mod limit;
pub mod order_by_validation_test;
pub mod processor_context_table_test;
pub mod select_alias_reuse_test;
pub mod select_safety_test;
pub mod select_validation_test;
pub mod show;
pub mod window;
