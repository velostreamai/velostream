//! Window processing tests
//!
//! Tests for window operations, windowed aggregations, and EMIT CHANGES functionality.

pub mod complex_having_clauses_test;
pub mod emit_changes_advanced_test;
pub mod emit_changes_basic_test;
pub mod emit_changes_late_data_semantics_test;
pub mod emit_changes_test;
pub mod financial_ticker_analytics_test;
pub mod fr079_aggregate_expressions_test;
pub mod fr079_phase1_detection_test;
pub mod group_by_window_having_debug_test;
pub mod group_by_window_having_order_test;
pub mod session_window_functions_test;
pub mod shared_test_utils;
pub mod statistical_functions_test;
pub mod timebased_joins_test;
pub mod unified_window_test;
pub mod window_edge_cases_test;
pub mod window_frame_execution_test;
pub mod window_gaps_test;
pub mod window_processing_test;
pub mod windowing_test;
