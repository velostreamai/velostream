//! Window processing tests
//!
//! Tests for window operations, windowed aggregations, and EMIT CHANGES functionality.

pub mod complex_having_clauses_sql_test;
pub mod emit_changes_advanced_sql_test;
pub mod emit_changes_basic_sql_test;
pub mod emit_changes_late_data_semantics_sql_test;
pub mod emit_changes_sql_test;
pub mod financial_ticker_analytics_test;
pub mod fr079_aggregate_expressions_sql_test;
pub mod fr079_phase1_detection_test;
pub mod group_by_window_having_debug_sql_test;
pub mod group_by_window_having_order_sql_test;
pub mod row_expiration_test;
pub mod rows_window_integration_test;
pub mod rows_window_sql_test;
pub mod rows_window_test;
pub mod session_window_functions_sql_test;
pub mod shared_test_utils;
pub mod statistical_functions_sql_test;
pub mod timebased_joins_test;
pub mod unified_window_test;
pub mod watermark_late_arrival_test;
pub mod window_boundaries_group_by_sql_test;
pub mod window_edge_cases_sql_test;
pub mod window_gaps_sql_test;
pub mod window_processing_sql_test;
pub mod window_v2_validation_test;
pub mod windowing_test;
