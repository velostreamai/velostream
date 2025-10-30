// SQL Parser Tests
// Tests for SQL parsing functionality

pub mod basic_parsing_test;
pub mod between_operator_test;
pub mod case_when_test;
pub mod concat_operator_test;
pub mod emit_mode_test;
pub mod fr079_window_group_by_clause_order_test; // FR-079: Test WINDOW/GROUP BY clause ordering
pub mod implicit_aggregation_test;
pub mod lifecycle_test;
pub mod metric_annotations_test; // FR-073 Phase 1
pub mod session_window_test;
pub mod tumbling_window_test;
pub mod advanced_window_test;
