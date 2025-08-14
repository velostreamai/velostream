// SQL Execution Tests
// Tests for SQL query execution functionality - logically organized

// Core execution functionality
pub mod basic_execution_test;
pub mod error_handling_test;
pub mod expression_evaluation_test;
pub mod operator_test;
pub mod window_processing_test;
pub mod windowing_test;

// Specialized features
pub mod csas_ctas_test;
pub mod group_by_test;
pub mod join_test;
pub mod limit_test;
pub mod subquery_test;
