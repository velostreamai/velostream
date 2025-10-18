// SQL module tests - comprehensive test suite for streaming SQL functionality

// SQL parsing tests
pub mod parser;

// SQL function tests
pub mod functions;

// SQL execution tests
pub mod execution;

// SQL system tests
pub mod system;

// SQL type tests
pub mod types;

// SQL configuration tests
pub mod config;

// SQL validation tests
pub mod validation;

// Query analysis tests
pub mod query_analyzer_explicit_type_test;
pub mod query_analyzer_test;

// General SQL tests
pub mod between_operator_test;
pub mod config_file_comprehensive_test;
pub mod config_file_test;
pub mod context_test;
pub mod lifecycle_test;

pub mod select_statement_matching_test;

// Concatenation operator tests
pub mod concat_operator_test;

// CSAS/CTAS configuration extraction tests
pub mod csas_ctas_config_extraction_test;

// SQL validator subquery tests
pub mod sql_validator_subquery_test;

// ProfilingMode parsing tests
pub mod profiling_mode_test;

// Node identification tests
pub mod node_identification_test;
