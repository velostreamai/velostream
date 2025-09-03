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

// SQL datasource tests
pub mod datasource;

// Query analysis tests
pub mod query_analyzer_test;

// General SQL tests
pub mod context_test;
pub mod lifecycle_test;

// COUNT_DISTINCT and APPROX_COUNT_DISTINCT tests
pub mod count_distinct_comprehensive_test;
pub mod count_distinct_functions_test;

// Job deployment and naming tests
pub mod job_name_generation_test;
pub mod multi_job_select_deployment_test;
pub mod select_statement_matching_test;

// Multi-job tests
pub mod multi_job_common_test;
pub mod multi_job_processors_test;
pub mod multi_job_test;

// Performance tests
pub mod conversion_performance_test;
