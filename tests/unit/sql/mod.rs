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
pub mod query_analyzer_explicit_type_test;
pub mod query_analyzer_test;

// General SQL tests
pub mod context_test;
pub mod lifecycle_test;

pub mod select_statement_matching_test;
