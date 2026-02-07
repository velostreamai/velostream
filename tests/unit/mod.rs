pub mod common;
// Configuration system tests - Tests for schema registry and validation
pub mod config;
// Generic datasource tests - Tests for datasource implementations
pub mod datasource;
pub mod kafka;
pub mod sql;
// Table tests - Tests for materialized table functionality
pub mod table;
pub mod test_messages;
pub mod test_utils;
// Stream job server tests - Tests for stream job functionality
pub mod stream_job;
// Server tests - Tests for server components including CTAS functionality
pub mod server;
// Serialization tests - Tests for different serialization formats
pub mod serialization;
// Shared observability test helpers
pub mod observability_test_helpers;
// Observability tests - Tests for telemetry and tracing
pub mod observability;
// Test harness tests - Tests for FR-084 SQL Application Test Harness
pub mod test_harness;
