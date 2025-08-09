// Test Module Organization
// Restructured for better categorization and maintainability

// Unit tests - Fast tests with no external dependencies
pub mod unit;

// Integration tests - Require running Kafka
pub mod integration;

// Performance tests - Wrapper for examples/performance/
pub mod performance;

// Multi-job server tests - Tests for multi-job functionality
pub mod multi_job;

// SQL tests - Streaming SQL parser and execution tests
pub mod sql;
