// Test Module Organization
// Restructured for better categorization and maintainability

// Unit tests - Fast tests with no external dependencies
pub mod unit;

// Integration tests - Require running Kafka
pub mod integration;

// Performance tests - Wrapper for examples/performance/
pub mod performance;
