//! SQL Execution Tests
//!
//! Tests for SQL query execution functionality organized by component.

// Core execution functionality tests
pub mod core;

// Expression evaluation tests
pub mod expression;

// Aggregation tests
pub mod aggregation;

// Query processor tests
pub mod processors;

// Utility tests
pub mod utils;

// Phase 1B: Time Semantics & Watermarks tests
pub mod phase_1b_watermarks_test;

// Phase 2: Error & Resource Enhancements tests
pub mod phase_2_error_resource_test;

// KTable SQL Query Integration tests
pub mod ktable_subquery_test;
