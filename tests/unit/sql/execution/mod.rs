//! SQL Execution Tests
//!
//! Tests for SQL query execution functionality organized by component.

// Query analyzer tests
pub mod analyzer;

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

// Stream-stream join tests
pub mod join;

// Phase 1A: Streaming tests
// TODO: Fix compilation errors - tests unimplemented FR-058 features
// pub mod phase_1a_streaming_test;

// Phase 1B: Time Semantics & Watermarks tests
pub mod phase_1b_watermarks_test;

// Phase 2: Error & Resource Enhancements tests
pub mod phase_2_error_resource_test;

// Common test utilities for all SQL execution tests
pub mod common_test_utils;

// Performance monitoring tests
pub mod performance;

// execute_with_record_sync multi-result tests
pub mod execute_sync_multi_result_test;
pub mod failure_path_test;
pub mod temporal_comparison_test;
pub mod window_system_columns_test;
