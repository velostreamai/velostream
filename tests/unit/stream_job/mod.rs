//! Stream Job Server Tests
//!
//! Comprehensive test suite for Velostream stream job functionality.
//! This module is organized into focused test categories for maintainability.

// =============================================================================
// SERVER OPERATION TESTS
// =============================================================================

/// Critical functionality tests that must always pass
pub mod critical_unit_test;

/// Stream job SQL server integration tests  
pub mod stream_job_server_test;

// =============================================================================
// PROCESSOR TESTS (using shared infrastructure)
// =============================================================================

/// Shared test infrastructure for all processor types
pub mod stream_job_test_infrastructure;

/// Test utilities and mock implementations
pub mod stream_job_test_utils;

/// Simple job processor tests (non-transactional, high throughput)
pub mod stream_job_simple_test;

/// Transactional job processor tests (ACID compliance, rollback)
pub mod stream_job_transactional_test;

/// Core processor functionality tests (success scenarios, performance)
pub mod stream_job_processors_core_test;

/// Processor failure scenario tests (error handling, resilience)
pub mod stream_job_processors_failure_test;

/// Conditional metrics evaluation tests (FR-073 Phase 4)
pub mod conditional_metrics_test;

/// Direct ProcessorMetricsHelper tests (performance optimizations)
pub mod processor_metrics_helper_test;

/// Template for adding new processor tests
pub mod stream_job_future_handler_test_template;

/// Processor lifecycle tests - verifies stop() and process_job completion
pub mod processor_lifecycle_test;

// =============================================================================
// FEATURE-SPECIFIC TESTS
// =============================================================================

/// Common data structures and utility function tests
pub mod stream_job_common_test;

/// Job name extraction tests (@name annotation, AST fallback, priority chain)
pub mod job_name_extraction_test;

/// General stream job integration tests
pub mod stream_job_test;
