//! Multi-Job Server Tests
//!
//! Comprehensive test suite for FerrisStreams multi-job functionality.
//! This module is organized into focused test categories for maintainability.

// =============================================================================
// SERVER OPERATION TESTS
// =============================================================================

/// Critical functionality tests that must always pass
pub mod critical_unit_test;

/// Multi-job SQL server integration tests  
pub mod sql_server_test;

// =============================================================================
// PROCESSOR TESTS (using shared infrastructure)
// =============================================================================

/// Shared test infrastructure for all processor types
pub mod multi_job_test_infrastructure;

/// Test utilities and mock implementations
pub mod multi_job_test_utils;

/// Simple job processor tests (non-transactional, high throughput)
pub mod multi_job_simple_test;

/// Transactional job processor tests (ACID compliance, rollback)
pub mod multi_job_transactional_test;

/// Core processor functionality tests (success scenarios, performance)
pub mod multi_job_processors_core_test;

/// Processor failure scenario tests (error handling, resilience)
pub mod multi_job_processors_failure_test;

/// Template for adding new processor tests
pub mod multi_job_future_handler_test_template;

// =============================================================================
// FEATURE-SPECIFIC TESTS
// =============================================================================

/// Common data structures and utility function tests
pub mod multi_job_common_test;

/// Job naming and SQL snippet extraction tests
pub mod job_name_generation_test;

/// SQL application deployment tests
pub mod multi_job_select_deployment_test;

/// General multi-job integration tests
pub mod multi_job_test;
