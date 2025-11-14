//! FR-082 Baseline Performance Tests
//!
//! **Purpose**: Measure baseline performance for 4 SQL scenarios before Phase 0 optimizations.
//!
//! ## Test Structure
//!
//! Tests are organized by FR-082 scenario classification:
//!
//! ### Scenario 1: ROWS WINDOW (No GROUP BY)
//! - **File**: `scenario_1_rows_window_baseline.rs`
//! - **Pattern**: Memory-bounded sliding buffers per partition
//! - **Status**: ⚠️ Baseline measurement pending
//! - **Test Command**:
//!   ```bash
//!   cargo test --tests --no-default-features --release scenario_1_rows_window -- --nocapture
//!   ```
//!
//! ### Scenario 2: Pure GROUP BY (No WINDOW)
//! - **File**: `scenario_2_pure_group_by_baseline.rs`
//! - **Pattern**: Hash table aggregation
//! - **Baseline**: 23,440 rec/sec (Job Server), 548,351 rec/sec (SQL Engine)
//! - **Overhead**: 95.8% (primary bottleneck: coordination)
//! - **Test Command**:
//!   ```bash
//!   cargo test --tests --no-default-features --release job_server_overhead_breakdown -- --nocapture
//!   ```
//!
//! ### Scenario 3a: TUMBLING + GROUP BY (Standard Emission)
//! - **File**: `scenario_3a_tumbling_standard_baseline.rs`
//! - **Pattern**: Batch emission on window close
//! - **Baseline**: 23,591 rec/sec (Job Server), 790,399 rec/sec (SQL Engine)
//! - **Overhead**: 97.0% (33.5x slowdown)
//! - **Test Command**:
//!   ```bash
//!   cargo test --tests --no-default-features --release job_server_tumbling_window_performance -- --nocapture
//!   ```
//!
//! ### Scenario 3b: TUMBLING + GROUP BY + EMIT CHANGES
//! - **File**: `scenario_3b_tumbling_emit_changes_baseline.rs`
//! - **Pattern**: Continuous emission on every update
//! - **Baseline**: 22,496 rec/sec (Job Server), ~790,399 rec/sec (SQL Engine)
//! - **Overhead vs 3a**: Only 4.6% (1.05x slowdown) ✅
//! - **Test Command**:
//!   ```bash
//!   cargo test --tests --no-default-features --release job_server_tumbling_emit_changes_performance -- --nocapture
//!   ```
//!
//! ## Running All Baseline Tests
//!
//! ### Run Individual Scenario
//! ```bash
//! # Scenario 2
//! cargo test --tests --no-default-features --release job_server_overhead_breakdown -- --nocapture
//!
//! # Scenario 3a
//! cargo test --tests --no-default-features --release job_server_tumbling_window_performance -- --nocapture
//!
//! # Scenario 3b
//! cargo test --tests --no-default-features --release job_server_tumbling_emit_changes_performance -- --nocapture
//! ```
//!
//! ### Run All Analysis Tests
//! ```bash
//! cargo test --tests --no-default-features --release analysis:: -- --nocapture
//! ```
//!
//! ## Environment Variables
//!
//! ### Test Record Counts
//! - **`PERF_TEST_MODE=ci`**: 1K records (fast CI/CD validation)
//! - **`PERF_TEST_MODE=performance`**: 100K records (full baseline measurement)
//! - **`PERF_TEST_MODE=custom`** + **`PERF_TEST_RECORDS=N`**: Custom record count
//!
//! ### Example:
//! ```bash
//! # Fast CI mode
//! PERF_TEST_MODE=ci cargo test --tests --no-default-features --release analysis::
//!
//! # Full performance mode
//! PERF_TEST_MODE=performance cargo test --tests --no-default-features --release analysis::
//!
//! # Custom record count
//! PERF_TEST_MODE=custom PERF_TEST_RECORDS=50000 cargo test --tests --no-default-features --release analysis::
//! ```
//!
//! ## Key Findings
//!
//! - **Job Server Overhead**: ~97% (primary bottleneck is coordination, not SQL engine)
//! - **EMIT CHANGES**: Only 4.6% overhead vs standard emission (excellent for real-time!)
//! - **SQL Engine Performance**: 548-790K rec/sec (performs well)
//! - **Phase 0 Focus**: Reduce job server coordination overhead
//! - **V2 Goal**: 1.5M rec/sec on 8 cores via hash-partitioned architecture

// ==============================
// Helper Modules
// ==============================

/// Shared test configuration (record counts, timeouts, etc.)
pub mod test_config;

/// Common test utilities and helper functions
pub mod test_helpers;

// ==============================
// Baseline Test Modules
// ==============================

/// Scenario 0: Pure SELECT (Passthrough) - ✅ Measures both SQL + Job Server
pub mod scenario_0_pure_select_baseline;

/// Scenario 1: ROWS WINDOW (No GROUP BY) - ⚠️ SQL Engine only (Job Server pending)
pub mod scenario_1_rows_window_baseline;

/// Scenario 2: Pure GROUP BY (No WINDOW) - ✅ Measures both SQL + Job Server (23.4K rec/sec)
pub mod scenario_2_pure_group_by_baseline;

/// Scenario 3a: TUMBLING + GROUP BY (Standard) - ✅ Measures both SQL + Job Server (23.6K rec/sec)
pub mod scenario_3a_tumbling_standard_baseline;

/// Scenario 3b: TUMBLING + GROUP BY + EMIT CHANGES - ✅ Measures both SQL + Job Server (22.5K rec/sec, only 4.6% slower!)
pub mod scenario_3b_tumbling_emit_changes_baseline;

/// Comprehensive Baseline Comparison: All 5 scenarios × 4 implementations
pub mod comprehensive_baseline_comparison;

/// Phase 5 Week 8: Performance Profiling & Bottleneck Analysis
pub mod phase5_week8_profiling;
