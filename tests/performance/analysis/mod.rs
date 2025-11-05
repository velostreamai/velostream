//! Performance Analysis and Profiling Tests
//!
//! Detailed profiling tests to identify performance bottlenecks.
//!
//! ## Running Tests
//!
//! ### CI/CD Mode (Fast - 1K records):
//! ```bash
//! PERF_TEST_MODE=ci cargo test --tests --no-default-features --release
//! ```
//!
//! ### Performance Mode (Full - 100K records):
//! ```bash
//! PERF_TEST_MODE=performance cargo test --tests --no-default-features --release
//! ```
//!
//! ### Custom Mode:
//! ```bash
//! PERF_TEST_MODE=custom PERF_TEST_RECORDS=50000 cargo test --tests --no-default-features --release
//! ```

// Test configuration and helpers
pub mod test_config;
pub mod test_helpers;

// Profiling tests
pub mod group_by_pure_profiling;
pub mod job_server_overhead_breakdown;
pub mod overhead_profiling;
pub mod rows_window_profiling;
pub mod session_window_profiling;
pub mod sliding_window_profiling;
pub mod tumbling_emit_changes_job_server_test;
pub mod tumbling_emit_changes_profiling;
pub mod tumbling_instrumented_profiling;
pub mod tumbling_job_server_test;
pub mod tumbling_window_profiling;
pub mod window_v2_benchmarks;
