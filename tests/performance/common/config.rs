//! Unified configuration for performance testing
//!
//! This module provides standardized configuration types to eliminate
//! the scattered configuration approaches across different test files.

use std::time::Duration;

/// Unified benchmark mode enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BenchmarkMode {
    /// Basic benchmarks for fundamental functionality
    Basic,
    /// Enhanced benchmarks with advanced features (watermarks, circuit breakers)
    Enhanced,
    /// Production-ready benchmarks simulating real-world scenarios
    Production,
}

/// Standardized benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Benchmark mode
    pub mode: BenchmarkMode,
    /// Number of records to process
    pub record_count: usize,
    /// Number of benchmark iterations
    pub iterations: usize,
    /// Timeout for each benchmark
    pub timeout: Duration,
    /// Whether running in GitHub Actions (CI/CD detection)
    pub is_github_actions: bool,
    /// Whether to enable verbose output
    pub verbose: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            mode: BenchmarkMode::Basic,
            record_count: if Self::is_ci_environment() { 100 } else { 1000 },
            iterations: if Self::is_ci_environment() { 1 } else { 3 },
            timeout: Duration::from_secs(if Self::is_ci_environment() { 30 } else { 60 }),
            is_github_actions: Self::is_ci_environment(),
            verbose: false,
        }
    }
}

impl BenchmarkConfig {
    /// Create config for basic benchmarks
    pub fn basic() -> Self {
        Self {
            mode: BenchmarkMode::Basic,
            ..Default::default()
        }
    }

    /// Create config for enhanced benchmarks
    pub fn enhanced() -> Self {
        Self {
            mode: BenchmarkMode::Enhanced,
            record_count: if Self::is_ci_environment() { 50 } else { 500 },
            ..Default::default()
        }
    }

    /// Create config for production benchmarks
    pub fn production() -> Self {
        Self {
            mode: BenchmarkMode::Production,
            record_count: if Self::is_ci_environment() { 200 } else { 2000 },
            iterations: if Self::is_ci_environment() { 2 } else { 5 },
            timeout: Duration::from_secs(if Self::is_ci_environment() { 60 } else { 120 }),
            ..Default::default()
        }
    }

    /// Create config with custom record count
    pub fn with_record_count(mut self, count: usize) -> Self {
        self.record_count = count;
        self
    }

    /// Create config with custom iterations
    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    /// Enable verbose output
    pub fn verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// Standardized CI/CD environment detection
    pub fn is_ci_environment() -> bool {
        std::env::var("GITHUB_ACTIONS").is_ok() || std::env::var("CI").is_ok()
    }

    /// Get appropriate batch size for the configuration
    pub fn batch_size(&self) -> usize {
        match self.mode {
            BenchmarkMode::Basic => 50,
            BenchmarkMode::Enhanced => 100,
            BenchmarkMode::Production => 200,
        }
    }
}
