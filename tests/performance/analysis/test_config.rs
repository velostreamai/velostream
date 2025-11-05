//! Performance Test Configuration Helper
//!
//! Provides configurable test parameters based on environment:
//! - CI/CD: Fast smoke tests (1K records)
//! - Performance: Full profiling (100K+ records)
//! - Custom: User-defined via PERF_TEST_RECORDS env var

use std::env;

/// Test mode based on environment configuration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TestMode {
    /// CI/CD mode: Fast smoke tests with minimal records
    CI,
    /// Performance mode: Full profiling with large datasets
    Performance,
    /// Custom mode: User-defined record count
    Custom(usize),
}

/// Performance test configuration
#[derive(Debug, Clone)]
pub struct PerfTestConfig {
    pub mode: TestMode,
    pub num_records: usize,
    pub batch_size: usize,
    pub verbose_output: bool,
}

impl PerfTestConfig {
    /// Get configuration based on environment variables
    ///
    /// Environment variables:
    /// - `PERF_TEST_MODE`: "ci" | "performance" | "custom"
    /// - `PERF_TEST_RECORDS`: Custom record count (only for custom mode)
    /// - `PERF_TEST_VERBOSE`: "true" | "false" (default: false)
    pub fn from_env() -> Self {
        let mode_str = env::var("PERF_TEST_MODE").unwrap_or_else(|_| "ci".to_string());

        let mode = match mode_str.to_lowercase().as_str() {
            "ci" => TestMode::CI,
            "performance" | "perf" => TestMode::Performance,
            "custom" => {
                let count = env::var("PERF_TEST_RECORDS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10_000);
                TestMode::Custom(count)
            }
            _ => TestMode::CI, // Default to CI mode
        };

        let verbose_output = env::var("PERF_TEST_VERBOSE")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        Self::from_mode(mode, verbose_output)
    }

    /// Create configuration from specific mode
    pub fn from_mode(mode: TestMode, verbose_output: bool) -> Self {
        let (num_records, batch_size) = match mode {
            TestMode::CI => (1_000, 100),           // Fast: 1K records, 100 batch
            TestMode::Performance => (100_000, 1_000), // Full: 100K records, 1K batch
            TestMode::Custom(count) => {
                let batch = (count / 100).max(100).min(1000);
                (count, batch)
            }
        };

        Self {
            mode,
            num_records,
            batch_size,
            verbose_output,
        }
    }

    /// Get a descriptive mode string for output
    pub fn mode_description(&self) -> &str {
        match self.mode {
            TestMode::CI => "CI/CD (fast smoke test)",
            TestMode::Performance => "Performance (full profiling)",
            TestMode::Custom(_) => "Custom",
        }
    }

    /// Check if we should skip detailed output
    pub fn should_skip_verbose(&self) -> bool {
        !self.verbose_output && matches!(self.mode, TestMode::CI)
    }
}

impl Default for PerfTestConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ci_mode_defaults() {
        let config = PerfTestConfig::from_mode(TestMode::CI, false);
        assert_eq!(config.num_records, 1_000);
        assert_eq!(config.batch_size, 100);
        assert!(!config.verbose_output);
    }

    #[test]
    fn test_performance_mode() {
        let config = PerfTestConfig::from_mode(TestMode::Performance, true);
        assert_eq!(config.num_records, 100_000);
        assert_eq!(config.batch_size, 1_000);
        assert!(config.verbose_output);
    }

    #[test]
    fn test_custom_mode() {
        let config = PerfTestConfig::from_mode(TestMode::Custom(50_000), false);
        assert_eq!(config.num_records, 50_000);
        assert_eq!(config.batch_size, 500);
    }
}
