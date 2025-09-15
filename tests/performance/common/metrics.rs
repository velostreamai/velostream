//! Unified metrics collection for performance testing
//!
//! This module provides consistent timing and measurement approaches
//! to replace the scattered measurement logic across test files.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Unified metrics collector for consistent performance measurement
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    start_time: Option<Instant>,
    measurements: HashMap<String, Vec<Duration>>,
    counters: HashMap<String, u64>,
    verbose: bool,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            start_time: None,
            measurements: HashMap::new(),
            counters: HashMap::new(),
            verbose: false,
        }
    }

    /// Create a new metrics collector with verbose output
    pub fn verbose() -> Self {
        Self {
            start_time: None,
            measurements: HashMap::new(),
            counters: HashMap::new(),
            verbose: true,
        }
    }

    /// Start timing a measurement
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        if self.verbose {
            println!("🚀 Starting benchmark measurement...");
        }
    }

    /// End timing and record measurement
    pub fn end(&mut self, measurement_name: &str) -> Duration {
        let duration = match self.start_time.take() {
            Some(start) => start.elapsed(),
            None => {
                eprintln!(
                    "⚠️  Warning: end() called without start() for {}",
                    measurement_name
                );
                Duration::from_nanos(0)
            }
        };

        self.measurements
            .entry(measurement_name.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if self.verbose {
            println!("⏱️  {} completed in {:?}", measurement_name, duration);
        }

        duration
    }

    /// Record a measurement with explicit duration
    pub fn record(&mut self, measurement_name: &str, duration: Duration) {
        self.measurements
            .entry(measurement_name.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if self.verbose {
            println!("📊 Recorded {}: {:?}", measurement_name, duration);
        }
    }

    /// Increment a counter
    pub fn increment(&mut self, counter_name: &str, value: u64) {
        *self.counters.entry(counter_name.to_string()).or_insert(0) += value;
    }

    /// Set a counter value
    pub fn set_counter(&mut self, counter_name: &str, value: u64) {
        self.counters.insert(counter_name.to_string(), value);
    }

    /// Calculate throughput (records per second)
    pub fn throughput(&self, measurement_name: &str, record_count: u64) -> Option<f64> {
        let durations = self.measurements.get(measurement_name)?;
        let avg_duration = durations.iter().sum::<Duration>() / durations.len() as u32;

        if avg_duration.is_zero() {
            return None;
        }

        Some(record_count as f64 / avg_duration.as_secs_f64())
    }

    /// Get average duration for a measurement
    pub fn average_duration(&self, measurement_name: &str) -> Option<Duration> {
        let durations = self.measurements.get(measurement_name)?;
        if durations.is_empty() {
            return None;
        }
        Some(durations.iter().sum::<Duration>() / durations.len() as u32)
    }

    /// Generate performance report
    pub fn report(&self) -> PerformanceReport {
        PerformanceReport::new(self)
    }
}

/// Performance report with standardized formatting
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    measurements: HashMap<String, Duration>,
    counters: HashMap<String, u64>,
    throughputs: HashMap<String, f64>,
}

impl PerformanceReport {
    fn new(collector: &MetricsCollector) -> Self {
        let measurements: HashMap<String, Duration> = collector
            .measurements
            .iter()
            .map(|(name, durations)| {
                let avg = durations.iter().sum::<Duration>() / durations.len() as u32;
                (name.clone(), avg)
            })
            .collect();

        let throughputs: HashMap<String, f64> = collector
            .counters
            .iter()
            .filter_map(|(name, count)| {
                let measurement_name = format!("{}_duration", name);
                collector
                    .average_duration(&measurement_name)
                    .and_then(|duration| {
                        if duration.is_zero() {
                            None
                        } else {
                            Some((name.clone(), *count as f64 / duration.as_secs_f64()))
                        }
                    })
            })
            .collect();

        Self {
            measurements,
            counters: collector.counters.clone(),
            throughputs,
        }
    }

    /// Print standardized performance report
    pub fn print(&self) {
        println!("\n📊 PERFORMANCE REPORT");
        println!("═══════════════════════");

        if !self.measurements.is_empty() {
            println!("\n⏱️  TIMING MEASUREMENTS:");
            for (name, duration) in &self.measurements {
                println!("   - {}: {:?}", name, duration);
            }
        }

        if !self.counters.is_empty() {
            println!("\n🔢 COUNTERS:");
            for (name, count) in &self.counters {
                println!("   - {}: {}", name, count);
            }
        }

        if !self.throughputs.is_empty() {
            println!("\n🚀 THROUGHPUT:");
            for (name, throughput) in &self.throughputs {
                println!("   - {}: {:.2} records/sec", name, throughput);
            }
        }

        println!("═══════════════════════\n");
    }

    /// Get measurement as GitHub Actions format for CI/CD
    pub fn github_actions_output(&self) -> String {
        let mut output = String::new();

        for (name, duration) in &self.measurements {
            output.push_str(&format!("- {}: {:?}\n", name, duration));
        }

        for (name, throughput) in &self.throughputs {
            output.push_str(&format!(
                "- {} Throughput: {:.0} records/sec\n",
                name, throughput
            ));
        }

        output
    }
}

/// Time a function execution and return both result and duration
pub fn time_function<T, F>(mut f: F) -> (T, Duration)
where
    F: FnMut() -> T,
{
    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();
    (result, duration)
}

/// Async version of time_function
pub async fn time_async_function<T, F, Fut>(mut f: F) -> (T, Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let result = f().await;
    let duration = start.elapsed();
    (result, duration)
}
