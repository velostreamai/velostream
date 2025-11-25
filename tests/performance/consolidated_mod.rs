//! Consolidated Performance Test Suite
//!
//! This module provides a unified structure for all Velostream performance tests,
//! organized by functional area with clear separation between unit benchmarks,
//! integration tests, and production simulations.

// Core Performance Test Modules (Future expansion structure)
pub mod benchmarks {
    //! Micro-benchmarks for individual components
    //!
    //! This module provides organized access to existing benchmarks
    //! and serves as a structure for future micro-benchmark additions.

    // Re-export existing benchmarks from reorganized modules (commented out due to import issues)
    // pub use super::unit::financial_precision as financial_precision;
    // pub use super::unit::serialization_formats as serialization;

    // Placeholder modules for future implementation
    pub mod memory_allocation {
        //! Memory allocation performance benchmarks
        //! This module will contain object pooling and allocation efficiency tests
        #[cfg(feature = "jemalloc")]
        pub use super::super::utils::memory::*;
    }

    pub mod codec_performance {
        //! Codec-specific performance benchmarks  
        //! This module will contain serialization format comparison tests
    }
}

pub mod integration {
    //! End-to-end performance tests
    //!
    //! This module organizes full pipeline performance testing

    // Re-export existing integration tests from reorganized modules (commented out due to import issues)
    // pub use super::unit::kafka_configurations as kafka_pipeline;
    // pub use super::unit::sql_execution as sql_execution;

    // Placeholder for future transaction processing tests
    pub mod transaction_processing {
        //! Transaction processing performance tests
        //! This module will contain exactly-once semantics performance validation
    }
}

pub mod load_testing {
    //! High-throughput and sustained load tests
    //!
    //! This module will contain production-scale performance validation

    pub mod throughput_benchmarks {
        //! Sustained throughput testing framework
        //! Tests for 1M+ records/sec sustained performance
    }

    pub mod memory_pressure {
        //! Memory pressure testing utilities
        //! Resource exhaustion and backpressure validation
        #[cfg(feature = "jemalloc")]
        pub use super::super::utils::memory::*;
    }

    pub mod scalability {
        //! Concurrent performance testing
        //! Multi-connection and parallel processing benchmarks
    }
}

pub mod profiling {
    //! Memory and CPU profiling utilities
    //!
    //! This module contains production-ready profiling tools

    pub mod memory_profiler {
        //! Memory allocation tracking and analysis
        #[cfg(feature = "jemalloc")]
        pub use super::super::utils::memory::*;
    }

    pub mod cpu_profiler {
        //! CPU utilization and performance profiling
    }

    pub mod allocation_tracker {
        //! Allocation pattern analysis
    }
}

// Convenient re-exports with organized naming
pub mod organized {
    //! Organized access to all performance tests

    pub mod benchmarks {}

    pub mod integration {}

    pub mod load_testing {}

    pub mod profiling {}
}

/// Performance test configuration and utilities
pub mod config {
    use std::time::Duration;

    /// Standard benchmark configuration
    pub struct BenchmarkConfig {
        pub iterations: u64,
        pub warmup_duration: Duration,
        pub measurement_duration: Duration,
        pub sample_size: usize,
    }

    impl Default for BenchmarkConfig {
        fn default() -> Self {
            Self {
                iterations: 1000,
                warmup_duration: Duration::from_secs(1),
                measurement_duration: Duration::from_secs(3),
                sample_size: 100,
            }
        }
    }

    /// Load test configuration for high-throughput testing
    pub struct LoadTestConfig {
        pub target_rps: u64,
        pub duration: Duration,
        pub ramp_up_duration: Duration,
        pub concurrent_connections: usize,
    }

    impl Default for LoadTestConfig {
        fn default() -> Self {
            Self {
                target_rps: 10_000,
                duration: Duration::from_secs(30),
                ramp_up_duration: Duration::from_secs(10),
                concurrent_connections: 10,
            }
        }
    }
}

/// Common utilities for performance testing
pub mod utils {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    /// Performance metrics collector
    pub struct MetricsCollector {
        pub start_time: Instant,
        pub operations_completed: AtomicU64,
        pub bytes_processed: AtomicU64,
        pub errors: AtomicU64,
    }

    impl MetricsCollector {
        pub fn new() -> Self {
            Self {
                start_time: Instant::now(),
                operations_completed: AtomicU64::new(0),
                bytes_processed: AtomicU64::new(0),
                errors: AtomicU64::new(0),
            }
        }

        pub fn record_operation(&self, bytes: u64) {
            self.operations_completed.fetch_add(1, Ordering::Relaxed);
            self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        }

        pub fn record_error(&self) {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }

        pub fn get_throughput_ops_per_sec(&self) -> f64 {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            let ops = self.operations_completed.load(Ordering::Relaxed) as f64;
            if elapsed > 0.0 { ops / elapsed } else { 0.0 }
        }

        pub fn get_throughput_bytes_per_sec(&self) -> f64 {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            let bytes = self.bytes_processed.load(Ordering::Relaxed) as f64;
            if elapsed > 0.0 { bytes / elapsed } else { 0.0 }
        }

        pub fn get_error_rate(&self) -> f64 {
            let total = self.operations_completed.load(Ordering::Relaxed) as f64;
            let errors = self.errors.load(Ordering::Relaxed) as f64;
            if total > 0.0 { errors / total } else { 0.0 }
        }
    }

    /// Memory allocation tracking
    #[cfg(feature = "jemalloc")]
    pub mod memory {
        use jemalloc_ctl::{epoch, stats};

        pub struct MemorySnapshot {
            pub allocated: usize,
            pub resident: usize,
            pub timestamp: std::time::Instant,
        }

        pub fn get_memory_snapshot() -> Result<MemorySnapshot, Box<dyn std::error::Error>> {
            epoch::advance()?;
            let allocated = stats::allocated::read()?;
            let resident = stats::resident::read()?;

            Ok(MemorySnapshot {
                allocated,
                resident,
                timestamp: std::time::Instant::now(),
            })
        }

        pub fn memory_diff(before: &MemorySnapshot, after: &MemorySnapshot) -> (isize, isize) {
            let allocated_diff = after.allocated as isize - before.allocated as isize;
            let resident_diff = after.resident as isize - before.resident as isize;
            (allocated_diff, resident_diff)
        }
    }

    /// CPU profiling utilities
    pub mod cpu {
        use std::time::{Duration, Instant};

        pub struct CpuProfiler {
            start: Instant,
            samples: Vec<Duration>,
        }

        impl CpuProfiler {
            pub fn new() -> Self {
                Self {
                    start: Instant::now(),
                    samples: Vec::new(),
                }
            }

            pub fn sample(&mut self) {
                self.samples.push(self.start.elapsed());
            }

            pub fn get_percentiles(&self) -> (Duration, Duration, Duration) {
                let mut sorted = self.samples.clone();
                sorted.sort();

                let len = sorted.len();
                if len == 0 {
                    return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
                }

                let p50 = sorted[len * 50 / 100];
                let p95 = sorted[len * 95 / 100];
                let p99 = sorted[len * 99 / 100];

                (p50, p95, p99)
            }
        }
    }
}

/// Test data generators for consistent performance testing
pub mod test_data {
    use std::collections::HashMap;
    use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

    /// Generate test records for performance testing
    pub fn generate_test_records(count: usize) -> Vec<StreamRecord> {
        let mut records = Vec::with_capacity(count);

        for i in 0..count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("STOCK{:04}", i % 100)),
            );
            fields.insert(
                "price".to_string(),
                FieldValue::ScaledInteger((100000 + i as i64 * 10) % 500000, 4),
            );
            fields.insert(
                "volume".to_string(),
                FieldValue::Integer((1000 + i * 10) as i64),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer(1672531200000 + i as i64),
            );

            records.push(StreamRecord {
                fields,
                timestamp: 1672531200000 + i as i64,
                offset: i as i64,
                partition: (i % 4) as i32,
                headers: HashMap::new(),
                event_time: None,
            });
        }

        records
    }

    /// Generate financial test data with realistic patterns
    pub fn generate_financial_records(count: usize) -> Vec<StreamRecord> {
        let mut records = Vec::with_capacity(count);
        let symbols = [
            "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "AMD",
        ];

        for i in 0..count {
            let symbol = symbols[i % symbols.len()];
            let base_price = match symbol {
                "AAPL" => 15000000,  // $150.00
                "GOOGL" => 25000000, // $250.00
                "MSFT" => 30000000,  // $300.00
                _ => 10000000,       // $100.00
            };

            // Add some realistic price movement
            let price_movement = ((i as i64 * 17) % 1000) - 500; // -$5.00 to +$5.00
            let final_price = base_price + price_movement * 10000;

            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
            fields.insert(
                "price".to_string(),
                FieldValue::ScaledInteger(final_price, 4),
            );
            fields.insert(
                "volume".to_string(),
                FieldValue::Integer((1000 + (i * 17) % 10000) as i64),
            );
            fields.insert(
                "bid".to_string(),
                FieldValue::ScaledInteger(final_price - 100, 4),
            ); // $0.01 spread
            fields.insert(
                "ask".to_string(),
                FieldValue::ScaledInteger(final_price + 100, 4),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer(1672531200000 + i as i64 * 1000),
            );

            records.push(StreamRecord {
                fields,
                timestamp: 1672531200000 + i as i64 * 1000,
                offset: i as i64,
                partition: (i % 4) as i32,
                headers: {
                    let mut headers = HashMap::new();
                    headers.insert("exchange".to_string(), "NASDAQ".to_string());
                    headers.insert("data_type".to_string(), "quote".to_string());
                    headers
                },
                event_time: None,
            });
        }

        records
    }
}
