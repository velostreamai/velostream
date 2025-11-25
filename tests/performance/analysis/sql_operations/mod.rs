//! SQL Operations Performance Analysis
//!
//! Organized by tier based on STREAMING_SQL_OPERATION_RANKING.md:
//! - Tier 1: Essential operations (90-100% probability)
//! - Tier 2: Common operations (60-89% probability)
//! - Tier 3: Advanced operations (30-59% probability)
//! - Tier 4: Specialized operations (10-29% probability)
//!
//! Each operation has dedicated benchmarks measuring:
//! - Peak throughput across implementations
//! - Comparison to Flink/ksqlDB baselines
//! - Memory usage patterns
//! - Implementation-specific performance
//!
//! Results feed back into STREAMING_SQL_OPERATION_RANKING.md for iterative optimization.

pub mod test_helpers;
pub mod tier1_essential;
pub mod tier2_common;
pub mod tier3_advanced;
pub mod tier4_specialized;
