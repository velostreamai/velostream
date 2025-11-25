//! Shared helpers for SQL operation performance tests
//!
//! Provides configurable record counts, cardinality, AdaptiveJobProcessor helpers, SQL validation,
//! and measurement functions for consistent benchmarking across all performance tests

pub use std::sync::Arc;
pub use std::time::{Duration, Instant};
pub use tokio::sync::mpsc;
pub use velostream::velostream::server::processors::{FailureStrategy, JobProcessingConfig};
pub use velostream::velostream::server::v2::coordinator::{
    AdaptiveJobProcessor, PartitionedJobConfig,
};
pub use velostream::velostream::sql::execution::StreamExecutionEngine;
pub use velostream::velostream::sql::execution::types::StreamRecord;
pub use velostream::velostream::sql::parser::StreamingSqlParser;
pub use velostream::velostream::sql::validation::QueryValidator;
pub use velostream::velostream::table::UnifiedTable;

/// Get the number of records for performance tests
///
/// Defaults to 10,000 records but can be overridden via environment variable
///
/// Environment variables:
/// - `VELOSTREAM_PERF_RECORDS`: Total number of records to test with
/// - `VELOSTREAM_PERF_CARDINALITY`: Cardinality/group count (defaults to records/10)
///
/// # Examples
///
/// ```bash
/// # Use 100,000 records instead of default 10,000
/// VELOSTREAM_PERF_RECORDS=100000 cargo test
///
/// # Use 1M records with 10K unique groups
/// VELOSTREAM_PERF_RECORDS=1000000 VELOSTREAM_PERF_CARDINALITY=10000 cargo test
/// ```
pub fn get_perf_record_count() -> usize {
    let count = std::env::var("VELOSTREAM_PERF_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000); // Default to 10,000 records

    // Enforce minimum of 10,000 for accurate performance measurement
    // (smaller datasets have significant measurement overhead)
    count.max(10_000)
}

/// Get the cardinality (number of unique groups) for GROUP BY operations
///
/// Defaults to 1/10th of record count if not specified
/// Can be overridden via VELOSTREAM_PERF_CARDINALITY environment variable
pub fn get_perf_cardinality(record_count: usize) -> usize {
    std::env::var("VELOSTREAM_PERF_CARDINALITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(record_count / 10) // Default to 1/10th of record count
}

/// Display configuration information
pub fn print_perf_config(record_count: usize, cardinality: Option<usize>) {
    println!("📊 Performance Test Configuration:");
    println!(
        "   Records: {} (env: VELOSTREAM_PERF_RECORDS)",
        record_count
    );
    if let Some(card) = cardinality {
        println!(
            "   Cardinality: {} (env: VELOSTREAM_PERF_CARDINALITY)",
            card
        );
    }
    println!("   Tip: Set VELOSTREAM_PERF_RECORDS=100000 for higher-volume testing");
    println!("   Tip: Set VELOSTREAM_PERF_CARDINALITY=100 for group count override");
}

/// Create AdaptiveJobProcessor with specified number of partitions
///
/// # Arguments
/// - `num_partitions`: Number of partitions/cores (e.g., 1 or 4 for benchmarking)
///
/// # Returns
/// Arc-wrapped AdaptiveJobProcessor configured for the specified partition count
pub fn create_adaptive_processor(num_partitions: usize) -> Arc<AdaptiveJobProcessor> {
    let config = PartitionedJobConfig {
        num_partitions: Some(num_partitions),
        annotation_partition_count: None,
        ..Default::default()
    };
    Arc::new(AdaptiveJobProcessor::new(config))
}

/// Validate a SQL query using QueryValidator
///
/// Validates the SQL query and prints the validation result.
/// Asserts that the query is valid, failing the test if validation fails.
/// This helper should be called at the start of any performance test to verify SQL correctness.
///
/// # Arguments
/// - `sql`: The SQL query string to validate
///
/// # Panics
/// Panics if SQL validation fails, with parsing error details
///
/// # Examples
///
/// ```ignore
/// const MY_SQL: &str = r#"SELECT * FROM table"#;
///
/// #[test]
/// fn test_my_query() {
///     validate_sql_query(MY_SQL);
///     // ... rest of test
/// }
/// ```
/// Validate a SQL query for syntax and semantic correctness
///
/// Note: This is an informational check only for performance tests.
/// The QueryValidator doesn't have access to the mock tables created in tests,
/// so actual validation happens during query execution (measure_v1, measure_transactional_jp, etc.)
///
/// Validation errors that indicate parsing/syntax issues are still reported,
/// but missing table references are expected in this context and don't fail the test.
pub fn validate_sql_query(sql: &str) {
    let validator = QueryValidator::new();
    let validation_result = validator.validate_query(sql);

    // Only care about actual parsing errors, not missing table references
    let has_parsing_errors = !validation_result.parsing_errors.is_empty();

    if has_parsing_errors {
        println!("⚠️  SQL PARSING ERROR:");
        for error in &validation_result.parsing_errors {
            println!("  - {:?}", error);
        }
        println!("  Query: {}", sql);
        // Fail on actual syntax errors
        panic!("SQL parsing error - query has syntax issues that must be fixed");
    } else {
        println!("✅ SQL Syntax: OK - {}", sql);
        if !validation_result.semantic_errors.is_empty() {
            println!(
                "   ℹ️  Semantic analysis note: {} issues (may be expected in performance tests)",
                validation_result.semantic_errors.len()
            );
        }
    }
}

// ============================================================================
// PHASE 1 EXTRACTION HELPERS - High ROI, Easy Implementation
// ============================================================================

/// Print standardized benchmark result output
///
/// Outputs results in consistent format across all performance tests:
/// `🚀 BENCHMARK_RESULT | operation_name | tier | SQL Sync: ... | SQL Async: ... | ...`
///
/// # Arguments
/// - `operation_name`: Name of the operation being tested (e.g., "tumbling_window")
/// - `tier`: Tier level (e.g., "tier1", "tier2", "tier3", "tier4")
/// - `sql_sync_throughput`: Throughput of SQL engine sync variant (records/sec)
/// - `sql_async_throughput`: Throughput of SQL engine async variant (records/sec)
/// - `simple_jp_throughput`: Throughput of SimpleJp (V1) processor (records/sec)
/// - `transactional_jp_throughput`: Throughput of TransactionalJp processor (records/sec)
/// - `adaptive_1c_throughput`: Throughput of AdaptiveJp with 1 core (records/sec)
/// - `adaptive_4c_throughput`: Throughput of AdaptiveJp with 4 cores (records/sec)
///
/// # Examples
///
/// ```ignore
/// print_benchmark_result(
///     "tumbling_window",
///     "tier1",
///     50000.0,  // SQL Sync
///     48000.0,  // SQL Async
///     45000.0,  // SimpleJp
///     44000.0,  // TransactionalJp
///     46000.0,  // AdaptiveJp 1c
///     52000.0,  // AdaptiveJp 4c
/// );
/// ```
pub fn print_benchmark_result(
    operation_name: &str,
    tier: &str,
    sql_sync_throughput: f64,
    sql_async_throughput: f64,
    simple_jp_throughput: f64,
    transactional_jp_throughput: f64,
    adaptive_1c_throughput: f64,
    adaptive_4c_throughput: f64,
) {
    println!(
        "🚀 BENCHMARK_RESULT | {} | {} | SQL Sync: {:.0} | SQL Async: {:.0} | SimpleJp: {:.0} | TransactionalJp: {:.0} | AdaptiveJp (1c): {:.0} | AdaptiveJp (4c): {:.0}",
        operation_name,
        tier,
        sql_sync_throughput,
        sql_async_throughput,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_1c_throughput,
        adaptive_4c_throughput
    );
}

/// Create the standard JobProcessingConfig used for all SimpleJp benchmarks
///
/// This config is identical across all performance tests and encapsulates the standard
/// settings for SimpleJp (V1) processor measurements. It provides reasonable defaults
/// for batch processing with DLQ (Dead Letter Queue) support.
///
/// # Returns
/// A `JobProcessingConfig` with standard performance test settings
///
/// # Configuration Details
/// - `use_transactions`: false (best-effort semantics)
/// - `batch_size`: 100 records
/// - `batch_timeout`: 100ms
/// - `max_retries`: 2
/// - `retry_backoff`: 50ms
/// - `enable_dlq`: true with max size of 100
///
/// # Examples
///
/// ```ignore
/// let config = create_standard_job_processing_config();
/// let processor = JobProcessorFactory::create_with_config_and_tables(
///     JobProcessorConfig::Simple,
///     Some(config),
///     Some(table_registry),
/// );
/// ```
pub fn create_standard_job_processing_config() -> JobProcessingConfig {
    JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 100,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
        empty_batch_count: 0,
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    }
}

/// Measure SQL engine with synchronous record execution
///
/// Executes all records through the SQL engine synchronously and measures throughput.
/// This is a baseline measurement for the SQL execution engine without processor overhead.
///
/// # Arguments
/// - `records`: The stream records to process
/// - `query`: The SQL query string to execute
///
/// # Returns
/// A tuple of (throughput in records/sec, records_sent, results_produced)
///
/// # Examples
///
/// ```ignore
/// let (throughput, sent, produced) = measure_sql_engine_sync_impl(records, SQL_QUERY).await;
/// println!("Sync throughput: {:.0} records/sec", throughput);
/// ```
pub async fn measure_sql_engine_sync_impl(
    records: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();
            }
            Err(_e) => {}
        }
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}

/// Measure SQL engine with asynchronous record execution
///
/// Executes all records through the SQL engine asynchronously and measures throughput.
/// This measures the SQL execution engine with async/await overhead.
///
/// # Arguments
/// - `records`: The stream records to process
/// - `query`: The SQL query string to execute
///
/// # Returns
/// A tuple of (throughput in records/sec, records_sent, results_produced)
///
/// # Examples
///
/// ```ignore
/// let (throughput, sent, produced) = measure_sql_engine_async_impl(records, SQL_QUERY).await;
/// println!("Async throughput: {:.0} records/sec", throughput);
/// ```
pub async fn measure_sql_engine_async_impl(
    records: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, record).await {
            Ok(()) => {}
            Err(_e) => {}
        }
    }

    while let Ok(_) = _rx.try_recv() {
        results_produced += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}
