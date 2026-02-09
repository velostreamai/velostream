//! Recursive CTEs Performance Benchmark - Tier 4 Operation
//!
//! **Operation #16 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 4 (Specialized)
//! - **Probability**: 12% of production streaming SQL jobs
//! - **Use Cases**: Hierarchical data, tree traversal, recursive patterns
//!
//! Recursive CTEs are rarely used in streaming but useful for specific hierarchical patterns.
//! Limited support in current implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{get_perf_record_count, validate_sql_query};

/// Generate test data for CTEs: employee hierarchy data with manager_id relationships
fn generate_recursive_cte_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let employee_id = i as i64;
            // Create a tree structure: each employee (except root) reports to employee % 5
            let manager_id = if i == 0 { -1 } else { (i % 5) as i64 };
            let name = format!("Employee_{}", i);
            let timestamp = (i * 1000) as i64;

            fields.insert("employee_id".to_string(), FieldValue::Integer(employee_id));
            fields.insert("manager_id".to_string(), FieldValue::Integer(manager_id));
            fields.insert("name".to_string(), FieldValue::String(name));
            fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

            StreamRecord::new(fields)
        })
        .collect()
}

/// Create the employees table for the recursive CTE test (hierarchical organization)
fn create_data_table() -> Arc<dyn UnifiedTable> {
    let table = OptimizedTableImpl::new();
    for i in 0..10 {
        let mut fields = HashMap::new();
        let manager_id = if i == 0 { -1 } else { (i % 5) as i64 };
        fields.insert("employee_id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("manager_id".to_string(), FieldValue::Integer(manager_id));
        fields.insert(
            "name".to_string(),
            FieldValue::String(format!("Employee_{}", i)),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer((i * 1000) as i64),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
}

/// SQL query with Recursive CTE: traverses employee hierarchy up to CEO
/// Tests ability to walk hierarchical structures using WITH RECURSIVE syntax
const RECURSIVE_CTE_SQL: &str = r#"
    WITH RECURSIVE org_hierarchy AS (
        -- Anchor: start with employees who report to root (manager_id = 0)
        SELECT
            employee_id,
            manager_id,
            name,
            1 as hierarchy_level
        FROM employees
        WHERE manager_id = 0

        UNION ALL

        -- Recursive: find employees reporting to each level
        SELECT
            e.employee_id,
            e.manager_id,
            e.name,
            oh.hierarchy_level + 1
        FROM employees e
        JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
        WHERE oh.hierarchy_level < 5
    )
    SELECT
        employee_id,
        manager_id,
        name,
        hierarchy_level
    FROM org_hierarchy
"#;

/// Test: Recursive CTE performance measurement
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_recursive_cte_performance() {
    // Validate SQL query
    validate_sql_query(RECURSIVE_CTE_SQL);

    let record_count = get_perf_record_count();
    let records = generate_recursive_cte_records(record_count);

    let (sql_sync_throughput, _, _) =
        measure_sql_engine_sync(records.clone(), RECURSIVE_CTE_SQL).await;
    let (sql_async_throughput, _, _) = measure_sql_engine(records.clone(), RECURSIVE_CTE_SQL).await;
    let (simple_jp_throughput, _) = measure_v1(records.clone(), RECURSIVE_CTE_SQL).await;
    let (transactional_jp_throughput, _) =
        measure_transactional_jp(records.clone(), RECURSIVE_CTE_SQL).await;
    let (adaptive_1c_throughput, _) =
        measure_adaptive_jp(records.clone(), RECURSIVE_CTE_SQL, 1).await;
    let (adaptive_4c_throughput, _) =
        measure_adaptive_jp(records.clone(), RECURSIVE_CTE_SQL, 4).await;

    println!(
        "🚀 BENCHMARK_RESULT | recursive_ctes | tier4 | SQL Sync: {:.0} | SQL Async: {:.0} | SimpleJp: {:.0} | TransactionalJp: {:.0} | AdaptiveJp (1c): {:.0} | AdaptiveJp (4c): {:.0}",
        sql_sync_throughput,
        sql_async_throughput,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_1c_throughput,
        adaptive_4c_throughput
    );
}

/// Measure SimpleJp (V1)
async fn measure_v1(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    let config = JobProcessingConfig {
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
    };

    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("employees".to_string(), create_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Simple,
        Some(config),
        Some(table_registry),
    );
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(60);
    let _result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "recursive_cte_v1_test".to_string(),
            shutdown_rx,
            None,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure TransactionalJp
async fn measure_transactional_jp(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("employees".to_string(), create_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Transactional,
        None,
        Some(table_registry),
    );
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(60);
    let _result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "recursive_cte_transactional_test".to_string(),
            shutdown_rx,
            None,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

async fn measure_adaptive_jp(
    records: Vec<StreamRecord>,
    query: &str,
    num_cores: usize,
) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("employees".to_string(), create_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Adaptive {
            num_partitions: Some(num_cores),
            enable_core_affinity: false,
        },
        None,
        Some(table_registry),
    );
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(60);
    let _result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            format!("recursive_cte_adaptive_{}c_test", num_cores),
            shutdown_rx,
            None,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure SQL Engine (sync version)
async fn measure_sql_engine_sync(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let parser = StreamingSqlParser::new();
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

/// Measure SQL Engine (async version)
async fn measure_sql_engine(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let parser = StreamingSqlParser::new();
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
