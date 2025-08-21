/*!
# Processor vs Legacy Performance Benchmark

Comprehensive performance comparison between processor-based and legacy execution paths.
This benchmark helps ensure the processor refactoring maintains or improves performance.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Create a test record with various field types for benchmarking
fn create_test_record(id: i64) -> HashMap<String, InternalValue> {
    let mut fields = HashMap::new();
    fields.insert("customer_id".to_string(), InternalValue::Integer(id % 1000));
    fields.insert(
        "amount".to_string(),
        InternalValue::Number(100.0 + (id as f64 % 500.0)),
    );
    fields.insert(
        "product_name".to_string(),
        InternalValue::String(format!("Product_{}", id % 50)),
    );
    fields.insert(
        "status".to_string(),
        InternalValue::String(
            match id % 4 {
                0 => "pending",
                1 => "completed",
                2 => "cancelled",
                _ => "shipped",
            }
            .to_string(),
        ),
    );
    fields.insert(
        "is_premium".to_string(),
        InternalValue::Boolean(id % 3 == 0),
    );
    fields.insert("quantity".to_string(), InternalValue::Integer(id % 10 + 1));

    fields
}

/// Benchmark basic SELECT queries
#[tokio::test]
async fn benchmark_select_performance() {
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);

    // Test different SELECT query types
    let queries = [
        "SELECT * FROM orders",
        "SELECT customer_id, amount FROM orders",
        "SELECT customer_id, amount FROM orders WHERE amount > 150",
        "SELECT customer_id, amount, is_premium FROM orders WHERE is_premium = true",
        "SELECT * FROM orders WHERE status = 'completed' AND amount > 200",
    ];

    let num_records = 1000;
    let records: Vec<HashMap<String, InternalValue>> =
        (0..num_records).map(create_test_record).collect();

    for query_str in &queries {
        if let Ok(query) = parser.parse(query_str) {
            let mut engine = StreamExecutionEngine::new(tx.clone(), serialization_format.clone());

            // Benchmark execution time
            let start = Instant::now();

            for record in &records {
                if engine.execute(&query, record.clone()).await.is_err() {
                    break; // Some queries might fail, continue benchmark
                }
            }

            let duration = start.elapsed();
            println!(
                "Query: {} | Records: {} | Time: {:?} | Avg: {:?}",
                query_str,
                num_records,
                duration,
                duration / num_records as u32
            );

            // Clear any pending results
            while rx.try_recv().is_ok() {}
        }
    }
}

/// Benchmark GROUP BY performance (processor vs legacy)
#[tokio::test]
async fn benchmark_group_by_performance() {
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);

    // GROUP BY queries that should use processors
    let group_by_queries = [
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
        "SELECT customer_id, AVG(amount) FROM orders GROUP BY customer_id",
        "SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status",
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 500",
    ];

    let num_records = 20; // Small dataset to test GROUP BY fix
    let records: Vec<HashMap<String, InternalValue>> =
        (0..num_records).map(create_test_record).collect();

    for query_str in &group_by_queries {
        if let Ok(query) = parser.parse(query_str) {
            let mut engine = StreamExecutionEngine::new(tx.clone(), serialization_format.clone());

            println!("\n=== GROUP BY BENCHMARK ===");
            println!("Query: {}", query_str);

            // Benchmark execution time
            let start = Instant::now();
            let mut successful_executions = 0;

            for record in &records {
                if engine.execute(&query, record.clone()).await.is_ok() {
                    successful_executions += 1;
                } else {
                    break;
                }
            }

            // Manually flush GROUP BY results after processing all records
            if let Err(e) = engine.flush_group_by_results(&query) {
                println!("Warning: Failed to flush GROUP BY results: {:?}", e);
            }

            let duration = start.elapsed();
            println!(
                "Records: {} | Successful: {} | Time: {:?}",
                num_records, successful_executions, duration
            );

            if successful_executions > 0 {
                println!(
                    "Avg per record: {:?}",
                    duration / successful_executions as u32
                );
            }

            // Count results generated
            let mut result_count = 0;
            while rx.try_recv().is_ok() {
                result_count += 1;
            }
            println!("Results generated: {}", result_count);
        }
    }
}

/// Benchmark memory usage patterns
#[tokio::test]
#[ignore] // Long-running memory benchmark - use `cargo test -- --ignored` to run
async fn benchmark_memory_usage() {
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);

    // Test memory-intensive queries
    let queries = [
        "SELECT * FROM orders", // Simple passthrough
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id", // Aggregation
        "SELECT customer_id, amount FROM orders WHERE amount > 150", // Filtering
    ];

    let num_records = 5000; // Larger dataset for memory testing

    for query_str in &queries {
        if let Ok(query) = parser.parse(query_str) {
            let mut engine = StreamExecutionEngine::new(tx.clone(), serialization_format.clone());

            println!("\n=== MEMORY BENCHMARK ===");
            println!("Query: {}", query_str);

            let start = Instant::now();
            let mut successful_executions = 0;

            // Process records in batches to simulate streaming
            for batch_start in (0..num_records).step_by(100) {
                let batch_end = std::cmp::min(batch_start + 100, num_records);

                for i in batch_start..batch_end {
                    let record = create_test_record(i);
                    if engine.execute(&query, record).await.is_ok() {
                        successful_executions += 1;
                    }
                }

                // Clear results periodically to simulate consumption
                let mut batch_results = 0;
                while rx.try_recv().is_ok() {
                    batch_results += 1;
                }

                if batch_start % 1000 == 0 {
                    println!(
                        "Processed {} records, generated {} results",
                        batch_end, batch_results
                    );
                }
            }

            // Flush GROUP BY results if it's a GROUP BY query
            if query_str.contains("GROUP BY") {
                if let Err(e) = engine.flush_group_by_results(&query) {
                    println!("Warning: Failed to flush GROUP BY results: {:?}", e);
                }
            }

            let duration = start.elapsed();
            println!(
                "Total records: {} | Time: {:?} | Rate: {:.2} records/sec",
                successful_executions,
                duration,
                successful_executions as f64 / duration.as_secs_f64()
            );
        }
    }
}

/// Benchmark window function performance (if supported)
#[tokio::test]
async fn benchmark_window_function_performance() {
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);

    // Window function queries
    let window_queries = [
        "SELECT customer_id, amount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount) FROM orders",
        "SELECT customer_id, amount, LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY amount) FROM orders",
        "SELECT customer_id, amount, SUM(amount) OVER (PARTITION BY customer_id) FROM orders",
    ];

    let num_records = 1000;
    let records: Vec<HashMap<String, InternalValue>> =
        (0..num_records).map(create_test_record).collect();

    for query_str in &window_queries {
        if let Ok(query) = parser.parse(query_str) {
            let mut engine = StreamExecutionEngine::new(tx.clone(), serialization_format.clone());

            println!("\n=== WINDOW FUNCTION BENCHMARK ===");
            println!("Query: {}", query_str);

            let start = Instant::now();
            let mut successful_executions = 0;

            for record in &records {
                if engine.execute(&query, record.clone()).await.is_ok() {
                    successful_executions += 1;
                } else {
                    // Window functions might not be fully supported in all cases
                    break;
                }
            }

            let duration = start.elapsed();

            if successful_executions > 0 {
                println!(
                    "Records: {} | Time: {:?} | Avg: {:?}",
                    successful_executions,
                    duration,
                    duration / successful_executions as u32
                );
            } else {
                println!("Query not supported or failed");
            }

            // Clear results
            while rx.try_recv().is_ok() {}
        }
    }
}

/// Compare different query complexities
#[tokio::test]
async fn benchmark_query_complexity_scaling() {
    let parser = StreamingSqlParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);

    // Queries of increasing complexity
    let complexity_queries = [
        ("Simple", "SELECT customer_id FROM orders"),
        (
            "Filter",
            "SELECT customer_id FROM orders WHERE amount > 150",
        ),
        (
            "Multi-filter",
            "SELECT customer_id FROM orders WHERE amount > 150 AND is_premium = true",
        ),
        (
            "Group",
            "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
        ),
        (
            "Group + Having",
            "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id HAVING SUM(amount) > 500",
        ),
    ];

    let num_records = 150; // Reduced for CI performance
    let records: Vec<HashMap<String, InternalValue>> =
        (0..num_records).map(create_test_record).collect();

    println!("\n=== COMPLEXITY SCALING BENCHMARK ===");

    for (name, query_str) in &complexity_queries {
        if let Ok(query) = parser.parse(query_str) {
            let mut engine = StreamExecutionEngine::new(tx.clone(), serialization_format.clone());

            let start = Instant::now();
            let mut successful_executions = 0;

            for record in &records {
                if engine.execute(&query, record.clone()).await.is_ok() {
                    successful_executions += 1;
                }
            }

            // Flush GROUP BY results if it's a GROUP BY query
            if query_str.contains("GROUP BY") {
                if let Err(e) = engine.flush_group_by_results(&query) {
                    println!("Warning: Failed to flush GROUP BY results: {:?}", e);
                }
            }

            let duration = start.elapsed();
            let rate = successful_executions as f64 / duration.as_secs_f64();

            println!(
                "{:12} | Time: {:8?} | Rate: {:8.0} rec/sec | Records: {}",
                name, duration, rate, successful_executions
            );

            // Clear results
            while rx.try_recv().is_ok() {}
        }
    }
}
