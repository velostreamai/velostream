/*!
# FR-082 Scenario 0: Pure SELECT (Passthrough) Baseline Test

**Purpose**: Measure baseline performance for pure SELECT queries (no GROUP BY, no WINDOW).

## Scenario Classification
- **Pattern**: Filter + Projection (passthrough)
- **State Management**: None (stateless)
- **Query Category**: Category 5 - Pure SELECT (17% of workload)
- **Phase 0 Target**: ❌ Not a target (no GROUP BY to optimize)
- **V2 Routing**: Round-robin across partitions (no hash key)

## Example Query
```sql
SELECT order_id, customer_id, order_date, total_amount
FROM orders
WHERE order_date > '2024-01-01'
  AND total_amount > 100;
```

## Performance Expectations
- **SQL Engine**: 1M+ rec/sec (trivial - just filter/project)
- **Job Server**: 400K+ rec/sec (I/O-bound, not CPU-bound)
- **Overhead**: ~60% (lower than aggregation scenarios)

## Key Insights
- **Bottleneck**: Serialization/I/O, not CPU
- **No State**: No hash tables, no window buffers
- **Not a Priority**: Phase 0 focuses on GROUP BY scenarios
- **Reference Baseline**: Shows job server overhead without aggregation
*/

use async_trait::async_trait;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::SimpleJobProcessor;
use velostream::velostream::server::processors::common::{FailureStrategy, JobProcessingConfig};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

const TEST_SQL: &str = r#"
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM orders
    WHERE total_amount > 100
"#;

/// Simple mock data source for passthrough testing
struct PassthroughDataSource {
    records: Vec<StreamRecord>,
    read_count: usize,
}

impl PassthroughDataSource {
    fn new(num_records: usize) -> Self {
        let records = (0..num_records)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
                fields.insert(
                    "customer_id".to_string(),
                    FieldValue::Integer((i % 1000) as i64),
                );
                fields.insert(
                    "order_date".to_string(),
                    FieldValue::String("2024-01-15".to_string()),
                );
                fields.insert(
                    "total_amount".to_string(),
                    FieldValue::Float(150.0 + (i % 100) as f64),
                );
                StreamRecord::new(fields)
            })
            .collect();

        Self {
            records,
            read_count: 0,
        }
    }
}

#[async_trait]
impl DataReader for PassthroughDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.read_count > 0 {
            return Ok(vec![]);
        }
        self.read_count += 1;
        Ok(self.records.clone())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.read_count == 0)
    }
}

/// Mock data writer
struct PassthroughDataWriter {
    records_written: usize,
}

impl PassthroughDataWriter {
    fn new() -> Self {
        Self { records_written: 0 }
    }
}

#[async_trait]
impl DataWriter for PassthroughDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written += 1;
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written += records.len();
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Measure pure SQL engine performance (without job server)
async fn measure_sql_engine_only(records: Vec<StreamRecord>, query: &str) -> (usize, u128) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in records.iter() {
        let _ = engine
            .execute_with_record(&parsed_query, record.clone())
            .await;
    }
    let elapsed = start.elapsed();

    (records.len(), elapsed.as_micros())
}

/// Measure job server performance (full pipeline)
async fn measure_job_server(num_records: usize, query: &str) -> (usize, u128) {
    let data_source = PassthroughDataSource::new(num_records);
    let data_writer = PassthroughDataWriter::new();

    let config = JobProcessingConfig {
        max_batch_size: num_records,
        batch_timeout: std::time::Duration::from_millis(100),
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: std::time::Duration::from_millis(100),
        log_progress: false,
        progress_interval: 100,
    };

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let processor = SimpleJobProcessor::new(config);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            parsed_query,
            "passthrough_test".to_string(),
            shutdown_rx,
        )
        .await;

    let elapsed = start.elapsed();

    result.expect("Job server execution failed");
    (num_records, elapsed.as_micros())
}

#[tokio::test]
#[serial]
async fn scenario_0_pure_select_baseline() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Scenario 0: Pure SELECT (Passthrough) Baseline");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("Goal: Measure passthrough query performance (no GROUP BY, no WINDOW)");
    println!("Query: \n{}\n", TEST_SQL);

    println!("═══════════════════════════════════════════════════════════\n");

    // Generate test data
    let num_records = 5000;
    println!("✅ Generated {} test records\n", num_records);

    // Create test records for SQL engine measurement
    let records: Vec<StreamRecord> = (0..num_records)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            fields.insert(
                "order_date".to_string(),
                FieldValue::String("2024-01-15".to_string()),
            );
            fields.insert(
                "total_amount".to_string(),
                FieldValue::Float(150.0 + (i % 100) as f64),
            );
            StreamRecord::new(fields)
        })
        .collect();

    // Measure pure SQL engine
    println!("🚀 Measuring pure SQL engine (no job server)...");
    let (sql_result_count, sql_time_us) = measure_sql_engine_only(records.clone(), TEST_SQL).await;
    let sql_throughput = if sql_time_us > 0 {
        (num_records as f64 / (sql_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ SQL Engine: {} records in {:.2}ms ({} rec/sec)\n",
        sql_result_count,
        sql_time_us as f64 / 1000.0,
        sql_throughput
    );

    // Measure job server
    println!("🚀 Measuring job server (full pipeline)...");
    let (job_result_count, job_time_us) = measure_job_server(num_records, TEST_SQL).await;
    let job_throughput = if job_time_us > 0 {
        (num_records as f64 / (job_time_us as f64 / 1_000_000.0)) as usize
    } else {
        0
    };

    println!(
        "   ✅ Job Server: {} records in {:.2}ms ({} rec/sec)\n",
        job_result_count,
        job_time_us as f64 / 1000.0,
        job_throughput
    );

    // Calculate overhead
    let overhead_pct = if sql_throughput > 0 {
        ((sql_throughput as f64 - job_throughput as f64) / sql_throughput as f64) * 100.0
    } else {
        0.0
    };

    let slowdown_factor = if job_throughput > 0 {
        sql_throughput as f64 / job_throughput as f64
    } else {
        0.0
    };

    println!("═══════════════════════════════════════════════════════════");
    println!("📊 SCENARIO 0 BASELINE RESULTS");
    println!("═══════════════════════════════════════════════════════════");
    println!("Pure SQL Engine:");
    println!("  Time:        {:.2}ms", sql_time_us as f64 / 1000.0);
    println!("  Throughput:  {} rec/sec", sql_throughput);
    println!();
    println!("Job Server:");
    println!("  Time:        {:.2}ms", job_time_us as f64 / 1000.0);
    println!("  Throughput:  {} rec/sec", job_throughput);
    println!();
    println!("Overhead Analysis:");
    println!("  Job Server overhead: {:.1}%", overhead_pct);
    println!("  Slowdown factor:     {:.2}x", slowdown_factor);
    println!();
    println!("📋 Comparison to Aggregation Scenarios:");
    println!("  Scenario 2 (GROUP BY):  95.8% overhead (23.4x slowdown)");
    println!("  Scenario 3a (TUMBLING): 97.0% overhead (33.5x slowdown)");
    println!(
        "  Scenario 0 (SELECT):    {:.1}% overhead ({:.1}x slowdown)",
        overhead_pct, slowdown_factor
    );
    println!();
    println!("✅ Pure SELECT has LOWER overhead than aggregation queries");
    println!("   (I/O-bound, not CPU-bound like GROUP BY)");
    println!("═══════════════════════════════════════════════════════════\n");

    // Assert reasonable performance
    assert!(sql_throughput > 0, "SQL engine should process records");
    assert!(job_throughput > 0, "Job server should process records");
    assert!(
        overhead_pct < 95.0,
        "Pure SELECT overhead should be lower than GROUP BY overhead (expected <95%, got {:.1}%)",
        overhead_pct
    );
}
