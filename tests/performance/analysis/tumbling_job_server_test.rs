/*!
# FR-082 Phase 4E: Job Server Tumbling Window Performance Test

Runs the IDENTICAL TUMBLING WINDOW + GROUP BY query through the job server
infrastructure to measure actual overhead vs pure SQL engine execution.

## Comparison
- Pure SQL Engine: 790K rec/sec (from tumbling_instrumented_profiling.rs)
- Job Server: ? rec/sec (this test)
*/

use async_trait::async_trait;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::SimpleJobProcessor;
use velostream::velostream::server::processors::common::{FailureStrategy, JobProcessingConfig};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Same query as tumbling_instrumented_profiling.rs
const TEST_SQL: &str = r#"
    SELECT
        trader_id,
        symbol,
        COUNT(*) as trade_count,
        AVG(price) as avg_price,
        SUM(quantity) as total_quantity,
        SUM(price * quantity) as total_value
    FROM market_data
    GROUP BY trader_id, symbol
    WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
"#;

fn generate_test_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000i64;

    for i in 0..count {
        let mut fields = HashMap::new();
        let trader_id = format!("TRADER{}", i % 20);
        let symbol = format!("SYM{}", i % 10);
        let price = 100.0 + (i as f64 % 50.0);
        let quantity = 100 + (i % 1000);
        let timestamp = base_time + (i as i64);

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
        fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    records
}

/// Mock data source for job server testing
struct MockDataSource {
    batch: Vec<StreamRecord>,
    batch_template: Vec<StreamRecord>,
    batches_read: usize,
    total_batches: usize,
}

impl MockDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        let batch: Vec<StreamRecord> = records.iter().take(batch_size).cloned().collect();
        let total_batches = (records.len() + batch_size - 1) / batch_size;

        Self {
            batch: batch.clone(),
            batch_template: batch,
            batches_read: 0,
            total_batches,
        }
    }
}

#[async_trait]
impl DataReader for MockDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.batches_read >= self.total_batches {
            return Ok(vec![]);
        }

        let batch = std::mem::take(&mut self.batch);
        self.batches_read += 1;
        self.batch = self.batch_template.clone();

        Ok(batch)
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
        Ok(self.batches_read < self.total_batches)
    }
}

/// Mock writer for job server testing
struct MockDataWriter {
    count: Arc<AtomicUsize>,
}

impl MockDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(records.len(), Ordering::SeqCst);
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

/// FR-082 Phase 4E: Job Server Tumbling Window Performance Test
#[tokio::test]
#[serial]
async fn job_server_tumbling_window_performance() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 PHASE 4E: Job Server Tumbling Window Test");
    println!("═══════════════════════════════════════════════════════════\n");
    println!("Goal: Measure job server overhead for TUMBLING WINDOW query");
    println!("Query: {}", TEST_SQL);
    println!("═══════════════════════════════════════════════════════════\n");

    let num_records = 5000;
    let batch_size = 1000;

    let records = generate_test_records(num_records);
    println!("✅ Generated {} test records\n", num_records);

    let data_source = MockDataSource::new(records, batch_size);
    let data_writer = MockDataWriter::new();

    let config = JobProcessingConfig {
        max_batch_size: batch_size,
        batch_timeout: Duration::from_millis(100),
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: Duration::from_millis(100),
        log_progress: false,
        progress_interval: 100,
    };

    let parser = StreamingSqlParser::new();
    let query = parser.parse(TEST_SQL).expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let processor = SimpleJobProcessor::new(config);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    println!("🚀 Starting job server processing...");
    let start = Instant::now();

    let result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            query,
            "tumbling_perf_test".to_string(),
            shutdown_rx,
        )
        .await;

    let duration = start.elapsed();

    match result {
        Ok(stats) => {
            let throughput = num_records as f64 / duration.as_secs_f64();

            println!("\n═══════════════════════════════════════════════════════════");
            println!("📊 JOB SERVER PERFORMANCE RESULTS");
            println!("═══════════════════════════════════════════════════════════");
            println!("Total records:         {}", num_records);
            println!("Processing time:       {:?}", duration);
            println!("Throughput:            {:.0} rec/sec", throughput);
            println!("Batches processed:     {}", stats.batches_processed);
            println!("Records processed:     {}", stats.records_processed);
            println!();
            println!("📈 COMPARISON:");
            println!("  Pure SQL Engine:     790,399 rec/sec (baseline)");
            println!(
                "  Job Server:          {:.0} rec/sec (this test)",
                throughput
            );
            println!();

            let slowdown_factor = 790399.0 / throughput;
            let overhead_percent = ((790399.0 - throughput) / 790399.0) * 100.0;

            println!("  Slowdown Factor:     {:.2}x", slowdown_factor);
            println!("  Overhead:            {:.1}%", overhead_percent);
            println!();

            if overhead_percent < 10.0 {
                println!("  ✅ Minimal overhead - job server is well optimized");
            } else if overhead_percent < 30.0 {
                println!("  ⚡ Acceptable overhead for production features");
            } else if overhead_percent < 50.0 {
                println!("  ⚠️  Moderate overhead - investigate batching/metrics");
            } else {
                println!("  ❌ Significant overhead detected");
                println!("      Primary suspects:");
                println!("      - Batch allocation and coordination");
                println!("      - Lock contention (Arc<Mutex<>>)");
                println!("      - Metrics collection");
                println!("      - Channel communication");
            }

            println!("═══════════════════════════════════════════════════════════\n");
        }
        Err(e) => {
            eprintln!("❌ Job server processing failed: {:?}", e);
            panic!("Test failed");
        }
    }
}
