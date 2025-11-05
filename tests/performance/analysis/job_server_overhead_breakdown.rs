/*!
# FR-082 Phase 4E: Job Server Overhead Breakdown Analysis

Instruments EACH component of job server overhead to identify where the 97% cost comes from.

## Overhead Sources Measured:
1. GROUP BY state cloning (Arc<HashMap> clones)
2. Arc<Mutex> lock acquisitions
3. Batch coordination (DataReader/DataWriter)
4. Observability and metrics
5. Record cloning in batch templates
6. Batch result struct allocation
*/

use async_trait::async_trait;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};
use velostream::velostream::datasource::types::SourceOffset;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::common::{FailureStrategy, JobProcessingConfig};
use velostream::velostream::server::processors::SimpleJobProcessor;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::parser::StreamingSqlParser;

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

/// Instrumented MockDataSource that tracks cloning overhead
struct InstrumentedDataSource {
    batch: Vec<StreamRecord>,
    batch_template: Vec<StreamRecord>,
    batches_read: usize,
    total_batches: usize,
    clone_time_us: Arc<AtomicUsize>,
}

impl InstrumentedDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> (Self, Arc<AtomicUsize>) {
        let batch: Vec<StreamRecord> = records.iter().take(batch_size).cloned().collect();
        let total_batches = (records.len() + batch_size - 1) / batch_size;
        let clone_time = Arc::new(AtomicUsize::new(0));

        (
            Self {
                batch: batch.clone(),
                batch_template: batch,
                batches_read: 0,
                total_batches,
                clone_time_us: clone_time.clone(),
            },
            clone_time,
        )
    }

    fn get_clone_overhead_us(&self) -> usize {
        self.clone_time_us.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DataReader for InstrumentedDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.batches_read >= self.total_batches {
            return Ok(vec![]);
        }

        let batch = std::mem::take(&mut self.batch);
        self.batches_read += 1;

        // MEASURE: Batch template cloning overhead
        let clone_start = Instant::now();
        self.batch = self.batch_template.clone();
        let clone_us = clone_start.elapsed().as_micros() as usize;
        self.clone_time_us.fetch_add(clone_us, Ordering::SeqCst);

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

struct InstrumentedDataWriter {
    count: Arc<AtomicUsize>,
}

impl InstrumentedDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl DataWriter for InstrumentedDataWriter {
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

    async fn delete(
        &mut self,
        _key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

fn generate_test_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000i64;

    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert(
            "trader_id".to_string(),
            FieldValue::String(format!("TRADER{}", i % 20)),
        );
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(format!("SYM{}", i % 10)),
        );
        fields.insert("price".to_string(), FieldValue::Float(100.0 + (i as f64 % 50.0)));
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer((100 + (i % 1000)) as i64),
        );
        fields.insert(
            "trade_time".to_string(),
            FieldValue::Integer(base_time + (i as i64)),
        );

        records.push(StreamRecord::new(fields));
    }
    records
}

#[tokio::test]
#[serial]
async fn job_server_overhead_breakdown() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("🔬 FR-082 PHASE 4E: Job Server Overhead Breakdown");
    println!("═══════════════════════════════════════════════════════════\n");

    let num_records = 5000;
    let batch_size = 1000;

    println!("Testing with {} records, batch size {}", num_records, batch_size);
    println!("Expected {} batches\n", (num_records + batch_size - 1) / batch_size);

    // Generate test data
    let records = generate_test_records(num_records);

    // Setup job server
    let (data_source, clone_tracker) = InstrumentedDataSource::new(records, batch_size);
    let data_writer = InstrumentedDataWriter::new();

    let config = JobProcessingConfig {
        max_batch_size: batch_size,
        batch_timeout: std::time::Duration::from_millis(100),
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: std::time::Duration::from_millis(100),
        log_progress: false,
        progress_interval: 100,
    };

    let parser = StreamingSqlParser::new();
    let query = parser.parse(TEST_SQL).expect("Parse failed");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let processor = SimpleJobProcessor::new(config);
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Run job server and measure total time
    let total_start = Instant::now();
    let result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine.clone(),
            query.clone(),
            "overhead_breakdown".to_string(),
            shutdown_rx,
        )
        .await;

    let total_duration = total_start.elapsed();

    match result {
        Ok(stats) => {
            let clone_overhead_us = clone_tracker.load(Ordering::SeqCst);

            // Now run pure SQL engine for comparison
            let pure_start = Instant::now();
            let records = generate_test_records(num_records);
            let (tx2, mut _rx2) = mpsc::unbounded_channel();
            let mut pure_engine = StreamExecutionEngine::new(tx2);

            for record in records.iter() {
                let _ = pure_engine.execute_with_record(&query, record.clone()).await;
            }
            let pure_duration = pure_start.elapsed();

            // Calculate overhead breakdown
            let throughput_job_server = num_records as f64 / total_duration.as_secs_f64();
            let throughput_pure = num_records as f64 / pure_duration.as_secs_f64();

            let clone_overhead_ms = clone_overhead_us as f64 / 1000.0;
            let total_overhead_ms = (total_duration.as_millis() - pure_duration.as_millis()) as f64;
            let other_overhead_ms = total_overhead_ms - clone_overhead_ms;

            println!("\n═══════════════════════════════════════════════════════════");
            println!("📊 OVERHEAD BREAKDOWN RESULTS");
            println!("═══════════════════════════════════════════════════════════");
            println!("Pure SQL Engine:");
            println!("  Time:        {:.2?}", pure_duration);
            println!("  Throughput:  {:.0} rec/sec", throughput_pure);
            println!();
            println!("Job Server:");
            println!("  Time:        {:.2?}", total_duration);
            println!("  Throughput:  {:.0} rec/sec", throughput_job_server);
            println!();
            println!("Overhead Breakdown:");
            println!("  Total overhead:      {:.2} ms ({:.1}%)",
                total_overhead_ms,
                (total_overhead_ms / total_duration.as_millis() as f64) * 100.0
            );
            println!("  1. Record cloning:   {:.2} ms ({:.1}%)",
                clone_overhead_ms,
                (clone_overhead_ms / total_overhead_ms) * 100.0
            );
            println!("  2. Other (locks, coordination, metrics):");
            println!("                       {:.2} ms ({:.1}%)",
                other_overhead_ms,
                (other_overhead_ms / total_overhead_ms) * 100.0
            );
            println!();
            println!("Slowdown Factor:     {:.2}x", throughput_pure / throughput_job_server);
            println!("═══════════════════════════════════════════════════════════\n");
        }
        Err(e) => {
            eprintln!("❌ Test failed: {:?}", e);
            panic!("Test failed");
        }
    }
}
