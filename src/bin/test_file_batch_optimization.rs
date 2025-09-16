use std::error::Error;
use std::time::Instant;
use velostream::velostream::datasource::{
    file::data_source::FileDataSource, traits::DataSource, BatchConfig, BatchStrategy,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("=== FileReader Batch Optimization Test ===\n");

    // Test 1: Traditional single record reading
    println!("1. Testing traditional single record reading...");
    let start = Instant::now();
    let traditional_count = test_traditional_reading().await?;
    let traditional_time = start.elapsed();
    println!("   Records read: {}", traditional_count);
    println!("   Time taken: {:?}\n", traditional_time);

    // Test 2: Fixed batch size optimization
    println!("2. Testing fixed batch size (50) optimization...");
    let start = Instant::now();
    let batch_count = test_batch_reading(BatchStrategy::FixedSize(50)).await?;
    let batch_time = start.elapsed();
    println!("   Records read: {}", batch_count);
    println!("   Time taken: {:?}", batch_time);

    if traditional_time > batch_time {
        let speedup = traditional_time.as_millis() as f64 / batch_time.as_millis() as f64;
        println!("   🚀 Batch reading is {:.2}x faster!\n", speedup);
    } else {
        println!("   ⚠️  Traditional reading was faster (small dataset)\n");
    }

    // Test 3: Adaptive batch sizing
    println!("3. Testing adaptive batch sizing...");
    let start = Instant::now();
    let adaptive_count = test_batch_reading(BatchStrategy::AdaptiveSize {
        min_size: 10,
        max_size: 100,
        target_latency: std::time::Duration::from_millis(10),
    })
    .await?;
    let adaptive_time = start.elapsed();
    println!("   Records read: {}", adaptive_count);
    println!("   Time taken: {:?}\n", adaptive_time);

    // Test 4: Memory-based batching
    println!("4. Testing memory-based batching (32KB)...");
    let start = Instant::now();
    let memory_count = test_batch_reading(BatchStrategy::MemoryBased(32768)).await?;
    let memory_time = start.elapsed();
    println!("   Records read: {}", memory_count);
    println!("   Time taken: {:?}\n", memory_time);

    // Test 5: Low-latency batching (eager processing)
    println!("5. Testing low-latency batching (eager processing)...");
    let start = Instant::now();
    let low_latency_count = test_batch_reading(BatchStrategy::LowLatency {
        max_batch_size: 5,
        max_wait_time: std::time::Duration::from_millis(2),
        eager_processing: true,
    })
    .await?;
    let low_latency_time = start.elapsed();
    println!("   Records read: {}", low_latency_count);
    println!("   Time taken: {:?}", low_latency_time);

    if low_latency_time < traditional_time {
        let speedup = traditional_time.as_millis() as f64 / low_latency_time.as_millis() as f64;
        println!("   🚀 Low-latency reading is {:.2}x faster!\n", speedup);
    } else {
        println!("   ⚡ Low-latency optimized for minimal delay over throughput\n");
    }

    // Test 6: Low-latency batching (standard mode)
    println!("6. Testing low-latency batching (standard mode)...");
    let start = Instant::now();
    let low_latency_std_count = test_batch_reading(BatchStrategy::LowLatency {
        max_batch_size: 10,
        max_wait_time: std::time::Duration::from_millis(5),
        eager_processing: false,
    })
    .await?;
    let low_latency_std_time = start.elapsed();
    println!("   Records read: {}", low_latency_std_count);
    println!("   Time taken: {:?}\n", low_latency_std_time);

    println!("=== Test Complete ===");
    println!("Strategy performance comparison:");
    println!("  Traditional: {:?}", traditional_time);
    println!(
        "  Fixed Batch: {:?} ({:.2}x speedup)",
        batch_time,
        traditional_time.as_millis() as f64 / batch_time.as_millis() as f64
    );
    println!("  Adaptive:    {:?}", adaptive_time);
    println!("  Memory-based:{:?}", memory_time);
    println!("  Low-latency (eager): {:?}", low_latency_time);
    println!("  Low-latency (std):   {:?}", low_latency_std_time);

    Ok(())
}

async fn test_traditional_reading() -> Result<usize, Box<dyn Error + Send + Sync>> {
    let mut file_source = FileDataSource::from_properties(
        &[
            (
                "path".to_string(),
                "./demo/datasource-demo/demo_data/financial_transactions.csv".to_string(),
            ),
            ("format".to_string(), "csv".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    file_source.self_initialize().await?;

    let mut reader = file_source.create_reader().await?;
    let mut total_records = 0;

    // Read one record at a time
    while reader.has_more().await? {
        let records = reader.read().await?;
        total_records += records.len();
        if records.is_empty() {
            break;
        }
    }

    Ok(total_records)
}

async fn test_batch_reading(
    strategy: BatchStrategy,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let mut file_source = FileDataSource::from_properties(
        &[
            (
                "path".to_string(),
                "./demo/datasource-demo/demo_data/financial_transactions.csv".to_string(),
            ),
            ("format".to_string(), "csv".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    file_source.self_initialize().await?;

    let batch_config = BatchConfig {
        enable_batching: true,
        strategy,
        max_batch_size: 1000,
        batch_timeout: std::time::Duration::from_millis(5000),
    };

    let mut reader = file_source
        .create_reader_with_batch_config(batch_config)
        .await?;
    let mut total_records = 0;
    let mut batch_count = 0;

    // Read in batches
    while reader.has_more().await? {
        let records = reader.read().await?;
        if records.is_empty() {
            break;
        }
        total_records += records.len();
        batch_count += 1;

        // Log batch details for first few batches
        if batch_count <= 3 {
            println!("   Batch {}: {} records", batch_count, records.len());
            if let Some(first_record) = records.first() {
                if let Some(amount) = first_record.fields.get("amount") {
                    println!("     First record amount: {:?}", amount);
                }
            }
        }
    }

    println!("   Total batches: {}", batch_count);
    Ok(total_records)
}
