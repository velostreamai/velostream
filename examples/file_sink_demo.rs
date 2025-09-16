//! File Sink Demo
//!
//! This example demonstrates the file sink implementation for writing streaming data
//! to files with various formats and rotation strategies.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use velostream::velostream::datasource::file::{
    config::{CompressionType, FileFormat, FileSinkConfig},
    FileDataSink,
};
use velostream::velostream::datasource::traits::DataSink;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 VeloStream File Sink Demo");
    println!("=================================");

    // Demo 1: JSON Lines output with file rotation
    println!("\n📝 Demo 1: JSON Lines with rotation");
    demo_json_sink().await?;

    // Demo 2: CSV output with compression
    println!("\n📊 Demo 2: CSV with compression");
    demo_csv_sink().await?;

    // Demo 3: High-throughput batch writing
    println!("\n⚡ Demo 3: High-throughput batch writing");
    demo_batch_writing().await?;

    println!("\n✅ All demos completed successfully!");
    Ok(())
}

async fn demo_json_sink() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration for JSON Lines output with 1MB file rotation
    let config = FileSinkConfig::new(
        "./demo_output/transactions.jsonl".to_string(),
        FileFormat::JsonLines,
    )
    .with_rotation_size(1024 * 1024) // 1MB files
    .with_compression(CompressionType::Gzip);

    let mut sink = FileDataSink::new();
    sink.initialize(config.into())
        .await
        .map_err(|e| format!("Initialize error: {}", e))?;

    let mut writer = sink
        .create_writer()
        .await
        .map_err(|e| format!("Create writer error: {}", e))?;

    // Generate sample transaction records
    for i in 1..=100 {
        let mut fields = HashMap::new();
        fields.insert(
            "transaction_id".to_string(),
            FieldValue::String(format!("txn_{:04}", i)),
        );
        fields.insert(
            "customer_id".to_string(),
            FieldValue::String(format!("cust_{}", 100 + (i % 50))),
        );
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(
                (1000 + i * 37) % 50000, // Random-ish amounts
                2,                       // 2 decimal places
            ),
        );
        fields.insert(
            "currency".to_string(),
            FieldValue::String("USD".to_string()),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64),
        );

        let record = StreamRecord {
            fields,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
            offset: i,
            partition: 0,
            headers: HashMap::new(),
            event_time: None, // Use processing time by default
        };
        writer
            .write(record)
            .await
            .map_err(|e| format!("Write error: {}", e))?;

        if i % 10 == 0 {
            sleep(Duration::from_millis(100)).await; // Simulate real-time processing
        }
    }

    writer
        .flush()
        .await
        .map_err(|e| format!("Flush error: {}", e))?;
    println!("  ✅ Written 100 transaction records to JSON Lines files");

    Ok(())
}

async fn demo_csv_sink() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration for CSV output with compression
    let config = FileSinkConfig::new(
        "./demo_output/user_metrics.csv".to_string(),
        FileFormat::Csv,
    )
    .with_compression(CompressionType::Snappy);

    let mut sink = FileDataSink::new();
    sink.initialize(config.into())
        .await
        .map_err(|e| format!("Initialize error: {}", e))?;

    let mut writer = sink
        .create_writer()
        .await
        .map_err(|e| format!("Create writer error: {}", e))?;

    // Generate sample user metrics records
    for i in 1..=50 {
        let mut fields = HashMap::new();
        fields.insert(
            "user_id".to_string(),
            FieldValue::String(format!("user_{:03}", i)),
        );
        fields.insert(
            "session_count".to_string(),
            FieldValue::Integer((i * 3) % 20),
        );
        fields.insert(
            "total_spent".to_string(),
            FieldValue::ScaledInteger(
                (i * 1250) % 100000, // Random amounts
                2,
            ),
        );
        fields.insert(
            "avg_session_time".to_string(),
            FieldValue::Float(300.0 + (i as f64 * 17.3) % 1800.0),
        );
        fields.insert(
            "last_active".to_string(),
            FieldValue::String(format!("2024-01-{:02}", 1 + (i % 30))),
        );

        let record = StreamRecord {
            fields,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
            offset: i,
            partition: 0,
            headers: HashMap::new(),
            event_time: None, // Use processing time by default
        };
        writer
            .write(record)
            .await
            .map_err(|e| format!("Write error: {}", e))?;
    }

    writer
        .flush()
        .await
        .map_err(|e| format!("Flush error: {}", e))?;
    println!("  ✅ Written 50 user metrics records to CSV file");

    Ok(())
}

async fn demo_batch_writing() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration optimized for high throughput
    let config = FileSinkConfig::new(
        "./demo_output/high_volume.jsonl".to_string(),
        FileFormat::JsonLines,
    )
    .with_rotation_size(5 * 1024 * 1024) // 5MB files
    .with_rotation_interval(60_000); // 1 minute rotation

    let mut sink = FileDataSink::new();
    sink.initialize(config.into())
        .await
        .map_err(|e| format!("Initialize error: {}", e))?;

    let mut writer = sink
        .create_writer()
        .await
        .map_err(|e| format!("Create writer error: {}", e))?;

    // Generate large batches of records
    let batch_size = 1000;
    let total_records = 10_000;

    println!(
        "  📊 Writing {} records in batches of {}...",
        total_records, batch_size
    );

    for batch in 0..(total_records / batch_size) {
        let mut records = Vec::new();

        for i in 0..batch_size {
            let record_id = batch * batch_size + i;
            let mut fields = HashMap::new();

            fields.insert("record_id".to_string(), FieldValue::Integer(record_id));
            fields.insert("batch_id".to_string(), FieldValue::Integer(batch));
            fields.insert(
                "value".to_string(),
                FieldValue::Float((record_id as f64) * 1.618 % 1000.0),
            );
            fields.insert(
                "category".to_string(),
                FieldValue::String(format!("cat_{}", record_id % 10)),
            );
            fields.insert(
                "processed_at".to_string(),
                FieldValue::Integer(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64),
            );

            records.push(StreamRecord {
                fields,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
                offset: record_id,
                partition: 0,
                headers: HashMap::new(),
                event_time: None, // Use processing time by default
            });
        }

        // Write entire batch
        writer
            .write_batch(records)
            .await
            .map_err(|e| format!("Batch write error: {}", e))?;

        if batch % 2 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    writer
        .flush()
        .await
        .map_err(|e| format!("Flush error: {}", e))?;
    println!(
        "\n  ✅ Written {} records in high-throughput batch mode",
        total_records
    );

    Ok(())
}
