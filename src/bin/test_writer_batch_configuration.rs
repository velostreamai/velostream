use ferrisstreams::ferris::datasource::{
    config::SinkConfig, file::data_sink::FileDataSink, kafka::data_sink::KafkaDataSink,
    traits::DataSink, BatchConfig, BatchStrategy,
};
use ferrisstreams::ferris::schema::{FieldDefinition, Schema};
use ferrisstreams::ferris::sql::ast::DataType;
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use log::info;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize env_logger to see the log output
    env_logger::init();

    info!("=== Testing Writer Batch Configuration ===\n");

    // Test Kafka Sink batch configurations
    test_kafka_sink_batch_configs().await?;

    // Test File Sink batch configurations
    test_file_sink_batch_configs().await?;

    info!("\n=== Writer Batch Configuration Tests Complete ===");
    Ok(())
}

async fn test_kafka_sink_batch_configs() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🎯 Testing Kafka Sink Batch Configurations");

    // Test 1: FixedSize batch strategy
    info!("\n1. Testing Kafka Sink with FixedSize batch strategy (100 records)...");
    let fixed_size_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(5000),
    };

    let mut kafka_sink = KafkaDataSink::new(
        "localhost:9092".to_string(),
        "test-output-fixed".to_string(),
    );

    let sink_config = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test-output-fixed".to_string(),
        properties: HashMap::new(),
    };

    kafka_sink.initialize(sink_config).await?;
    let _fixed_writer = kafka_sink
        .create_writer_with_batch_config(fixed_size_config)
        .await?;

    // Test 2: LowLatency batch strategy (eager processing)
    info!("\n2. Testing Kafka Sink with LowLatency batch strategy (eager processing)...");
    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency {
            max_batch_size: 5,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true,
        },
        max_batch_size: 10,
        batch_timeout: Duration::from_millis(100),
    };

    let mut kafka_sink_ll = KafkaDataSink::new(
        "localhost:9092".to_string(),
        "test-output-low-latency".to_string(),
    );

    let sink_config_ll = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test-output-low-latency".to_string(),
        properties: HashMap::new(),
    };

    kafka_sink_ll.initialize(sink_config_ll).await?;
    let _ll_writer = kafka_sink_ll
        .create_writer_with_batch_config(low_latency_config)
        .await?;

    // Test 3: MemoryBased batch strategy
    info!("\n3. Testing Kafka Sink with MemoryBased batch strategy (1MB)...");
    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(1024 * 1024), // 1MB
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(10000),
    };

    let mut kafka_sink_mem = KafkaDataSink::new(
        "localhost:9092".to_string(),
        "test-output-memory".to_string(),
    );

    let sink_config_mem = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test-output-memory".to_string(),
        properties: HashMap::new(),
    };

    kafka_sink_mem.initialize(sink_config_mem).await?;
    let _mem_writer = kafka_sink_mem
        .create_writer_with_batch_config(memory_config)
        .await?;

    Ok(())
}

async fn test_file_sink_batch_configs() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("\n🗂️  Testing File Sink Batch Configurations");

    // Test 1: FixedSize batch strategy
    info!("\n1. Testing File Sink with FixedSize batch strategy (50 records)...");
    let fixed_size_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(50),
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(2000),
    };

    let mut file_sink = FileDataSink::new();

    let sink_config = SinkConfig::File {
        path: "./test_output_fixed.csv".to_string(),
        format: ferrisstreams::ferris::datasource::config::FileFormat::Csv {
            header: true,
            delimiter: ',',
            quote: '"',
        },
        properties: HashMap::new(),
        compression: None,
    };

    file_sink.initialize(sink_config).await?;
    let mut fixed_writer = file_sink
        .create_writer_with_batch_config(fixed_size_config)
        .await?;

    // Write some test data
    let test_records = create_test_records(10);
    for record in test_records {
        fixed_writer.write(record).await?;
    }
    fixed_writer.flush().await?;

    // Test 2: LowLatency batch strategy (eager processing)
    info!("\n2. Testing File Sink with LowLatency batch strategy (eager processing)...");
    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency {
            max_batch_size: 1,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true,
        },
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(50),
    };

    let mut file_sink_ll = FileDataSink::new();

    let sink_config_ll = SinkConfig::File {
        path: "./test_output_low_latency.json".to_string(),
        format: ferrisstreams::ferris::datasource::config::FileFormat::Json,
        properties: HashMap::new(),
        compression: None,
    };

    file_sink_ll.initialize(sink_config_ll).await?;
    let mut ll_writer = file_sink_ll
        .create_writer_with_batch_config(low_latency_config)
        .await?;

    // Write test data with immediate flushing expected
    let test_records = create_test_records(5);
    for record in test_records {
        ll_writer.write(record).await?;
        ll_writer.flush().await?; // Should be immediate for eager processing
    }

    // Test 3: MemoryBased batch strategy with compression
    info!("\n3. Testing File Sink with MemoryBased batch strategy (2MB)...");
    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // 2MB
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(30000),
    };

    let mut file_sink_mem = FileDataSink::new();

    let sink_config_mem = SinkConfig::File {
        path: "./test_output_memory.json".to_string(),
        format: ferrisstreams::ferris::datasource::config::FileFormat::Json,
        properties: HashMap::new(),
        compression: None, // Will be automatically enabled by batch config for large memory targets
    };

    file_sink_mem.initialize(sink_config_mem).await?;
    let mut mem_writer = file_sink_mem
        .create_writer_with_batch_config(memory_config)
        .await?;

    // Write a larger dataset
    let test_records = create_test_records(100);
    for record in test_records {
        mem_writer.write(record).await?;
    }
    mem_writer.flush().await?;

    Ok(())
}

fn create_test_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "name".to_string(),
            FieldValue::String(format!("Record-{}", i)),
        );
        fields.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(12345 + (i as i64 * 100), 2),
        ); // $123.45, $124.45, etc.
        fields.insert("active".to_string(), FieldValue::Boolean(i % 2 == 0));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Timestamp(chrono::Utc::now().naive_utc()),
        );

        records.push(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: i as i64,
            partition: 0,
            headers: HashMap::new(),
            event_time: None,
        });
    }

    records
}

fn create_test_schema() -> Schema {
    Schema::new(vec![
        FieldDefinition::new("id".to_string(), DataType::Integer, false),
        FieldDefinition::new("name".to_string(), DataType::String, false),
        FieldDefinition::new("amount".to_string(), DataType::Float, false), // Will be ScaledInteger
        FieldDefinition::new("active".to_string(), DataType::Boolean, false),
        FieldDefinition::new("timestamp".to_string(), DataType::Timestamp, false),
    ])
}
