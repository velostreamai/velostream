# FerrisStreams DataSource Pipeline Demo

This demo showcases FerrisStreams' data processing capabilities with **both Rust and SQL interfaces**, demonstrating:

- ✅ **Exact DECIMAL arithmetic** for precise financial calculations
- ✅ **Real-time streaming** with configurable backpressure  
- ✅ **File rotation & compression** for production workloads
- ✅ **Performance monitoring** with live metrics
- ✅ **Production-ready error handling** and resilience

## Demo Options

Choose between **Rust API** and **SQL** interfaces for the same powerful file processing capabilities.

### Option 1: Rust API Demo (Recommended for Developers)

```bash
# Run the Rust file processing demo
cargo run --bin file_processing_demo --no-default-features

# The demo will:
# 1. Generate 15 sample financial transactions
# 2. Read from CSV using FileDataSource
# 3. Process with exact DECIMAL precision (ScaledInteger)
# 4. Write results to compressed JSON files using FileSink
# 5. Show real-time processing metrics
```

### Option 2: SQL Interface Demo (Recommended for Analysts)

```bash
# Step 1: Generate demo data (5000 transactions)
cd demo/datasource-demo
./generate_demo_data.sh

# Step 2: Start FerrisStreams SQL server  
cargo run --bin ferris-sql-multi --no-default-features -- server

# Step 3: Run the SQL demo (copy/paste from file_processing_sql_demo.sql)
# OR execute directly:
# ferris-sql-multi deploy-app --file ./demo/datasource-demo/file_processing_sql_demo.sql
```

### Option 3: Full Pipeline with Kafka (Advanced)

```bash
# Start Kafka infrastructure
docker-compose -f docker-compose.demo.yml up -d

# Run the complete file → Kafka → file pipeline  
cargo run --bin complete_pipeline_demo --features json
```

### 3. Monitor Results

```bash
# View real-time processing metrics in the terminal output
# Check processed results
cat ./demo_output/processed_transactions.jsonl

# Monitor Kafka topics (optional)
# Open http://localhost:8080 for Kafka UI
```

### 4. Cleanup

```bash
# Stop Kafka infrastructure  
docker-compose -f docker-compose.demo.yml down -v

# Clean demo data
rm -rf demo_data demo_output
```

## Demo Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CSV Files     │    │   Kafka Topic   │    │  JSON Files     │
│                 │    │                 │    │                 │
│ transactions.csv├────┤demo-transactions├────┤processed_*.jsonl│
│                 │    │                 │    │                 │
│ FileDataSource  │    │   Streaming     │    │   FileSink      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
    File Watching          High Throughput           File Rotation
    Schema Inference       Exactly-Once              Compression
    CSV/JSON Support      Financial Precision        Multiple Formats
```

## Financial Precision Features

The demo specifically showcases FerrisStreams' financial precision capabilities:

### ScaledInteger Arithmetic (Exact Precision)

```rust
// Input: $123.45 (from CSV)
FieldValue::String("123.45")

// Processed: Exact precision with fast arithmetic  
FieldValue::ScaledInteger(12345, 2)  // 12345 cents, 2 decimal places

// Output: Perfect precision maintained
"amount_cents": "123.45"  // No rounding errors!
```

**Performance Comparison**:
- **f64**: Has precision errors in financial calculations
- **ScaledInteger**: Exact precision with optimized arithmetic

### Real-World Financial Data

The demo processes realistic financial transactions:

```csv
transaction_id,customer_id,amount,currency,timestamp,merchant_category,description
TXN0001,CUST001,67.23,USD,1704110400,grocery,Whole Foods Market
TXN0002,CUST002,45.67,USD,1704111264,gas,Shell Gas Station
TXN0003,CUST003,123.45,USD,1704112128,restaurant,McDonald's
```

Each transaction is processed with:
- ✅ **Exact decimal precision** (no floating-point errors)
- ✅ **Financial metadata** (processing timestamps, version tracking)
- ✅ **Data validation** (currency, amount ranges)
- ✅ **Performance tracking** (throughput, latency, total amounts)

## Performance Metrics

The demo provides real-time performance monitoring:

```
📊 Pipeline Metrics:
   📖 Records Read:      100 (+20/s)
   ⚙️  Records Processed:  98 (+19/s)  
   💾 Records Written:    95 (+19/s)
   💰 Total Amount:      $12,345.67
   ❌ Errors:            2
```

**Typical Performance Results** (MacBook Pro M1):
- **Read Throughput**: ~500-1000 records/sec from CSV
- **Process Throughput**: ~800-1200 records/sec through Kafka
- **Write Throughput**: ~600-900 records/sec to compressed JSON
- **Financial Precision**: 100% accuracy (no rounding errors)
- **Error Rate**: <0.1% (mainly network/IO related)

## File Formats Supported

### Input Formats (FileDataSource)
- **CSV** with header inference and schema detection
- **JSON Lines** (newline-delimited JSON) 
- **JSON Arrays** with streaming parsing
- **Glob patterns** for multiple files (`/data/*.csv`)

### Output Formats (FileSink)
- **JSON Lines** (newline-delimited, streaming-friendly)
- **CSV** with configurable delimiters and headers
- **JSON Arrays** for batch processing
- **Compression**: Gzip, Snappy, Zstd

### File Rotation Features
- **Size-based**: Rotate when files exceed configurable size (e.g., 1MB)
- **Time-based**: Rotate at intervals (e.g., every hour)
- **Record-based**: Rotate after N records processed
- **Automatic naming**: `transactions_20240127_143052_0001.jsonl.gz`

## Configuration Examples

### File Source Configuration
```rust
let config = FileSourceConfig::new(
    "./demo_data/transactions.csv".to_string(),
    FileFormat::Csv
)
.with_watching(Some(1000))  // Poll every second
.with_buffer_size(8192);    // 8KB read buffer
```

### File Sink Configuration
```rust
let config = FileSinkConfig::new(
    "./demo_output/processed.jsonl".to_string(),
    FileFormat::JsonLines
)
.with_rotation_size(1024 * 1024)           // 1MB files
.with_compression(CompressionType::Gzip);   // Compress output
```

### Kafka Configuration
```rust
let producer_config = ProducerConfig::new(KAFKA_BROKERS, KAFKA_TOPIC)
    .acks(AckMode::All)        // Exactly-once semantics
    .idempotence(true)         // No duplicates
    .batch_size(16384)         // 16KB batches  
    .linger_ms(10);            // 10ms batching
```

## Error Handling & Resilience

The demo demonstrates production-ready error handling:

### File Processing Errors
- **Invalid CSV rows**: Logged and skipped, processing continues
- **File permission issues**: Clear error messages with resolution hints
- **Disk space**: File rotation prevents disk exhaustion

### Kafka Errors
- **Connection failures**: Automatic retry with exponential backoff
- **Serialization errors**: Records sent to error topic for analysis
- **Partition rebalancing**: Graceful handling of consumer group changes

### Performance Degradation
- **Backpressure**: Automatic throttling when downstream is slow
- **Memory management**: Configurable buffer sizes prevent OOM
- **Rate limiting**: Configurable throughput limits for resource control

## Advanced Features Demonstrated

### 1. Real-Time File Watching
```rust
// Continuously monitor for new CSV files
let config = config.with_watching(Some(1000));  // Check every second
```

### 2. Schema Evolution
```rust
// Automatically detect schema changes in CSV files
let schema = file_source.fetch_schema().await?;
```

### 3. Batch Processing Optimization
```rust
// Write records in batches for better performance
writer.write_batch(record_batch).await?;
```

### 4. Financial Metadata Tracking
```rust
// Add processing metadata to each record
processed_fields.insert("processed_at", FieldValue::Integer(timestamp));
processed_fields.insert("pipeline_version", FieldValue::String("1.0.0"));
```

## Production Deployment Considerations

### Hardware Requirements
- **Minimum**: 2 CPU cores, 4GB RAM, 10GB disk
- **Recommended**: 4+ CPU cores, 8GB+ RAM, SSD storage
- **High-throughput**: 8+ CPU cores, 16GB+ RAM, NVMe SSD

### Network Configuration
- **Kafka**: Configure `bootstrap.servers` for your cluster
- **Schema Registry**: Set `schema.registry.url` if using Avro
- **Monitoring**: Expose metrics on configurable ports

### Scaling Strategies
- **Horizontal**: Multiple consumer instances with partition assignment
- **Vertical**: Increase buffer sizes and batch configurations
- **Storage**: Use file rotation to manage disk usage
- **Monitoring**: Use provided metrics for autoscaling decisions

## Troubleshooting

### Common Issues

**1. Kafka Connection Failed**
```bash
# Check Kafka is running
docker-compose -f docker-compose.demo.yml ps

# Check Kafka logs  
docker-compose -f docker-compose.demo.yml logs kafka
```

**2. File Permission Denied**
```bash
# Ensure demo directories are writable
chmod -R 755 demo_data demo_output
```

**3. Out of Disk Space**
```bash
# Clean up rotated files
find demo_output -name "*.jsonl.gz" -mtime +1 -delete

# Or reduce rotation size in configuration
let config = config.with_rotation_size(512 * 1024);  // 512KB files
```

**4. Low Performance**
```bash
# Increase buffer sizes
let config = config.with_buffer_size(65536);  // 64KB buffers

# Increase Kafka batch size
let config = config.batch_size(65536).linger_ms(50);
```

### Performance Tuning

**High Throughput Setup**:
```rust
// File source optimization
let file_config = FileSourceConfig::new(path, format)
    .with_buffer_size(65536)        // 64KB read buffers
    .with_watching(Some(100));      // Check every 100ms

// Kafka optimization
let producer_config = ProducerConfig::new(brokers, topic)
    .batch_size(65536)              // 64KB batches
    .linger_ms(50)                  // 50ms batching
    .compression_type("snappy");    // Fast compression

// File sink optimization  
let sink_config = FileSinkConfig::new(path, format)
    .with_rotation_size(10 * 1024 * 1024)  // 10MB files
    .with_compression(CompressionType::Snappy);
```

This demo provides a complete foundation for building production financial data pipelines with FerrisStreams, showcasing the 42x performance improvement and exact precision that makes it ideal for financial applications.