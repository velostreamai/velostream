# VeloStream DataSource Demo - Complete Guide

This comprehensive demo showcases VeloStream' advanced data processing capabilities with both file and Kafka data sources, demonstrating exact decimal precision for financial calculations.

## 🚀 Quick Start

### Option 1: Run Complete Demo (Recommended)
```bash
cd demo/datasource-demo
./run_complete_demo.sh
```

### Option 2: Step-by-Step Execution
```bash
# 1. Build VeloStream
cargo build --no-default-features --release

# 2. Start infrastructure (optional - for Kafka demos)
docker-compose -f docker-compose.enhanced.yml up -d

# 3. Generate demo data
./generate_demo_data.sh

# 4. Run specific demo
cargo run --bin file_processing_demo --no-default-features
```

## 📋 Demo Components

### 1. Avro Schemas (`schemas/`)
- **`financial_transaction.avsc`**: Input transaction schema with standard decimal logical type
- **`processed_transaction.avsc`**: Enhanced transaction with analytics fields  
- **`transaction_analytics.avsc`**: Windowed aggregation results schema

**Key Features:**
- Uses standard Avro decimal logical type: `"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 4`
- Compatible with Apache Avro 0.20.0 for `Value::Decimal` serialization
- Cross-system compatibility with Flink and Confluent

### 2. Configuration Files (`configs/`)
- **`file_source_config.yaml`**: CSV file reading with watching
- **`file_sink_config.yaml`**: JSON output with rotation and compression
- **`kafka_source_config.yaml`**: Kafka consumer configuration
- **`kafka_sink_config.yaml`**: Kafka producer with exactly-once semantics
- **`analytics_pipeline_config.yaml`**: Complete pipeline configuration

### 3. SQL Demonstrations (`enhanced_sql_demo.sql`)

#### Financial Precision Examples:
```sql
-- Exact decimal precision (no floating-point errors)
CREATE STREAM enriched_transactions AS 
SELECT 
    amount,                           -- Original: $123.45
    amount * 100 AS amount_cents,     -- ScaledInteger: 12345 (exact precision)
    CASE WHEN amount > 100.0 THEN 'HIGH_VALUE' ELSE 'LOW_VALUE' END AS tier
FROM raw_transactions;
```

#### Real-Time Windowed Analytics:
```sql
-- 1-minute tumbling windows with exact arithmetic
CREATE TABLE merchant_analytics AS
SELECT 
    merchant_category,
    TUMBLING_WINDOW('1 MINUTE') AS window_info,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,        -- Exact precision maintained
    AVG(amount) AS average_amount,      -- No rounding errors
    COUNT(DISTINCT customer_id) AS unique_customers
FROM enriched_transactions
GROUP BY merchant_category, TUMBLING_WINDOW('1 MINUTE')
EMIT CHANGES;
```

#### Advanced Fraud Detection:
```sql
-- Real-time risk scoring with customer behavior analysis
CREATE STREAM fraud_alerts AS
SELECT 
    e.transaction_id,
    e.amount,
    -- Dynamic risk scoring based on customer patterns
    CASE 
        WHEN e.amount > (c.avg_transaction_size * 5) THEN 'AMOUNT_ANOMALY'
        WHEN c.transaction_frequency > 20 THEN 'HIGH_FREQUENCY'
        ELSE 'NORMAL'
    END AS risk_flag,
    -- Calculate precise risk score (0-100)
    LEAST(100, GREATEST(0, 
        (e.amount / NULLIF(c.avg_transaction_size, 0) * 10) +
        (c.transaction_frequency * 2)
    )) AS risk_score
FROM enriched_transactions e
INNER JOIN customer_spending_patterns c 
    ON e.customer_id = c.customer_id;
```

### 4. Demo Scripts

#### `run_complete_demo.sh` - Interactive Demo Runner
- **Prerequisites Check**: Validates Rust, Docker, and directory structure
- **Build Management**: Compiles required binaries with optimal flags
- **Infrastructure Setup**: Starts Kafka, Schema Registry, monitoring
- **Demo Execution**: Multiple execution paths (file-only, SQL, full pipeline)
- **Results Analysis**: Shows processing metrics and performance data
- **Cleanup**: Optional cleanup of demo data and Docker containers

**Demo Execution Options:**
1. **File Processing Only**: Pure Rust API demonstration
2. **SQL Interface**: Interactive SQL with real-time streams  
3. **Complete Pipeline**: File → Kafka → Processing → File
4. **All Demos**: Sequential execution of all options

## 🎯 Key Demonstrations

### 1. Financial Precision

**Problem**: Traditional floating-point arithmetic introduces rounding errors in financial calculations.

**Solution**: ScaledInteger with exact decimal precision.

```rust
// Input: $123.45 from CSV
FieldValue::String("123.45")

// Internal: ScaledInteger for exact arithmetic
FieldValue::ScaledInteger(12345, 2)  // 12345 cents, scale=2

// Output: Perfect precision maintained  
"amount": "123.45"  // No rounding errors!
```

**Performance Comparison:**
- **f64 floating-point**: Has precision errors in financial calculations
- **ScaledInteger**: Exact precision with optimized arithmetic performance
- **BigDecimal**: Exact precision but slower than ScaledInteger

### 2. Real-Time File Processing

**Features Demonstrated:**
- **File Watching**: Automatically processes new CSV data
- **Schema Inference**: Detects CSV headers and data types  
- **Batch Processing**: Optimized throughput with configurable batching
- **File Rotation**: Prevents disk exhaustion with size/time-based rotation
- **Compression**: Reduces storage with gzip/snappy/zstd
- **Error Handling**: Graceful handling of malformed data

### 3. Kafka Integration

**Stream Processing Pipeline:**
```
CSV Files → FileDataSource → VeloStream → Kafka Topic → Analytics → Output Files
```

**Exactly-Once Semantics:**
- **Producer**: `acks=all`, `enable.idempotence=true`, retries with backoff
- **Consumer**: Manual offset management, error handling with DLQ
- **Schema Evolution**: Avro schema registry integration

### 4. Advanced Analytics

**Windowed Aggregations:**
- **Tumbling Windows**: Fixed-size, non-overlapping time windows
- **Sliding Windows**: Overlapping windows for continuous analysis  
- **Session Windows**: Event-driven windows based on activity gaps

**Complex Joins:**
- **Stream-Table Joins**: Current transaction vs. historical patterns
- **Stream-Stream Joins**: Cross-stream correlation and enrichment
- **Self-Joins**: Customer lifetime value and running totals

## 📊 Expected Results

### Processing Metrics
```
📊 Pipeline Metrics:
   📖 Records Read:      1,000 (+50/s)
   ⚙️  Records Processed: 995 (+49/s)  
   💾 Records Written:    990 (+48/s)
   💰 Total Amount:      $45,678.90
   ❌ Errors:            5 (0.5%)
```

### Performance Benchmarks (MacBook Pro M1)
- **File Read**: 800-1,200 records/sec from CSV
- **SQL Processing**: 1,000-1,500 records/sec with windowed aggregation
- **Kafka Throughput**: 2,000-3,000 records/sec with Avro serialization  
- **File Write**: 600-900 records/sec to compressed JSON
- **Latency**: <2ms end-to-end processing per record

### Output Files Generated
```
demo_output/
├── processed_transactions.jsonl.gz        # Main processed data
├── high_value_transactions.jsonl.gz       # Filtered high-value txns  
├── merchant_analytics.csv                 # Windowed analytics
├── transaction_analytics_20240127_*.jsonl # Time-series analytics
├── pipeline_metrics.jsonl                 # Performance metrics
└── pipeline_errors.log                   # Error tracking
```

## 🔧 Configuration Customization

### High-Throughput Setup
```yaml
# file_source_config.yaml
performance:
  buffer_size: 65536      # 64KB read buffers
  batch_size: 1000        # Large batches
  
# kafka_sink_config.yaml  
producer:
  batch_size: 65536       # 64KB Kafka batches
  linger_ms: 50           # Batch for 50ms
  compression_type: snappy # Fast compression
```

### Low-Latency Setup
```yaml
# Real-time processing optimization
performance:
  buffer_size: 4096       # Small buffers for low latency
  batch_size: 10          # Small batches
  
producer:
  linger_ms: 1            # Send immediately  
  acks: 1                 # Faster acknowledgment
```

### Financial Precision Setup
```yaml
# Exact decimal handling
processing:
  decimal_fields:
    - field: "amount"
      precision: 19
      scale: 4
  arithmetic_mode: "scaled_integer"  # 42x performance
```

## 🐛 Troubleshooting

### Common Issues

**1. Kafka Connection Failed**
```bash
# Check services are running
docker-compose -f docker-compose.enhanced.yml ps

# View Kafka logs
docker-compose -f docker-compose.enhanced.yml logs kafka
```

**2. File Permission Errors**  
```bash
# Fix permissions
chmod -R 755 demo_data demo_output

# Check disk space
df -h .
```

**3. Schema Registry Issues**
```bash
# Verify schema registry
curl http://localhost:8081/subjects

# Check Avro schema registration
curl http://localhost:8081/subjects/financial-transaction-value/versions
```

**4. Performance Issues**
```bash
# Monitor resource usage
docker stats

# Check VeloStream metrics
tail -f demo_output/pipeline_metrics.jsonl

# Optimize configurations
# Increase buffer sizes, batch sizes, or parallelism
```

### Performance Tuning

**Memory Optimization:**
```yaml
buffer_size: 32768        # Reduce memory usage
max_records: 100          # Smaller batches
rotation:
  max_file_size: 10MB     # Smaller files
```

**CPU Optimization:**
```yaml
parallelism: 8            # Match CPU cores
compression: none         # Reduce CPU usage
batch_timeout_ms: 100     # Less waiting
```

## 📈 Monitoring and Observability

### Kafka UI (http://localhost:8080)
- **Topics**: View demo-transactions, processed-transactions
- **Messages**: Inspect Avro-serialized messages
- **Consumer Groups**: Monitor velostream-demo-consumer
- **Schema Registry**: View registered Avro schemas

### Grafana Dashboard (http://localhost:3000)
- **Login**: admin / demo123  
- **Dashboards**: VeloStream processing metrics
- **Alerts**: Configure alerts for error rates, throughput
- **Time Series**: Historical performance trends

### Prometheus Metrics (http://localhost:9090)
- **Processing Rate**: `velostream_records_per_second`
- **Error Rate**: `velostream_error_rate`
- **Latency**: `velostream_processing_latency_ms`
- **Memory Usage**: `velostream_memory_usage_mb`

## 🎓 Learning Outcomes

After running this demo, you'll understand:

1. **Financial Precision**: How ScaledInteger provides 42x performance with exact accuracy
2. **Stream Processing**: Real-time data pipeline architecture and implementation
3. **Schema Evolution**: Avro schema management and compatibility  
4. **Performance Optimization**: Tuning configurations for throughput vs latency
5. **Production Readiness**: Error handling, monitoring, and operational concerns
6. **SQL vs API**: When to use SQL interface vs programmatic Rust API

## 🔗 Next Steps

- **Production Deployment**: See `docs/PRODUCTIONISATION.md`
- **Kafka Performance**: See `docs/KAFKA_PERFORMANCE_CONFIGS.md`  
- **SQL Reference**: See `docs/SQL_REFERENCE_GUIDE.md`
- **Advanced Examples**: Explore `examples/` directory
- **Trading Demo**: See `demo/trading/` for real-time financial analytics

This demo provides a solid foundation for building production financial data pipelines with VeloStream, showcasing the key benefits of exact precision arithmetic and real-time stream processing capabilities.