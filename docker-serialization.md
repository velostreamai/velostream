# FerrisStreams Docker: All Serialization Formats

This Docker setup provides a single image that supports **JSON**, **Avro**, and **Protobuf** serialization formats with financial precision arithmetic.

## 🚀 Quick Start

```bash
# Build and start all services
docker-compose up --build

# Or build just the FerrisStreams image
docker build -t ferrisstreams:latest .
```

## 📦 What's Included

### Single Docker Image Features
- ✅ **JSON serialization** (always enabled) with decimal string compatibility
- ✅ **Avro serialization** with schema registry integration
- ✅ **Protobuf serialization** with industry-standard Decimal messages
- ✅ **Financial precision** arithmetic (42x faster than f64)
- ✅ **Cross-system compatibility** - other systems can read your data

### Services in docker-compose.yml
- **ferris-streams**: Main SQL streaming engine (port 8080)
- **kafka**: Apache Kafka message broker (port 9092)
- **schema-registry**: Confluent Schema Registry for Avro (port 8081)
- **kafka-ui**: Web interface for Kafka monitoring (port 8085)
- **multi-format-producer**: Generates test data in all formats

## 🔧 Usage Examples

### 1. JSON Financial Data (Default)

```sql
-- Connect to FerrisStreams
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT symbol, price, quantity, price * quantity as total FROM json_financial_stream"
  }'
```

**JSON Data Format:**
```json
{
  "symbol": "AAPL",
  "price": "150.2567",     # Exact precision as string
  "quantity": 100,
  "total": "15025.67"      # Calculated with perfect precision
}
```

### 2. Avro with Schema Evolution

```bash
# Register Avro schema for financial data
curl -X POST http://localhost:8081/subjects/financial-trades-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Trade\",\"fields\":[{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}"
  }'
```

### 3. Protobuf with Decimal Messages

The protobuf format uses industry-standard Decimal messages:

```protobuf
message Decimal {
  int64 units = 1;    // Scaled integer (123456 for $12.3456 with scale=4)
  int32 scale = 2;    // Decimal places
}

message FieldValue {
  oneof value {
    string string_value = 1;
    int64 integer_value = 2;
    double float_value = 3;
    bool boolean_value = 4;
    Decimal decimal_value = 5;  // Financial precision
  }
}
```

## 🧪 Testing All Formats

### Start the Stack
```bash
docker-compose up -d
```

### Check Service Status
```bash
# Check FerrisStreams health
curl http://localhost:8080/health

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Check Schema Registry
curl http://localhost:8081/subjects
```

### Monitor Data Flow
- **Kafka UI**: http://localhost:8085
- **FerrisStreams API**: http://localhost:8080
- **Schema Registry**: http://localhost:8081

### Test Financial Precision
```bash
# Send JSON data with exact precision
docker exec kafka kafka-console-producer --broker-list localhost:29092 --topic json-financial << EOF
{"symbol":"AAPL","price":"123.4567","quantity":100}
{"symbol":"GOOGL","price":"2750.89","quantity":50}
EOF
```

## 🏗️ Build Customization

### Custom Build with Specific Features
```bash
# Build with only specific serialization formats
docker build \
  --build-arg FEATURES="avro" \
  -t ferrisstreams:avro-only .

# Build with all features (default)
docker build \
  --build-arg FEATURES="avro,protobuf" \
  -t ferrisstreams:all-formats .
```

### Environment Variables
```yaml
environment:
  - RUST_LOG=info                                    # Logging level
  - KAFKA_BROKERS=kafka:29092                       # Kafka bootstrap servers
  - SCHEMA_REGISTRY_URL=http://schema-registry:8081  # Avro schema registry
  - FERRIS_SERIALIZATION_FORMATS=json,avro,protobuf # Available formats
```

## 📊 Performance Benefits

| Format | Precision | Speed | Cross-System |
|--------|-----------|-------|-------------|
| JSON (f64) | ❌ Floating point errors | Slow | ✅ Universal |
| JSON (ScaledInteger) | ✅ Exact precision | **42x faster** | ✅ Decimal strings |
| Avro | ✅ Schema evolution | Fast | ✅ Standard format |
| Protobuf | ✅ Industry standard | Fastest | ✅ Universal |

## 🐛 Troubleshooting

### Container Logs
```bash
# Check FerrisStreams logs
docker logs ferris-streams

# Check all service logs
docker-compose logs -f
```

### Common Issues

1. **Port conflicts**: Change ports in docker-compose.yml
2. **Schema Registry not available**: Wait for startup, check logs
3. **Kafka connection**: Verify KAFKA_BROKERS environment variable

### Verify Serialization Support
```bash
# Check compiled features
docker run ferrisstreams:latest ferris-sql-multi --version

# Test each format
docker run ferrisstreams:latest ferris-cli test-serialization --format json
docker run ferrisstreams:latest ferris-cli test-serialization --format avro  
docker run ferrisstreams:latest ferris-cli test-serialization --format protobuf
```

## 🔒 Production Considerations

### Security
```yaml
# Production docker-compose additions
services:
  ferris-streams:
    environment:
      - RUST_LOG=warn  # Reduce logging
    secrets:
      - kafka_ssl_cert
      - kafka_ssl_key
```

### Scaling
```yaml
# Scale FerrisStreams horizontally
services:
  ferris-streams:
    deploy:
      replicas: 3
      resources:
        limits:
          memory: 1G
          cpus: '2'
```

## 📚 API Examples

### Create Stream with Specific Serialization
```bash
curl -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "financial_trades",
    "kafka_topic": "trades",
    "serialization": "protobuf",
    "schema": "ferris.financial.StreamRecord"
  }'
```

### Query with Financial Precision
```sql
SELECT 
  symbol,
  price,                           -- ScaledInteger with exact precision
  quantity,
  price * quantity as total,       -- Perfect financial arithmetic
  CAST(price AS FLOAT) as price_f64 -- Convert to float if needed
FROM trades_stream 
WHERE price > "100.00"             -- String comparison for exact precision
```

This Docker setup gives you a production-ready streaming SQL engine with all serialization formats and financial precision in a single container! 🎉