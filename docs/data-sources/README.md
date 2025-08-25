# Data Sources Documentation

This directory contains all documentation related to the pluggable data sources architecture for FerrisStreams.

## 📚 Documentation Structure

### Planning & Design
- **[FEATURE_REQUEST_PLUGGABLE_DATASOURCES.md](./FEATURE_REQUEST_PLUGGABLE_DATASOURCES.md)** - Original feature request and requirements
- **[ARCHITECTURAL_DECOUPLING_PLAN.md](./ARCHITECTURAL_DECOUPLING_PLAN.md)** - 10-day implementation plan (currently on Day 2)
- **[KAFKA_COUPLING_AUDIT.md](./KAFKA_COUPLING_AUDIT.md)** - Audit of existing Kafka dependencies

### Implementation Status

#### ✅ Completed (Days 1-8)
- **Day 1**: Kafka dependency audit and mapping
- **Day 2**: Core trait definitions and configuration system
  - Created `DataSource`, `DataSink`, `DataReader`, `DataWriter` traits
  - Implemented URI-based configuration system
  - Built factory registry pattern
- **Day 3**: Kafka adapter implementation
  - Full adapter with backward compatibility
  - Stream-based async reading
- **Day 4**: ProcessorContext refactoring
  - Extracted 450+ lines to dedicated files
  - Clean separation of concerns
- **Day 5**: Integration testing framework
  - Comprehensive test coverage
  - Performance benchmarks
- **Day 6**: Schema management system
  - Provider-based architecture
  - Schema evolution and caching
- **Day 7**: Error handling and recovery
  - Circuit breakers and retry logic
  - Dead letter queues
- **Day 8**: Configuration & URI parsing ✅
  - Complete URI parser with multi-host support
  - Validation framework with detailed errors
  - Environment-based configuration
  - Builder pattern with fluent API

#### 🚧 In Progress (Day 9)
- Documentation updates
- Module organization cleanup

#### 📅 Upcoming (Day 10)
- Day 10: Performance optimization and final testing

## 🏗️ Architecture Overview

The pluggable data sources architecture enables FerrisStreams to:
- **Core Data Sources**: PostgreSQL, S3, File, Iceberg, ClickHouse, and Kafka
- **Heterogeneous data flow**: Read from one source, write to another
- **Single Binary, Scale Out**: K8s native autoscaling with horizontal pod scaling
- **Maintain backward compatibility**: Existing Kafka code continues to work
- **Support multiple protocols**: Through a unified trait-based interface

### Single Binary, Scale Out Model

FerrisStreams is designed as a **single binary** that can **scale out horizontally** using Kubernetes native autoscaling:

- **📦 Single Binary**: One executable handles all data source types (Kafka, ClickHouse, PostgreSQL, S3, etc.)
- **⚡ K8s Native Autoscaling**: Automatic horizontal pod scaling based on CPU, memory, or custom metrics
- **🔄 Stateless Design**: Each instance is stateless, enabling seamless scaling
- **🎯 Resource Efficiency**: Scale specific workloads independently using K8s deployments
- **💡 Zero Configuration**: Auto-discovery and dynamic resource allocation

### Core Supported Data Sources

1. **PostgreSQL** - Database source/sink with CDC support
2. **S3** - Object storage for batch processing
3. **File** - Local/network file systems
4. **Iceberg** - Table format for analytics
5. **ClickHouse** - Columnar OLAP database
6. **Kafka** - Streaming message broker (existing)

### Core Components

```
src/ferris/sql/datasource/
├── mod.rs          # Module exports only (following Rust best practices)
├── traits.rs       # Core traits (DataSource, DataSink, DataReader, DataWriter)
├── types.rs        # Type definitions (SourceOffset, Metadata, Errors)
├── config/         # Configuration subsystem
│   ├── mod.rs      # Module exports
│   ├── types.rs    # Config types (DataSourceConfig, ConfigError)
│   └── ...         # URI parsing, validation, environment
├── kafka/          # Kafka adapter implementation
│   ├── mod.rs      # Module exports
│   ├── data_source.rs  # KafkaDataSource implementation
│   ├── data_sink.rs    # KafkaDataSink implementation
│   ├── reader.rs       # Stream-based reader
│   ├── writer.rs       # Producer wrapper
│   └── error.rs        # Kafka-specific errors
└── registry.rs     # Factory registry for dynamic source/sink creation
```

### Example Usage

```rust
// File CSV to Kafka
let source = create_source("file:///data/orders.csv?format=csv&delimiter=,")?;
let sink = create_sink("kafka://localhost:9092/orders-stream")?;

// Kafka to File JSON-Lines
let source = create_source("kafka://localhost:9092/events")?;
let sink = create_sink("file:///output/events.jsonl?format=jsonl")?;

// File Parquet to PostgreSQL
let source = create_source("file:///data/analytics/*.parquet")?;
let sink = create_sink("postgresql://localhost/warehouse?table=facts")?;

// PostgreSQL CDC to Kafka
let source = create_source("postgresql://localhost/db?table=orders&cdc=true")?;
let sink = create_sink("kafka://localhost:9092/orders-stream")?;

// S3 to ClickHouse Analytics
let source = create_source("s3://bucket/data/*.parquet?region=us-west-2")?;
let sink = create_sink("clickhouse://localhost:8123/warehouse?table=facts")?;

// File to Iceberg
let source = create_source("file:///data/input/*.json?format=json")?;
let sink = create_sink("iceberg://catalog/namespace/table")?;
```

## 🔗 Quick Links

- [Current Implementation](../../src/ferris/sql/datasource/)
- [SQL Engine Core](../../src/ferris/sql/)
- [Kafka Module](../../src/ferris/kafka/) (being decoupled)

## 📊 Progress Tracking

```
Week 1: Core Decoupling
[▓▓▓▓▓▓▓▓▓▓] 100% - Days 1-5 Complete ✅
  ✅ Day 1: Kafka audit
  ✅ Day 2: Core traits
  ✅ Day 3: Kafka adapter
  ✅ Day 4: ProcessorContext
  ✅ Day 5: Integration testing

Week 2: Advanced Features  
[▓▓▓▓▓▓▓▓░░] 80% - Days 6-9 In Progress
  ✅ Day 6: Schema management
  ✅ Day 7: Error handling
  ✅ Day 8: Configuration & URI
  🚧 Day 9: Documentation
  ⏳ Day 10: Performance

Overall Progress
[▓▓▓▓▓▓▓▓░░] 80% - 8/10 Days Complete ✅
```

## 🎯 Next Steps

1. **Day 9** (In Progress): Complete documentation updates
2. **Day 10**: Performance optimization and benchmarking
3. **Post-Plan**: Begin implementing additional data sources:
   - File I/O adapter (CSV, JSON, Parquet)
   - PostgreSQL CDC adapter
   - S3 batch processing
   - ClickHouse analytics sink
   - Iceberg table format support

## 📝 Notes

- ✅ Architecture successfully decoupled from Kafka
- ✅ SQL engine core now uses trait-based abstractions
- ✅ Full backward compatibility maintained
- ✅ Module organization follows Rust best practices (no definitions in mod.rs)
- ✅ Comprehensive configuration system with URI parsing
- ✅ Schema management with provider-based architecture
- ✅ Advanced error handling with circuit breakers
- 🎯 Ready to add new data sources without breaking changes