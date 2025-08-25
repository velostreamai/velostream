# Data Sources Documentation

This directory contains all documentation related to the pluggable data sources architecture for FerrisStreams.

## 📚 Documentation Structure

### Planning & Design
- **[FEATURE_REQUEST_PLUGGABLE_DATASOURCES.md](./FEATURE_REQUEST_PLUGGABLE_DATASOURCES.md)** - Original feature request and requirements
- **[ARCHITECTURAL_DECOUPLING_PLAN.md](./ARCHITECTURAL_DECOUPLING_PLAN.md)** - 10-day implementation plan (currently on Day 2)
- **[KAFKA_COUPLING_AUDIT.md](./KAFKA_COUPLING_AUDIT.md)** - Audit of existing Kafka dependencies

### Implementation Status

#### ✅ Completed (Days 1-2)
- Day 1: Kafka dependency audit and mapping
- Day 2: Core trait definitions and configuration system
  - Created `DataSource`, `DataSink`, `DataReader`, `DataWriter` traits
  - Implemented URI-based configuration system
  - Built factory registry pattern

#### 🚧 In Progress (Day 3)
- Kafka adapter implementation
- Backward compatibility layer

#### 📅 Upcoming (Days 4-10)
- Day 4: ProcessorContext refactoring
- Day 5: Integration testing
- Day 6: Schema management
- Day 7: Error handling
- Day 8: Configuration & URI parsing
- Day 9: Documentation
- Day 10: Performance optimization

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
├── mod.rs          # Core traits (DataSource, DataSink, DataReader, DataWriter)
├── config.rs       # Configuration and URI parsing
└── registry.rs     # Factory registry for dynamic source/sink creation
```

### Example Usage

```rust
// PostgreSQL CDC to Kafka
let source = create_source("postgresql://localhost/db?table=orders&cdc=true")?;
let sink = create_sink("kafka://localhost:9092/orders-stream")?;

// Kafka to ClickHouse 
let source = create_source("kafka://localhost:9092/events")?;
let sink = create_sink("clickhouse://localhost:8123/analytics?table=events")?;

// File to Iceberg
let source = create_source("file:///data/input/*.csv")?;
let sink = create_sink("iceberg://catalog/namespace/table")?;

// S3 to ClickHouse Analytics
let source = create_source("s3://bucket/data/*.parquet")?;
let sink = create_sink("clickhouse://localhost:8123/warehouse?table=facts")?;
```

## 🔗 Quick Links

- [Current Implementation](../../src/ferris/sql/datasource/)
- [SQL Engine Core](../../src/ferris/sql/)
- [Kafka Module](../../src/ferris/kafka/) (being decoupled)

## 📊 Progress Tracking

```
Week 1: Core Decoupling
[▓▓▓▓░░░░░░] 40% - Day 2/5 Complete ✅

Week 2: Advanced Features  
[░░░░░░░░░░] 0% - Not Started

Overall Progress
[▓▓░░░░░░░░] 20% - 2/10 Days Complete ✅
```

## 🎯 Next Steps

1. **Day 3**: Implement Kafka adapter with new traits
2. **Day 4**: Refactor ProcessorContext for heterogeneous sources
3. **Day 5**: Integration testing and validation

## 📝 Notes

- Architecture is already 90% ready for pluggable sources
- SQL engine core has minimal Kafka coupling
- Focus on additive changes to maintain backward compatibility