# Documentation

This directory contains comprehensive documentation for FerrisStreams.

## 📚 Available Documentation

### Architecture Documentation
- **[SQL Table Architecture](architecture/SQL_TABLE_KTABLE_ARCHITECTURE.md)** - Datasource-agnostic materialized views and stream-table joins
- **[Schema Registry Architecture](architecture/schema-registry-architecture.md)** - Complete schema registry system design

### Core Guides
- **[Builder Pattern Guide](developer/BUILDER_PATTERN_GUIDE.md)** - Learn when and how to use the fluent builder APIs
- **[Type-Safe Kafka](feature/TYPE_SAFE_KAFKA.md)** - Comprehensive guide to type-safe Kafka operations
- **[Streaming Kafka API](developer/STREAMING_KAFKA_API.md)** - High-level API documentation and examples
- **[Headers Guide](developer/HEADERS_GUIDE.md)** - Working with Kafka message headers

### Serialization System
- **[Serialization Guide](developer/SERIALIZATION_GUIDE.md)** - Complete guide to serialization formats (JSON, Avro, Protobuf)
- **[Serialization Quick Reference](developer/SERIALIZATION_QUICK_REFERENCE.md)** - Quick reference for serialization patterns
- **[Serialization Migration Guide](SERIALIZATION_MIGRATION_GUIDE.md)** - Migration from old hardcoded JSON system

### SQL Streaming
- **[SQL Reference Guide](SQL_REFERENCE_GUIDE.md)** - Complete SQL syntax and function reference
- **[JOIN Operations Guide](JOIN_OPERATIONS_GUIDE.md)** - Comprehensive guide to JOIN operations and windowed JOINs
- **[SQL Feature Request](SQL_FEATURE_REQUEST.md)** - Comprehensive SQL implementation roadmap and current status

### Performance & Optimization
- **[Performance Configs](KAFKA_PERFORMANCE_CONFIGS.md)** - Configuration guide for optimizing throughput
- **[Advanced Performance Optimizations](developer/ADVANCED_PERFORMANCE_OPTIMIZATIONS.md)** - Advanced techniques for maximum performance

### Development & Testing
- **[Test Coverage Improvement Plan](feature/TEST_COVERAGE_IMPROVEMENT_PLAN.md)** - Testing strategy and coverage goals
- **[Docker Kafka Setup](developer/DOCKER_KAFKA.md)** - Quick Docker setup for development

### Quick Reference
- **[Quick Reference](QUICK_REFERENCE.md)** - Common patterns and code snippets

## 🚀 Getting Started

New to FerrisStreams? Start with:
1. [Main README](../README.md) - Project overview and installation
2. [Quick Reference](QUICK_REFERENCE.md) - Common patterns
3. [Builder Pattern Guide](developer/BUILDER_PATTERN_GUIDE.md) - Core API patterns
4. [SQL Reference Guide](SQL_REFERENCE_GUIDE.md) - **NEW!** SQL streaming capabilities
5. [Examples](../examples/README.md) - Working code examples

## 🔥 New Features: SQL Streaming

FerrisStreams now includes a comprehensive SQL interface for stream processing:

### Key Capabilities
- **Enterprise Job Management**: Complete lifecycle with versioning, deployment strategies (Blue-Green, Canary, Rolling)
- **JOIN Operations**: Full support for INNER, LEFT, RIGHT, FULL OUTER JOINs with temporal windowing  
- **Windowed JOINs**: Time-based correlation for streaming data with configurable grace periods
- **Stream-Table JOINs**: Optimized reference data lookups with materialized tables
- **JSON Processing**: Native JSON parsing with JSONPath support for complex Kafka payloads  
- **Advanced Functions**: 42+ SQL functions including aggregations, string manipulation, and analytics
- **Real-Time Processing**: Event-at-a-time processing with bounded memory and backpressure handling
- **Type Safety**: Full Rust type safety throughout the SQL execution pipeline

### Quick Examples
```sql
-- Deploy a real-time analytics job with JSON processing
DEPLOY JOB user_analytics VERSION '1.0.0' AS
SELECT 
    JSON_VALUE(payload, '$.user.id') as user_id,
    CAST(JSON_VALUE(payload, '$.amount'), 'FLOAT') as amount,
    SUBSTRING(JSON_VALUE(payload, '$.description'), 1, 50) as short_desc
FROM kafka_events 
WHERE JSON_VALUE(payload, '$.type') = 'purchase'
STRATEGY CANARY(25);

-- Real-time data enrichment with JOINs
DEPLOY JOB order_enrichment VERSION '1.0.0' AS
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.tier,
    p.product_name,
    o.quantity * p.unit_price as total_value
FROM streaming_orders o
INNER JOIN customer_table c ON o.customer_id = c.customer_id
INNER JOIN product_catalog p ON o.product_id = p.product_id
WHERE c.tier IN ('gold', 'platinum')
STRATEGY BLUE_GREEN;

-- Job lifecycle management
PAUSE JOB user_analytics;
SHOW STATUS user_analytics;
ROLLBACK JOB user_analytics VERSION '0.9.0';
```

**Get Started**: See the [SQL Reference Guide](SQL_REFERENCE_GUIDE.md) for complete syntax and examples.

## 🔧 Performance Tuning

For performance-critical applications:
1. [Performance Configs](KAFKA_PERFORMANCE_CONFIGS.md) - Basic optimization
2. [Advanced Performance Optimizations](developer/ADVANCED_PERFORMANCE_OPTIMIZATIONS.md) - Advanced techniques
3. [Performance Examples](../examples/performance/) - Benchmarking code

## 🧪 Development

For contributors and advanced users:
1. [Test Coverage Plan](feature/TEST_COVERAGE_IMPROVEMENT_PLAN.md) - Testing approach
2. [Docker Kafka](developer/DOCKER_KAFKA.md) - Development environment
3. [Examples Directory](../examples/) - Code examples and tests