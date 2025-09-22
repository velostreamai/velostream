# Batch Configuration Guide

## Overview

Velostream supports comprehensive batch processing configuration via SQL `WITH` clauses. This allows you to optimize performance for high-throughput scenarios by configuring how records are grouped and processed together.

## Batch Strategies

### 1. Fixed Size Batching
Process records in fixed-size batches.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'fixed_size',
    'batch.size' = '1000',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.size` (default: 100): Number of records per batch
- `batch.timeout` (default: 1000ms): Maximum wait time before processing incomplete batch

### 2. Time Window Batching
Process records within time-based windows.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'time_window',
    'batch.window' = '5s',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.window` (default: 1s): Time window duration (supports ms, s, m, h, d)

### 3. Memory-Based Batching
Process records based on memory consumption.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'memory_based',
    'batch.memory_size' = '2097152',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.memory_size` (default: 1048576): Memory limit in bytes (1MB default)

### 4. Adaptive Size Batching
Dynamic batching that adapts based on system performance.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'adaptive_size',
    'batch.min_size' = '10',
    'batch.adaptive_max_size' = '2000',
    'batch.target_latency' = '100ms',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.min_size` (default: 10): Minimum batch size
- `batch.adaptive_max_size` (default: 1000): Maximum batch size
- `batch.target_latency` (default: 100ms): Target processing latency

### 5. Low Latency Batching
Optimized for minimal processing delay with eager processing.

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'batch.strategy' = 'low_latency',
    'batch.low_latency_max_size' = '5',
    'batch.low_latency_wait' = '10ms',
    'batch.eager_processing' = 'true',
    'batch.enable' = 'true'
);
```

**Configuration Keys:**
- `batch.low_latency_max_size` (default: 1): Maximum records before immediate processing
- `batch.low_latency_wait` (default: 1ms): Maximum wait time
- `batch.eager_processing` (default: true): Enable immediate processing

## Failure Strategy Configuration

Configure how processing failures are handled:

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '5',
    'retry_backoff' = '2000ms'
);
```

**Available Strategies:**
- `LogAndContinue`: Log errors and continue processing
- `SendToDLQ`: Route failed records to Dead Letter Queue
- `FailBatch`: Fail entire batch on any record failure
- `RetryWithBackoff`: Retry with exponential backoff

**Configuration Keys:**
- `failure_strategy`: Strategy for handling failures
- `max_retries` (default: 3): Maximum retry attempts
- `retry_backoff` (default: 1000ms): Backoff delay between retries

## Source Configuration

Batch and failure strategies also apply to data sources:

```sql
CREATE STREAM my_stream AS
SELECT * FROM kafka_source
WITH (
    'source.failure_strategy' = 'RetryWithBackoff',
    'source.retry_backoff' = '1000ms',
    'source.max_retries' = '3'
);
```

## Combined Configuration Example

```sql
CREATE STREAM optimized_stream AS
SELECT * FROM kafka_source
WITH (
    -- Global Batch Configuration
    'batch.strategy' = 'adaptive_size',
    'batch.enable' = 'true',
    'batch.min_size' = '50',
    'batch.adaptive_max_size' = '2000',
    'batch.target_latency' = '100ms',
    
    -- Failure Handling
    'failure_strategy' = 'RetryWithBackoff',
    'max_retries' = '5',
    'retry_backoff' = '2000ms',
    
    -- Source Configuration  
    'source.failure_strategy' = 'LogAndContinue'
);
```

## Performance Considerations

### Throughput Optimization
- **FixedSize**: Best for consistent workloads, use larger batch sizes (500-2000)
- **TimeWindow**: Good for time-sensitive processing, balance window size with latency
- **AdaptiveSize**: Best for variable workloads, automatically optimizes

### Memory Management
- **MemoryBased**: Prevents memory overflow, useful for large record processing
- Monitor memory usage with different batch sizes

### Latency Optimization
- **LowLatency**: Use for real-time requirements, smaller batch sizes
- **EagerProcessing**: Process immediately when conditions met

## Duration Format Support

All time-based configurations support multiple formats:
- `ms`, `milliseconds`: Milliseconds
- `s`, `seconds`: Seconds  
- `m`, `minutes`: Minutes
- `h`, `hours`: Hours
- `d`, `days`: Days

Examples: `500ms`, `30s`, `5m`, `2h`, `1d`

## Validation and Schema

All batch configuration is validated against a comprehensive schema:
- Type checking (string, integer, duration, boolean)
- Value validation (positive numbers, valid enums)
- Required field validation
- Default value provision

## Best Practices

1. **Start with defaults** and tune based on performance metrics
2. **Monitor batch processing** latency and throughput
3. **Use adaptive strategies** for variable workloads
4. **Combine with appropriate failure strategies** for reliability
5. **Test with realistic data volumes** before production deployment

## Testing and Validation

Velostream includes comprehensive test binaries for batch configuration:

```bash
# Test simple batch configurations
cargo run --bin test_batch_with_clause --no-default-features

# Test failure strategy configurations  
cargo run --bin test_failure_strategy_config --no-default-features
```

This ensures all configurations are validated and working before deployment.