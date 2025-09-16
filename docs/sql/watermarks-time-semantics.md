# Watermarks & Time Semantics Guide

## Overview

Phase 1B introduces comprehensive time semantics to VeloStream, enabling proper handling of out-of-order and late-arriving data through watermark-based processing. This guide explains how to configure and use event-time processing, watermarks, and late data strategies.

## Key Concepts

### Event-Time vs Processing-Time

**Processing-Time**: When the record was processed by the engine (always available in `timestamp` field)
**Event-Time**: When the event actually occurred (optional, extracted from event data into `event_time` field)

```rust
pub struct StreamRecord {
    pub data: HashMap<String, FieldValue>,
    pub timestamp: NaiveDateTime,    // Processing-time (always set)
    pub event_time: Option<NaiveDateTime>, // Event-time (optional)
    pub headers: Option<HashMap<String, String>>,
}
```

### Watermarks

Watermarks are timestamps that indicate "all events with event-time ≤ watermark have been seen". They drive window emission and late data detection.

## Configuration

### Basic Watermark Configuration

```rust
use velostream::velo::sql::execution::{
    watermarks::{WatermarkStrategy, WatermarkConfig},
    config::{LateDataStrategy, StreamingConfig}
};

let config = StreamingConfig {
    // Enable event-time semantics
    event_time_semantics: true,
    
    // Configure watermarks
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(10) // Allow 10s out-of-order
        },
        idle_timeout: Some(Duration::from_secs(60)), // 60s idle timeout
    }),
    
    // Late data handling strategy
    late_data_strategy: LateDataStrategy::DeadLetter,
    
    // ... other config
};
```

### Watermark Strategies

#### 1. BoundedOutOfOrderness
For streams with known maximum out-of-orderness:

```rust
WatermarkStrategy::BoundedOutOfOrderness {
    max_out_of_orderness: Duration::from_secs(10)
}
```

**Use case**: Financial data feeds with known processing delays

#### 2. Ascending
For strictly ordered streams:

```rust
WatermarkStrategy::Ascending
```

**Use case**: Log files, sequential data sources

#### 3. Punctuated
Using explicit watermark events:

```rust
WatermarkStrategy::Punctuated {
    punctuation_field: "watermark_timestamp".to_string()
}
```

**Use case**: Custom watermark injection, IoT sensors with heartbeats

### Late Data Strategies

#### Drop
Silently discard late data:
```rust
LateDataStrategy::Drop
```

#### DeadLetter
Route to dead letter queue for analysis:
```rust
LateDataStrategy::DeadLetter
```

#### IncludeInNextWindow
Process in the next available window:
```rust
LateDataStrategy::IncludeInNextWindow
```

#### UpdatePrevious
Update previous window results (use with caution):
```rust
LateDataStrategy::UpdatePrevious
```

## SQL Integration

### Event-Time Extraction

Configure event-time extraction from JSON fields:

```sql
CREATE STREAM trades AS
SELECT 
    ticker, 
    price, 
    volume,
    EXTRACT_TIMESTAMP('event_timestamp') as event_time
FROM kafka_source
WITH (
    'event.time.field' = 'event_timestamp',
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS'
);
```

### Event-Time Windows

Use event-time for windowing operations:

```sql
-- Tumbling window based on event-time
SELECT 
    ticker,
    COUNT(*) as trade_count,
    AVG(price) as avg_price
FROM trades
WHERE volume > 1000
GROUP BY 
    ticker,
    TUMBLE(event_time, INTERVAL '1' MINUTE)
EMIT CHANGES;
```

### Watermark-Aware Processing

Windows emit only when watermarks advance beyond window end:

```sql
-- 5-minute tumbling windows with 30-second grace period
SELECT 
    merchant_category,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM transactions
GROUP BY 
    merchant_category,
    TUMBLE(event_time, INTERVAL '5' MINUTE)
WITH (
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s',
    'late.data.strategy' = 'dead_letter'
)
EMIT CHANGES;
```

## Programming API

### Engine Configuration

```rust
use velostream::velo::sql::execution::{
    StreamExecutionEngine, ProcessorContext, 
    watermarks::WatermarkManager
};

// Create engine with watermark support
let (tx, rx) = mpsc::unbounded_channel();
let mut engine = StreamExecutionEngine::new(tx);

// Enable watermarks on processor context
let mut context = ProcessorContext::new();
context.enable_watermarks(watermark_config);

// Process records with watermark awareness
engine.execute_with_context(&query, record, &mut context).await?;
```

### Custom Watermark Generation

```rust
use velostream::velo::sql::execution::watermarks::{
    WatermarkManager, WatermarkStrategy
};

// Create custom watermark manager
let mut watermark_manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
    max_out_of_orderness: Duration::from_secs(5)
});

// Update watermarks with new event
watermark_manager.update_watermark("source1", event_time)?;

// Get current watermark
let current_watermark = watermark_manager.get_global_watermark();
```

### Late Data Handling

```rust
use velostream::velo::sql::execution::watermarks::LateDataAction;

// Check if data is late
if let Some(action) = watermark_manager.handle_late_data(&record, &strategy) {
    match action {
        LateDataAction::Drop => {
            // Record dropped
        },
        LateDataAction::DeadLetter => {
            // Route to dead letter queue
        },
        LateDataAction::ProcessInWindow(window_id) => {
            // Process in specified window
        }
    }
}
```

## Monitoring & Observability

### Watermark Metrics

```rust
// Track watermark lag
watermark_manager.record_watermark_lag(source_id, lag_duration);

// Monitor late data rates
watermark_manager.record_late_data_event(source_id, lateness);
```

### Key Metrics to Monitor

- **Watermark Lag**: Difference between current time and watermark
- **Late Data Rate**: Percentage of records arriving late
- **Window Emission Delay**: Time between window end and emission
- **Dead Letter Queue Size**: Volume of rejected late data

## Performance Considerations

### Memory Management

- **Window Buffer Size**: Configure based on expected out-of-orderness
- **Watermark Update Frequency**: Balance accuracy vs performance
- **Late Data Buffer**: Size based on late data strategy

### Optimization Tips

1. **Choose Appropriate Strategy**: Use `Ascending` for ordered data
2. **Tune Out-of-Orderness**: Minimize while maintaining data completeness
3. **Monitor Late Data**: Adjust watermark configuration based on patterns
4. **Idle Source Handling**: Configure timeouts for sparse data sources

## Examples

### Financial Trading System

```rust
// High-frequency trading with tight timing requirements
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_millis(100) // 100ms max delay
        },
        idle_timeout: Some(Duration::from_secs(5)),
    }),
    late_data_strategy: LateDataStrategy::Drop, // Drop late trades
    // ...
};
```

### IoT Sensor Network

```rust
// IoT sensors with variable network delays
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(30) // 30s tolerance
        },
        idle_timeout: Some(Duration::from_minutes(5)),
    }),
    late_data_strategy: LateDataStrategy::IncludeInNextWindow,
    // ...
};
```

### Log Processing

```rust
// Sequential log file processing
let config = StreamingConfig {
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::Ascending, // Strictly ordered
        idle_timeout: None, // No idle timeout needed
    }),
    late_data_strategy: LateDataStrategy::Drop,
    // ...
};
```

## Troubleshooting

### Common Issues

1. **Windows Not Emitting**: Check watermark advancement
2. **High Late Data Rate**: Increase max_out_of_orderness
3. **Memory Growth**: Monitor window buffer sizes
4. **Performance Issues**: Optimize watermark update frequency

### Debug Tools

```rust
// Enable watermark debugging
let config = StreamingConfig {
    debug_watermarks: true,
    // ...
};

// Log watermark information
watermark_manager.log_watermark_status();
```

## Related Features

- [Observability Guide](../ops/OBSERVABILITY.md) - Monitoring watermark performance
- [Resource Management](../ops/RESOURCE_MANAGEMENT.md) - Memory management for windows
- [Circuit Breakers](./CIRCUIT_BREAKERS.md) - Fault tolerance with watermarks

## References

- [Apache Flink Watermarks](https://flink.apache.org/news/2015/12/04/Introducing-windows.html)
- [Streaming Systems Book](https://www.oreilly.com/library/view/streaming-systems/9781491983874/)
- [Dataflow Model Paper](https://research.google.com/pubs/pub43864.html)