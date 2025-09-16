# Resource Management & Circuit Breakers Guide

## Overview

Phase 2 introduces comprehensive resource management and fault tolerance capabilities to VeloStream. This includes memory allocation limits, connection pooling, circuit breakers, and retry logic to ensure robust production operations.

## Resource Management

### Core Components

```rust
use velostream::velo::sql::execution::{
    resource_manager::{ResourceManager, ResourceLimitsConfig},
    config::StreamingConfig
};

// Configure resource limits
let config = StreamingConfig {
    max_memory_mb: Some(4096),      // 4GB memory limit
    max_connections: Some(1000),    // 1K concurrent connections
    
    resource_limits: Some(ResourceLimitsConfig {
        max_concurrent_queries: 100,
        max_window_buffer_size: 1_000_000, // 1M records
        max_aggregation_groups: 50_000,
        connection_pool_size: 20,
        memory_pressure_threshold: 0.8, // 80% memory usage
    }),
    // ...
};
```

### Memory Management

#### Allocation Tracking
```rust
use velostream::velo::sql::execution::resource_manager::ResourceManager;

// Create resource manager
let resource_manager = ResourceManager::new(resource_config)?;

// Allocate memory for operation
let allocation = resource_manager.allocate_memory(
    "window_buffer", 
    1024 * 1024 // 1MB
)?;

// Track memory usage
let current_usage = resource_manager.get_memory_usage();
println!("Memory usage: {:.1}% ({} bytes)", 
    current_usage.percentage, 
    current_usage.bytes_used);
```

#### Automatic Memory Management
- **Garbage Collection**: Automatic cleanup of expired windows and aggregations
- **Memory Pressure Detection**: Triggers cleanup when approaching limits
- **Spill to Disk**: Large aggregations can spill to temporary files

### Connection Pool Management

#### Database Connections
```rust
// Configure connection pooling
let pool_config = ConnectionPoolConfig {
    max_size: 20,
    min_idle: 5,
    connection_timeout: Duration::from_secs(30),
    idle_timeout: Some(Duration::from_secs(600)), // 10 min idle timeout
    max_lifetime: Some(Duration::from_secs(3600)), // 1 hour max lifetime
};

// Get connection from pool
let conn = resource_manager.get_connection().await?;
```

#### Kafka Connection Management
```rust
// Kafka producer/consumer pool
let kafka_config = KafkaPoolConfig {
    producer_pool_size: 10,
    consumer_pool_size: 5,
    connection_retry_attempts: 3,
    connection_backoff: Duration::from_millis(500),
};
```

## Circuit Breakers

### Configuration

```rust
use velostream::velo::sql::execution::{
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    config::StreamingConfig
};

let config = StreamingConfig {
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 5,        // Open after 5 failures
        success_threshold: 3,        // Close after 3 successes
        timeout: Duration::from_secs(60), // 60s timeout in open state
        
        // Advanced configuration
        sliding_window_size: 100,    // Track last 100 calls
        minimum_throughput: 10,      // Min calls before opening
        slow_call_threshold: Duration::from_secs(5), // 5s = slow call
        slow_call_rate_threshold: 0.5, // 50% slow calls = unhealthy
    }),
    // ...
};
```

### Circuit Breaker States

#### Closed (Normal Operation)
- All requests pass through
- Failures are counted
- Opens when failure threshold reached

#### Open (Failing Fast)
- All requests immediately fail
- No downstream calls made
- Transitions to half-open after timeout

#### Half-Open (Testing Recovery)
- Limited requests pass through
- Success transitions to closed
- Failure transitions back to open

### Programming API

```rust
use velostream::velo::sql::execution::circuit_breaker::CircuitBreaker;

// Create circuit breaker
let circuit_breaker = CircuitBreaker::new(circuit_config)?;

// Protect operation with circuit breaker
let result = circuit_breaker.execute(|| async {
    // Your risky operation here
    database_query().await
}).await;

match result {
    Ok(data) => println!("Operation succeeded: {:?}", data),
    Err(CircuitBreakerError::Open) => {
        println!("Circuit breaker is open - failing fast");
    },
    Err(CircuitBreakerError::OperationFailed(e)) => {
        println!("Operation failed: {}", e);
    }
}
```

## Retry Logic

### Retry Strategies

#### Exponential Backoff
```rust
use velostream::velo::sql::execution::{
    error_handling::{RetryConfig, BackoffStrategy}
};

let retry_config = RetryConfig {
    max_retries: 3,
    backoff_strategy: BackoffStrategy::ExponentialBackoff {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
        multiplier: 2.0,
    },
    
    // Retry conditions
    retry_on: vec![
        ErrorType::Network,
        ErrorType::Timeout,
        ErrorType::ServiceUnavailable,
    ],
    
    // Don't retry these
    no_retry_on: vec![
        ErrorType::Authentication,
        ErrorType::Authorization,
        ErrorType::BadRequest,
    ],
};
```

#### Fixed Interval
```rust
let retry_config = RetryConfig {
    max_retries: 5,
    backoff_strategy: BackoffStrategy::FixedInterval {
        interval: Duration::from_secs(1)
    },
    // ...
};
```

#### Linear Backoff
```rust
let retry_config = RetryConfig {
    max_retries: 4,
    backoff_strategy: BackoffStrategy::LinearBackoff {
        initial_delay: Duration::from_millis(100),
        increment: Duration::from_millis(100),
    },
    // ...
};
```

### Retry Implementation

```rust
use velostream::velo::sql::execution::error_handling::RetryExecutor;

// Create retry executor
let retry_executor = RetryExecutor::new(retry_config);

// Execute with retry logic
let result = retry_executor.execute_with_retry(|| async {
    // Your operation that might fail
    kafka_producer.send(record).await
}).await?;
```

## Dead Letter Queue

### Configuration
```rust
use velostream::velo::sql::error::recovery::DeadLetterQueue;

let dlq_config = DeadLetterQueueConfig {
    topic: "failed-records".to_string(),
    max_retries: 3,
    retry_delay: Duration::from_secs(60),
    retention_period: Duration::from_days(7),
    
    // Serialization for failed records
    include_original_record: true,
    include_error_details: true,
    include_stack_trace: true,
};
```

### Usage
```rust
// Route failed records to DLQ
match process_record(record).await {
    Ok(result) => handle_success(result),
    Err(error) => {
        dead_letter_queue.send_to_dlq(record, error).await?;
    }
}
```

## Monitoring & Alerts

### Resource Metrics

```rust
// Memory usage monitoring
let memory_stats = resource_manager.get_memory_stats();
println!("Memory - Used: {}MB, Available: {}MB, Pressure: {:.1}%",
    memory_stats.used_mb,
    memory_stats.available_mb,
    memory_stats.pressure_percentage
);

// Connection pool monitoring
let pool_stats = resource_manager.get_connection_pool_stats();
println!("Connections - Active: {}, Idle: {}, Total: {}",
    pool_stats.active,
    pool_stats.idle,
    pool_stats.total
);
```

### Circuit Breaker Metrics

```rust
// Circuit breaker status
let cb_stats = circuit_breaker.get_stats();
println!("Circuit Breaker - State: {:?}, Failures: {}, Success Rate: {:.1}%",
    cb_stats.state,
    cb_stats.failure_count,
    cb_stats.success_rate * 100.0
);
```

### Key Metrics to Monitor

- **Memory Usage Percentage**: Alert at 80%+
- **Connection Pool Exhaustion**: Alert when pool is 90%+ utilized
- **Circuit Breaker State**: Alert when breakers are open
- **Retry Exhaustion Rate**: Alert when retry limits are frequently hit
- **Dead Letter Queue Growth**: Monitor for processing issues

## SQL Integration

### Resource Limits in Queries

```sql
-- Configure query-specific resource limits
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY customer_id
WITH (
    'max.memory.mb' = '512',
    'max.groups' = '10000',
    'spill.to.disk' = 'true'
)
EMIT CHANGES;
```

### Circuit Breaker Integration

```sql
-- Enable circuit breaker for external calls
CREATE STREAM enriched_orders AS
SELECT 
    o.*,
    LOOKUP('customer_service', o.customer_id) as customer_info
FROM orders o
WITH (
    'circuit.breaker.enabled' = 'true',
    'circuit.breaker.failure.threshold' = '5',
    'circuit.breaker.timeout' = '30s'
);
```

## Production Configuration

### High Availability Setup

```rust
let production_config = StreamingConfig {
    // Generous resource limits
    max_memory_mb: Some(16_384), // 16GB
    max_connections: Some(2_000),
    
    // Resource management
    resource_limits: Some(ResourceLimitsConfig {
        max_concurrent_queries: 500,
        max_window_buffer_size: 10_000_000,
        max_aggregation_groups: 1_000_000,
        connection_pool_size: 50,
        memory_pressure_threshold: 0.75,
    }),
    
    // Circuit breakers for external services
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 10,
        success_threshold: 5,
        timeout: Duration::from_secs(120),
        sliding_window_size: 200,
        minimum_throughput: 20,
    }),
    
    // Aggressive retry logic
    retry_policy: Some(RetryConfig {
        max_retries: 5,
        backoff_strategy: BackoffStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        },
    }),
    
    // Dead letter queue
    dead_letter_queue: Some(DeadLetterQueueConfig {
        topic: "production-dlq".to_string(),
        max_retries: 3,
        retry_delay: Duration::from_minutes(5),
        retention_period: Duration::from_days(30),
    }),
    
    // ...
};
```

### Development Configuration

```rust
let dev_config = StreamingConfig {
    // Conservative limits for development
    max_memory_mb: Some(1_024), // 1GB
    max_connections: Some(100),
    
    // Relaxed circuit breaker for testing
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 20, // More tolerant
        success_threshold: 2,
        timeout: Duration::from_secs(30),
        // ...
    }),
    
    // Immediate retry for faster feedback
    retry_policy: Some(RetryConfig {
        max_retries: 2,
        backoff_strategy: BackoffStrategy::FixedInterval {
            interval: Duration::from_millis(100)
        },
    }),
    
    // ...
};
```

## Troubleshooting

### Common Issues

1. **Out of Memory Errors**
   - Increase `max_memory_mb`
   - Reduce window buffer sizes
   - Enable disk spilling

2. **Connection Pool Exhausted**
   - Increase pool size
   - Check for connection leaks
   - Reduce connection idle timeout

3. **Circuit Breaker Constantly Open**
   - Check downstream service health
   - Adjust failure threshold
   - Verify timeout settings

4. **High Retry Rates**
   - Investigate root cause failures
   - Adjust retry conditions
   - Consider circuit breaker tuning

### Debug Configuration

```rust
let debug_config = StreamingConfig {
    debug_resource_management: true,
    debug_circuit_breakers: true,
    debug_retries: true,
    
    // Detailed logging
    log_resource_allocations: true,
    log_circuit_breaker_state_changes: true,
    log_retry_attempts: true,
    
    // ...
};
```

## Related Features

- [Watermarks & Time Semantics](../sql/WATERMARKS_TIME_SEMANTICS.md) - Memory management for windows
- [Observability Guide](OBSERVABILITY.md) - Monitoring resource usage
- [Performance Optimization](./PERFORMANCE_OPTIMIZATION.md) - Resource optimization strategies

## References

- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Retry Pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/retry)
- [Bulkhead Pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/bulkhead)