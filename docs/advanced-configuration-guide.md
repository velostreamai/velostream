# Phase 1B-4 Configuration Guide

## Overview

This guide covers the configuration of all Phase 1B-4 features in FerrisStreams: Watermarks & Time Semantics (1B), Resource Management & Circuit Breakers (2), Advanced Query Features (3), and Observability (4).

## Complete Configuration Structure

```rust
use ferrisstreams::ferris::sql::execution::config::*;
use std::time::Duration;

let complete_config = StreamingConfig {
    // Basic engine configuration
    max_memory_mb: Some(8192),           // 8GB memory limit
    max_connections: Some(1000),         // Max concurrent connections
    checkpoint_interval_ms: 30000,       // 30s checkpoint interval
    
    // Phase 1B: Time Semantics & Watermarks
    event_time_semantics: true,
    watermarks: Some(WatermarkConfig {
        strategy: WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(10)
        },
        idle_timeout: Some(Duration::from_secs(60)),
    }),
    late_data_strategy: LateDataStrategy::DeadLetter,
    
    // Phase 2: Resource Management  
    resource_limits: Some(ResourceLimitsConfig {
        max_concurrent_queries: 200,
        max_window_buffer_size: 5_000_000,
        max_aggregation_groups: 100_000,
        connection_pool_size: 50,
        memory_pressure_threshold: 0.8,
    }),
    
    // Phase 2: Circuit Breakers
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 3,
        timeout: Duration::from_secs(60),
        sliding_window_size: 100,
        minimum_throughput: 10,
        slow_call_threshold: Duration::from_secs(5),
        slow_call_rate_threshold: 0.5,
    }),
    
    // Phase 2: Retry Logic
    retry_policy: Some(RetryConfig {
        max_retries: 3,
        backoff_strategy: BackoffStrategy::ExponentialBackoff {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
        },
        retry_on: vec![
            ErrorType::Network,
            ErrorType::Timeout,
            ErrorType::ServiceUnavailable,
        ],
        no_retry_on: vec![
            ErrorType::Authentication,
            ErrorType::BadRequest,
        ],
    }),
    
    // Phase 4: Observability
    tracing: Some(TracingConfig::production()),
    prometheus: Some(PrometheusConfig::default()),
    profiling: Some(ProfilingConfig::production()),
    
    // Debug settings
    debug_watermarks: false,
    debug_resource_management: false,
    debug_circuit_breakers: false,
    debug_retries: false,
};
```

## Phase 1B: Watermarks & Time Semantics

### Watermark Strategies

#### BoundedOutOfOrderness (Recommended for most use cases)
```rust
watermarks: Some(WatermarkConfig {
    strategy: WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(30) // Allow 30s out-of-order
    },
    idle_timeout: Some(Duration::from_minutes(5)),    // 5min idle timeout
}),
```

#### Ascending (For strictly ordered streams)
```rust
watermarks: Some(WatermarkConfig {
    strategy: WatermarkStrategy::Ascending,
    idle_timeout: None, // Not needed for ordered streams
}),
```

#### Punctuated (For custom watermark events)
```rust
watermarks: Some(WatermarkConfig {
    strategy: WatermarkStrategy::Punctuated {
        punctuation_field: "watermark_ts".to_string()
    },
    idle_timeout: Some(Duration::from_minutes(1)),
}),
```

### Late Data Strategies

```rust
// Drop late data silently
late_data_strategy: LateDataStrategy::Drop,

// Send to dead letter queue for analysis
late_data_strategy: LateDataStrategy::DeadLetter,

// Include in next available window
late_data_strategy: LateDataStrategy::IncludeInNextWindow,

// Update previous window results (use with caution)
late_data_strategy: LateDataStrategy::UpdatePrevious,
```

### Event-Time Configuration

```rust
// Enable event-time processing
event_time_semantics: true,

// SQL-level configuration for event-time extraction
WITH (
    'event.time.field' = 'timestamp',
    'event.time.format' = 'yyyy-MM-dd HH:mm:ss.SSS',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s'
)
```

## Phase 2: Resource Management & Circuit Breakers

### Resource Limits Configuration

```rust
resource_limits: Some(ResourceLimitsConfig {
    // Query concurrency limits
    max_concurrent_queries: 500,         // Max parallel queries
    max_window_buffer_size: 10_000_000,  // Max records per window buffer
    max_aggregation_groups: 1_000_000,   // Max GROUP BY groups
    
    // Connection management
    connection_pool_size: 100,           // Database connection pool size
    connection_timeout: Duration::from_secs(30),
    connection_idle_timeout: Duration::from_secs(300),
    connection_max_lifetime: Duration::from_secs(3600),
    
    // Memory management
    memory_pressure_threshold: 0.75,     // Trigger cleanup at 75% usage
    enable_spill_to_disk: true,          // Spill large aggregations
    spill_threshold_mb: 1024,            // Spill when allocation > 1GB
    
    // Kafka-specific settings
    kafka_producer_pool_size: 20,
    kafka_consumer_pool_size: 10,
    kafka_connection_retry_attempts: 5,
    kafka_connection_backoff: Duration::from_millis(500),
}),
```

### Circuit Breaker Configuration

```rust
circuit_breaker: Some(CircuitBreakerConfig {
    // Basic thresholds
    failure_threshold: 10,               // Open after 10 failures
    success_threshold: 5,                // Close after 5 successes in half-open
    timeout: Duration::from_secs(120),   // Stay open for 2 minutes
    
    // Advanced settings
    sliding_window_size: 200,            // Track last 200 calls
    minimum_throughput: 20,              // Min calls before opening
    
    // Slow call detection  
    slow_call_threshold: Duration::from_secs(10), // 10s = slow
    slow_call_rate_threshold: 0.6,      // 60% slow calls = unhealthy
    
    // Failure rate calculation
    failure_rate_threshold: 0.5,        // 50% failure rate = unhealthy
    wait_duration_in_open_state: Duration::from_secs(60),
}),
```

### Retry Configuration

```rust
retry_policy: Some(RetryConfig {
    max_retries: 5,
    backoff_strategy: BackoffStrategy::ExponentialBackoff {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(60),
        multiplier: 2.0,
        jitter: true, // Add randomness to prevent thundering herd
    },
    
    // Conditional retry logic
    retry_on: vec![
        ErrorType::Network,
        ErrorType::Timeout, 
        ErrorType::ServiceUnavailable,
        ErrorType::InternalServerError,
    ],
    
    no_retry_on: vec![
        ErrorType::Authentication,
        ErrorType::Authorization,
        ErrorType::BadRequest,
        ErrorType::Forbidden,
        ErrorType::NotFound,
    ],
    
    // Additional settings
    retry_timeout: Some(Duration::from_secs(300)), // Total retry timeout
    enable_circuit_breaker_integration: true,     // Respect circuit breaker state
}),
```

### Dead Letter Queue Configuration

```rust
dead_letter_queue: Some(DeadLetterQueueConfig {
    topic: "failed-records".to_string(),
    
    // Retry settings for DLQ
    max_retries: 3,
    retry_delay: Duration::from_minutes(5),
    retry_backoff_multiplier: 2.0,
    
    // Retention settings  
    retention_period: Duration::from_days(30),
    max_queue_size: 1_000_000,
    
    // Metadata inclusion
    include_original_record: true,
    include_error_details: true,
    include_stack_trace: true,
    include_processing_context: true,
    
    // Serialization settings
    compression: CompressionType::Gzip,
    serialization_format: SerializationFormat::Json,
}),
```

## Phase 4: Observability Configuration

### Tracing Configuration

```rust
// Development tracing
tracing: Some(TracingConfig {
    enabled: true,
    log_level: LogLevel::Debug,
    include_query_text: true,
    include_record_data: false, // Don't log sensitive data
    max_span_duration: Duration::from_minutes(5),
    
    // Sampling for performance  
    sampling_rate: 1.0, // 100% sampling in dev
    trace_id_header: "x-trace-id".to_string(),
    
    // Output configuration
    output_format: TracingOutputFormat::Structured,
    console_output: true,
    file_output: Some("logs/tracing.log".to_string()),
}),

// Production tracing (more selective)
tracing: Some(TracingConfig {
    enabled: true,
    log_level: LogLevel::Info,
    include_query_text: false,     // Don't log queries in production
    include_record_data: false,
    max_span_duration: Duration::from_minutes(1),
    
    sampling_rate: 0.1, // 10% sampling for performance
    trace_id_header: "x-trace-id".to_string(),
    
    output_format: TracingOutputFormat::Json,
    console_output: false,
    file_output: Some("/var/log/ferris/tracing.json".to_string()),
}),
```

### Prometheus Metrics Configuration

```rust
prometheus: Some(PrometheusConfig {
    enabled: true,
    port: 9091,
    path: "/metrics".to_string(),
    
    // Metric collection intervals
    collection_interval: Duration::from_secs(15),
    push_gateway_url: None, // Use pull model
    
    // Histogram buckets for latency metrics
    query_duration_buckets: vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ],
    
    // Memory usage buckets (in MB)
    memory_usage_buckets: vec![
        10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0, 4000.0, 8000.0
    ],
    
    // Labels to include with all metrics
    global_labels: vec![
        ("environment".to_string(), "production".to_string()),
        ("instance".to_string(), hostname()),
        ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
    ],
    
    // Feature toggles
    collect_system_metrics: true,
    collect_sql_metrics: true, 
    collect_streaming_metrics: true,
    collect_resource_metrics: true,
}),
```

### Profiling Configuration

```rust
// Development profiling (detailed)
profiling: Some(ProfilingConfig {
    enabled: true,
    
    // CPU profiling
    cpu_profiling_enabled: true,
    cpu_sampling_rate: Duration::from_millis(10),
    
    // Memory profiling  
    memory_profiling_enabled: true,
    memory_sampling_rate: Duration::from_secs(1),
    track_allocations: true,
    
    // Performance analysis
    enable_bottleneck_detection: true,
    bottleneck_threshold: Duration::from_millis(100),
    
    // Report generation
    auto_report_generation: true,
    report_interval: Duration::from_minutes(30),
    report_output_dir: "profiling_reports".to_string(),
    
    // Flame graph generation
    generate_flame_graphs: true,
    flame_graph_duration: Duration::from_minutes(5),
    
    max_profiling_session_duration: Duration::from_hours(8),
}),

// Production profiling (lightweight)
profiling: Some(ProfilingConfig {
    enabled: true,
    
    cpu_profiling_enabled: true,
    cpu_sampling_rate: Duration::from_millis(100), // Less frequent
    
    memory_profiling_enabled: true, 
    memory_sampling_rate: Duration::from_secs(10),
    track_allocations: false, // Reduce overhead
    
    enable_bottleneck_detection: true,
    bottleneck_threshold: Duration::from_millis(500), // Higher threshold
    
    auto_report_generation: true,
    report_interval: Duration::from_hours(4), // Less frequent reports
    report_output_dir: "/var/log/ferris/profiling".to_string(),
    
    generate_flame_graphs: false, // Disable in production
    max_profiling_session_duration: Duration::from_hours(24),
}),
```

## Environment-Specific Configurations

### Development Configuration

```rust
pub fn development_config() -> StreamingConfig {
    StreamingConfig {
        // Generous limits for development
        max_memory_mb: Some(2048),
        max_connections: Some(100),
        checkpoint_interval_ms: 10000, // Frequent checkpoints
        
        // Relaxed time semantics
        event_time_semantics: true,
        watermarks: Some(WatermarkConfig {
            strategy: WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness: Duration::from_secs(60) // Very tolerant
            },
            idle_timeout: Some(Duration::from_secs(30)),
        }),
        late_data_strategy: LateDataStrategy::IncludeInNextWindow,
        
        // Tolerant resource limits
        resource_limits: Some(ResourceLimitsConfig {
            max_concurrent_queries: 50,
            max_window_buffer_size: 1_000_000,
            max_aggregation_groups: 10_000,
            connection_pool_size: 10,
            memory_pressure_threshold: 0.9,
        }),
        
        // Relaxed circuit breaker
        circuit_breaker: Some(CircuitBreakerConfig {
            failure_threshold: 20,
            success_threshold: 2,
            timeout: Duration::from_secs(30),
            sliding_window_size: 50,
            minimum_throughput: 5,
        }),
        
        // Minimal retries for fast feedback
        retry_policy: Some(RetryConfig {
            max_retries: 2,
            backoff_strategy: BackoffStrategy::FixedInterval {
                interval: Duration::from_millis(100)
            },
        }),
        
        // Detailed observability
        tracing: Some(TracingConfig::development()),
        prometheus: Some(PrometheusConfig::development()),
        profiling: Some(ProfilingConfig::development()),
        
        // Debug everything
        debug_watermarks: true,
        debug_resource_management: true,
        debug_circuit_breakers: true,
        debug_retries: true,
    }
}
```

### Production Configuration

```rust
pub fn production_config() -> StreamingConfig {
    StreamingConfig {
        // Production-scale limits
        max_memory_mb: Some(16384), // 16GB
        max_connections: Some(2000),
        checkpoint_interval_ms: 60000, // 1 minute checkpoints
        
        // Tight time semantics  
        event_time_semantics: true,
        watermarks: Some(WatermarkConfig {
            strategy: WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness: Duration::from_secs(10) // Tight tolerance
            },
            idle_timeout: Some(Duration::from_minutes(5)),
        }),
        late_data_strategy: LateDataStrategy::DeadLetter,
        
        // Production resource limits
        resource_limits: Some(ResourceLimitsConfig {
            max_concurrent_queries: 1000,
            max_window_buffer_size: 50_000_000,
            max_aggregation_groups: 5_000_000,
            connection_pool_size: 100,
            memory_pressure_threshold: 0.75, // Conservative threshold
        }),
        
        // Sensitive circuit breaker
        circuit_breaker: Some(CircuitBreakerConfig {
            failure_threshold: 5,
            success_threshold: 5,
            timeout: Duration::from_secs(120),
            sliding_window_size: 200,
            minimum_throughput: 50,
        }),
        
        // Aggressive retries
        retry_policy: Some(RetryConfig {
            max_retries: 5,
            backoff_strategy: BackoffStrategy::ExponentialBackoff {
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(60),
                multiplier: 2.0,
            },
        }),
        
        // Production observability
        tracing: Some(TracingConfig::production()),
        prometheus: Some(PrometheusConfig::production()),
        profiling: Some(ProfilingConfig::production()),
        
        // No debug logging
        debug_watermarks: false,
        debug_resource_management: false,
        debug_circuit_breakers: false,
        debug_retries: false,
    }
}
```

### Testing Configuration

```rust
pub fn testing_config() -> StreamingConfig {
    StreamingConfig {
        // Minimal resources for tests
        max_memory_mb: Some(512),
        max_connections: Some(10),
        checkpoint_interval_ms: 1000, // Fast checkpoints
        
        // Simple time semantics
        event_time_semantics: false, // Use processing time
        watermarks: None,
        late_data_strategy: LateDataStrategy::Drop,
        
        // Minimal resource limits
        resource_limits: Some(ResourceLimitsConfig {
            max_concurrent_queries: 5,
            max_window_buffer_size: 1000,
            max_aggregation_groups: 100,
            connection_pool_size: 2,
            memory_pressure_threshold: 0.95,
        }),
        
        // Disabled circuit breaker for deterministic tests
        circuit_breaker: None,
        
        // No retries for fast test execution
        retry_policy: None,
        
        // Minimal observability
        tracing: None,
        prometheus: None,
        profiling: None,
        
        // Debug for test diagnosis
        debug_watermarks: true,
        debug_resource_management: true,
        debug_circuit_breakers: true,
        debug_retries: true,
    }
}
```

## Configuration Validation

```rust
impl StreamingConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        // Memory validation
        if let Some(memory_mb) = self.max_memory_mb {
            if memory_mb < 128 {
                return Err(ConfigValidationError::InvalidMemoryLimit {
                    provided: memory_mb,
                    minimum: 128,
                });
            }
        }
        
        // Watermark validation
        if self.event_time_semantics && self.watermarks.is_none() {
            return Err(ConfigValidationError::MissingWatermarkConfig);
        }
        
        // Circuit breaker validation
        if let Some(cb_config) = &self.circuit_breaker {
            if cb_config.failure_threshold == 0 {
                return Err(ConfigValidationError::InvalidCircuitBreakerConfig);
            }
        }
        
        // Resource limits validation
        if let Some(limits) = &self.resource_limits {
            if limits.memory_pressure_threshold > 1.0 {
                return Err(ConfigValidationError::InvalidThreshold {
                    field: "memory_pressure_threshold".to_string(),
                    value: limits.memory_pressure_threshold,
                    max: 1.0,
                });
            }
        }
        
        Ok(())
    }
}
```

## Related Documentation

- [Watermarks & Time Semantics](sql/WATERMARKS_TIME_SEMANTICS.md) - Detailed Phase 1B configuration
- [Resource Management](ops/RESOURCE_MANAGEMENT.md) - Phase 2 configuration details
- [Observability Guide](ops/OBSERVABILITY.md) - Phase 4 monitoring setup
- [Advanced Query Features](sql/ADVANCED_QUERY_FEATURES.md) - Phase 3 SQL features