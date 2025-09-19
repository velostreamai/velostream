# Multi-Source/Multi-Sink API Reference

## Overview

This document provides detailed API reference for Velostream' multi-source and multi-sink processing capabilities. It covers the core traits, methods, and configuration options for implementing custom sources and sinks.

## Table of Contents

1. [Core APIs](#core-apis)
2. [Processor APIs](#processor-apis)
3. [Helper Functions](#helper-functions)
4. [Configuration Types](#configuration-types)
5. [Error Handling](#error-handling)
6. [Examples](#examples)

---

## Core APIs

### JobProcessor Trait Methods

#### `SimpleJobProcessor::process_multi_job()`

Process records from multiple datasources with best-effort semantics.

```rust
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>>
```

**Parameters:**
- `readers`: Map of source name to DataReader instances
- `writers`: Map of sink name to DataWriter instances  
- `engine`: Shared SQL execution engine
- `query`: Parsed SQL query to execute
- `job_name`: Unique identifier for logging/metrics
- `shutdown_rx`: Channel to receive shutdown signals

**Returns:**
- `Ok(JobExecutionStats)`: Processing completed successfully
- `Err(...)`: Processing failed with error details

**Example:**
```rust
let config = JobProcessingConfig {
    use_transactions: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    max_batch_size: 1000,
    batch_timeout: Duration::from_millis(100),
    // ... other fields
};

let processor = SimpleJobProcessor::new(config);

let mut readers = HashMap::new();
readers.insert("source1".to_string(), kafka_reader);
readers.insert("source2".to_string(), file_reader);

let mut writers = HashMap::new();
writers.insert("sink1".to_string(), kafka_writer);

let stats = processor.process_multi_job(
    readers,
    writers, 
    engine,
    query,
    "my-job".to_string(),
    shutdown_rx,
).await?;

println!("Processed {} records", stats.records_processed);
```

#### `TransactionalJobProcessor::process_multi_job()`

Process records with full ACID transaction guarantees across all sources and sinks.

```rust
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>>
```

**Key Differences from Simple Mode:**
- Begins transactions on all transaction-capable sources/sinks
- Commits atomically across all resources
- Rolls back on any failure
- Higher latency but guaranteed consistency

**Transaction Behavior:**
```rust
// Pseudo-code for transaction flow
for each batch {
    begin_transactions_all_sources();
    begin_transactions_all_sinks();
    
    match process_records() {
        Ok(_) => commit_all_transactions(),
        Err(_) => abort_all_transactions(),
    }
}
```

---

## Helper Functions

### Multi-Source Creation

#### `create_multi_source_readers()`

Create multiple DataReader instances from source requirements.

```rust
pub async fn create_multi_source_readers(
    sources: &[DataSourceRequirement],
    default_topic: &str,
    job_name: &str,
    batch_config: &Option<BatchConfig>,
) -> MultiSourceCreationResult
```

**Parameters:**
- `sources`: Array of source requirements from query analysis
- `default_topic`: Fallback topic for Kafka sources
- `job_name`: Job identifier for logging
- `batch_config`: Optional batch processing configuration

**Returns:**
- `Ok(HashMap<String, Box<dyn DataReader>>)`: Map of source name to reader
- `Err(String)`: Error message if any source creation failed

**Example:**
```rust
let sources = vec![
    DataSourceRequirement {
        name: "orders".to_string(),
        source_type: DataSourceType::Kafka,
        properties: kafka_props,
    },
    DataSourceRequirement {
        name: "customers".to_string(),
        source_type: DataSourceType::File,
        properties: file_props,
    },
];

let readers = create_multi_source_readers(
    &sources,
    "default-topic",
    "my-job",
    &None,
).await?;

println!("Created {} source readers", readers.len());
```

#### `create_multi_sink_writers()`

Create multiple DataWriter instances from sink requirements.

```rust
pub async fn create_multi_sink_writers(
    sinks: &[DataSinkRequirement],
    job_name: &str,
    batch_config: &Option<BatchConfig>,
) -> MultiSinkCreationResult
```

**Parameters:**
- `sinks`: Array of sink requirements from query analysis
- `job_name`: Job identifier for logging
- `batch_config`: Optional batch processing configuration

**Returns:**
- `Ok(HashMap<String, Box<dyn DataWriter>>)`: Map of sink name to writer
- `Err(String)`: Error message if any sink creation failed

**Behavior:**
- Creates all possible sinks
- Logs warnings for failed sinks but doesn't fail completely
- Returns empty HashMap if no sinks could be created

**Example:**
```rust
let sinks = vec![
    DataSinkRequirement {
        name: "kafka_output".to_string(),
        sink_type: DataSinkType::Kafka,
        properties: kafka_sink_props,
    },
    DataSinkRequirement {
        name: "file_backup".to_string(),
        sink_type: DataSinkType::File,
        properties: file_sink_props,
    },
];

let writers = create_multi_sink_writers(
    &sinks,
    "my-job",
    &batch_config,
).await?;

if writers.is_empty() {
    println!("No sinks created, will use stdout");
}
```

---

## Configuration Types

### JobProcessingConfig

Configuration for job processing behavior.

```rust
pub struct JobProcessingConfig {
    /// Use transactional processing
    pub use_transactions: bool,
    
    /// Strategy for handling failures
    pub failure_strategy: FailureStrategy,
    
    /// Maximum records per batch
    pub max_batch_size: usize,
    
    /// Maximum time to wait for batch completion
    pub batch_timeout: Duration,
    
    /// Maximum number of retries for failed operations
    pub max_retries: u32,
    
    /// Backoff time between retries
    pub retry_backoff: Duration,
    
    /// Interval for progress logging (batches)
    pub progress_interval: u64,
    
    /// Enable progress logging
    pub log_progress: bool,
}
```

**Default Configuration:**
```rust
impl Default for JobProcessingConfig {
    fn default() -> Self {
        Self {
            use_transactions: false,
            failure_strategy: FailureStrategy::LogAndContinue,
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(1000),
            max_retries: 3,
            retry_backoff: Duration::from_millis(500),
            progress_interval: 10,
            log_progress: true,
        }
    }
}
```

### FailureStrategy

Defines how processing failures are handled.

```rust
pub enum FailureStrategy {
    /// Log error and continue processing
    LogAndContinue,
    
    /// Fail entire batch on any error
    FailBatch,
    
    /// Retry with exponential backoff
    RetryWithBackoff,
}
```

**Usage Guidelines:**

| Strategy | Throughput | Data Loss Risk | Use Case |
|----------|------------|----------------|----------|
| `LogAndContinue` | High | Possible | Analytics, monitoring |
| `FailBatch` | Medium | Low | Business data, compliance |
| `RetryWithBackoff` | Low | Very Low | Critical transactions |

### DataSourceRequirement

Specifies a data source to be created.

```rust
pub struct DataSourceRequirement {
    /// Source identifier
    pub name: String,
    
    /// Type of data source
    pub source_type: DataSourceType,
    
    /// Configuration properties
    pub properties: HashMap<String, String>,
}
```

**Example:**
```rust
let kafka_source = DataSourceRequirement {
    name: "orders".to_string(),
    source_type: DataSourceType::Kafka,
    properties: {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("topic".to_string(), "orders".to_string());
        props.insert("source.format".to_string(), "json".to_string());
        props
    },
};
```

### DataSinkRequirement

Specifies a data sink to be created.

```rust
pub struct DataSinkRequirement {
    /// Sink identifier  
    pub name: String,
    
    /// Type of data sink
    pub sink_type: DataSinkType,
    
    /// Configuration properties
    pub properties: HashMap<String, String>,
}
```

---

## Error Handling

### Error Types

#### `MultiSourceCreationResult`
```rust
pub type MultiSourceCreationResult = Result<HashMap<String, Box<dyn DataReader>>, String>;
```

**Error Conditions:**
- Source service unavailable
- Invalid configuration
- Authentication failure
- Network connectivity issues

#### `MultiSinkCreationResult`
```rust
pub type MultiSinkCreationResult = Result<HashMap<String, Box<dyn DataWriter>>, String>;
```

**Error Conditions:**
- Sink service unavailable
- Permission issues
- Invalid destination paths
- Configuration errors

### Error Handling Patterns

#### **Graceful Degradation**
```rust
// Sinks use graceful degradation
match create_multi_sink_writers(&sinks, job_name, &batch_config).await {
    Ok(writers) => {
        if writers.is_empty() {
            // Fall back to stdout
            writers.insert("stdout".to_string(), stdout_writer);
        }
    }
    Err(e) => {
        warn!("Sink creation failed: {}, using stdout only", e);
        writers.insert("stdout_fallback".to_string(), stdout_writer);
    }
}
```

#### **Source Failure Handling**
```rust
// Sources require all to succeed
match create_multi_source_readers(&sources, topic, job_name, &batch_config).await {
    Ok(readers) => {
        // Proceed with processing
        process_multi_job(readers, writers, ...).await?;
    }
    Err(e) => {
        // Cannot proceed without sources
        return Err(format!("Failed to create sources: {}", e));
    }
}
```

#### **Runtime Error Recovery**
```rust
// Inside processor loop
loop {
    match process_multi_source_batch(...).await {
        Ok(()) => {
            // Batch succeeded
            stats.batches_processed += 1;
        }
        Err(e) => {
            match config.failure_strategy {
                FailureStrategy::LogAndContinue => {
                    warn!("Batch failed but continuing: {:?}", e);
                    stats.batches_failed += 1;
                }
                FailureStrategy::FailBatch => {
                    error!("Batch failed, stopping: {:?}", e);
                    return Err(e);
                }
                FailureStrategy::RetryWithBackoff => {
                    warn!("Batch failed, retrying: {:?}", e);
                    tokio::time::sleep(config.retry_backoff).await;
                    // Retry logic...
                }
            }
        }
    }
}
```

---

## Examples

### Complete Multi-Source Job Implementation

```rust
use velostream::velo::server::processors::{
    SimpleJobProcessor, JobProcessingConfig, FailureStrategy,
    create_multi_source_readers, create_multi_sink_writers,
};
use velostream::velo::sql::{StreamExecutionEngine, StreamingSqlParser};
use velostream::velo::sql::query_analyzer::{
    QueryAnalyzer, DataSourceRequirement, DataSinkRequirement,
    DataSourceType, DataSinkType,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Define sources and sinks
    let sources = vec![
        DataSourceRequirement {
            name: "orders".to_string(),
            source_type: DataSourceType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
                props.insert("topic".to_string(), "orders".to_string());
                props
            },
        },
        DataSourceRequirement {
            name: "customers".to_string(),
            source_type: DataSourceType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "customers.json".to_string());
                props
            },
        },
    ];

    let sinks = vec![
        DataSinkRequirement {
            name: "processed".to_string(),
            sink_type: DataSinkType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
                props.insert("topic".to_string(), "processed-orders".to_string());
                props
            },
        },
    ];

    // 2. Create readers and writers
    let readers = create_multi_source_readers(
        &sources,
        "default-topic",
        "example-job",
        &None,
    ).await?;

    let writers = create_multi_sink_writers(
        &sinks,
        "example-job", 
        &None,
    ).await?;

    println!("Created {} sources and {} sinks", readers.len(), writers.len());

    // 3. Set up SQL processing
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse(
        "SELECT o.order_id, c.customer_name FROM orders o JOIN customers c ON o.customer_id = c.id"
    )?;

    // 4. Configure processor
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(1000),
        max_retries: 3,
        retry_backoff: Duration::from_millis(500),
        progress_interval: 10,
        log_progress: true,
    };

    let processor = SimpleJobProcessor::new(config);

    // 5. Set up shutdown handling
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Shutdown signal received");
        shutdown_tx.send(()).await.unwrap();
    });

    // 6. Run the job
    println!("Starting multi-source job processing...");
    let stats = processor.process_multi_job(
        readers,
        writers,
        engine,
        query,
        "example-multi-job".to_string(),
        shutdown_rx,
    ).await?;

    println!("Job completed!");
    println!("  Batches processed: {}", stats.batches_processed);
    println!("  Records processed: {}", stats.records_processed);
    println!("  Records failed: {}", stats.records_failed);

    Ok(())
}
```

### Custom Source Implementation

```rust
use velostream::velo::datasource::{DataReader, DataWriter};
use async_trait::async_trait;

pub struct CustomDataReader {
    // Your custom fields
}

#[async_trait]
impl DataReader for CustomDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        // Your custom read logic
        Ok(vec![])
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Your custom logic
        Ok(false)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Your custom commit logic
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        true // or false based on your implementation
    }

    // Implement other transaction methods if supports_transactions() returns true
}
```

### Error Handling Example

```rust
async fn robust_multi_source_job() -> Result<JobExecutionStats, Box<dyn std::error::Error>> {
    let sources = define_sources();
    let sinks = define_sinks();

    // Try to create sources with error handling
    let readers = match create_multi_source_readers(&sources, "default", "job", &None).await {
        Ok(readers) => {
            if readers.is_empty() {
                return Err("No sources could be created".into());
            }
            readers
        }
        Err(e) => {
            eprintln!("Source creation failed: {}", e);
            return Err(e.into());
        }
    };

    // Try to create sinks with fallback
    let mut writers = match create_multi_sink_writers(&sinks, "job", &None).await {
        Ok(writers) => writers,
        Err(e) => {
            eprintln!("Sink creation failed: {}, using stdout", e);
            let mut fallback = HashMap::new();
            fallback.insert(
                "stdout".to_string(),
                Box::new(StdoutWriter::new()) as Box<dyn DataWriter>
            );
            fallback
        }
    };

    // Ensure at least stdout is available
    if writers.is_empty() {
        writers.insert(
            "stdout_fallback".to_string(),
            Box::new(StdoutWriter::new()) as Box<dyn DataWriter>
        );
    }

    // Continue with job processing...
    let processor = SimpleJobProcessor::new(JobProcessingConfig::default());
    // ... rest of implementation
    
    Ok(JobExecutionStats::default())
}
```

---

## Best Practices

### Performance Optimization

1. **Batch Size Tuning**
   ```rust
   // For high throughput
   let config = JobProcessingConfig {
       max_batch_size: 2000,
       batch_timeout: Duration::from_millis(50),
       ..Default::default()
   };

   // For low latency  
   let config = JobProcessingConfig {
       max_batch_size: 10,
       batch_timeout: Duration::from_millis(10),
       ..Default::default()
   };
   ```

2. **Memory Management**
   ```rust
   let batch_config = Some(BatchConfig {
       strategy: BatchStrategy::MemoryBased,
       memory_limit_mb: Some(256),
       max_batch_size: Some(1000),
       batch_timeout: Some(Duration::from_millis(1000)),
   });
   ```

### Error Handling

1. **Graceful Degradation**
   - Always provide fallback sinks (stdout)
   - Log warnings for partial failures
   - Continue processing with available resources

2. **Retry Logic**
   ```rust
   let config = JobProcessingConfig {
       failure_strategy: FailureStrategy::RetryWithBackoff,
       max_retries: 5,
       retry_backoff: Duration::from_millis(1000),
       ..Default::default()
   };
   ```

### Monitoring

1. **Comprehensive Logging**
   ```rust
   let config = JobProcessingConfig {
       log_progress: true,
       progress_interval: 100, // Log every 100 batches
       ..Default::default()
   };
   ```

2. **Metrics Collection**
   - Monitor `JobExecutionStats` for performance metrics
   - Track source/sink health separately
   - Set up alerts for failure rates and processing lag

This API reference provides the foundation for implementing sophisticated multi-source streaming applications with Velostream. For additional examples and advanced use cases, refer to the main documentation and example applications.