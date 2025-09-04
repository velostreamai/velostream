# Multi-Job Processor Testing Infrastructure

This directory contains a comprehensive, reusable testing infrastructure for all multi-job processors in FerrisStreams. The infrastructure provides consistent failure scenario testing across different processor implementations.

## Architecture Overview

### Core Components

1. **`stream_job_test_infrastructure.rs`** - Shared testing infrastructure
2. **`multi_job_simple_test.rs`** - Tests for SimpleJobProcessor
3. **`multi_job_transactional_test.rs`** - Tests for TransactionalJobProcessor  
4. **`multi_job_future_handler_test_template.rs`** - Template for future processors

## Shared Test Infrastructure

### Advanced Mock Components

#### `AdvancedMockDataReader`
Comprehensive mock DataReader with configurable failure injection:

```rust
let reader = AdvancedMockDataReader::new(test_batches)
    .with_read_failure_on_batch(1)           // Fail read on specific batch
    .with_commit_failure_on_batch(2)         // Fail commit after specific batch
    .with_has_more_failure()                 // Fail has_more() calls
    .with_timeout_simulation()               // Simulate network timeouts
    .with_transaction_support();             // Enable transaction support
```

#### `AdvancedMockDataWriter`
Comprehensive mock DataWriter with configurable failure scenarios:

```rust
let writer = AdvancedMockDataWriter::new()
    .with_write_failure_on_batch(0)         // Fail writes on specific batch
    .with_flush_failure_on_batch(1)         // Fail flush after specific batch
    .with_commit_failure_on_batch(2)        // Fail commit after specific batch
    .with_disk_full_simulation()            // Simulate disk full condition
    .with_network_partition_simulation()    // Simulate network partition
    .with_partial_batch_failure()           // Fail only some records in batch
    .with_transaction_support();            // Enable transaction support
```

### Multi-Job Processor Trait

The `MultiJobProcessor` trait provides a common interface for testing different processors:

```rust
#[async_trait]
pub trait MultiJobProcessor {
    type StatsType;

    async fn process_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        engine: Arc<Mutex<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self::StatsType, Box<dyn std::error::Error + Send + Sync>>;

    fn get_config(&self) -> &JobProcessingConfig;
}
```

## Comprehensive Failure Scenarios

### Available Test Scenarios

1. **`test_source_read_failure_scenario`** - Tests DataReader failures during read operations
2. **`test_sink_write_failure_scenario`** - Tests DataWriter failures during write operations  
3. **`test_disk_full_scenario`** - Tests disk full conditions with different retry strategies
4. **`test_network_partition_scenario`** - Tests network partition failures and recovery
5. **`test_partial_batch_failure_scenario`** - Tests scenarios where only some records fail
6. **`test_shutdown_signal_scenario`** - Tests graceful shutdown signal handling
7. **`test_empty_batch_handling_scenario`** - Tests handling of empty batches

### Comprehensive Test Suite

Run all scenarios for a processor:

```rust
run_comprehensive_failure_tests(&processor, "YourProcessorName").await;
```

## How to Add Tests for a New Processor

### Step 1: Copy the Template

Copy `multi_job_future_handler_test_template.rs` to `multi_job_[your_processor]_test.rs`.

### Step 2: Create Processor Wrapper

```rust
struct YourJobProcessorWrapper {
    processor: YourJobProcessor,
}

#[async_trait]
impl MultiJobProcessor for YourJobProcessorWrapper {
    type StatsType = YourStatsType;  // Update if different

    async fn process_job(&self, ...) -> Result<Self::StatsType, ...> {
        self.processor.process_job(...).await
    }

    fn get_config(&self) -> &JobProcessingConfig {
        &self.processor.config
    }
}
```

### Step 3: Configure Test Scenarios

```rust
#[tokio::test]
async fn test_your_processor_comprehensive_failure_scenarios() {
    // Test with different failure strategies
    let config = JobProcessingConfig {
        use_transactions: true,  // Configure for your processor
        failure_strategy: FailureStrategy::LogAndContinue,
        // ... other config
    };
    
    let processor = YourJobProcessorWrapper::new(config);
    run_comprehensive_failure_tests(&processor, "YourProcessor").await;
}
```

### Step 4: Add Processor-Specific Tests

```rust
#[tokio::test]
async fn test_your_processor_unique_features() {
    // Test features unique to your processor
    // e.g., special configuration, performance optimizations, etc.
}
```

## Test Categories

### Universal Tests (Run on All Processors)
- Source/sink failure handling
- Network partition scenarios  
- Disk full conditions
- Shutdown signal handling
- Empty batch processing
- Configuration edge cases

### Processor-Specific Tests
- **SimpleJobProcessor**: Non-transactional behavior, throughput optimization
- **TransactionalJobProcessor**: Transaction rollback, ACID compliance
- **YourProcessor**: Your unique features and optimizations

## Failure Strategy Testing

Each processor is tested with all failure strategies:

### `FailureStrategy::LogAndContinue`
- Logs errors but continues processing
- Should complete all scenarios successfully
- May have some failed records/batches in stats

### `FailureStrategy::RetryWithBackoff`
- Retries failed operations with exponential backoff
- Some tests may timeout (expected behavior)
- Use timeouts to prevent infinite retries in tests

### `FailureStrategy::FailBatch`  
- Fails the current batch but continues with next
- Should complete with failed batches in stats
- No retries, faster processing

### `FailureStrategy::SendToDLQ`
- Sends failed records to Dead Letter Queue
- Currently logs instead of actual DLQ (TODO)
- Should continue processing

## Best Practices

### Test Naming Convention
- `test_[processor_name]_[scenario]_scenario`
- `test_[processor_name]_comprehensive_failure_scenarios`
- `test_[processor_name]_vs_[other_processor]_comparison`

### Test Structure
```rust
#[tokio::test]
async fn test_your_scenario() {
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\\n=== Test: Your Scenario Description ===");
    
    // Setup test data
    let test_batches = vec![...];
    
    // Configure mocks with specific failure behavior
    let reader = AdvancedMockDataReader::new(test_batches)...;
    let writer = AdvancedMockDataWriter::new()...;
    
    // Configure processor
    let config = JobProcessingConfig {...};
    let processor = YourProcessor::new(config);
    
    // Run test
    let result = processor.process_job(...).await;
    
    // Assertions and logging
    assert!(result.is_ok(), "Description of expected behavior");
    println!("Test completed: {:?}", result);
}
```

### Timeout Handling
For processors with `RetryWithBackoff`, use timeouts to prevent infinite test runs:

```rust
let result = tokio::time::timeout(
    Duration::from_secs(5),
    processor.process_job(...)
).await;

match result {
    Ok(job_result) => println!("Completed: {:?}", job_result),
    Err(_) => println!("Timed out (expected for RetryWithBackoff)"),
}
```

## Running Tests

### Run All Multi-Job Tests
```bash
cargo test multi_job --no-default-features -- --nocapture
```

### Run Specific Processor Tests
```bash
cargo test multi_job_simple_test --no-default-features -- --nocapture
cargo test multi_job_transactional_test --no-default-features -- --nocapture
```

### Run Individual Test Scenarios
```bash
cargo test test_comprehensive_failure_scenarios --no-default-features -- --nocapture
```

## Future Enhancements

### Planned Additions
1. **Performance benchmarking** infrastructure
2. **Memory usage** tracking during failures
3. **Concurrent failure** scenarios (multiple failures at once)
4. **Recovery time** measurements
5. **Dead Letter Queue** actual implementation testing
6. **Schema evolution** failure testing

### Extension Points
- Add new mock failure types to `AdvancedMockDataReader/Writer`
- Create specialized test scenarios for new failure modes
- Add processor-specific performance benchmarks
- Integrate with chaos engineering frameworks

## Troubleshooting

### Common Issues

1. **Tests timing out**
   - Use shorter timeouts for `RetryWithBackoff` tests
   - Reduce `retry_backoff` duration in test configs
   - Limit `max_retries` to prevent excessive retry loops

2. **Compilation errors**
   - Ensure all imports are correct in your test file
   - Check that your processor implements the expected interface
   - Verify that the `MultiJobProcessor` wrapper is implemented correctly

3. **Test inconsistencies**
   - Initialize env_logger in each test: `let _ = env_logger::builder().is_test(true).try_init();`
   - Use unique job names to avoid conflicts
   - Reset mock state between tests if needed

### Debug Tips

1. **Enable detailed logging**:
   ```bash
   RUST_LOG=debug cargo test your_test --no-default-features -- --nocapture
   ```

2. **Use `println!` for test progress**:
   ```rust
   println!("\\n=== Test: {} ===", test_name);
   println!("Result: {:?}", result);
   ```

3. **Check mock behavior**:
   ```rust
   let written_records = writer.get_written_records().await;
   println!("Written {} records", written_records.len());
   ```

This infrastructure ensures consistent, comprehensive testing across all multi-job processors while making it easy to add tests for new processors in the future.