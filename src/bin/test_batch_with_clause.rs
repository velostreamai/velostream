//! Test binary for batch configuration WITH clauses
//!
//! This binary demonstrates and tests the batch configuration functionality
//! directly accessible via WITH clauses in SQL.

use ferrisstreams::ferris::{
    datasource::BatchStrategy,
    sql::config::with_clause_parser::WithClauseParser,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Testing Batch Configuration via WITH Clauses ===\n");

    // Test 1: Basic fixed size batch configuration
    test_basic_fixed_size_batch().await?;

    // Test 2: Time window batch strategy
    test_time_window_batch().await?;

    // Test 3: Low latency batch strategy
    test_low_latency_batch().await?;

    // Test 4: Memory-based batch strategy
    test_memory_based_batch().await?;

    // Test 5: Adaptive size batch strategy
    test_adaptive_size_batch().await?;

    // Test 6: Batch disabled
    test_batch_disabled().await?;

    // Test 7: Error handling
    test_invalid_configuration().await?;

    println!("\n✅ All batch configuration tests completed successfully!");
    Ok(())
}

async fn test_basic_fixed_size_batch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 1: Basic Fixed Size Batch Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'fixed_size',
        'sink.batch.size' = '250',
        'sink.batch.max_size' = '1000',
        'sink.batch.timeout' = '500ms'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    
    assert!(config.batch_config.is_some());
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, true);
    assert_eq!(batch_config.max_batch_size, 1000);
    assert_eq!(batch_config.batch_timeout, Duration::from_millis(500));
    
    match batch_config.strategy {
        BatchStrategy::FixedSize(size) => {
            assert_eq!(size, 250);
            println!("  ✅ Fixed size batch strategy: {} records", size);
        }
        _ => return Err("Expected FixedSize strategy".into()),
    }

    println!("  ✅ Max batch size: {}", batch_config.max_batch_size);
    println!("  ✅ Batch timeout: {:?}", batch_config.batch_timeout);
    println!("  ✅ Batching enabled: {}\n", batch_config.enable_batching);

    Ok(())
}

async fn test_time_window_batch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 2: Time Window Batch Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'time-window-topic',
        'sink.batch.strategy' = 'time_window',
        'sink.batch.window' = '2s',
        'sink.batch.max_size' = '500'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::TimeWindow(duration) => {
            assert_eq!(duration, Duration::from_secs(2));
            println!("  ✅ Time window duration: {:?}", duration);
        }
        _ => return Err("Expected TimeWindow strategy".into()),
    }
    
    assert_eq!(batch_config.max_batch_size, 500);
    println!("  ✅ Max batch size: {}\n", batch_config.max_batch_size);

    Ok(())
}

async fn test_low_latency_batch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 3: Low Latency Batch Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'low-latency-topic',
        'sink.batch.strategy' = 'low_latency',
        'sink.batch.low_latency_max_size' = '3',
        'sink.batch.low_latency_wait' = '1ms',
        'sink.batch.eager_processing' = 'true'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::LowLatency { max_batch_size, max_wait_time, eager_processing } => {
            assert_eq!(max_batch_size, 3);
            assert_eq!(max_wait_time, Duration::from_millis(1));
            assert_eq!(eager_processing, true);
            println!("  ✅ Low latency max batch size: {}", max_batch_size);
            println!("  ✅ Low latency wait time: {:?}", max_wait_time);
            println!("  ✅ Eager processing: {}", eager_processing);
        }
        _ => return Err("Expected LowLatency strategy".into()),
    }

    println!();
    Ok(())
}

async fn test_memory_based_batch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 4: Memory-Based Batch Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'memory-based-topic',
        'sink.batch.strategy' = 'memory_based',
        'sink.batch.memory_size' = '2097152'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::MemoryBased(size) => {
            assert_eq!(size, 2097152); // 2MB
            println!("  ✅ Memory-based batch size: {} bytes ({}MB)", size, size / (1024 * 1024));
        }
        _ => return Err("Expected MemoryBased strategy".into()),
    }

    println!();
    Ok(())
}

async fn test_adaptive_size_batch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 5: Adaptive Size Batch Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'adaptive-topic',
        'sink.batch.strategy' = 'adaptive_size',
        'sink.batch.min_size' = '25',
        'sink.batch.adaptive_max_size' = '800',
        'sink.batch.target_latency' = '120ms'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    let batch_config = config.batch_config.unwrap();
    
    match batch_config.strategy {
        BatchStrategy::AdaptiveSize { min_size, max_size, target_latency } => {
            assert_eq!(min_size, 25);
            assert_eq!(max_size, 800);
            assert_eq!(target_latency, Duration::from_millis(120));
            println!("  ✅ Adaptive min size: {}", min_size);
            println!("  ✅ Adaptive max size: {}", max_size);
            println!("  ✅ Target latency: {:?}", target_latency);
        }
        _ => return Err("Expected AdaptiveSize strategy".into()),
    }

    println!();
    Ok(())
}

async fn test_batch_disabled() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 6: Batch Disabled Configuration");

    let parser = WithClauseParser::new();
    
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'disabled-batch-topic',
        'sink.batch.enable' = 'false'
    "#;

    let config = parser.parse_with_clause(with_clause)?;
    let batch_config = config.batch_config.unwrap();
    
    assert_eq!(batch_config.enable_batching, false);
    println!("  ✅ Batching disabled: {}\n", !batch_config.enable_batching);

    Ok(())
}

async fn test_invalid_configuration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 7: Invalid Configuration Error Handling");

    let parser = WithClauseParser::new();
    
    // Test invalid batch strategy
    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'error-topic',
        'sink.batch.strategy' = 'invalid_strategy'
    "#;

    let result = parser.parse_with_clause(with_clause);
    assert!(result.is_err());
    println!("  ✅ Invalid batch strategy correctly rejected");

    // Test invalid batch size
    let with_clause2 = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'error-topic',
        'sink.batch.max_size' = 'not_a_number'
    "#;

    let result2 = parser.parse_with_clause(with_clause2);
    assert!(result2.is_err());
    println!("  ✅ Invalid batch size correctly rejected");

    // Test invalid boolean value
    let with_clause3 = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'error-topic',
        'sink.batch.enable' = 'maybe'
    "#;

    let result3 = parser.parse_with_clause(with_clause3);
    assert!(result3.is_err());
    println!("  ✅ Invalid boolean value correctly rejected\n");

    Ok(())
}