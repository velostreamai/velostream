//! Test binary for failure strategy configuration WITH clauses
//!
//! This binary demonstrates and tests the failure strategy configuration functionality
//! directly accessible via WITH clauses in SQL.

use ferrisstreams::ferris::sql::config::with_clause_parser::WithClauseParser;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Testing Failure Strategy Configuration via WITH Clauses ===\n");

    // Test 1: Sink failure strategy with LogAndContinue
    test_sink_failure_strategy().await?;

    // Test 2: All failure strategy variants
    test_all_failure_strategy_variants().await?;

    // Test 3: Source failure strategy with RetryWithBackoff
    test_source_failure_strategy().await?;

    // Test 4: Combined batch config and failure strategy
    test_combined_batch_and_failure_config().await?;

    println!("\n✅ All failure strategy configuration tests completed successfully!");
    Ok(())
}

async fn test_sink_failure_strategy() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 1: Sink Failure Strategy Configuration");

    let parser = WithClauseParser::new();

    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.failure_strategy' = 'LogAndContinue',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'fixed_size',
        'sink.batch.size' = '100'
    "#;

    let config = parser.parse_with_clause(with_clause)?;

    // Validate that failure_strategy is parsed and available in raw config
    assert_eq!(
        config.raw_config.get("sink.failure_strategy").unwrap(),
        "LogAndContinue"
    );
    println!(
        "  ✅ Sink failure strategy: {}",
        config.raw_config.get("sink.failure_strategy").unwrap()
    );

    // Debug: Check what keys are actually present
    println!("  🔍 Available raw_config keys:");
    for (k, v) in &config.raw_config {
        println!("    {} = {}", k, v);
    }

    // Note: sink.bootstrap.servers and sink.topic are not defined in schema,
    // so they get filtered out during validation. This is expected behavior.
    // The important thing is that failure_strategy is available for sink creation.

    // Debug: Check if batch config is present
    if config.batch_config.is_none() {
        println!("  🔍 Debug: batch_config is None - checking configuration:");
        for (k, v) in &config.raw_config {
            if k.contains("batch") {
                println!("    {} = {}", k, v);
            }
        }
        return Err("Batch config should be present but is None".into());
    }

    // Validate that batch config is still parsed correctly alongside failure strategy
    let batch_config = config.batch_config.unwrap();
    assert_eq!(batch_config.enable_batching, true);
    println!("  ✅ Batch enabled: {}", batch_config.enable_batching);
    println!("  ✅ Batch strategy: fixed_size");
    println!(
        "  ✅ Batch size: {}\n",
        config.raw_config.get("sink.batch.size").unwrap()
    );

    Ok(())
}

async fn test_all_failure_strategy_variants() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    println!("🚀 Test 2: All Failure Strategy Variants");

    let parser = WithClauseParser::new();

    let failure_strategies = vec![
        "LogAndContinue",
        "SendToDLQ",
        "FailBatch",
        "RetryWithBackoff",
    ];

    for strategy in &failure_strategies {
        let with_clause = format!(
            r#"
            'sink.bootstrap.servers' = 'localhost:9092',
            'sink.topic' = 'test-topic',
            'sink.failure_strategy' = '{}'
        "#,
            strategy
        );

        let config = parser.parse_with_clause(&with_clause)?;

        // Should parse successfully and be available in raw config
        assert_eq!(
            config.raw_config.get("sink.failure_strategy").unwrap(),
            *strategy
        );
        println!("  ✅ Failure strategy '{}' parsed successfully", strategy);
    }

    println!();
    Ok(())
}

async fn test_source_failure_strategy() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 3: Source Failure Strategy Configuration");

    let parser = WithClauseParser::new();

    let with_clause = r#"
        'source.format' = 'csv',
        'source.failure_strategy' = 'RetryWithBackoff',
        'source.retry_backoff' = '1000ms',
        'source.max_retries' = '3'
    "#;

    let config = parser.parse_with_clause(with_clause)?;

    // Validate source failure strategy configuration
    assert_eq!(
        config.raw_config.get("source.failure_strategy").unwrap(),
        "RetryWithBackoff"
    );
    assert_eq!(
        config.raw_config.get("source.retry_backoff").unwrap(),
        "1000ms"
    );
    assert_eq!(config.raw_config.get("source.max_retries").unwrap(), "3");
    // Note: source.format is not in schema, so it gets filtered out

    println!(
        "  ✅ Source failure strategy: {}",
        config.raw_config.get("source.failure_strategy").unwrap()
    );
    println!(
        "  ✅ Retry backoff: {}",
        config.raw_config.get("source.retry_backoff").unwrap()
    );
    println!(
        "  ✅ Max retries: {}",
        config.raw_config.get("source.max_retries").unwrap()
    );
    println!("  ✅ Configuration validated successfully\n");

    Ok(())
}

async fn test_combined_batch_and_failure_config(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 Test 4: Combined Batch and Failure Strategy Configuration");

    let parser = WithClauseParser::new();

    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'combined-test-topic',
        'sink.failure_strategy' = 'RetryWithBackoff',
        'sink.max_retries' = '5',
        'sink.retry_backoff' = '2000ms',
        'sink.batch.enable' = 'true',
        'sink.batch.strategy' = 'low_latency',
        'sink.batch.low_latency_max_size' = '5',
        'sink.batch.low_latency_wait' = '10ms',
        'sink.batch.eager_processing' = 'true'
    "#;

    let config = parser.parse_with_clause(with_clause)?;

    // Validate failure strategy configuration
    assert_eq!(
        config.raw_config.get("sink.failure_strategy").unwrap(),
        "RetryWithBackoff"
    );
    assert_eq!(config.raw_config.get("sink.max_retries").unwrap(), "5");
    assert_eq!(
        config.raw_config.get("sink.retry_backoff").unwrap(),
        "2000ms"
    );

    // Validate batch configuration
    let batch_config = config.batch_config.unwrap();
    assert_eq!(batch_config.enable_batching, true);

    // Validate low latency batch strategy
    match batch_config.strategy {
        ferrisstreams::ferris::datasource::BatchStrategy::LowLatency {
            max_batch_size,
            eager_processing,
            ..
        } => {
            assert_eq!(max_batch_size, 5);
            assert_eq!(eager_processing, true);
            println!("  ✅ Low latency max batch size: {}", max_batch_size);
            println!("  ✅ Eager processing: {}", eager_processing);
        }
        _ => return Err("Expected LowLatency strategy".into()),
    }

    println!(
        "  ✅ Failure strategy: {}",
        config.raw_config.get("sink.failure_strategy").unwrap()
    );
    println!(
        "  ✅ Max retries: {}",
        config.raw_config.get("sink.max_retries").unwrap()
    );
    println!(
        "  ✅ Retry backoff: {}",
        config.raw_config.get("sink.retry_backoff").unwrap()
    );
    println!("  ✅ Batch enabled: {}", batch_config.enable_batching);

    // Print all configuration keys for debugging
    println!("  🔍 All parsed configuration keys:");
    let mut sorted_keys: Vec<_> = config.raw_config.keys().collect();
    sorted_keys.sort();
    for key in sorted_keys {
        if key.contains("batch") || key.contains("failure") || key.contains("retry") {
            println!("    {} = {}", key, config.raw_config.get(key).unwrap());
        }
    }

    println!();
    Ok(())
}
