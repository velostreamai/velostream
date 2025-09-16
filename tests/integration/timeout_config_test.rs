use std::time::Duration;
use velostream::velostream::kafka::producer_config::ProducerConfig;

/// Debug test to verify timeout configuration is working
#[tokio::test]
async fn test_timeout_configuration_debug() {
    println!("=== Testing Timeout Configuration ===");

    // Test 1: Default configuration
    let default_config = ProducerConfig::new("localhost:9092", "test-topic");
    println!("Default config:");
    println!("  message_timeout: {:?}", default_config.message_timeout);
    println!(
        "  transaction_timeout: {:?}",
        default_config.transaction_timeout
    );

    // Test 2: Transactional with default timeout (should not adjust)
    let transactional_config =
        ProducerConfig::new("localhost:9092", "test-topic").transactional("test-tx-id");
    println!("\nTransactional config (default timeout):");
    println!(
        "  message_timeout: {:?}",
        transactional_config.message_timeout
    );
    println!(
        "  transaction_timeout: {:?}",
        transactional_config.transaction_timeout
    );

    // Test 3: Transactional with short timeout (should adjust message timeout)
    let short_tx_config = ProducerConfig::new("localhost:9092", "test-topic")
        .transactional("test-tx-id")
        .transaction_timeout(Duration::from_secs(5));
    println!("\nTransactional config (5s timeout):");
    println!("  message_timeout: {:?}", short_tx_config.message_timeout);
    println!(
        "  transaction_timeout: {:?}",
        short_tx_config.transaction_timeout
    );

    // Test 4: Just transaction_timeout method
    let tx_timeout_only = ProducerConfig::new("localhost:9092", "test-topic")
        .transaction_timeout(Duration::from_secs(10));
    println!("\nTransaction timeout only (10s):");
    println!("  message_timeout: {:?}", tx_timeout_only.message_timeout);
    println!(
        "  transaction_timeout: {:?}",
        tx_timeout_only.transaction_timeout
    );

    // Verify the constraint is satisfied
    assert!(
        short_tx_config.message_timeout <= short_tx_config.transaction_timeout,
        "message_timeout ({:?}) should be <= transaction_timeout ({:?})",
        short_tx_config.message_timeout,
        short_tx_config.transaction_timeout
    );

    assert!(
        tx_timeout_only.message_timeout <= tx_timeout_only.transaction_timeout,
        "message_timeout ({:?}) should be <= transaction_timeout ({:?})",
        tx_timeout_only.message_timeout,
        tx_timeout_only.transaction_timeout
    );

    println!("\n✅ All timeout configurations are correct!");
}
