//! Integration tests for the SQL Application Test Harness (FR-084)
//!
//! These tests verify the test harness works end-to-end with real Kafka
//! using testcontainers.
//!
//! These tests require the `test-support` feature and Docker to be available.
//! Run with: `cargo test --features test-support`

// Only compile these tests when the test-support feature is enabled
#![cfg(feature = "test-support")]

use std::time::Duration;
use velostream::velostream::test_harness::{TestHarnessError, TestHarnessInfra};

/// Test that we can start a Kafka container via testcontainers
#[tokio::test]
async fn test_testcontainers_kafka_startup() {
    // Skip if Docker is not available
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    // Start infrastructure with testcontainers
    let result = TestHarnessInfra::with_testcontainers().await;

    match result {
        Ok(mut infra) => {
            // Verify we got bootstrap servers
            assert!(
                infra.bootstrap_servers().is_some(),
                "Bootstrap servers should be available after container start"
            );

            let bootstrap_servers = infra.bootstrap_servers().unwrap();
            println!("Kafka container started at: {}", bootstrap_servers);

            // Start infrastructure (creates admin client, temp dir)
            infra.start().await.expect("Failed to start infrastructure");

            // Create a test topic
            let topic_name = infra
                .create_topic("test_topic", 1)
                .await
                .expect("Failed to create topic");
            println!("Created topic: {}", topic_name);

            // Verify topic name includes run_id
            assert!(topic_name.contains("test_"));
            assert!(topic_name.ends_with("_test_topic"));

            // Stop infrastructure (cleans up topics)
            infra.stop().await.expect("Failed to stop infrastructure");
        }
        Err(e) => {
            // Docker might not be available in CI
            if e.to_string().contains("Docker") || e.to_string().contains("container") {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

/// Test topic creation and deletion
#[tokio::test]
async fn test_topic_lifecycle() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker test");
        return;
    }

    let result = TestHarnessInfra::with_testcontainers().await;

    if let Ok(mut infra) = result {
        infra.start().await.expect("Failed to start");

        // Create multiple topics
        let topic1 = infra
            .create_topic("orders", 3)
            .await
            .expect("Failed to create topic1");
        let topic2 = infra
            .create_topic("products", 1)
            .await
            .expect("Failed to create topic2");

        println!("Created topics: {}, {}", topic1, topic2);

        // Delete one topic
        infra
            .delete_topic("orders")
            .await
            .expect("Failed to delete topic");

        // Stop will clean up remaining topics
        infra.stop().await.expect("Failed to stop");
    } else {
        println!("Skipping - Docker not available");
    }
}

/// Test producer and consumer creation
#[tokio::test]
async fn test_producer_consumer_creation() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker test");
        return;
    }

    let result = TestHarnessInfra::with_testcontainers().await;

    if let Ok(mut infra) = result {
        infra.start().await.expect("Failed to start");

        // Create producer
        let producer = infra.create_producer();
        assert!(producer.is_ok(), "Should be able to create producer");

        // Create consumer
        let consumer = infra.create_consumer("test-group");
        assert!(consumer.is_ok(), "Should be able to create consumer");

        infra.stop().await.expect("Failed to stop");
    } else {
        println!("Skipping - Docker not available");
    }
}

/// Test config overrides
#[tokio::test]
async fn test_config_overrides() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker test");
        return;
    }

    let result = TestHarnessInfra::with_testcontainers().await;

    if let Ok(mut infra) = result {
        infra.start().await.expect("Failed to start");

        let overrides = infra.config_overrides();

        // Should have bootstrap.servers override
        assert!(
            overrides.contains_key("bootstrap.servers"),
            "Should have bootstrap.servers in overrides"
        );

        // Bootstrap servers should point to localhost
        let bs = overrides.get("bootstrap.servers").unwrap();
        assert!(
            bs.starts_with("127.0.0.1:"),
            "Bootstrap servers should be localhost"
        );

        infra.stop().await.expect("Failed to stop");
    } else {
        println!("Skipping - Docker not available");
    }
}

/// Test temp directory management
#[tokio::test]
async fn test_temp_directory() {
    if std::env::var("SKIP_DOCKER_TESTS").is_ok() {
        println!("Skipping Docker test");
        return;
    }

    let result = TestHarnessInfra::with_testcontainers().await;

    if let Ok(mut infra) = result {
        infra.start().await.expect("Failed to start");

        // Should have temp directory - clone the path to avoid borrow issues
        let temp_path = {
            let temp_dir = infra.temp_dir();
            assert!(temp_dir.is_some(), "Should have temp directory");
            temp_dir.unwrap().clone()
        };

        assert!(temp_path.exists(), "Temp directory should exist");
        assert!(
            temp_path.to_string_lossy().contains("velo_test_"),
            "Temp dir should contain run prefix"
        );

        // Get a file path in temp dir
        let file_path = infra.temp_file_path("output.jsonl");
        assert!(file_path.is_some(), "Should get file path");

        infra.stop().await.expect("Failed to stop");

        // After stop, temp dir should be cleaned up
        assert!(
            !temp_path.exists(),
            "Temp directory should be removed after stop"
        );
    } else {
        println!("Skipping - Docker not available");
    }
}
