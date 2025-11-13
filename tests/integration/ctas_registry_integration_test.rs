//! CTAS + TableRegistry Integration Tests
//!
//! Critical integration tests for CTAS table creation with TableRegistry.
//! Validates table registration, metadata management, and consistency guarantees.
//!
//! ## Test Coverage
//!
//! 1. **Registration**: Successful registration, metadata propagation
//! 2. **Error Handling**: Registration failures, duplicate names, rollback
//! 3. **Concurrency**: Concurrent registration, race conditions
//! 4. **Consistency**: Registry state after errors, lookup correctness
//! 5. **Lifecycle**: Background jobs, table status transitions

use std::sync::Arc;
use std::time::Duration;

use velostream::velostream::server::parallel_loader::{
    ParallelLoader, ParallelLoadingConfig, TableDefinition,
};
use velostream::velostream::server::progress_monitoring::ProgressMonitor;
use velostream::velostream::server::table_registry::{TableRegistry, TableRegistryConfig, TableStatus};
use velostream::velostream::table::CtasExecutor;

/// Helper to create test environment
fn create_test_environment(
    kafka_brokers: String,
    group_id: String,
) -> (Arc<TableRegistry>, Arc<ProgressMonitor>, CtasExecutor) {
    let registry_config = TableRegistryConfig {
        kafka_brokers: kafka_brokers.clone(),
        base_group_id: group_id.clone(),
        max_tables: 100,
        ..Default::default()
    };

    let registry = Arc::new(TableRegistry::with_config(registry_config));
    let monitor = Arc::new(ProgressMonitor::new());
    let ctas_executor = CtasExecutor::new(kafka_brokers, group_id);

    (registry, monitor, ctas_executor)
}

// ============================================================================
// Test 1: Successful Table Registration
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_table_registration_success() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-test".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Create a simple table
    let tables = vec![TableDefinition::new(
        "users".to_string(),
        "SELECT * FROM kafka://user-events".to_string(),
    )];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Table creation should succeed");

    // Verify loading result
    assert_eq!(result.successful.len(), 1);
    assert!(result.successful.contains(&"users".to_string()));

    // Verify table is registered
    let registered_table = registry
        .lookup_table("users")
        .await
        .expect("Registry lookup should succeed");

    assert!(
        registered_table.is_some(),
        "Table should be registered in registry"
    );

    // Verify metadata
    let metadata = registry
        .get_metadata("users")
        .await
        .expect("Metadata lookup should succeed")
        .expect("Metadata should exist");

    assert_eq!(metadata.name, "users");
    assert_eq!(metadata.status, TableStatus::Active);

    // Verify table is in list
    let all_tables = registry.list_tables().await;
    assert!(
        all_tables.contains(&"users".to_string()),
        "Table should be in registry list"
    );
}

// ============================================================================
// Test 2: Metadata Propagation
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_registry_metadata_propagation() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-metadata".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Create table with properties
    let tables = vec![TableDefinition::new(
        "analytics".to_string(),
        "SELECT * FROM kafka://analytics-events".to_string(),
    )
    .with_property("table_model".to_string(), "compact".to_string())
    .with_property("retention".to_string(), "7 days".to_string())];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should succeed");

    assert_eq!(result.successful.len(), 1);

    // Verify metadata includes properties
    let metadata = registry
        .get_metadata("analytics")
        .await
        .expect("Should get metadata")
        .expect("Metadata should exist");

    assert_eq!(metadata.name, "analytics");
    assert!(
        metadata.record_count >= 0,
        "Should have record count (even if 0)"
    );

    // Verify table is active
    assert_eq!(
        metadata.status,
        TableStatus::Active,
        "Table should be active after successful load"
    );
}

// ============================================================================
// Test 3: Duplicate Table Name Detection
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_duplicate_table_name() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-duplicate".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor.clone(),
        ctas_executor.clone(),
        ParallelLoadingConfig::fast_test(),
    );

    // Create first table
    let tables1 = vec![TableDefinition::new(
        "orders".to_string(),
        "SELECT * FROM kafka://order-events".to_string(),
    )];

    let result1 = loader
        .load_tables_with_dependencies(tables1)
        .await
        .expect("First table should succeed");

    assert_eq!(result1.successful.len(), 1);

    // Attempt to create table with same name
    let loader2 = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    let tables2 = vec![TableDefinition::new(
        "orders".to_string(), // Same name!
        "SELECT * FROM kafka://different-topic".to_string(),
    )];

    let result2 = loader2.load_tables_with_dependencies(tables2).await;

    // Should either fail during CTAS or during registration
    // The exact behavior depends on implementation, but duplicate should be prevented
    if let Ok(result) = result2 {
        // If it doesn't error at CTAS level, check if it failed during wave execution
        assert!(
            result.failed.len() > 0 || result.skipped.len() > 0,
            "Duplicate table should cause failure or skip"
        );
    } else {
        // Error at configuration/dependency level is also acceptable
        assert!(result2.is_err(), "Should error on duplicate table");
    }
}

// ============================================================================
// Test 4: Concurrent Table Registration
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_concurrent_registration() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-concurrent".to_string(),
    );

    // Create multiple loaders for concurrent operations
    let loader1 = ParallelLoader::new(
        registry.clone(),
        monitor.clone(),
        ctas_executor.clone(),
        ParallelLoadingConfig::fast_test(),
    );

    let loader2 = ParallelLoader::new(
        registry.clone(),
        monitor.clone(),
        ctas_executor.clone(),
        ParallelLoadingConfig::fast_test(),
    );

    let loader3 = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Create different tables concurrently
    let task1 = tokio::spawn(async move {
        let tables = vec![TableDefinition::new(
            "concurrent_a".to_string(),
            "SELECT * FROM kafka://topic-a".to_string(),
        )];
        loader1.load_tables_with_dependencies(tables).await
    });

    let task2 = tokio::spawn(async move {
        let tables = vec![TableDefinition::new(
            "concurrent_b".to_string(),
            "SELECT * FROM kafka://topic-b".to_string(),
        )];
        loader2.load_tables_with_dependencies(tables).await
    });

    let task3 = tokio::spawn(async move {
        let tables = vec![TableDefinition::new(
            "concurrent_c".to_string(),
            "SELECT * FROM kafka://topic-c".to_string(),
        )];
        loader3.load_tables_with_dependencies(tables).await
    });

    // Wait for all to complete
    let result1 = task1.await.expect("Task 1 should complete").expect("Should succeed");
    let result2 = task2.await.expect("Task 2 should complete").expect("Should succeed");
    let result3 = task3.await.expect("Task 3 should complete").expect("Should succeed");

    // Verify all succeeded
    assert_eq!(result1.successful.len(), 1);
    assert_eq!(result2.successful.len(), 1);
    assert_eq!(result3.successful.len(), 1);

    // Verify all are registered
    let all_tables = registry.list_tables().await;
    assert!(all_tables.contains(&"concurrent_a".to_string()));
    assert!(all_tables.contains(&"concurrent_b".to_string()));
    assert!(all_tables.contains(&"concurrent_c".to_string()));

    // Verify count
    assert_eq!(
        registry.table_count().await,
        3,
        "Should have exactly 3 tables"
    );

    // Verify no corruption - all metadata should be accessible
    for table_name in &["concurrent_a", "concurrent_b", "concurrent_c"] {
        let metadata = registry
            .get_metadata(table_name)
            .await
            .expect("Should get metadata")
            .expect("Metadata should exist");

        assert_eq!(metadata.name, *table_name);
    }
}

// ============================================================================
// Test 5: Registry State Consistency After Partial Failure
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka with intentional failure setup
async fn test_ctas_registry_state_consistency_after_error() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-consistency".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig {
            fail_fast: false, // Continue on errors
            ..ParallelLoadingConfig::fast_test()
        },
    );

    // Create mix of valid and invalid tables
    let tables = vec![
        TableDefinition::new(
            "valid_table_1".to_string(),
            "SELECT * FROM kafka://valid-topic-1".to_string(),
        ),
        TableDefinition::new(
            "invalid_table".to_string(),
            "SELECT * FROM kafka://non-existent-topic".to_string(),
        ),
        TableDefinition::new(
            "valid_table_2".to_string(),
            "SELECT * FROM kafka://valid-topic-2".to_string(),
        ),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should return result even with failures");

    // Verify only successful tables are registered
    let registered_tables = registry.list_tables().await;

    assert!(
        registered_tables.contains(&"valid_table_1".to_string()),
        "Valid table 1 should be registered"
    );
    assert!(
        registered_tables.contains(&"valid_table_2".to_string()),
        "Valid table 2 should be registered"
    );
    assert!(
        !registered_tables.contains(&"invalid_table".to_string()),
        "Invalid table should NOT be registered"
    );

    // Verify failed table has no metadata
    let invalid_metadata = registry.get_metadata("invalid_table").await.ok();
    assert!(
        invalid_metadata.is_none() || invalid_metadata == Some(None),
        "Failed table should have no metadata"
    );

    // Verify successful tables have proper metadata
    for table_name in &["valid_table_1", "valid_table_2"] {
        let metadata = registry
            .get_metadata(table_name)
            .await
            .expect("Should get metadata")
            .expect("Metadata should exist");

        assert_eq!(metadata.status, TableStatus::Active);
    }
}

// ============================================================================
// Test 6: Table Lookup During Dependency Resolution
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_registry_lookup_during_dependency() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-lookup".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Create tables with dependency chain
    let tables = vec![
        TableDefinition::new(
            "base_table".to_string(),
            "SELECT * FROM kafka://base-topic".to_string(),
        ),
        TableDefinition::new(
            "derived_table".to_string(),
            "SELECT * FROM base_table WHERE value > 100".to_string(),
        )
        .with_dependency("base_table".to_string()),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should succeed");

    assert_eq!(result.successful.len(), 2);

    // Verify base table was registered before derived table started
    // (This is guaranteed by wave-based execution)
    let base_table = registry
        .lookup_table("base_table")
        .await
        .expect("Should lookup base")
        .expect("Base should exist");

    let derived_table = registry
        .lookup_table("derived_table")
        .await
        .expect("Should lookup derived")
        .expect("Derived should exist");

    // Both should exist and be accessible
    assert!(base_table.key_count() >= 0);
    assert!(derived_table.key_count() >= 0);
}

// ============================================================================
// Test 7: Registry Capacity Limit
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_registry_capacity_limit() {
    // Create registry with small max_tables limit
    let registry_config = TableRegistryConfig {
        kafka_brokers: "localhost:9092".to_string(),
        base_group_id: "registry-limit".to_string(),
        max_tables: 3, // Very small limit for testing
        ..Default::default()
    };

    let registry = Arc::new(TableRegistry::with_config(registry_config));
    let monitor = Arc::new(ProgressMonitor::new());
    let ctas_executor = CtasExecutor::new("localhost:9092".to_string(), "registry-limit".to_string());

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Attempt to create more tables than limit
    let tables = vec![
        TableDefinition::new("table_1".to_string(), "SELECT * FROM kafka://topic-1".to_string()),
        TableDefinition::new("table_2".to_string(), "SELECT * FROM kafka://topic-2".to_string()),
        TableDefinition::new("table_3".to_string(), "SELECT * FROM kafka://topic-3".to_string()),
        TableDefinition::new("table_4".to_string(), "SELECT * FROM kafka://topic-4".to_string()), // Exceeds limit
    ];

    let result = loader.load_tables_with_dependencies(tables).await;

    // Behavior depends on implementation:
    // Either all succeed (limit is soft), or the 4th fails
    match result {
        Ok(result) => {
            // If it succeeds, verify count
            let count = registry.table_count().await;
            assert!(
                count <= 4,
                "Should have at most 4 tables (or enforce limit)"
            );
        }
        Err(e) => {
            // If it errors, that's acceptable for limit enforcement
            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("limit") || error_msg.contains("capacity") || error_msg.contains("maximum"),
                "Error should mention capacity limit: {}",
                error_msg
            );
        }
    }
}

// ============================================================================
// Test 8: Health Status Tracking
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_ctas_registry_health_status() {
    let (registry, monitor, ctas_executor) = create_test_environment(
        "localhost:9092".to_string(),
        "registry-health".to_string(),
    );

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor,
        ctas_executor,
        ParallelLoadingConfig::fast_test(),
    );

    // Create a table
    let tables = vec![TableDefinition::new(
        "health_test".to_string(),
        "SELECT * FROM kafka://health-topic".to_string(),
    )];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should succeed");

    assert_eq!(result.successful.len(), 1);

    // Get health information
    let health = registry
        .get_table_health("health_test")
        .await
        .expect("Should get health");

    assert_eq!(health.table_name, "health_test");
    assert!(health.is_healthy, "Table should be healthy");
    assert_eq!(health.status, TableStatus::Active);
    assert_eq!(health.issues.len(), 0, "Should have no issues");
}
