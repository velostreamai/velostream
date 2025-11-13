//! ParallelLoader + CTAS Integration Tests
//!
//! Critical integration tests for Phase 4 parallel table loading with real CTAS execution.
//! These tests validate the end-to-end functionality of dependency-aware parallel table creation.
//!
//! ## Test Coverage
//!
//! 1. **Dependency Resolution**: Automatic extraction from SQL, wave-based execution
//! 2. **Error Handling**: Partial failures, circular dependencies, missing tables
//! 3. **Concurrency**: Semaphore limits, parallel execution, race conditions
//! 4. **TableRegistry Integration**: Registration, lookup, consistency
//! 5. **Production Scenarios**: Timeouts, progress tracking, complex dependencies

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use velostream::velostream::server::parallel_loader::{
    ParallelLoader, ParallelLoadingConfig, TableDefinition,
};
use velostream::velostream::server::progress_monitoring::ProgressMonitor;
use velostream::velostream::server::table_registry::{TableRegistry, TableRegistryConfig};
use velostream::velostream::table::CtasExecutor;

/// Helper to create a test ParallelLoader with proper configuration
fn create_test_loader(
    kafka_brokers: String,
    group_id: String,
    config: ParallelLoadingConfig,
) -> (ParallelLoader, Arc<TableRegistry>, Arc<ProgressMonitor>) {
    let registry_config = TableRegistryConfig {
        kafka_brokers: kafka_brokers.clone(),
        base_group_id: group_id.clone(),
        ..Default::default()
    };

    let registry = Arc::new(TableRegistry::with_config(registry_config));
    let monitor = Arc::new(ProgressMonitor::new());
    let ctas_executor = CtasExecutor::new(kafka_brokers, group_id);

    let loader = ParallelLoader::new(
        registry.clone(),
        monitor.clone(),
        ctas_executor,
        config,
    );

    (loader, registry, monitor)
}

// ============================================================================
// Test 1: Basic Parallel CTAS with Dependencies
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka - run with: cargo test --ignored
async fn test_parallel_ctas_simple_dependency_chain() {
    // Setup
    let (loader, registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-test".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    // Create tables with simple dependency chain: A <- B <- C
    let tables = vec![
        TableDefinition::new(
            "test_table_a".to_string(),
            "SELECT * FROM kafka://test-topic-a".to_string(),
        ),
        TableDefinition::new(
            "test_table_b".to_string(),
            "SELECT * FROM test_table_a WHERE value > 0".to_string(),
        )
        .with_dependency("test_table_a".to_string()),
        TableDefinition::new(
            "test_table_c".to_string(),
            "SELECT * FROM test_table_b WHERE status = 'active'".to_string(),
        )
        .with_dependency("test_table_b".to_string()),
    ];

    // Execute parallel loading
    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Parallel loading should succeed");

    // Verify results
    assert_eq!(
        result.successful.len(),
        3,
        "All 3 tables should load successfully"
    );
    assert!(result.successful.contains(&"test_table_a".to_string()));
    assert!(result.successful.contains(&"test_table_b".to_string()));
    assert!(result.successful.contains(&"test_table_c".to_string()));

    assert_eq!(result.failed.len(), 0, "No tables should fail");
    assert_eq!(result.skipped.len(), 0, "No tables should be skipped");

    // Verify wave structure (3 sequential waves)
    assert_eq!(
        result.wave_stats.len(),
        3,
        "Should have 3 sequential waves"
    );
    assert_eq!(result.wave_stats[0].tables, vec!["test_table_a"]);
    assert_eq!(result.wave_stats[1].tables, vec!["test_table_b"]);
    assert_eq!(result.wave_stats[2].tables, vec!["test_table_c"]);

    // Verify tables are registered
    let table_a = registry
        .lookup_table("test_table_a")
        .await
        .expect("Table A should be registered");
    assert!(table_a.is_some(), "Table A should exist in registry");

    let table_b = registry
        .lookup_table("test_table_b")
        .await
        .expect("Table B should be registered");
    assert!(table_b.is_some(), "Table B should exist in registry");

    let table_c = registry
        .lookup_table("test_table_c")
        .await
        .expect("Table C should be registered");
    assert!(table_c.is_some(), "Table C should exist in registry");
}

// ============================================================================
// Test 2: Automatic Dependency Extraction from SQL
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_parallel_ctas_automatic_dependency_extraction() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-auto-dep".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    // Create tables WITHOUT explicit dependencies
    // Dependencies should be auto-extracted from SQL
    let tables = vec![
        TableDefinition::new(
            "raw_data".to_string(),
            "SELECT * FROM kafka://source-topic".to_string(),
        ),
        // NO .with_dependency() call - should auto-detect from SQL!
        TableDefinition::new(
            "filtered_data".to_string(),
            "SELECT * FROM raw_data WHERE amount > 100".to_string(),
        ),
        TableDefinition::new(
            "aggregated_data".to_string(),
            "SELECT category, SUM(amount) FROM filtered_data GROUP BY category".to_string(),
        ),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should auto-extract dependencies");

    // Verify correct execution order
    assert_eq!(result.successful.len(), 3);
    assert_eq!(
        result.wave_stats.len(),
        3,
        "Should create 3 waves from auto-detected dependencies"
    );

    // Wave 1: raw_data (no dependencies)
    assert_eq!(result.wave_stats[0].tables, vec!["raw_data"]);

    // Wave 2: filtered_data (depends on raw_data - auto-detected!)
    assert_eq!(result.wave_stats[1].tables, vec!["filtered_data"]);

    // Wave 3: aggregated_data (depends on filtered_data - auto-detected!)
    assert_eq!(result.wave_stats[2].tables, vec!["aggregated_data"]);
}

// ============================================================================
// Test 3: Diamond Dependency Pattern
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_parallel_ctas_diamond_dependency() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-diamond".to_string(),
        ParallelLoadingConfig::with_max_parallel(2), // Allow parallel execution
    );

    // Diamond pattern:
    //       A
    //      / \
    //     B   C
    //      \ /
    //       D

    let tables = vec![
        TableDefinition::new(
            "source_a".to_string(),
            "SELECT * FROM kafka://events".to_string(),
        ),
        TableDefinition::new(
            "branch_b".to_string(),
            "SELECT * FROM source_a WHERE type = 'purchase'".to_string(),
        )
        .with_dependency("source_a".to_string()),
        TableDefinition::new(
            "branch_c".to_string(),
            "SELECT * FROM source_a WHERE type = 'view'".to_string(),
        )
        .with_dependency("source_a".to_string()),
        TableDefinition::new(
            "merged_d".to_string(),
            "SELECT b.*, c.* FROM branch_b b JOIN branch_c c ON b.user_id = c.user_id"
                .to_string(),
        )
        .with_dependencies(vec!["branch_b".to_string(), "branch_c".to_string()]),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Diamond pattern should work");

    assert_eq!(result.successful.len(), 4);
    assert_eq!(
        result.wave_stats.len(),
        3,
        "Should have 3 waves for diamond"
    );

    // Wave 1: source_a
    assert_eq!(result.wave_stats[0].tables, vec!["source_a"]);

    // Wave 2: branch_b and branch_c (parallel execution!)
    assert_eq!(
        result.wave_stats[1].tables.len(),
        2,
        "B and C should run in parallel"
    );
    assert!(result.wave_stats[1].tables.contains(&"branch_b".to_string()));
    assert!(result.wave_stats[1].tables.contains(&"branch_c".to_string()));

    // Wave 3: merged_d
    assert_eq!(result.wave_stats[2].tables, vec!["merged_d"]);

    // Verify success
    assert!(result.is_complete_success());
    assert_eq!(result.success_rate(), 100.0);
}

// ============================================================================
// Test 4: Circular Dependency Detection
// ============================================================================

#[tokio::test]
async fn test_parallel_ctas_circular_dependency_detection() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-circular".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    // Create circular dependency: A -> B -> C -> A
    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM table_c".to_string())
            .with_dependency("table_c".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
        TableDefinition::new("table_c".to_string(), "SELECT * FROM table_b".to_string())
            .with_dependency("table_b".to_string()),
    ];

    // Should fail with circular dependency error
    let result = loader.load_tables_with_dependencies(tables).await;

    assert!(
        result.is_err(),
        "Circular dependency should cause error"
    );

    let error = result.unwrap_err();
    let error_msg = error.to_string().to_lowercase();

    assert!(
        error_msg.contains("circular") || error_msg.contains("cycle"),
        "Error should mention circular dependency: {}",
        error_msg
    );
}

// ============================================================================
// Test 5: Missing Dependency Detection
// ============================================================================

#[tokio::test]
async fn test_parallel_ctas_missing_dependency_detection() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-missing".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    // Table B depends on non-existent Table A
    let tables = vec![TableDefinition::new(
        "table_b".to_string(),
        "SELECT * FROM table_a".to_string(),
    )
    .with_dependency("table_a".to_string())];

    // Should fail with missing dependency error
    let result = loader.load_tables_with_dependencies(tables).await;

    assert!(result.is_err(), "Missing dependency should cause error");

    let error = result.unwrap_err();
    let error_msg = error.to_string().to_lowercase();

    assert!(
        error_msg.contains("missing") || error_msg.contains("table_a"),
        "Error should mention missing dependency: {}",
        error_msg
    );
}

// ============================================================================
// Test 6: Partial Wave Failure Handling
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka with intentional failure setup
async fn test_parallel_ctas_partial_wave_failure() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-partial-fail".to_string(),
        ParallelLoadingConfig {
            max_parallel: 2,
            fail_fast: false, // Don't stop on first failure
            table_load_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    );

    // Wave 1: A (success), B (will fail - bad topic), C (success)
    // Wave 2: D (depends on B - should be skipped or fail)
    let tables = vec![
        TableDefinition::new(
            "success_a".to_string(),
            "SELECT * FROM kafka://valid-topic-a".to_string(),
        ),
        TableDefinition::new(
            "fail_b".to_string(),
            "SELECT * FROM kafka://non-existent-topic".to_string(),
        ),
        TableDefinition::new(
            "success_c".to_string(),
            "SELECT * FROM kafka://valid-topic-c".to_string(),
        ),
        TableDefinition::new(
            "dependent_d".to_string(),
            "SELECT * FROM fail_b WHERE value > 0".to_string(),
        )
        .with_dependency("fail_b".to_string()),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should return result even with partial failures");

    // Verify partial success
    assert!(
        result.successful.len() >= 2,
        "At least A and C should succeed"
    );
    assert!(result.successful.contains(&"success_a".to_string()));
    assert!(result.successful.contains(&"success_c".to_string()));

    // Verify failures
    assert!(
        result.failed.len() > 0 || result.skipped.len() > 0,
        "Should have failures or skipped tables"
    );

    // Dependent table should fail or be skipped
    let dependent_failed = result.failed.contains_key("dependent_d");
    let dependent_skipped = result.skipped.contains(&"dependent_d".to_string());

    assert!(
        dependent_failed || dependent_skipped,
        "Dependent table D should fail or be skipped when B fails"
    );

    assert!(!result.is_complete_success(), "Should not be complete success");
    assert!(result.success_rate() < 100.0, "Success rate should be < 100%");
}

// ============================================================================
// Test 7: Semaphore Concurrency Limit
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_parallel_ctas_semaphore_limit() {
    let (loader, _registry, monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-semaphore".to_string(),
        ParallelLoadingConfig {
            max_parallel: 2, // Limit to 2 concurrent tables
            table_load_timeout: Duration::from_secs(30),
            ..Default::default()
        },
    );

    // Create 6 independent tables (should execute in 3 waves of 2)
    let tables = vec![
        TableDefinition::new("table_1".to_string(), "SELECT * FROM kafka://topic-1".to_string()),
        TableDefinition::new("table_2".to_string(), "SELECT * FROM kafka://topic-2".to_string()),
        TableDefinition::new("table_3".to_string(), "SELECT * FROM kafka://topic-3".to_string()),
        TableDefinition::new("table_4".to_string(), "SELECT * FROM kafka://topic-4".to_string()),
        TableDefinition::new("table_5".to_string(), "SELECT * FROM kafka://topic-5".to_string()),
        TableDefinition::new("table_6".to_string(), "SELECT * FROM kafka://topic-6".to_string()),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should succeed");

    assert_eq!(result.successful.len(), 6, "All tables should succeed");

    // Should have 1 wave (all independent), but only 2 tables execute concurrently
    // We can't directly verify concurrency, but we can check wave structure
    assert_eq!(result.wave_stats.len(), 1, "Should have 1 wave (all independent)");
    assert_eq!(result.wave_stats[0].tables.len(), 6, "Wave should have all 6 tables");

    // Verify progress monitoring tracked all tables
    let summary = monitor.get_summary().await;
    assert_eq!(summary.completed, 6, "All 6 tables should be completed");
}

// ============================================================================
// Test 8: Progress Monitoring Integration
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_parallel_ctas_progress_monitoring() {
    let (loader, _registry, monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-progress".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    let tables = vec![
        TableDefinition::new("progress_a".to_string(), "SELECT * FROM kafka://topic-a".to_string()),
        TableDefinition::new(
            "progress_b".to_string(),
            "SELECT * FROM progress_a WHERE value > 0".to_string(),
        )
        .with_dependency("progress_a".to_string()),
    ];

    // Subscribe to progress updates
    let mut progress_rx = monitor.subscribe_to_progress();

    // Start loading
    let loader_handle = tokio::spawn(async move {
        loader.load_tables_with_dependencies(tables).await
    });

    // Monitor progress updates
    let mut updates_received = 0;
    let mut tables_seen = std::collections::HashSet::new();

    // Receive updates with timeout
    while updates_received < 10 {
        match tokio::time::timeout(Duration::from_secs(5), progress_rx.recv()).await {
            Ok(Ok(progress_map)) => {
                for (table_name, _progress) in progress_map {
                    tables_seen.insert(table_name);
                }
                updates_received += 1;
            }
            Ok(Err(_)) => break, // Channel closed
            Err(_) => break,     // Timeout
        }
    }

    // Wait for loading to complete
    let result = loader_handle
        .await
        .expect("Task should complete")
        .expect("Loading should succeed");

    // Verify we received progress updates for both tables
    assert!(
        tables_seen.contains("progress_a") || tables_seen.contains("progress_b"),
        "Should have received progress updates for tables"
    );

    // Verify final summary
    let summary = monitor.get_summary().await;
    assert_eq!(summary.completed, 2, "Both tables should be completed");

    assert_eq!(result.successful.len(), 2);
}

// ============================================================================
// Test 9: CTAS with Properties
// ============================================================================

#[tokio::test]
#[ignore] // Requires Kafka
async fn test_parallel_ctas_with_properties() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-props".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    // Create tables with different configurations
    let tables = vec![
        TableDefinition::new(
            "compact_table".to_string(),
            "SELECT * FROM kafka://large-topic".to_string(),
        )
        .with_property("table_model".to_string(), "compact".to_string())
        .with_property("retention".to_string(), "30 days".to_string()),
        TableDefinition::new(
            "normal_table".to_string(),
            "SELECT * FROM kafka://small-topic".to_string(),
        )
        .with_property("table_model".to_string(), "normal".to_string())
        .with_property("kafka.batch.size".to_string(), "1000".to_string()),
    ];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Should create tables with properties");

    assert_eq!(result.successful.len(), 2);
    assert!(result.successful.contains(&"compact_table".to_string()));
    assert!(result.successful.contains(&"normal_table".to_string()));
}

// ============================================================================
// Test 10: Empty Table List
// ============================================================================

#[tokio::test]
async fn test_parallel_ctas_empty_table_list() {
    let (loader, _registry, _monitor) = create_test_loader(
        "localhost:9092".to_string(),
        "parallel-empty".to_string(),
        ParallelLoadingConfig::fast_test(),
    );

    let tables: Vec<TableDefinition> = vec![];

    let result = loader
        .load_tables_with_dependencies(tables)
        .await
        .expect("Empty list should succeed");

    assert_eq!(result.successful.len(), 0);
    assert_eq!(result.failed.len(), 0);
    assert_eq!(result.skipped.len(), 0);
    assert_eq!(result.wave_stats.len(), 0);
    assert!(result.is_complete_success());
}
