//! Tests for ParallelLoader
//!
//! Comprehensive test suite for parallel table loading with dependency management.

use std::sync::Arc;
use velostream::velostream::server::parallel_loader::{
    ParallelLoader, ParallelLoadingConfig, TableDefinition,
};
use velostream::velostream::server::progress_monitoring::ProgressMonitor;
use velostream::velostream::server::table_registry::TableRegistry;
use velostream::velostream::table::CtasExecutor;

/// Helper function to create a test ParallelLoader with mock CTAS executor
fn create_test_loader(
    registry: Arc<TableRegistry>,
    monitor: Arc<ProgressMonitor>,
    config: ParallelLoadingConfig,
) -> ParallelLoader {
    // Use registry's config to create CTAS executor
    let ctas_executor = CtasExecutor::new(
        registry.config().kafka_brokers.clone(),
        registry.config().base_group_id.clone(),
    );

    ParallelLoader::new(registry, monitor, ctas_executor, config)
}

#[tokio::test]
async fn test_parallel_loader_creation() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::default();

    let _loader = create_test_loader(registry, monitor, config);
}

#[tokio::test]
async fn test_empty_table_list() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    let tables: Vec<TableDefinition> = vec![];
    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    assert_eq!(result.successful.len(), 0);
    assert_eq!(result.failed.len(), 0);
    assert_eq!(result.skipped.len(), 0);
    assert_eq!(result.wave_stats.len(), 0);
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_single_table_no_dependencies() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    let tables = vec![TableDefinition::new(
        "table_a".to_string(),
        "SELECT * FROM source".to_string(),
    )];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    assert_eq!(result.successful.len(), 1);
    assert!(result.successful.contains(&"table_a".to_string()));
    assert_eq!(result.failed.len(), 0);
    assert_eq!(result.wave_stats.len(), 1);
    assert_eq!(result.success_rate(), 100.0);
    assert!(result.is_complete_success());
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_multiple_independent_tables() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::with_max_parallel(2);

    let loader = create_test_loader(registry, monitor, config);

    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM source_a".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM source_b".to_string()),
        TableDefinition::new("table_c".to_string(), "SELECT * FROM source_c".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // All tables should succeed
    assert_eq!(result.successful.len(), 3);
    assert!(result.successful.contains(&"table_a".to_string()));
    assert!(result.successful.contains(&"table_b".to_string()));
    assert!(result.successful.contains(&"table_c".to_string()));

    // Should be loaded in 1 wave (all independent)
    assert_eq!(result.wave_stats.len(), 1);
    assert_eq!(result.wave_stats[0].tables.len(), 3);
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_simple_dependency_chain() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    // B depends on A
    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM source".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // Both should succeed
    assert_eq!(result.successful.len(), 2);

    // Should be loaded in 2 waves
    assert_eq!(result.wave_stats.len(), 2);
    assert_eq!(result.wave_stats[0].tables, vec!["table_a"]);
    assert_eq!(result.wave_stats[1].tables, vec!["table_b"]);
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_parallel_loading_within_wave() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::with_max_parallel(2);

    let loader = create_test_loader(registry, monitor, config);

    // A and B both depend on C (can load in parallel after C)
    let tables = vec![
        TableDefinition::new("table_c".to_string(), "SELECT * FROM source".to_string()),
        TableDefinition::new("table_a".to_string(), "SELECT * FROM table_c".to_string())
            .with_dependency("table_c".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_c".to_string())
            .with_dependency("table_c".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    assert_eq!(result.successful.len(), 3);
    assert_eq!(result.wave_stats.len(), 2);

    // Wave 1: table_c
    assert_eq!(result.wave_stats[0].tables, vec!["table_c"]);

    // Wave 2: table_a and table_b (in parallel)
    assert_eq!(result.wave_stats[1].tables.len(), 2);
    assert!(result.wave_stats[1].tables.contains(&"table_a".to_string()));
    assert!(result.wave_stats[1].tables.contains(&"table_b".to_string()));
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_diamond_dependency() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    //     A
    //    / \
    //   B   C
    //    \ /
    //     D
    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM source".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
        TableDefinition::new("table_c".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
        TableDefinition::new(
            "table_d".to_string(),
            "SELECT * FROM table_b JOIN table_c".to_string(),
        )
        .with_dependencies(vec!["table_b".to_string(), "table_c".to_string()]),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    assert_eq!(result.successful.len(), 4);
    assert_eq!(result.wave_stats.len(), 3);

    // Wave 1: A
    assert_eq!(result.wave_stats[0].tables, vec!["table_a"]);

    // Wave 2: B and C in parallel
    assert_eq!(result.wave_stats[1].tables.len(), 2);

    // Wave 3: D
    assert_eq!(result.wave_stats[2].tables, vec!["table_d"]);
}

#[tokio::test]
async fn test_circular_dependency_detection() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    // A -> B -> C -> A (cycle)
    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM table_c".to_string())
            .with_dependency("table_c".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
        TableDefinition::new("table_c".to_string(), "SELECT * FROM table_b".to_string())
            .with_dependency("table_b".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await;

    // Should fail with configuration error
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Circular dependency") || error_msg.contains("cycle"));
}

#[tokio::test]
async fn test_missing_dependency_detection() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    // table_a depends on table_b which doesn't exist
    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM table_b".to_string())
            .with_dependency("table_b".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await;

    // Should fail with validation error
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("table_b") || error_msg.contains("Missing"));
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_result_statistics() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM source".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM source".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // Test result methods
    assert_eq!(result.total_tables(), 2);
    assert_eq!(result.success_rate(), 100.0);
    assert!(result.is_complete_success());
    assert!(result.total_duration.as_secs() < 60); // Should be fast
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_wave_statistics() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    let tables = vec![
        TableDefinition::new("table_a".to_string(), "SELECT * FROM source".to_string()),
        TableDefinition::new("table_b".to_string(), "SELECT * FROM table_a".to_string())
            .with_dependency("table_a".to_string()),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    // Check wave stats
    assert_eq!(result.wave_stats.len(), 2);

    let wave1 = &result.wave_stats[0];
    assert_eq!(wave1.wave_number, 1);
    assert_eq!(wave1.successful, 1);
    assert_eq!(wave1.failed, 0);

    let wave2 = &result.wave_stats[1];
    assert_eq!(wave2.wave_number, 2);
    assert_eq!(wave2.successful, 1);
    assert_eq!(wave2.failed, 0);
}

#[tokio::test]
async fn test_table_definition_builder() {
    let table_def = TableDefinition::new("test".to_string(), "SELECT *".to_string())
        .with_dependency("dep1".to_string())
        .with_dependency("dep2".to_string())
        .with_property("key1".to_string(), "value1".to_string());

    assert_eq!(table_def.name, "test");
    assert_eq!(table_def.dependencies.len(), 2);
    assert!(table_def.dependencies.contains("dep1"));
    assert!(table_def.dependencies.contains("dep2"));
    assert_eq!(table_def.properties.get("key1").unwrap(), "value1");
}

#[tokio::test]
async fn test_config_defaults() {
    let config = ParallelLoadingConfig::default();
    assert_eq!(config.max_parallel, 4);
    assert!(!config.fail_fast);
    assert!(!config.continue_on_dependency_failure);

    let fast_config = ParallelLoadingConfig::fast_test();
    assert_eq!(fast_config.max_parallel, 2);

    let custom_config = ParallelLoadingConfig::with_max_parallel(8);
    assert_eq!(custom_config.max_parallel, 8);
}

#[tokio::test]
#[ignore] // Requires Kafka - see integration tests
async fn test_complex_multi_wave_scenario() {
    let registry = Arc::new(TableRegistry::new());
    let monitor = Arc::new(ProgressMonitor::new());
    let config = ParallelLoadingConfig::fast_test();

    let loader = create_test_loader(registry, monitor, config);

    // Complex scenario: multiple independent sources, enriched tables, and aggregations
    let tables = vec![
        // Wave 1: Raw sources
        TableDefinition::new(
            "raw_events".to_string(),
            "SELECT * FROM kafka_events".to_string(),
        ),
        TableDefinition::new(
            "raw_users".to_string(),
            "SELECT * FROM kafka_users".to_string(),
        ),
        TableDefinition::new(
            "raw_products".to_string(),
            "SELECT * FROM kafka_products".to_string(),
        ),
        // Wave 2: Enriched tables
        TableDefinition::new(
            "enriched_events".to_string(),
            "SELECT * FROM raw_events".to_string(),
        )
        .with_dependency("raw_events".to_string()),
        TableDefinition::new(
            "user_profiles".to_string(),
            "SELECT * FROM raw_users".to_string(),
        )
        .with_dependency("raw_users".to_string()),
        // Wave 3: Aggregations
        TableDefinition::new(
            "user_activity".to_string(),
            "SELECT * FROM enriched_events JOIN user_profiles".to_string(),
        )
        .with_dependencies(vec![
            "enriched_events".to_string(),
            "user_profiles".to_string(),
        ]),
    ];

    let result = loader.load_tables_with_dependencies(tables).await.unwrap();

    assert_eq!(result.successful.len(), 6);
    assert_eq!(result.wave_stats.len(), 3);

    // Wave 1: 3 raw sources
    assert_eq!(result.wave_stats[0].tables.len(), 3);

    // Wave 2: 2 enriched tables
    assert_eq!(result.wave_stats[1].tables.len(), 2);

    // Wave 3: 1 aggregation
    assert_eq!(result.wave_stats[2].tables.len(), 1);
}
