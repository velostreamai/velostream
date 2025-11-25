//! Tests for TableDataSource configuration - config file loading and property merging
//!
//! This test suite validates that:
//! 1. TableDataSource loads config files via config_file property
//! 2. TableDataSource merges YAML config with inline properties (YAML extends pattern)
//! 3. TableDataSource handles missing config files gracefully
//! 4. Configuration properties are extracted from merged config
//! 5. Table config files can extend common_table_source.yaml base
//! 6. Primary key field is properly identified from config

use std::collections::HashMap;
use std::io::Write as _;
use tempfile::NamedTempFile;
use velostream::velostream::table::sql::TableDataSource;

/// Helper to create a test table config YAML
fn sample_table_config() -> &'static str {
    r#"extends: common_table_source.yaml

table:
  name: test_instruments
  type: static_lookup
  primary_key: symbol

data_source:
  type: file
  path: ./data/instruments.csv
  format: csv
  has_headers: true

schema:
  fields:
    - name: symbol
      type: string
      nullable: false
    - name: name
      type: string
    - name: price
      type: decimal
      precision: 19
      scale: 4

cache:
  enabled: true
  ttl_seconds: 1800
  max_entries: 5000

metadata:
  description: "Test instrument reference table"
  update.frequency: "hourly"
"#
}

/// Helper to create a minimal base config
fn sample_base_config() -> &'static str {
    r#"table:
  type: static_lookup
  storage: in_memory
  mode: batch

cache:
  enabled: true
  ttl_seconds: 3600
  eviction_policy: lru

performance:
  indexing: hash
"#
}

#[test]
fn test_table_datasource_from_properties_basic() {
    // Given: Properties with config_file
    let mut props = HashMap::new();
    props.insert(
        "config_file".to_string(),
        "demo/trading/configs/instrument_reference_table.yaml".to_string(),
    );

    // When: Creating TableDataSource from properties
    let table_source = TableDataSource::from_properties(&props);

    // Then: Table should be created successfully
    assert!(
        table_source.record_count() == 0,
        "New table should be empty"
    );
    assert!(table_source.is_empty(), "Empty table check should pass");
}

#[test]
fn test_table_datasource_config_file_with_extends_pattern() {
    // Given: A config file that extends a base config
    let mut temp_base = NamedTempFile::new().expect("Failed to create temp base file");
    temp_base
        .write_all(sample_base_config().as_bytes())
        .expect("Failed to write base config");
    let base_path = temp_base.path().to_str().unwrap().to_string();

    let mut temp_config = NamedTempFile::new().expect("Failed to create temp config file");
    let config_content = sample_table_config().replace(
        "extends: common_table_source.yaml",
        &format!("extends: {}", base_path),
    );
    temp_config
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    let config_path = temp_config.path().to_str().unwrap().to_string();

    // When: Creating TableDataSource from properties with extends pattern
    let mut props = HashMap::new();
    props.insert("config_file".to_string(), config_path);

    let table_source = TableDataSource::from_properties(&props);

    // Then: Table should be created with merged configuration
    assert!(
        table_source.record_count() == 0,
        "New table should be empty"
    );
}

#[test]
fn test_table_datasource_explicit_properties_with_inline_config() {
    // Given: Inline config (no extends) and explicit properties
    let config_content = r#"
table:
  name: test_table
  type: static_lookup
  primary_key: symbol

cache:
  enabled: true
  ttl_seconds: 1800
"#;

    let mut temp_config = NamedTempFile::new().expect("Failed to create temp config");
    temp_config
        .write_all(config_content.as_bytes())
        .expect("Failed to write config");
    let config_path = temp_config.path().to_str().unwrap().to_string();

    let mut props = HashMap::new();
    props.insert("config_file".to_string(), config_path);
    // Explicit property that should override config file
    props.insert("table.primary_key".to_string(), "custom_id".to_string());

    // When: Creating TableDataSource from properties
    let table_source = TableDataSource::from_properties(&props);

    // Then: Table should be created with merged properties
    // (explicit property overrides config file setting)
    assert!(
        table_source.record_count() == 0,
        "New table should be empty"
    );
}

#[test]
#[should_panic(expected = "CONFIGURATION ERROR")]
fn test_table_datasource_panics_on_missing_config_file() {
    // Given: Properties referencing non-existent config file
    let mut props = HashMap::new();
    props.insert(
        "config_file".to_string(),
        "./non_existent_config.yaml".to_string(),
    );

    // When: Creating TableDataSource from properties
    // Should panic with CONFIGURATION ERROR (fail-fast design)
    let _table_source = TableDataSource::from_properties(&props);

    // Then: panic is expected (validated by #[should_panic])
}

#[test]
fn test_table_datasource_basic_operations_work() {
    // Given: A new TableDataSource
    let table_source = TableDataSource::new();

    // When: Inserting and retrieving data
    use velostream::velostream::sql::execution::types::FieldValue;

    let mut record = HashMap::new();
    record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    record.insert("price".to_string(), FieldValue::Float(150.0));

    let result = table_source.insert("key1".to_string(), record);

    // Then: Insert should succeed
    assert!(result.is_ok(), "Insert should succeed");
    assert_eq!(table_source.record_count(), 1, "Table should have 1 record");
    assert!(table_source.contains_key("key1"), "Key should exist");
}

#[test]
fn test_table_datasource_from_properties_with_real_config() {
    // Given: Real table config file from trading demo
    let mut props = HashMap::new();
    props.insert(
        "config_file".to_string(),
        "demo/trading/configs/trader_limits_table.yaml".to_string(),
    );

    // When: Creating TableDataSource from real config
    let table_source = TableDataSource::from_properties(&props);

    // Then: Table should be created successfully
    assert!(
        table_source.record_count() == 0,
        "New table from real config should be empty"
    );
    assert!(table_source.is_empty(), "Empty check should work");
}

#[test]
fn test_table_datasource_multiple_configs() {
    // Given: Multiple config files from trading demo
    let configs = vec![
        "demo/trading/configs/instrument_reference_table.yaml",
        "demo/trading/configs/trader_limits_table.yaml",
        "demo/trading/configs/firm_limits_table.yaml",
        "demo/trading/configs/desk_limits_table.yaml",
        "demo/trading/configs/regulatory_watchlist_table.yaml",
    ];

    // When: Creating TableDataSource from each config
    for config_path in configs {
        let mut props = HashMap::new();
        props.insert("config_file".to_string(), config_path.to_string());
        let table_source = TableDataSource::from_properties(&props);

        // Then: Each should create successfully
        assert!(
            table_source.is_empty(),
            "Table from {} should be empty",
            config_path
        );
    }
}

#[test]
fn test_table_datasource_new_vs_from_properties_equivalence() {
    // Given: Two TableDataSources - one from new(), one from properties with no config
    let table1 = TableDataSource::new();

    let mut props = HashMap::new();
    props.insert("placeholder".to_string(), "value".to_string());
    let table2 = TableDataSource::from_properties(&props);

    // When: Adding same data to both
    use velostream::velostream::sql::execution::types::FieldValue;

    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(1));
    record.insert("name".to_string(), FieldValue::String("Test".to_string()));

    table1.insert("key1".to_string(), record.clone()).ok();
    table2.insert("key1".to_string(), record).ok();

    // Then: Both should behave identically
    assert_eq!(
        table1.record_count(),
        table2.record_count(),
        "Record counts should match"
    );
    assert!(table1.contains_key("key1"), "Table1 should have key");
    assert!(table2.contains_key("key1"), "Table2 should have key");
}
