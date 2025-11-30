//! Data generation tests for the SQL Application Test Harness
//!
//! Tests schema-driven test data generation:
//! - Basic schema parsing and generation
//! - All supported field types
//! - Constraints (range, enum, length)

#![cfg(feature = "test-support")]

use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::test_harness::generator::SchemaDataGenerator;
use velostream::velostream::test_harness::schema::Schema;

/// Test schema data generation with basic market data schema
#[test]
fn test_schema_data_generation() {
    let schema_yaml = r#"
name: market_data
description: Market data events for testing
fields:
  - name: symbol
    type: string
    constraints:
      enum_values:
        values: [AAPL, GOOGL, MSFT]
  - name: price
    type: float
    constraints:
      range:
        min: 100.0
        max: 500.0
  - name: volume
    type: integer
    constraints:
      range:
        min: 100
        max: 10000
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    assert_eq!(schema.name, "market_data");
    assert_eq!(schema.fields.len(), 3);

    let mut generator = SchemaDataGenerator::new(Some(42)); // Fixed seed for reproducibility
    let records = generator
        .generate(&schema, 100)
        .expect("Failed to generate records");

    assert_eq!(records.len(), 100, "Should generate 100 records");

    for record in &records {
        assert!(record.contains_key("symbol"), "Record should have symbol");
        assert!(record.contains_key("price"), "Record should have price");
        assert!(record.contains_key("volume"), "Record should have volume");

        // Verify symbol is one of the enum values
        if let Some(FieldValue::String(symbol)) = record.get("symbol") {
            assert!(
                ["AAPL", "GOOGL", "MSFT"].contains(&symbol.as_str()),
                "Symbol should be one of the enum values"
            );
        }

        // Verify price is in range
        if let Some(FieldValue::Float(price)) = record.get("price") {
            assert!(
                *price >= 100.0 && *price <= 500.0,
                "Price should be in range [100, 500]"
            );
        }

        // Verify volume is in range
        if let Some(FieldValue::Integer(volume)) = record.get("volume") {
            assert!(
                *volume >= 100 && *volume <= 10000,
                "Volume should be in range [100, 10000]"
            );
        }
    }

    println!(
        "✅ Schema data generation test passed with {} records",
        records.len()
    );
}

/// Test schema-driven data generation with all supported types
#[test]
fn test_schema_generation_all_types() {
    let schema_yaml = r#"
name: all_types_test
description: Schema with all supported types
fields:
  - name: int_field
    type: integer
    constraints:
      range:
        min: 0
        max: 100
  - name: float_field
    type: float
    constraints:
      range:
        min: 0.0
        max: 1.0
  - name: string_field
    type: string
    constraints:
      length:
        min: 5
        max: 10
  - name: bool_field
    type: boolean
  - name: timestamp_field
    type: timestamp
  - name: enum_field
    type: string
    constraints:
      enum_values:
        values: [RED, GREEN, BLUE]
"#;

    let schema: Schema = serde_yaml::from_str(schema_yaml).expect("Failed to parse schema");
    let mut generator = SchemaDataGenerator::new(Some(12345));
    let records = generator.generate(&schema, 20).expect("Failed to generate");

    assert_eq!(records.len(), 20);

    for record in &records {
        // Verify all fields exist
        assert!(record.contains_key("int_field"));
        assert!(record.contains_key("float_field"));
        assert!(record.contains_key("string_field"));
        assert!(record.contains_key("bool_field"));
        assert!(record.contains_key("timestamp_field"));
        assert!(record.contains_key("enum_field"));

        // Verify integer range
        if let Some(FieldValue::Integer(v)) = record.get("int_field") {
            assert!(*v >= 0 && *v <= 100);
        }

        // Verify float range
        if let Some(FieldValue::Float(v)) = record.get("float_field") {
            assert!(*v >= 0.0 && *v <= 1.0);
        }

        // Verify enum values
        if let Some(FieldValue::String(v)) = record.get("enum_field") {
            assert!(["RED", "GREEN", "BLUE"].contains(&v.as_str()));
        }
    }

    println!("✅ All types schema generation test passed");
}
