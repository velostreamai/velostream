# Testing the Test Harness

This document describes how to test the test harness itself.

## Test Organization

```
tests/
├── unit/
│   └── test_harness/
│       ├── mod.rs
│       ├── schema_test.rs
│       ├── generator_test.rs
│       ├── assertions_test.rs
│       ├── spec_test.rs
│       └── utils_test.rs
└── integration/
    └── test_harness_integration_test.rs
```

## Running Tests

### All Unit Tests

```bash
# Run all test harness unit tests
cargo test --lib test_harness

# With output
cargo test --lib test_harness -- --nocapture

# Specific module
cargo test --lib test_harness::generator
cargo test --lib test_harness::assertions
cargo test --lib test_harness::schema
```

### Integration Tests

Integration tests require Docker for Kafka:

```bash
# Run integration tests
cargo test --tests integration::test_harness_integration_test

# With features
cargo test --tests --features test-support integration::test_harness_integration_test
```

### Specific Test

```bash
# Run single test
cargo test --lib test_harness::generator::tests::test_enum_generation

# Pattern matching
cargo test --lib "test_harness::generator::tests::test_*"
```

## Unit Test Examples

### Schema Tests

```rust
// tests/unit/test_harness/schema_test.rs

use velostream::velostream::test_harness::schema::{Schema, FieldDefinition};

#[test]
fn test_schema_parsing() {
    let yaml = r#"
        schema:
          name: orders
          namespace: ecommerce
        fields:
          - name: order_id
            type: string
            constraints:
              pattern: "ORD-[0-9]{8}"
          - name: amount
            type: decimal
            precision: 19
            scale: 2
            constraints:
              min: 0.01
              max: 10000.00
    "#;

    let schema: Schema = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(schema.name, "orders");
    assert_eq!(schema.namespace, Some("ecommerce".to_string()));
    assert_eq!(schema.fields.len(), 2);

    let order_id = &schema.fields[0];
    assert_eq!(order_id.name, "order_id");
    assert_eq!(order_id.field_type, "string");
    assert!(order_id.constraints.pattern.is_some());

    let amount = &schema.fields[1];
    assert_eq!(amount.name, "amount");
    assert_eq!(amount.precision, Some(19));
    assert_eq!(amount.scale, Some(2));
}

#[test]
fn test_schema_validation() {
    let schema = Schema {
        name: "test".to_string(),
        namespace: None,
        description: None,
        fields: vec![],  // Empty fields
    };

    let result = schema.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no fields"));
}
```

### Generator Tests

```rust
// tests/unit/test_harness/generator_test.rs

use velostream::velostream::test_harness::generator::SchemaDataGenerator;
use velostream::velostream::sql::execution::types::FieldValue;

#[test]
fn test_enum_generation() {
    let schema = parse_test_schema(r#"
        fields:
          - name: status
            type: string
            constraints:
              enum: [PENDING, ACTIVE, CLOSED]
    "#);

    let mut gen = SchemaDataGenerator::new(Some(42));
    let records = gen.generate(&schema, 100).unwrap();

    let valid_values: HashSet<_> = ["PENDING", "ACTIVE", "CLOSED"]
        .iter().map(|s| s.to_string()).collect();

    for record in records {
        let status = record.fields.get("status").unwrap();
        if let FieldValue::String(s) = status {
            assert!(valid_values.contains(s));
        } else {
            panic!("Expected string value");
        }
    }
}

#[test]
fn test_weighted_enum_generation() {
    let schema = parse_test_schema(r#"
        fields:
          - name: status
            type: string
            constraints:
              enum: [A, B, C]
              weights: [0.8, 0.15, 0.05]
    "#);

    let mut gen = SchemaDataGenerator::new(Some(42));
    let records = gen.generate(&schema, 10000).unwrap();

    let mut counts: HashMap<String, usize> = HashMap::new();
    for record in records {
        if let FieldValue::String(s) = record.fields.get("status").unwrap() {
            *counts.entry(s.clone()).or_default() += 1;
        }
    }

    // A should be most frequent (80%)
    assert!(counts["A"] > counts["B"]);
    assert!(counts["B"] > counts["C"]);

    // Approximate check (with some tolerance)
    let a_ratio = counts["A"] as f64 / 10000.0;
    assert!(a_ratio > 0.7 && a_ratio < 0.9);
}

#[test]
fn test_range_generation() {
    let schema = parse_test_schema(r#"
        fields:
          - name: quantity
            type: integer
            constraints:
              min: 1
              max: 100
    "#);

    let mut gen = SchemaDataGenerator::new(Some(42));
    let records = gen.generate(&schema, 1000).unwrap();

    for record in records {
        if let FieldValue::Integer(val) = record.fields.get("quantity").unwrap() {
            assert!(*val >= 1 && *val <= 100);
        }
    }
}

#[test]
fn test_reference_data() {
    let mut gen = SchemaDataGenerator::new(Some(42));

    // Load reference data
    gen.load_reference_data("customers", "id", vec![
        FieldValue::String("CUST001".to_string()),
        FieldValue::String("CUST002".to_string()),
        FieldValue::String("CUST003".to_string()),
    ]);

    let schema = parse_test_schema(r#"
        fields:
          - name: customer_id
            type: string
            constraints:
              references:
                schema: customers
                field: id
    "#);

    let records = gen.generate(&schema, 100).unwrap();

    let valid_ids: HashSet<_> = ["CUST001", "CUST002", "CUST003"]
        .iter().map(|s| s.to_string()).collect();

    for record in records {
        if let FieldValue::String(s) = record.fields.get("customer_id").unwrap() {
            assert!(valid_ids.contains(s));
        }
    }
}

#[test]
fn test_reproducibility() {
    let schema = parse_test_schema(r#"
        fields:
          - name: value
            type: integer
            constraints:
              min: 0
              max: 1000
    "#);

    // Same seed should produce same results
    let mut gen1 = SchemaDataGenerator::new(Some(42));
    let mut gen2 = SchemaDataGenerator::new(Some(42));

    let records1 = gen1.generate(&schema, 100).unwrap();
    let records2 = gen2.generate(&schema, 100).unwrap();

    for (r1, r2) in records1.iter().zip(records2.iter()) {
        assert_eq!(r1.fields.get("value"), r2.fields.get("value"));
    }
}
```

### Assertion Tests

```rust
// tests/unit/test_harness/assertions_test.rs

use velostream::velostream::test_harness::assertions::{AssertionRunner, AssertionResult};
use velostream::velostream::test_harness::executor::CapturedOutput;

fn create_test_output(records: Vec<HashMap<String, FieldValue>>) -> CapturedOutput {
    CapturedOutput {
        sink_name: "test_output".to_string(),
        records: records.into_iter().map(|fields| StreamRecord {
            fields,
            timestamp: None,
            metadata: None,
        }).collect(),
        execution_time_ms: 100,
        memory_peak_bytes: None,
        memory_growth_bytes: None,
    }
}

#[test]
fn test_record_count_equals() {
    let output = create_test_output(vec![
        hashmap! { "id" => FieldValue::Integer(1) },
        hashmap! { "id" => FieldValue::Integer(2) },
        hashmap! { "id" => FieldValue::Integer(3) },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "record_count".to_string(),
        params: hashmap! {
            "operator" => "equals".into(),
            "expected" => 3.into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(result.passed);
}

#[test]
fn test_record_count_fails() {
    let output = create_test_output(vec![
        hashmap! { "id" => FieldValue::Integer(1) },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "record_count".to_string(),
        params: hashmap! {
            "operator" => "equals".into(),
            "expected" => 10.into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(!result.passed);
    assert!(result.expected.as_ref().unwrap().contains("10"));
    assert!(result.actual.as_ref().unwrap().contains("1"));
}

#[test]
fn test_schema_contains() {
    let output = create_test_output(vec![
        hashmap! {
            "id" => FieldValue::Integer(1),
            "name" => FieldValue::String("test".to_string()),
            "value" => FieldValue::Float(123.45),
        },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "schema_contains".to_string(),
        params: hashmap! {
            "fields" => vec!["id", "name"].into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(result.passed);
}

#[test]
fn test_no_nulls() {
    let output = create_test_output(vec![
        hashmap! {
            "id" => FieldValue::Integer(1),
            "name" => FieldValue::String("test".to_string()),
        },
        hashmap! {
            "id" => FieldValue::Integer(2),
            "name" => FieldValue::Null,  // Null value!
        },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "no_nulls".to_string(),
        params: hashmap! {
            "fields" => vec!["name"].into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(!result.passed);
    assert!(result.message.contains("null"));
}

#[test]
fn test_field_values_between() {
    let output = create_test_output(vec![
        hashmap! { "price" => FieldValue::Float(100.0) },
        hashmap! { "price" => FieldValue::Float(150.0) },
        hashmap! { "price" => FieldValue::Float(200.0) },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "field_values".to_string(),
        params: hashmap! {
            "field" => "price".into(),
            "operator" => "between".into(),
            "min" => 50.0.into(),
            "max" => 250.0.into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(result.passed);
}

#[test]
fn test_aggregate_check_sum() {
    let output = create_test_output(vec![
        hashmap! { "amount" => FieldValue::Float(100.0) },
        hashmap! { "amount" => FieldValue::Float(200.0) },
        hashmap! { "amount" => FieldValue::Float(300.0) },
    ]);

    let assertion = AssertionConfig {
        assertion_type: "aggregate_check".to_string(),
        params: hashmap! {
            "expression" => "SUM(amount)".into(),
            "operator" => "equals".into(),
            "expected" => 600.0.into(),
        },
        message: None,
    };

    let result = AssertionRunner::run_assertion(&assertion, &output);
    assert!(result.passed);
}
```

## Integration Test Examples

```rust
// tests/integration/test_harness_integration_test.rs

use velostream::velostream::test_harness::infra::TestHarnessInfra;
use tokio;

#[tokio::test]
async fn test_kafka_infrastructure() {
    let mut infra = TestHarnessInfra::with_kafka("localhost:9092");

    // Start infrastructure
    let start_result = infra.start().await;
    assert!(start_result.is_ok(), "Failed to start: {:?}", start_result);

    // Create topic
    let topic_name = format!("test_topic_{}", uuid::Uuid::new_v4());
    let create_result = infra.create_topic(&topic_name).await;
    assert!(create_result.is_ok());

    // Verify topic exists (by creating producer)
    let producer = infra.create_producer();
    assert!(producer.is_ok());

    // Cleanup
    let delete_result = infra.delete_topic(&topic_name).await;
    assert!(delete_result.is_ok());

    infra.stop().await.unwrap();
}

#[tokio::test]
async fn test_config_overrides() {
    use velostream::velostream::test_harness::config_override::ConfigOverrides;

    let mut config = HashMap::new();
    config.insert("source.topic".to_string(), "original".to_string());
    config.insert("sink.topic".to_string(), "output".to_string());

    let overrides = ConfigOverrides::new()
        .with_topic_prefix("test_123_")
        .with_bootstrap_servers("kafka:9092");

    overrides.apply(&mut config);

    assert_eq!(config.get("source.topic"), Some(&"test_123_original".to_string()));
    assert_eq!(config.get("bootstrap.servers"), Some(&"kafka:9092".to_string()));
}

#[tokio::test]
async fn test_temp_directory() {
    let mut infra = TestHarnessInfra::with_kafka("localhost:9092");
    infra.start().await.unwrap();

    let temp_dir = infra.get_temp_dir();
    assert!(temp_dir.is_some());
    assert!(temp_dir.unwrap().exists());

    infra.stop().await.unwrap();
}
```

## Test Utilities

### Helper Macros

```rust
// tests/test_utils.rs

macro_rules! hashmap {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(map.insert($key.to_string(), $value);)*
        map
    }};
}

macro_rules! assert_passes {
    ($result:expr) => {
        assert!($result.passed, "Expected assertion to pass: {}", $result.message);
    };
}

macro_rules! assert_fails {
    ($result:expr) => {
        assert!(!$result.passed, "Expected assertion to fail: {}", $result.message);
    };
}
```

### Test Schema Parser

```rust
fn parse_test_schema(yaml: &str) -> Schema {
    let full_yaml = format!(
        r#"
        schema:
          name: test
        {}
        "#,
        yaml
    );
    serde_yaml::from_str(&full_yaml).expect("Failed to parse test schema")
}
```

### Test Record Builder

```rust
fn create_record<const N: usize>(fields: [(&str, impl Into<FieldValue>); N]) -> StreamRecord {
    let mut map = HashMap::new();
    for (name, value) in fields {
        map.insert(name.to_string(), value.into());
    }
    StreamRecord {
        fields: map,
        timestamp: None,
        metadata: None,
    }
}
```

## Test Coverage

### Measuring Coverage

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Run coverage for test harness module
cargo tarpaulin --lib --out Html -- test_harness

# View report
open tarpaulin-report.html
```

### Coverage Goals

| Module | Target Coverage |
|--------|-----------------|
| `schema.rs` | 90% |
| `generator.rs` | 85% |
| `assertions.rs` | 90% |
| `spec.rs` | 80% |
| `report.rs` | 75% |
| `infra.rs` | 70% (integration-heavy) |

## Debugging Tests

### Verbose Output

```bash
RUST_LOG=debug cargo test --lib test_harness -- --nocapture
```

### Specific Test Debug

```bash
RUST_LOG=velostream::test_harness::generator=trace cargo test --lib test_enum_generation -- --nocapture
```

### Async Test Debugging

```rust
#[tokio::test]
async fn test_with_tracing() {
    // Install test subscriber
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Your test code...
}
```
