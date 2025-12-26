//! Tests for @data.* annotation parsing and schema generation

use velostream::velostream::test_harness::{
    DataHint, DataHintParser, DataHintType, GlobalDataHints, generate_schema_from_hints,
    schema_to_yaml,
};

#[test]
fn test_parse_global_hints() {
    let sql = r#"
-- @data.record_count: 1000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"
-- @data.seed: 42

CREATE STREAM test_stream AS
SELECT * FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let global = parser.get_global_hints();
    assert_eq!(global.record_count, Some(1000));
    assert_eq!(global.time_simulation, Some("sequential".to_string()));
    assert_eq!(global.time_start, Some("-1h".to_string()));
    assert_eq!(global.time_end, Some("now".to_string()));
    assert_eq!(global.seed, Some(42));
}

#[test]
fn test_parse_field_type_hints() {
    let sql = r#"
-- @data.symbol.type: string
-- @data.price.type: decimal(4)
-- @data.volume.type: integer
-- @data.is_active.type: boolean
-- @data.created_at.type: timestamp
-- @data.order_id.type: uuid

CREATE STREAM test_stream AS
SELECT symbol, price, volume, is_active, created_at, order_id
FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let hints = parser.get_field_hints();
    assert_eq!(hints.len(), 6);

    // Find each field and verify type
    let symbol_hint = hints.iter().find(|h| h.field_name == "symbol").unwrap();
    assert_eq!(symbol_hint.field_type, Some(DataHintType::String));

    let price_hint = hints.iter().find(|h| h.field_name == "price").unwrap();
    assert_eq!(price_hint.field_type, Some(DataHintType::Decimal(4)));

    let volume_hint = hints.iter().find(|h| h.field_name == "volume").unwrap();
    assert_eq!(volume_hint.field_type, Some(DataHintType::Integer));

    let is_active_hint = hints.iter().find(|h| h.field_name == "is_active").unwrap();
    assert_eq!(is_active_hint.field_type, Some(DataHintType::Boolean));

    let created_at_hint = hints.iter().find(|h| h.field_name == "created_at").unwrap();
    assert_eq!(created_at_hint.field_type, Some(DataHintType::Timestamp));

    let order_id_hint = hints.iter().find(|h| h.field_name == "order_id").unwrap();
    assert_eq!(order_id_hint.field_type, Some(DataHintType::Uuid));
}

#[test]
fn test_parse_enum_shorthand() {
    let sql = r#"
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT", "AMZN"]
-- @data.symbol.weights: [0.3, 0.3, 0.2, 0.2]

CREATE STREAM test_stream AS
SELECT symbol FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let hints = parser.get_field_hints();
    let symbol_hint = hints.iter().find(|h| h.field_name == "symbol").unwrap();

    assert_eq!(symbol_hint.field_type, Some(DataHintType::String));
    assert_eq!(
        symbol_hint.enum_values,
        Some(vec![
            "AAPL".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "AMZN".to_string()
        ])
    );
    assert_eq!(symbol_hint.enum_weights, Some(vec![0.3, 0.3, 0.2, 0.2]));
}

#[test]
fn test_parse_range_with_distribution() {
    let sql = r#"
-- @data.price.type: decimal(4)
-- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02, drift: 0.0001, group_by: symbol

CREATE STREAM test_stream AS
SELECT price FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let hints = parser.get_field_hints();
    let price_hint = hints.iter().find(|h| h.field_name == "price").unwrap();

    assert_eq!(price_hint.field_type, Some(DataHintType::Decimal(4)));
    assert_eq!(price_hint.range, Some((100.0, 500.0)));
    assert_eq!(price_hint.distribution, Some("random_walk".to_string()));
    assert_eq!(price_hint.volatility, Some(0.02));
    assert_eq!(price_hint.drift, Some(0.0001));
    assert_eq!(price_hint.group_by, Some("symbol".to_string()));
}

#[test]
fn test_parse_timestamp_sequential() {
    let sql = r#"
-- @data.event_time.type: timestamp
-- @data.event_time: timestamp, sequential: true, start: "-1h", end: "now"

CREATE STREAM test_stream AS
SELECT event_time FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let hints = parser.get_field_hints();
    let time_hint = hints.iter().find(|h| h.field_name == "event_time").unwrap();

    assert_eq!(time_hint.field_type, Some(DataHintType::Timestamp));
    assert_eq!(time_hint.sequential, Some(true));

    // Global hints should have the time range
    let global = parser.get_global_hints();
    assert_eq!(global.time_start, Some("-1h".to_string()));
    assert_eq!(global.time_end, Some("now".to_string()));
}

#[test]
fn test_parse_multiline_field_properties() {
    let sql = r#"
-- @data.volume.type: integer
-- @data.volume.distribution: log_normal
-- @data.volume.range: [100, 50000]

CREATE STREAM test_stream AS
SELECT volume FROM input_stream;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    let hints = parser.get_field_hints();
    let volume_hint = hints.iter().find(|h| h.field_name == "volume").unwrap();

    assert_eq!(volume_hint.field_type, Some(DataHintType::Integer));
    assert_eq!(volume_hint.distribution, Some("log_normal".to_string()));
    // Note: range via multiline syntax isn't parsed the same way as shorthand
    // This test documents current behavior
}

#[test]
fn test_generate_schema_from_hints() {
    let global_hints = GlobalDataHints {
        record_count: Some(5000),
        time_simulation: Some("sequential".to_string()),
        time_start: Some("-1h".to_string()),
        time_end: Some("now".to_string()),
        seed: None,
        source_name: None,
    };

    let field_hints = vec![
        DataHint {
            field_name: "symbol".to_string(),
            field_type: Some(DataHintType::String),
            enum_values: Some(vec!["AAPL".to_string(), "GOOGL".to_string()]),
            enum_weights: Some(vec![0.6, 0.4]),
            ..Default::default()
        },
        DataHint {
            field_name: "price".to_string(),
            field_type: Some(DataHintType::Decimal(4)),
            range: Some((100.0, 500.0)),
            distribution: Some("random_walk".to_string()),
            volatility: Some(0.02),
            group_by: Some("symbol".to_string()),
            ..Default::default()
        },
        DataHint {
            field_name: "volume".to_string(),
            field_type: Some(DataHintType::Integer),
            range: Some((100.0, 50000.0)),
            distribution: Some("log_normal".to_string()),
            ..Default::default()
        },
    ];

    let schema = generate_schema_from_hints("test_source", &global_hints, &field_hints)
        .expect("Failed to generate schema");

    assert_eq!(schema.name, "test_source");
    assert_eq!(schema.record_count, 5000);
    assert_eq!(schema.fields.len(), 3);

    // Verify YAML output
    let yaml = schema_to_yaml(&schema).expect("Failed to serialize schema");
    assert!(yaml.contains("name: test_source"));
    assert!(yaml.contains("symbol"));
    assert!(yaml.contains("price"));
    assert!(yaml.contains("volume"));
}

#[test]
fn test_full_sql_with_data_hints() {
    let sql = r#"
-- =============================================================================
-- DATA GENERATION HINTS for in_market_data
-- =============================================================================
-- @data.record_count: 1000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"

-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOGL", "MSFT", "AMZN"]
-- @data.symbol.weights: [0.25, 0.25, 0.25, 0.25]

-- @data.price.type: decimal(4)
-- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02, group_by: symbol

-- @data.volume.type: integer
-- @data.volume: range [100, 50000], distribution: log_normal

-- @data.event_time.type: timestamp
-- @data.event_time: timestamp, sequential: true

-- =============================================================================
-- SQL QUERIES
-- =============================================================================

CREATE STREAM market_data_ts AS
SELECT
    symbol PRIMARY KEY,
    price,
    volume,
    event_time
FROM in_market_data
EMIT CHANGES;
"#;

    let mut parser = DataHintParser::new();
    parser.parse(sql).expect("Failed to parse data hints");

    // Verify global hints
    let global = parser.get_global_hints();
    assert_eq!(global.record_count, Some(1000));
    assert_eq!(global.time_simulation, Some("sequential".to_string()));

    // Verify field hints
    let hints = parser.get_field_hints();
    assert_eq!(hints.len(), 4);

    // Generate schema
    let schema = generate_schema_from_hints("in_market_data", global, &hints)
        .expect("Failed to generate schema");

    assert_eq!(schema.name, "in_market_data");
    assert_eq!(schema.record_count, 1000);
    assert_eq!(schema.fields.len(), 4);

    // Verify we can serialize to YAML
    let yaml = schema_to_yaml(&schema).expect("Failed to serialize schema");
    assert!(yaml.contains("random_walk"));
    assert!(yaml.contains("log_normal"));
}

#[test]
fn test_error_on_missing_type() {
    let global_hints = GlobalDataHints::default();

    let field_hints = vec![DataHint {
        field_name: "unknown_field".to_string(),
        field_type: None, // No type specified
        ..Default::default()
    }];

    // Should fail because type is required and can't be inferred
    let result = generate_schema_from_hints("test_source", &global_hints, &field_hints);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("requires explicit type"));
}

#[test]
fn test_type_inference_from_constraints() {
    let global_hints = GlobalDataHints::default();

    // Enum implies string type
    let enum_hint = DataHint {
        field_name: "status".to_string(),
        field_type: None,
        enum_values: Some(vec!["ACTIVE".to_string(), "INACTIVE".to_string()]),
        ..Default::default()
    };

    // Range implies numeric type
    let range_hint = DataHint {
        field_name: "amount".to_string(),
        field_type: None,
        range: Some((0.0, 1000.0)),
        ..Default::default()
    };

    // Sequential implies timestamp
    let time_hint = DataHint {
        field_name: "created_at".to_string(),
        field_type: None,
        sequential: Some(true),
        ..Default::default()
    };

    let field_hints = vec![enum_hint, range_hint, time_hint];

    let schema = generate_schema_from_hints("test_source", &global_hints, &field_hints)
        .expect("Failed to generate schema with inferred types");

    assert_eq!(schema.fields.len(), 3);
}
