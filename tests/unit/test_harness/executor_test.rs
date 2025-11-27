//! Unit tests for the test harness executor
//!
//! Tests the extract_stream_name function for CREATE STREAM and CREATE TABLE
//! Tests parse_with_properties for extracting source/sink topics

use velostream::velostream::test_harness::executor::{
    extract_sources_and_sinks, extract_stream_name, parse_with_properties,
};

#[test]
fn test_extract_stream_name_csas() {
    let sql = "CREATE STREAM my_output AS SELECT * FROM input";
    assert_eq!(extract_stream_name(sql), Some("my_output".to_string()));
}

#[test]
fn test_extract_stream_name_csas_with_newlines() {
    let sql = r#"
CREATE STREAM enriched_data AS
SELECT id, name
FROM raw_data
"#;
    assert_eq!(extract_stream_name(sql), Some("enriched_data".to_string()));
}

#[test]
fn test_extract_stream_name_ctas() {
    let sql = "CREATE TABLE my_table AS SELECT COUNT(*) FROM input";
    assert_eq!(extract_stream_name(sql), Some("my_table".to_string()));
}

#[test]
fn test_extract_stream_name_ctas_with_aggregation() {
    let sql = r#"
CREATE TABLE aggregated_results AS
SELECT category, SUM(amount) as total
FROM transactions
GROUP BY category
"#;
    assert_eq!(
        extract_stream_name(sql),
        Some("aggregated_results".to_string())
    );
}

#[test]
fn test_extract_stream_name_with_underscores() {
    let sql = "CREATE STREAM my_complex_stream_name AS SELECT * FROM src";
    assert_eq!(
        extract_stream_name(sql),
        Some("my_complex_stream_name".to_string())
    );
}

#[test]
fn test_extract_stream_name_plain_select() {
    // Plain SELECT without CREATE should return None
    let sql = "SELECT * FROM source";
    assert_eq!(extract_stream_name(sql), None);
}

#[test]
fn test_extract_stream_name_lowercase() {
    // Case insensitive
    let sql = "create stream lowercase_stream as select * from input";
    assert_eq!(
        extract_stream_name(sql),
        Some("lowercase_stream".to_string())
    );
}

#[test]
fn test_extract_stream_name_mixed_case() {
    let sql = "Create Table MixedCase AS SELECT * FROM input";
    assert_eq!(extract_stream_name(sql), Some("MixedCase".to_string()));
}

// ============== WITH Properties Tests ==============

#[test]
fn test_parse_with_properties_single_source_sink() {
    let sql = r#"
CREATE STREAM output AS SELECT * FROM input
WITH (
    'input.type' = 'kafka_source',
    'input.topic' = 'input_topic',
    'output.type' = 'kafka_sink',
    'output.topic' = 'output_topic'
);
"#;
    let props = parse_with_properties(sql);
    assert_eq!(props.get("input.type"), Some(&"kafka_source".to_string()));
    assert_eq!(props.get("input.topic"), Some(&"input_topic".to_string()));
    assert_eq!(props.get("output.type"), Some(&"kafka_sink".to_string()));
    assert_eq!(props.get("output.topic"), Some(&"output_topic".to_string()));
}

#[test]
fn test_parse_with_properties_multiple_sources() {
    let sql = r#"
CREATE STREAM enriched AS SELECT * FROM orders JOIN customers ON orders.cid = customers.id
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_topic',
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_topic',
    'enriched.type' = 'kafka_sink',
    'enriched.topic' = 'enriched_output'
);
"#;
    let props = parse_with_properties(sql);
    assert_eq!(props.get("orders.type"), Some(&"kafka_source".to_string()));
    assert_eq!(props.get("orders.topic"), Some(&"orders_topic".to_string()));
    assert_eq!(
        props.get("customers.type"),
        Some(&"kafka_source".to_string())
    );
    assert_eq!(
        props.get("customers.topic"),
        Some(&"customers_topic".to_string())
    );
    assert_eq!(props.get("enriched.type"), Some(&"kafka_sink".to_string()));
    assert_eq!(
        props.get("enriched.topic"),
        Some(&"enriched_output".to_string())
    );
}

#[test]
fn test_extract_sources_and_sinks_single() {
    let sql = r#"
CREATE STREAM output AS SELECT * FROM input
WITH (
    'input.type' = 'kafka_source',
    'input.topic' = 'my_input',
    'output.type' = 'kafka_sink',
    'output.topic' = 'my_output'
);
"#;
    let props = parse_with_properties(sql);
    let (sources, sinks) = extract_sources_and_sinks(&props);

    assert_eq!(sources.len(), 1);
    assert_eq!(sinks.len(), 1);

    let source = sources.iter().find(|(name, _)| name == "input").unwrap();
    assert_eq!(source.1, Some("my_input".to_string()));

    let sink = sinks.iter().find(|(name, _)| name == "output").unwrap();
    assert_eq!(sink.1, Some("my_output".to_string()));
}

#[test]
fn test_extract_sources_and_sinks_multiple() {
    let sql = r#"
CREATE STREAM enriched AS SELECT * FROM orders JOIN customers ON orders.cid = customers.id
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_topic',
    'customers.type' = 'kafka_source',
    'customers.topic' = 'customers_topic',
    'enriched.type' = 'kafka_sink',
    'enriched.topic' = 'enriched_output'
);
"#;
    let props = parse_with_properties(sql);
    let (sources, sinks) = extract_sources_and_sinks(&props);

    assert_eq!(sources.len(), 2);
    assert_eq!(sinks.len(), 1);

    // Check sources have correct topics
    let orders = sources.iter().find(|(name, _)| name == "orders").unwrap();
    assert_eq!(orders.1, Some("orders_topic".to_string()));

    let customers = sources
        .iter()
        .find(|(name, _)| name == "customers")
        .unwrap();
    assert_eq!(customers.1, Some("customers_topic".to_string()));

    // Check sink
    let sink = sinks.iter().find(|(name, _)| name == "enriched").unwrap();
    assert_eq!(sink.1, Some("enriched_output".to_string()));
}

#[test]
fn test_extract_sources_no_topic_specified() {
    let sql = r#"
CREATE STREAM output AS SELECT * FROM input
WITH (
    'input.type' = 'kafka_source',
    'output.type' = 'kafka_sink'
);
"#;
    let props = parse_with_properties(sql);
    let (sources, sinks) = extract_sources_and_sinks(&props);

    assert_eq!(sources.len(), 1);
    assert_eq!(sinks.len(), 1);

    // No topic specified, should be None
    let source = sources.iter().find(|(name, _)| name == "input").unwrap();
    assert_eq!(source.1, None);

    let sink = sinks.iter().find(|(name, _)| name == "output").unwrap();
    assert_eq!(sink.1, None);
}
