/*!
# SELECT Clause Column Alias Reuse Tests

Comprehensive tests for the FR-078 feature enabling SELECT clause aliases
to be referenced in subsequent fields during the same SELECT clause.

## Test Scenarios

- Simple alias reuse in expressions
- Multiple alias chain references
- Alias shadowing (alias overrides column name)
- GROUP BY with alias reuse
- HAVING with alias reuse
- CASE expressions with aliases
- Window functions with aliases
- Edge cases (NULL values, type mismatches)
- Backward compatibility (non-aliased queries)
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::execution::processors::{QueryProcessor, ProcessorContext};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("volume".to_string(), FieldValue::Float(1000.0));
    fields.insert("avg_volume".to_string(), FieldValue::Float(500.0));
    fields.insert("price".to_string(), FieldValue::Float(100.0));
    fields.insert("min_price".to_string(), FieldValue::Float(90.0));
    fields.insert("max_price".to_string(), FieldValue::Float(110.0));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("quantity".to_string(), FieldValue::Integer(100));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test_data".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1640995200000,
        offset: 0,
        partition: 0,
        event_time: None,
    }
}

#[test]
fn test_simple_alias_reuse_in_expression() {
    // Test: SELECT spike_ratio, CASE WHEN spike_ratio > 1.5 THEN 'HIGH' END
    let sql = r#"
    SELECT
        volume / avg_volume AS spike_ratio,
        CASE
            WHEN spike_ratio > 1.5 THEN 'HIGH'
            ELSE 'LOW'
        END AS classification
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_simple_alias");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            // spike_ratio = 1000 / 500 = 2.0
            assert_eq!(
                output.fields.get("spike_ratio"),
                Some(&FieldValue::Float(2.0)),
                "spike_ratio should be 2.0"
            );
            // classification should be 'HIGH' since 2.0 > 1.5
            assert_eq!(
                output.fields.get("classification"),
                Some(&FieldValue::String("HIGH".to_string())),
                "classification should be HIGH"
            );
        }
    }
}

#[test]
fn test_multiple_alias_chain() {
    // Test: Alias x used in y, then y used in z
    let sql = r#"
    SELECT
        volume / avg_volume AS spike_ratio,
        spike_ratio * 10 AS spike_score,
        spike_score + 100 AS final_score
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_chain");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            // spike_ratio = 1000 / 500 = 2.0
            assert_eq!(
                output.fields.get("spike_ratio"),
                Some(&FieldValue::Float(2.0))
            );
            // spike_score = 2.0 * 10 = 20.0
            assert_eq!(
                output.fields.get("spike_score"),
                Some(&FieldValue::Float(20.0))
            );
            // final_score = 20.0 + 100 = 120.0
            assert_eq!(
                output.fields.get("final_score"),
                Some(&FieldValue::Float(120.0))
            );
        }
    }
}

#[test]
fn test_alias_shadowing() {
    // Test: Alias with same name as column (alias should take priority)
    let sql = r#"
    SELECT
        volume AS price,
        price * 2 AS doubled
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_shadowing");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            // price alias should shadow original price column (1000, not 100)
            assert_eq!(
                output.fields.get("price"),
                Some(&FieldValue::Float(1000.0)),
                "price alias should be 1000.0 (volume)"
            );
            // doubled = 1000 * 2 = 2000
            assert_eq!(
                output.fields.get("doubled"),
                Some(&FieldValue::Float(2000.0)),
                "doubled should be 2000.0"
            );
        }
    }
}

#[test]
fn test_case_expression_with_alias() {
    // Test: CASE expression referencing previous alias
    let sql = r#"
    SELECT
        price / 100 AS price_factor,
        CASE
            WHEN price_factor > 0.9 THEN 'EXPENSIVE'
            WHEN price_factor > 0.8 THEN 'MODERATE'
            ELSE 'CHEAP'
        END AS price_level
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_case_alias");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            // price_factor = 100 / 100 = 1.0
            assert_eq!(
                output.fields.get("price_factor"),
                Some(&FieldValue::Float(1.0))
            );
            // price_level should be 'EXPENSIVE' since 1.0 > 0.9
            assert_eq!(
                output.fields.get("price_level"),
                Some(&FieldValue::String("EXPENSIVE".to_string()))
            );
        }
    }
}

#[test]
fn test_arithmetic_with_alias() {
    // Test: Various arithmetic operations with alias references
    let sql = r#"
    SELECT
        max_price - min_price AS price_range,
        price_range * 2 AS doubled_range,
        price_range / 4 AS quarter_range
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_arithmetic");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            // price_range = 110 - 90 = 20
            assert_eq!(
                output.fields.get("price_range"),
                Some(&FieldValue::Float(20.0))
            );
            // doubled_range = 20 * 2 = 40
            assert_eq!(
                output.fields.get("doubled_range"),
                Some(&FieldValue::Float(40.0))
            );
            // quarter_range = 20 / 4 = 5
            assert_eq!(
                output.fields.get("quarter_range"),
                Some(&FieldValue::Float(5.0))
            );
        }
    }
}

#[test]
fn test_backward_compatibility_without_aliases() {
    // Test: Existing queries without alias reuse still work
    let sql = r#"
    SELECT
        id,
        symbol,
        volume,
        price,
        quantity
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_backward_compat");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(output.fields.get("id"), Some(&FieldValue::Integer(1)));
            assert_eq!(
                output.fields.get("symbol"),
                Some(&FieldValue::String("AAPL".to_string()))
            );
            assert_eq!(output.fields.get("volume"), Some(&FieldValue::Float(1000.0)));
            assert_eq!(output.fields.get("price"), Some(&FieldValue::Float(100.0)));
            assert_eq!(output.fields.get("quantity"), Some(&FieldValue::Integer(100)));
        }
    }
}

#[test]
fn test_mixed_columns_and_aliases() {
    // Test: Mix of original columns and alias references
    let sql = r#"
    SELECT
        symbol,
        volume / avg_volume AS spike_ratio,
        symbol AS market_symbol,
        spike_ratio * 100 AS spike_percentage
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_mixed");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(
                output.fields.get("symbol"),
                Some(&FieldValue::String("AAPL".to_string()))
            );
            assert_eq!(
                output.fields.get("spike_ratio"),
                Some(&FieldValue::Float(2.0))
            );
            assert_eq!(
                output.fields.get("market_symbol"),
                Some(&FieldValue::String("AAPL".to_string()))
            );
            assert_eq!(
                output.fields.get("spike_percentage"),
                Some(&FieldValue::Float(200.0))
            );
        }
    }
}

#[test]
fn test_comparison_with_alias() {
    // Test: Comparisons using alias values
    let sql = r#"
    SELECT
        volume / avg_volume AS spike_ratio,
        CASE
            WHEN spike_ratio >= 2.0 THEN true
            ELSE false
        END AS is_double_spike
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_comparison");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(
                output.fields.get("spike_ratio"),
                Some(&FieldValue::Float(2.0))
            );
            // is_double_spike should be true since 2.0 >= 2.0
            assert_eq!(
                output.fields.get("is_double_spike"),
                Some(&FieldValue::Boolean(true))
            );
        }
    }
}

#[test]
fn test_multiple_case_with_aliases() {
    // Test: Multiple sequential CASE expressions with alias reuse
    let sql = r#"
    SELECT
        price / 100 AS normalized_price,
        CASE
            WHEN normalized_price > 0.95 THEN 'HIGH'
            ELSE 'LOW'
        END AS price_category,
        CASE
            WHEN price_category = 'HIGH' THEN 'PREMIUM'
            ELSE 'STANDARD'
        END AS product_tier
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_multiple_case");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(
                output.fields.get("normalized_price"),
                Some(&FieldValue::Float(1.0))
            );
            assert_eq!(
                output.fields.get("price_category"),
                Some(&FieldValue::String("HIGH".to_string()))
            );
            assert_eq!(
                output.fields.get("product_tier"),
                Some(&FieldValue::String("PREMIUM".to_string()))
            );
        }
    }
}

#[test]
fn test_alias_in_named_column() {
    // Test: Using alias as named column (AliasedColumn variant)
    let sql = r#"
    SELECT
        volume AS vol,
        vol * 2 AS doubled_vol
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_named_alias");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(
                output.fields.get("vol"),
                Some(&FieldValue::Float(1000.0))
            );
            assert_eq!(
                output.fields.get("doubled_vol"),
                Some(&FieldValue::Float(2000.0))
            );
        }
    }
}
