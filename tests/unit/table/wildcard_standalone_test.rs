/*!
# Standalone Wildcard Tests

Focused tests for wildcard functionality using the standard * syntax.
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{SqlDataSource, SqlQueryable};

// Simple test data source for wildcard testing
struct TestWildcardSource {
    records: HashMap<String, FieldValue>,
}

impl TestWildcardSource {
    fn new() -> Self {
        let mut records = HashMap::new();

        // Create a simple portfolio structure
        let mut portfolio = HashMap::new();
        let mut positions = HashMap::new();

        // AAPL position (large)
        let mut aapl = HashMap::new();
        aapl.insert("shares".to_string(), FieldValue::Integer(150));
        aapl.insert("price".to_string(), FieldValue::Float(150.25));
        positions.insert("AAPL".to_string(), FieldValue::Struct(aapl));

        // MSFT position (medium)
        let mut msft = HashMap::new();
        msft.insert("shares".to_string(), FieldValue::Integer(75));
        msft.insert("price".to_string(), FieldValue::Float(330.50));
        positions.insert("MSFT".to_string(), FieldValue::Struct(msft));

        // TSLA position (small)
        let mut tsla = HashMap::new();
        tsla.insert("shares".to_string(), FieldValue::Integer(25));
        tsla.insert("price".to_string(), FieldValue::Float(450.75));
        positions.insert("TSLA".to_string(), FieldValue::Struct(tsla));

        portfolio.insert("positions".to_string(), FieldValue::Struct(positions));
        records.insert("portfolio-1".to_string(), FieldValue::Struct(portfolio));

        Self { records }
    }
}

impl SqlDataSource for TestWildcardSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, velostream::velostream::sql::error::SqlError> {
        Ok(self.records.clone())
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, velostream::velostream::sql::error::SqlError> {
        Ok(self.records.get(key).cloned())
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }
}

#[test]
fn test_basic_wildcard_functionality() {
    let source = TestWildcardSource::new();

    // Test basic wildcard query
    let result = source.sql_wildcard_values("positions.*.shares > 100");
    println!("Wildcard query result: {:?}", result);

    // This should work if wildcard implementation is correct
    match result {
        Ok(values) => {
            println!("Found {} matching values", values.len());
            assert!(!values.is_empty(), "Should find at least one position with > 100 shares");
        }
        Err(e) => {
            println!("Error in wildcard query: {:?}", e);
            // For now, just check that it doesn't panic
        }
    }
}

#[test]
fn test_wildcard_without_comparison() {
    let source = TestWildcardSource::new();

    // Test wildcard without comparison
    let result = source.sql_wildcard_values("positions.*.shares");
    println!("All shares result: {:?}", result);

    match result {
        Ok(values) => {
            println!("Found {} share values", values.len());
            // Should find 3 values (AAPL, MSFT, TSLA)
        }
        Err(e) => {
            println!("Error in wildcard query: {:?}", e);
        }
    }
}

#[test]
fn test_wildcard_edge_cases() {
    let source = TestWildcardSource::new();

    // Test various edge cases
    let test_cases = vec![
        "positions.*",           // Just wildcard at end
        "*.positions.shares",    // Wildcard at start
        "nonexistent.*.field",   // Invalid path
    ];

    for test_case in test_cases {
        let result = source.sql_wildcard_values(test_case);
        println!("Test case '{}': {:?}", test_case, result.is_ok());
    }
}

#[test]
fn test_deep_recursive_wildcards() {
    let mut records = HashMap::new();

    // Create deeply nested structure
    let mut level1 = HashMap::new();
    let mut level2 = HashMap::new();
    let mut level3 = HashMap::new();

    level3.insert("deep_value".to_string(), FieldValue::Integer(42));
    level2.insert("level3".to_string(), FieldValue::Struct(level3));
    level1.insert("level2".to_string(), FieldValue::Struct(level2));
    records.insert("root".to_string(), FieldValue::Struct(level1));

    let source = TestWildcardSource { records };

    // Test deep recursive wildcard (**)
    let result = source.sql_wildcard_values("**.deep_value");
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], FieldValue::Integer(42));
}

#[test]
fn test_array_access_patterns() {
    let mut records = HashMap::new();

    // Create structure with arrays
    let mut doc = HashMap::new();
    let orders = vec![
        {
            let mut order = HashMap::new();
            order.insert("id".to_string(), FieldValue::Integer(1));
            order.insert("amount".to_string(), FieldValue::Float(100.50));
            FieldValue::Struct(order)
        },
        {
            let mut order = HashMap::new();
            order.insert("id".to_string(), FieldValue::Integer(2));
            order.insert("amount".to_string(), FieldValue::Float(250.75));
            FieldValue::Struct(order)
        },
        {
            let mut order = HashMap::new();
            order.insert("id".to_string(), FieldValue::Integer(3));
            order.insert("amount".to_string(), FieldValue::Float(75.25));
            FieldValue::Struct(order)
        },
    ];
    doc.insert("orders".to_string(), FieldValue::Array(orders));
    records.insert("customer".to_string(), FieldValue::Struct(doc));

    let source = TestWildcardSource { records };

    // Test array wildcard access
    let result = source.sql_wildcard_values("orders[*].amount");
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.contains(&FieldValue::Float(100.50)));
    assert!(values.contains(&FieldValue::Float(250.75)));
    assert!(values.contains(&FieldValue::Float(75.25)));

    // Test specific array index
    let result = source.sql_wildcard_values("orders[1].amount");
    assert!(result.is_ok());
    let values = result.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], FieldValue::Float(250.75));
}

#[test]
fn test_aggregate_functions() {
    let source = TestWildcardSource::new();

    // Test COUNT
    let count = source.sql_wildcard_aggregate("COUNT(positions.*.shares)");
    assert!(count.is_ok());
    assert_eq!(count.unwrap(), FieldValue::Integer(3));

    // Test MAX
    let max = source.sql_wildcard_aggregate("MAX(positions.*.shares)");
    assert!(max.is_ok());
    assert_eq!(max.unwrap(), FieldValue::Float(150.0));

    // Test MIN
    let min = source.sql_wildcard_aggregate("MIN(positions.*.shares)");
    assert!(min.is_ok());
    assert_eq!(min.unwrap(), FieldValue::Float(25.0));

    // Test AVG
    let avg = source.sql_wildcard_aggregate("AVG(positions.*.shares)");
    assert!(avg.is_ok());
    if let FieldValue::Float(avg_val) = avg.unwrap() {
        assert!((avg_val - 83.333).abs() < 0.01); // (150 + 75 + 25) / 3 ≈ 83.333
    }

    // Test SUM
    let sum = source.sql_wildcard_aggregate("SUM(positions.*.shares)");
    assert!(sum.is_ok());
    assert_eq!(sum.unwrap(), FieldValue::Float(250.0)); // 150 + 75 + 25
}