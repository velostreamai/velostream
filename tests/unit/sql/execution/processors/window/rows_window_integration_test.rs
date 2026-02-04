//! Integration tests for ROWS WINDOW functions producing real values (not NULL)
//!
//! Validates that ROWS WINDOW in OVER clauses produce correct results when
//! there is no top-level WINDOW clause (SelectProcessor path).

mod rows_window_integration_tests {
    use std::collections::HashMap;
    use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

    use crate::unit::sql::execution::processors::window::shared_test_utils::SqlExecutor;

    fn make_record(id: i64, price: f64, symbol: &str) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        let mut record = StreamRecord::new(fields);
        record.timestamp = id * 1000;
        record
    }

    #[tokio::test]
    async fn test_lag_returns_previous_values() {
        let records = vec![
            make_record(1, 100.0, "AAPL"),
            make_record(2, 101.0, "AAPL"),
            make_record(3, 102.0, "AAPL"),
            make_record(4, 103.0, "AAPL"),
            make_record(5, 104.0, "AAPL"),
        ];

        let sql = r#"
            SELECT
                id,
                price,
                LAG(price) OVER (ROWS WINDOW BUFFER 100 ROWS) as prev_price
            FROM trades
        "#;

        let results = SqlExecutor::execute_query(sql, records).await;

        assert_eq!(results.len(), 5, "Expected 5 results");

        // First record: LAG should be NULL (no previous)
        assert_eq!(
            results[0].fields.get("prev_price"),
            Some(&FieldValue::Null),
            "First record LAG should be NULL"
        );

        // Subsequent records: LAG should return the previous record's price
        let expected_lag = [100.0, 101.0, 102.0, 103.0];
        for (i, expected) in expected_lag.iter().enumerate() {
            match results[i + 1].fields.get("prev_price") {
                Some(FieldValue::Float(v)) => {
                    assert!(
                        (*v - expected).abs() < 0.001,
                        "Record {}: LAG expected {}, got {}",
                        i + 1,
                        expected,
                        v
                    );
                }
                other => panic!(
                    "Record {}: Expected Float for prev_price, got {:?}",
                    i + 1,
                    other
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_avg_over_rows_window() {
        let records = vec![
            make_record(1, 100.0, "AAPL"),
            make_record(2, 200.0, "AAPL"),
            make_record(3, 300.0, "AAPL"),
        ];

        let sql = r#"
            SELECT
                id,
                price,
                AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS) as avg_price
            FROM trades
        "#;

        let results = SqlExecutor::execute_query(sql, records).await;

        assert_eq!(results.len(), 3, "Expected 3 results");

        // Record 1: AVG of [100] = 100
        match results[0].fields.get("avg_price") {
            Some(FieldValue::Float(v)) => {
                assert!(
                    (*v - 100.0).abs() < 0.001,
                    "Record 0 avg_price expected 100.0, got {}",
                    v
                );
            }
            other => panic!("Record 0: Expected Float for avg_price, got {:?}", other),
        }

        // Record 2: AVG of [100, 200] = 150
        match results[1].fields.get("avg_price") {
            Some(FieldValue::Float(v)) => {
                assert!(
                    (*v - 150.0).abs() < 0.001,
                    "Record 1 avg_price expected 150.0, got {}",
                    v
                );
            }
            other => panic!("Record 1: Expected Float for avg_price, got {:?}", other),
        }

        // Record 3: AVG of [100, 200, 300] = 200
        match results[2].fields.get("avg_price") {
            Some(FieldValue::Float(v)) => {
                assert!(
                    (*v - 200.0).abs() < 0.001,
                    "Record 2 avg_price expected 200.0, got {}",
                    v
                );
            }
            other => panic!("Record 2: Expected Float for avg_price, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_multiple_window_functions_in_select() {
        let records = vec![
            make_record(1, 100.0, "AAPL"),
            make_record(2, 200.0, "AAPL"),
            make_record(3, 300.0, "AAPL"),
        ];

        let sql = r#"
            SELECT
                id,
                price,
                LAG(price) OVER (ROWS WINDOW BUFFER 100 ROWS) as prev_price,
                AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS) as avg_price,
                ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS) as row_num
            FROM trades
        "#;

        let results = SqlExecutor::execute_query(sql, records).await;

        assert_eq!(results.len(), 3, "Expected 3 results");

        // Verify all fields are non-NULL (except first LAG)
        for (i, result) in results.iter().enumerate() {
            assert!(
                result.fields.contains_key("prev_price"),
                "Record {}: missing prev_price",
                i
            );
            match result.fields.get("avg_price") {
                Some(FieldValue::Float(v)) => assert!(v.is_finite(), "avg_price should be finite"),
                other => panic!(
                    "Record {}: Expected Float for avg_price, got {:?}",
                    i, other
                ),
            }
            match result.fields.get("row_num") {
                Some(FieldValue::Integer(n)) => {
                    assert_eq!(*n, (i + 1) as i64, "ROW_NUMBER should be sequential")
                }
                other => panic!(
                    "Record {}: Expected Integer for row_num, got {:?}",
                    i, other
                ),
            }
        }

        // First record: LAG is NULL
        assert_eq!(results[0].fields.get("prev_price"), Some(&FieldValue::Null));
        // Second record: LAG is 100.0
        match results[1].fields.get("prev_price") {
            Some(FieldValue::Float(v)) => assert!((*v - 100.0).abs() < 0.001),
            other => panic!("Expected Float for prev_price, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_row_number_produces_sequential_values() {
        let records = vec![
            make_record(1, 100.0, "AAPL"),
            make_record(2, 200.0, "AAPL"),
            make_record(3, 300.0, "AAPL"),
            make_record(4, 400.0, "AAPL"),
            make_record(5, 500.0, "AAPL"),
        ];

        let sql = r#"
            SELECT
                id,
                ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS) as row_num
            FROM trades
        "#;

        let results = SqlExecutor::execute_query(sql, records).await;

        assert_eq!(results.len(), 5, "Expected 5 results");

        for (i, result) in results.iter().enumerate() {
            match result.fields.get("row_num") {
                Some(FieldValue::Integer(n)) => {
                    assert_eq!(
                        *n,
                        (i + 1) as i64,
                        "ROW_NUMBER at position {} should be {}",
                        i,
                        i + 1
                    );
                }
                other => panic!(
                    "Record {}: Expected Integer for row_num, got {:?}",
                    i, other
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_first_value_returns_first_record() {
        let records = vec![
            make_record(1, 100.0, "AAPL"),
            make_record(2, 200.0, "AAPL"),
            make_record(3, 300.0, "AAPL"),
        ];

        let sql = r#"
            SELECT
                id,
                price,
                FIRST_VALUE(price) OVER (ROWS WINDOW BUFFER 100 ROWS) as first_price
            FROM trades
        "#;

        let results = SqlExecutor::execute_query(sql, records).await;

        assert_eq!(results.len(), 3, "Expected 3 results");

        // FIRST_VALUE should always return the first record's price (100.0)
        for (i, result) in results.iter().enumerate() {
            match result.fields.get("first_price") {
                Some(FieldValue::Float(v)) => {
                    assert!(
                        (*v - 100.0).abs() < 0.001,
                        "Record {}: FIRST_VALUE expected 100.0, got {}",
                        i,
                        v
                    );
                }
                other => panic!(
                    "Record {}: Expected Float for first_price, got {:?}",
                    i, other
                ),
            }
        }
    }
}
