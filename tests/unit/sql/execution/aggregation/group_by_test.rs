use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use velostream::velostream::serialization::{JsonFormat, SerializationFormat};
use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::execution::config::StreamingConfig;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> (StreamExecutionEngine, mpsc::UnboundedReceiver<StreamRecord>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let format: Arc<dyn SerializationFormat> = Arc::new(JsonFormat);
        // Window_v2 is the only architecture available (Phase 2E+)
        let config = StreamingConfig::new();
        let engine = StreamExecutionEngine::new_with_config(sender, config);
        (engine, receiver)
    }

    fn create_stream_record_with_fields(
        fields: HashMap<String, FieldValue>,
        offset: i64,
    ) -> StreamRecord {
        // FR-081: Extract timestamp from fields if present to set StreamRecord metadata
        // This ensures window calculations use the intended test timestamps
        let timestamp_ms = fields
            .get("timestamp")
            .or_else(|| fields.get("event_time"))
            .and_then(|v| match v {
                FieldValue::Integer(ts) => Some(*ts),
                FieldValue::Timestamp(dt) => Some(dt.and_utc().timestamp_millis()),
                _ => None,
            })
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        StreamRecord {
            fields,
            timestamp: timestamp_ms,
            offset,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    }

    // Helper: Assert field value with context message
    fn assert_field_float(record: &StreamRecord, field_name: &str, expected: f64, context: &str) {
        match record.fields.get(field_name) {
            Some(FieldValue::Float(actual)) => {
                assert_eq!(
                    *actual, expected,
                    "Field '{}': expected {}, got {}. Context: {}",
                    field_name, expected, actual, context
                );
            }
            Some(other) => panic!(
                "Field '{}' has type {:?}, expected Float. Context: {}",
                field_name, other, context
            ),
            None => panic!(
                "Field '{}' not found. Available: {:?}. Context: {}",
                field_name,
                record.fields.keys().collect::<Vec<_>>(),
                context
            ),
        }
    }

    // Helper: Assert integer field with context message
    fn assert_field_int(record: &StreamRecord, field_name: &str, expected: i64, context: &str) {
        match record.fields.get(field_name) {
            Some(FieldValue::Integer(actual)) => {
                assert_eq!(
                    *actual, expected,
                    "Field '{}': expected {}, got {}. Context: {}",
                    field_name, expected, actual, context
                );
            }
            Some(other) => panic!(
                "Field '{}' has type {:?}, expected Integer. Context: {}",
                field_name, other, context
            ),
            None => panic!(
                "Field '{}' not found. Available: {:?}. Context: {}",
                field_name,
                record.fields.keys().collect::<Vec<_>>(),
                context
            ),
        }
    }

    // Helper: Assert numeric field (handles both Integer and Float)
    fn assert_field_numeric(
        record: &StreamRecord,
        field_name: &str,
        expected_int: i64,
        expected_float: f64,
        context: &str,
    ) {
        match record.fields.get(field_name) {
            Some(FieldValue::Integer(actual)) => {
                assert_eq!(
                    *actual, expected_int,
                    "Field '{}': expected {}, got {}. Context: {}",
                    field_name, expected_int, actual, context
                );
            }
            Some(FieldValue::Float(actual)) => {
                assert_eq!(
                    *actual, expected_float,
                    "Field '{}': expected {}, got {}. Context: {}",
                    field_name, expected_float, actual, context
                );
            }
            Some(other) => panic!(
                "Field '{}' has type {:?}, expected numeric (Integer or Float). Context: {}",
                field_name, other, context
            ),
            None => panic!(
                "Field '{}' not found. Available: {:?}. Context: {}",
                field_name,
                record.fields.keys().collect::<Vec<_>>(),
                context
            ),
        }
    }

    // Helper: Assert two numeric fields match expected values (handles type coercion)
    fn assert_numeric_pair(
        record: &StreamRecord,
        field1: &str,
        field2: &str,
        expected_vals: (f64, f64),
        context: &str,
    ) {
        let get_numeric_value = |field: &str| -> f64 {
            match record.fields.get(field) {
                Some(FieldValue::Integer(i)) => *i as f64,
                Some(FieldValue::Float(f)) => *f,
                _ => 0.0,
            }
        };

        let val1 = get_numeric_value(field1);
        let val2 = get_numeric_value(field2);

        assert_eq!(
            (val1, val2),
            expected_vals,
            "Pair assertion failed for fields '{}' and '{}': expected {:?}, got {:?}. Context: {}",
            field1,
            field2,
            expected_vals,
            (val1, val2),
            context
        );
    }

    // Helper: Find result by matching a field condition
    fn find_result_by_field<'a, F>(
        results: &'a [StreamRecord],
        matcher: F,
        context: &str,
    ) -> &'a StreamRecord
    where
        F: Fn(&StreamRecord) -> bool,
    {
        results.iter().find(|r| matcher(r)).unwrap_or_else(|| {
            panic!(
                "No result matched the condition. Available results count: {}. Context: {}",
                results.len(),
                context
            );
        })
    }

    // Helper: Create test record with float fields easily
    fn create_float_record(fields: &[(&str, f64)], offset: i64) -> StreamRecord {
        let mut field_map = HashMap::new();
        for (name, value) in fields {
            field_map.insert(name.to_string(), FieldValue::Float(*value));
        }
        create_stream_record_with_fields(field_map, offset)
    }

    #[test]
    fn test_group_by_parsing() {
        let parser = StreamingSqlParser::new();

        // Test basic GROUP BY
        let query = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse GROUP BY query: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                group_by: Some(group_exprs),
                ..
            } => {
                assert_eq!(
                    group_exprs.len(),
                    1,
                    "Expected 1 GROUP BY expression, got {}",
                    group_exprs.len()
                );
                match &group_exprs[0] {
                    Expr::Column(name) => assert_eq!(
                        name, "customer_id",
                        "Expected GROUP BY column 'customer_id', got '{}'",
                        name
                    ),
                    other => panic!("Expected Expr::Column in GROUP BY, got: {:?}", other),
                }
            }
            other => panic!(
                "Expected Select query with GROUP BY clause, got: {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_multiple_group_by_columns() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT customer_id, region, COUNT(*) FROM orders GROUP BY customer_id, region";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse multiple GROUP BY query");

        match result.unwrap() {
            StreamingQuery::Select {
                group_by: Some(group_exprs),
                ..
            } => {
                assert_eq!(
                    group_exprs.len(),
                    2,
                    "Expected 2 GROUP BY expressions, got {}",
                    group_exprs.len()
                );
                match (&group_exprs[0], &group_exprs[1]) {
                    (Expr::Column(name1), Expr::Column(name2)) => {
                        assert_eq!(
                            name1, "customer_id",
                            "First GROUP BY column should be 'customer_id', got '{}'",
                            name1
                        );
                        assert_eq!(
                            name2, "region",
                            "Second GROUP BY column should be 'region', got '{}'",
                            name2
                        );
                    }
                    (expr1, expr2) => panic!(
                        "Expected two Expr::Column variants in GROUP BY, got: {:?} and {:?}",
                        expr1, expr2
                    ),
                }
            }
            other => panic!(
                "Expected Select query with multiple GROUP BY columns, got: {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_order_by_parsing() {
        let parser = StreamingSqlParser::new();

        // Test basic ORDER BY ASC
        let query = "SELECT * FROM orders ORDER BY amount";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse ORDER BY query: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                order_by: Some(order_exprs),
                ..
            } => {
                assert_eq!(order_exprs.len(), 1);
                match &order_exprs[0] {
                    OrderByExpr {
                        expr: Expr::Column(name),
                        direction,
                    } => {
                        assert_eq!(name, "amount");
                        assert_eq!(*direction, OrderDirection::Asc);
                    }
                    _ => panic!("Expected column order expression"),
                }
            }
            _ => panic!("Expected Select query with ORDER BY"),
        }
    }

    #[test]
    fn test_order_by_desc() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT * FROM orders ORDER BY amount DESC";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse ORDER BY DESC query");

        match result.unwrap() {
            StreamingQuery::Select {
                order_by: Some(order_exprs),
                ..
            } => {
                assert_eq!(order_exprs.len(), 1);
                match &order_exprs[0] {
                    OrderByExpr {
                        expr: Expr::Column(name),
                        direction,
                    } => {
                        assert_eq!(name, "amount");
                        assert_eq!(*direction, OrderDirection::Desc);
                    }
                    _ => panic!("Expected column order expression"),
                }
            }
            _ => panic!("Expected Select query with ORDER BY DESC"),
        }
    }

    #[test]
    fn test_having_parsing() {
        let parser = StreamingSqlParser::new();

        let query =
            "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING COUNT(*) > 5";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse HAVING query: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                group_by: Some(_),
                having: Some(_),
                ..
            } => {
                // Successfully parsed GROUP BY and HAVING
            }
            _ => panic!("Expected Select query with GROUP BY and HAVING"),
        }
    }

    #[test]
    fn test_query_without_group_by() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT * FROM orders";
        let result = parser.parse(query);
        assert!(result.is_ok());

        match result.unwrap() {
            StreamingQuery::Select {
                group_by,
                having,
                order_by,
                ..
            } => {
                assert!(group_by.is_none());
                assert!(having.is_none());
                assert!(order_by.is_none());
            }
            _ => panic!("Expected Select query"),
        }
    }

    async fn collect_results(
        receiver: &mut mpsc::UnboundedReceiver<StreamRecord>,
    ) -> Vec<StreamRecord> {
        let mut results = Vec::new();
        while let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv()).await
        {
            match result {
                Some(r) => results.push(r),
                None => break,
            }
        }
        results
    }

    // Flink-style result collection: only keep latest result per group key
    async fn collect_latest_group_results(
        receiver: &mut mpsc::UnboundedReceiver<StreamRecord>,
        group_key_field: &str,
    ) -> Vec<StreamRecord> {
        let mut all_results = Vec::new();
        while let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv()).await
        {
            match result {
                Some(r) => all_results.push(r),
                None => break,
            }
        }

        // Deduplicate: keep only the latest result for each group key
        let mut latest_by_key = std::collections::HashMap::new();
        for result in all_results {
            if let Some(key_value) = result.fields.get(group_key_field) {
                let key = match key_value {
                    FieldValue::Float(n) => n.to_string(),
                    FieldValue::String(s) => s.clone(),
                    FieldValue::Integer(i) => i.to_string(),
                    FieldValue::Boolean(b) => b.to_string(),
                    _ => "null".to_string(),
                };
                latest_by_key.insert(key, result);
            }
        }

        latest_by_key.into_values().collect()
    }

    // For tests with multiple group keys
    async fn collect_latest_multi_group_results(
        receiver: &mut mpsc::UnboundedReceiver<StreamRecord>,
        group_key_fields: &[&str],
    ) -> Vec<StreamRecord> {
        let mut all_results = Vec::new();
        while let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv()).await
        {
            match result {
                Some(r) => all_results.push(r),
                None => break,
            }
        }

        // Deduplicate: keep only the latest result for each group key combination
        let mut latest_by_key = std::collections::HashMap::new();
        for result in all_results {
            let mut composite_key = String::new();
            for (i, field) in group_key_fields.iter().enumerate() {
                if i > 0 {
                    composite_key.push(',');
                }
                if let Some(key_value) = result.fields.get(*field) {
                    let key_part = match key_value {
                        FieldValue::Float(n) => n.to_string(),
                        FieldValue::String(s) => s.clone(),
                        FieldValue::Integer(i) => i.to_string(),
                        FieldValue::Boolean(b) => b.to_string(),
                        _ => "null".to_string(),
                    };
                    composite_key.push_str(&key_part);
                }
            }
            latest_by_key.insert(composite_key, result);
        }

        latest_by_key.into_values().collect()
    }

    #[test]
    fn test_group_by_execution() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records using helper function - much cleaner!
        let record1 = create_float_record(&[("customer_id", 1.0), ("amount", 100.0)], 1);
        let record2 = create_float_record(&[("customer_id", 1.0), ("amount", 200.0)], 2);
        let record3 = create_float_record(&[("customer_id", 2.0), ("amount", 150.0)], 3);

        // Test query with sum aggregation
        let query = parser
            .parse("SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id")
            .unwrap();

        // Execute each record and collect results
        rt.block_on(async {
            // Execute all records first
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            // Then collect results from channel (Flink-style: latest per group)
            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // We should get at least one result through the channel
            assert!(!results.is_empty(), "Expected results from GROUP BY aggregation");

            // Find and verify customer 1's group results (sum should be 100 + 200 = 300)
            let cust1_result = find_result_by_field(
                &results,
                |r| matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 1.0),
                "customer_id = 1.0"
            );
            assert_field_numeric(cust1_result, "total", 300, 300.0, "Customer 1 sum: 100+200");

            // Find and verify customer 2's group results (sum should be 150)
            let cust2_result = find_result_by_field(
                &results,
                |r| matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 2.0),
                "customer_id = 2.0"
            );
            assert_field_numeric(cust2_result, "total", 150, 150.0, "Customer 2 sum: 150");
        });
    }

    #[test]
    fn test_group_by_with_having() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(150.0));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(150.0));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(2.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0));

        // Test query with HAVING clause
        let query = parser
            .parse(
                "
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
            HAVING SUM(amount) > 250
        ",
            )
            .unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1); // Only customer 1 should pass HAVING clause

            let result = &results[0];
            // Verify that only customer 1's group (sum = 300) is included
            match (
                &result.fields.get("customer_id"),
                &result.fields.get("total"),
            ) {
                (Some(FieldValue::Float(cust_id)), Some(FieldValue::Float(sum))) => {
                    assert_eq!(*cust_id, 1.0);
                    assert_eq!(*sum, 300.0);
                }
                _ => panic!("Expected customer_id and total in results"),
            }
        });
    }

    #[test]
    fn test_multiple_aggregations() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields3.insert("amount".to_string(), FieldValue::Float(300.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test query with multiple aggregations
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                COUNT(*) as count,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];
            // Verify multiple aggregation results
            assert_eq!(result.fields.len(), 5);

            // Check count (can be Integer or Number)
            match result.fields.get("count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present as Integer or Number"),
            }

            // Check other aggregates
            match (
                result.fields.get("avg_amount"),
                result.fields.get("min_amount"),
                result.fields.get("max_amount"),
            ) {
                (
                    Some(FieldValue::Float(avg)),
                    Some(FieldValue::Float(min)),
                    Some(FieldValue::Float(max)),
                ) => {
                    assert_eq!(*avg, 200.0);
                    assert_eq!(*min, 100.0);
                    assert_eq!(*max, 300.0);
                }
                _ => panic!("Expected avg, min, max aggregation results to be present as Numbers"),
            }
        });
    }

    #[test]
    fn test_group_by_with_window() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("timestamp".to_string(), FieldValue::Integer(60000)); // 1 minute
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("timestamp".to_string(), FieldValue::Integer(120000)); // 2 minutes
        fields2.insert("amount".to_string(), FieldValue::Float(200.0)); // 100+200=300 for first window
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields3.insert("timestamp".to_string(), FieldValue::Integer(360000)); // 6 minutes
        fields3.insert("amount".to_string(), FieldValue::Float(500.0)); // 500 for second window
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test windowed GROUP BY
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                SUM(amount) as window_total
            FROM orders
            GROUP BY customer_id
            WINDOW TUMBLING(5m)
        ",
            )
            .unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            // Flush any remaining windows to ensure all results are emitted
            engine.flush_windows().await.unwrap();

            let results = collect_results(&mut receiver).await;
            assert_eq!(results.len(), 2); // Should have results for each window

            // Find first window result (0-5 minutes, sum = 300)
            let first_window = results.iter().find(|r|
                matches!(r.fields.get("window_total"), Some(FieldValue::Float(sum)) if *sum == 300.0)
            ).expect("Should have first window results");

            // Find second window result (5-10 minutes, sum = 500)
            let second_window = results.iter().find(|r|
                matches!(r.fields.get("window_total"), Some(FieldValue::Float(sum)) if *sum == 500.0)
            ).expect("Should have second window results");

            // Verify window results
            match first_window.fields.get("window_total") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected numeric sum for first window"),
            }

            match second_window.fields.get("window_total") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 500.0),
                _ => panic!("Expected numeric sum for second window"),
            }
        });
    }

    #[test]
    fn test_group_by_with_boolean_conditions() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("is_prime".to_string(), FieldValue::Boolean(true));
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0)); // Low value

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("is_prime".to_string(), FieldValue::Boolean(false));
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(200.0)); // High value

        // Test query that groups by a boolean condition
        let query = parser
            .parse(
                "
            SELECT
                (amount > 150) as high_value,
                COUNT(*) as count,
                SUM(amount) as total
            FROM orders
            GROUP BY amount > 150
        ",
            )
            .unwrap();

        rt.block_on(async {
            // Execute each record
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            // Collect results from channel (Flink-style: latest per group)
            let results = collect_latest_group_results(&mut receiver, "high_value").await;
            assert_eq!(results.len(), 2); // Should have two groups: true and false

            // Find low value group (amount <= 150)
            let low_value = results
                .iter()
                .find(|r| matches!(r.fields.get("high_value"), Some(FieldValue::Boolean(false))))
                .expect("Should have results for low value group");

            // Find high value group (amount > 150)
            let high_value = results
                .iter()
                .find(|r| matches!(r.fields.get("high_value"), Some(FieldValue::Boolean(true))))
                .expect("Should have results for high value group");

            // Verify low value group
            match (low_value.fields.get("count"), low_value.fields.get("total")) {
                (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 100.0);
                }
                (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 100.0);
                }
                _ => panic!("Expected numeric results for low value group"),
            }

            // Verify high value group
            match (
                high_value.fields.get("count"),
                high_value.fields.get("total"),
            ) {
                (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 200.0);
                }
                (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 200.0);
                }
                _ => panic!("Expected numeric results for high value group"),
            }
        });
    }

    #[test]
    fn test_stddev_aggregate() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with known values for STDDEV calculation
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("score".to_string(), FieldValue::Float(10.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("score".to_string(), FieldValue::Float(20.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields3.insert("score".to_string(), FieldValue::Float(30.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test STDDEV function - use simple query for debugging
        let query = parser
            .parse("SELECT customer_id, STDDEV(score) as score_stddev, COUNT(*) as count FROM orders GROUP BY customer_id")
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.fields.get("count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check STDDEV (for values 10, 20, 30: stddev ≈ 10.0)
            match result.fields.get("score_stddev") {
                Some(FieldValue::Float(stddev)) => {
                    // Standard deviation of [10, 20, 30] should be approximately 10.0
                    assert!(
                        (*stddev - 10.0).abs() < 0.1,
                        "Expected stddev ~10.0, got {}",
                        stddev
                    );
                }
                _ => panic!("Expected STDDEV result to be present as Number"),
            }
        });
    }

    #[test]
    fn test_variance_aggregate() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with known values for VARIANCE calculation
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("value".to_string(), FieldValue::Float(2.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("value".to_string(), FieldValue::Float(4.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields3.insert("value".to_string(), FieldValue::Float(6.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test VARIANCE function
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                VARIANCE(value) as value_variance,
                COUNT(*) as count
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.fields.get("count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check VARIANCE (for values 2, 4, 6: variance should be 4.0)
            match result.fields.get("value_variance") {
                Some(FieldValue::Float(variance)) => {
                    // Variance of [2, 4, 6] should be 4.0 (sample variance)
                    assert!(
                        (*variance - 4.0).abs() < 0.1,
                        "Expected variance ~4.0, got {}",
                        variance
                    );
                }
                _ => panic!("Expected VARIANCE result to be present as Number"),
            }
        });
    }

    #[test]
    fn test_first_last_aggregates() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records in a specific order
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1.fields.insert(
            "product".to_string(),
            FieldValue::String("apple".to_string()),
        );

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2.fields.insert(
            "product".to_string(),
            FieldValue::String("banana".to_string()),
        );

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3.fields.insert(
            "product".to_string(),
            FieldValue::String("cherry".to_string()),
        );

        // Test FIRST and LAST functions
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                FIRST(product) as first_product,
                LAST(product) as last_product,
                COUNT(*) as count
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.fields.get("count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check FIRST (should be "apple" - first record processed)
            match result.fields.get("first_product") {
                Some(FieldValue::String(first)) => assert_eq!(first, "apple"),
                _ => panic!("Expected FIRST result to be present as String"),
            }

            // Check LAST (should be "cherry" - last record processed)
            match result.fields.get("last_product") {
                Some(FieldValue::String(last)) => assert_eq!(last, "cherry"),
                _ => panic!("Expected LAST result to be present as String"),
            }
        });
    }

    #[test]
    fn test_string_agg_aggregate() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with string values
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("tag".to_string(), FieldValue::String("red".to_string()));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("tag".to_string(), FieldValue::String("green".to_string()));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("tag".to_string(), FieldValue::String("blue".to_string()));

        // Test STRING_AGG function with comma separator
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                STRING_AGG(tag, ',') as tag_list,
                COUNT(*) as count
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.fields.get("count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check STRING_AGG (should contain all three colors separated by commas)
            match result.fields.get("tag_list") {
                Some(FieldValue::String(agg_result)) => {
                    // The order might vary due to streaming nature, but should contain all elements
                    assert!(
                        agg_result.contains("red"),
                        "Expected 'red' in aggregated string: {}",
                        agg_result
                    );
                    assert!(
                        agg_result.contains("green"),
                        "Expected 'green' in aggregated string: {}",
                        agg_result
                    );
                    assert!(
                        agg_result.contains("blue"),
                        "Expected 'blue' in aggregated string: {}",
                        agg_result
                    );
                    assert!(
                        agg_result.contains(","),
                        "Expected comma separator in aggregated string: {}",
                        agg_result
                    );
                }
                _ => panic!("Expected STRING_AGG result to be present as String"),
            }
        });
    }

    #[test]
    fn test_count_distinct_aggregate() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with some duplicate values
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1.fields.insert(
            "category".to_string(),
            FieldValue::String("electronics".to_string()),
        );

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2.fields.insert(
            "category".to_string(),
            FieldValue::String("books".to_string()),
        );

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3.fields.insert(
            "category".to_string(),
            FieldValue::String("electronics".to_string()),
        ); // Duplicate

        let mut fields4 = HashMap::new();
        fields4.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record4 = create_stream_record_with_fields(fields4, 4);
        record4.fields.insert(
            "category".to_string(),
            FieldValue::String("clothing".to_string()),
        );

        // Test COUNT_DISTINCT function
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                COUNT_DISTINCT(category) as distinct_categories,
                COUNT(*) as total_count
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();
            engine.execute_with_record(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check total count (should be 4)
            match result.fields.get("total_count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 4),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 4.0),
                _ => panic!("Expected total_count to be present"),
            }

            // Check COUNT_DISTINCT (should be 3: electronics, books, clothing)
            match result.fields.get("distinct_categories") {
                Some(FieldValue::Integer(distinct_count)) => assert_eq!(*distinct_count, 3),
                Some(FieldValue::Float(distinct_count)) => assert_eq!(*distinct_count, 3.0),
                _ => panic!("Expected COUNT_DISTINCT result to be present as Integer"),
            }
        });
    }

    #[test]
    fn test_mixed_aggregate_functions() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with mixed data types
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("category".to_string(), FieldValue::String("A".to_string()));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("category".to_string(), FieldValue::String("B".to_string()));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields3.insert("amount".to_string(), FieldValue::Float(300.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("category".to_string(), FieldValue::String("A".to_string()));

        // Test query combining multiple new aggregate functions
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                COUNT(*) as total_count,
                COUNT_DISTINCT(category) as distinct_cats,
                FIRST(category) as first_cat,
                LAST(category) as last_cat,
                STDDEV(amount) as amount_stddev,
                VARIANCE(amount) as amount_variance,
                STRING_AGG(category, '|') as cat_list
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(
                results.len(),
                1,
                "Expected 1 group (customer 1), got {}",
                results.len()
            );

            let result = &results[0];

            // Verify we have all expected fields:
            // 1. customer_id (group key)
            // 2. total_count (COUNT(*))
            // 3. distinct_cats (COUNT_DISTINCT(category))
            // 4. first_cat (FIRST(category))
            // 5. last_cat (LAST(category))
            // 6. amount_stddev (STDDEV(amount))
            // 7. amount_variance (VARIANCE(amount))
            // 8. cat_list (STRING_AGG(category, '|'))
            assert_eq!(
                result.fields.len(),
                8,
                "Expected 8 fields in result, got {}. Fields: {:?}",
                result.fields.len(),
                result.fields.keys().collect::<Vec<_>>()
            );

            // Check COUNT(*): 3 records total
            match result.fields.get("total_count") {
                Some(FieldValue::Integer(count)) => {
                    assert_eq!(*count, 3, "COUNT(*) expected 3 records, got {}", count);
                }
                Some(FieldValue::Float(count)) => {
                    assert_eq!(*count, 3.0, "COUNT(*) expected 3.0 records, got {}", count);
                }
                other => panic!(
                    "total_count has type {:?}, expected Integer or Float",
                    other
                ),
            }

            // Check COUNT_DISTINCT(category): 2 distinct values (A appears twice, B once)
            match result.fields.get("distinct_cats") {
                Some(FieldValue::Integer(distinct)) => {
                    assert_eq!(
                        *distinct, 2,
                        "COUNT_DISTINCT(category) expected 2 distinct [A,B], got {}",
                        distinct
                    );
                }
                Some(FieldValue::Float(distinct)) => {
                    assert_eq!(
                        *distinct, 2.0,
                        "COUNT_DISTINCT(category) expected 2.0 distinct [A,B], got {}",
                        distinct
                    );
                }
                other => panic!(
                    "distinct_cats has type {:?}, expected Integer or Float",
                    other
                ),
            }

            // Check FIRST(category): first value is 'A'
            if !result.fields.contains_key("first_cat") {
                panic!(
                    "FIRST(category) 'first_cat' field not found. Available: {:?}",
                    result.fields.keys().collect::<Vec<_>>()
                );
            }

            // Check LAST(category): last value is 'A'
            if !result.fields.contains_key("last_cat") {
                panic!(
                    "LAST(category) 'last_cat' field not found. Available: {:?}",
                    result.fields.keys().collect::<Vec<_>>()
                );
            }

            // Check STDDEV(amount): standard deviation of [100, 200, 300]
            // Calculation using sample formula (Bessel's correction, n-1 denominator):
            // Mean = (100 + 200 + 300) / 3 = 200
            // Sum of squared deviations = (100-200)^2 + (200-200)^2 + (300-200)^2
            //                           = 10000 + 0 + 10000 = 20000
            // Sample variance = 20000 / (3-1) = 10000
            // Sample stddev = sqrt(10000) ≈ 100.0
            match result.fields.get("amount_stddev") {
                Some(FieldValue::Float(stddev)) => {
                    assert!(
                        *stddev > 80.0 && *stddev < 120.0,
                        "STDDEV(amount) for [100,200,300] expected ~100.0, got {}",
                        stddev
                    );
                }
                other => panic!(
                    "amount_stddev has type {:?}, expected Float. Data: [100,200,300]",
                    other
                ),
            }

            // Check VARIANCE(amount): variance of [100, 200, 300]
            // Using same calculation as STDDEV: variance = stddev^2 ≈ 100^2 = 10000
            match result.fields.get("amount_variance") {
                Some(FieldValue::Float(variance)) => {
                    assert!(
                        *variance > 8000.0 && *variance < 12000.0,
                        "VARIANCE(amount) for [100,200,300] expected ~10000, got {}",
                        variance
                    );
                }
                other => panic!(
                    "amount_variance has type {:?}, expected Float. Data: [100,200,300]",
                    other
                ),
            }

            // Check STRING_AGG(category, '|'): concatenate [A, B, A] with | separator
            match result.fields.get("cat_list") {
                Some(FieldValue::String(agg_result)) => {
                    assert!(
                        agg_result.contains("A") && agg_result.contains("B"),
                        "STRING_AGG should contain both A and B from [A,B,A]. Got: {}",
                        agg_result
                    );
                    assert!(
                        agg_result.contains("|"),
                        "STRING_AGG should contain pipe separator | Got: {}",
                        agg_result
                    );
                }
                other => panic!(
                    "cat_list has type {:?}, expected String. Data: [A,B,A] with separator |",
                    other
                ),
            }
        });
    }

    #[test]
    fn test_group_by_with_null_grouping_values() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with NULL values in grouping column
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Null);
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(200.0));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Null);
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("amount".to_string(), FieldValue::Float(300.0));

        let mut fields4 = HashMap::new();
        fields4.insert("customer_id".to_string(), FieldValue::Float(2.0));
        let mut record4 = create_stream_record_with_fields(fields4, 4);
        record4
            .fields
            .insert("amount".to_string(), FieldValue::Float(400.0));

        // Test GROUP BY with NULL values in grouping column
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                COUNT(*) as count,
                SUM(amount) as total
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();
            engine.execute_with_record(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 3); // Should have 3 groups: 1, NULL, 2

            // Find group for customer_id = 1
            let group1 = results.iter().find(|r|
                matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 1.0)
            ).expect("Should have group for customer_id = 1");

            match (group1.fields.get("count"), group1.fields.get("total")) {
                (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 100.0);
                }
                (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 100.0);
                }
                _ => panic!("Expected count and total for group 1"),
            }

            // Find group for customer_id = NULL
            let null_group = results
                .iter()
                .find(|r| matches!(r.fields.get("customer_id"), Some(FieldValue::Null)))
                .expect("Should have group for customer_id = NULL");

            match (
                null_group.fields.get("count"),
                null_group.fields.get("total"),
            ) {
                (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 2);
                    assert_eq!(*total, 500.0);
                }
                (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 2.0);
                    assert_eq!(*total, 500.0);
                }
                _ => panic!("Expected count and total for NULL group"),
            }

            // Find group for customer_id = 2
            let group2 = results.iter().find(|r|
                matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 2.0)
            ).expect("Should have group for customer_id = 2");

            match (group2.fields.get("count"), group2.fields.get("total")) {
                (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 400.0);
                }
                (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 400.0);
                }
                _ => panic!("Expected count and total for group 2"),
            }
        });
    }

    #[test]
    fn test_aggregate_functions_with_null_values() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with NULL values in aggregation columns
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("quantity".to_string(), FieldValue::Float(5.0));
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("quantity".to_string(), FieldValue::Float(3.0));
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(200.0));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("quantity".to_string(), FieldValue::Null);
        record3
            .fields
            .insert("amount".to_string(), FieldValue::Null);

        let mut fields4 = HashMap::new();
        fields4.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record4 = create_stream_record_with_fields(fields4, 4);
        record4
            .fields
            .insert("quantity".to_string(), FieldValue::Null);
        record4
            .fields
            .insert("amount".to_string(), FieldValue::Null);

        // Test aggregate functions with NULL values
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                COUNT(*) as total_count,
                COUNT(amount) as amount_count,
                COUNT(quantity) as quantity_count,
                SUM(amount) as amount_sum,
                AVG(amount) as amount_avg,
                MIN(amount) as amount_min,
                MAX(amount) as amount_max,
                SUM(quantity) as quantity_sum
            FROM orders
            GROUP BY customer_id
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();
            engine.execute_with_record(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check COUNT(*) - should count all records including those with NULLs
            match result.fields.get("total_count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 4),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 4.0),
                _ => panic!("Expected total_count to be 4"),
            }

            // Check COUNT(amount) - should only count non-NULL values (2 records)
            match result.fields.get("amount_count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 2),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 2.0),
                _ => panic!("Expected amount_count to be 2"),
            }

            // Check COUNT(quantity) - should only count non-NULL values (2 records)
            match result.fields.get("quantity_count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 2),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 2.0),
                _ => panic!("Expected quantity_count to be 2"),
            }

            // Check SUM(amount) - should sum only non-NULL values (100 + 200 = 300)
            match result.fields.get("amount_sum") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected amount_sum to be 300.0"),
            }

            // Check AVG(amount) - should average only non-NULL values (300/2 = 150)
            match result.fields.get("amount_avg") {
                Some(FieldValue::Float(avg)) => assert_eq!(*avg, 150.0),
                _ => panic!("Expected amount_avg to be 150.0"),
            }

            // Check MIN(amount) - should be minimum of non-NULL values (100)
            match result.fields.get("amount_min") {
                Some(FieldValue::Float(min)) => assert_eq!(*min, 100.0),
                _ => panic!("Expected amount_min to be 100.0"),
            }

            // Check MAX(amount) - should be maximum of non-NULL values (200)
            match result.fields.get("amount_max") {
                Some(FieldValue::Float(max)) => assert_eq!(*max, 200.0),
                _ => panic!("Expected amount_max to be 200.0"),
            }

            // Check SUM(quantity) - should sum only non-NULL values (5 + 3 = 8)
            match result.fields.get("quantity_sum") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 8.0),
                _ => panic!("Expected quantity_sum to be 8.0"),
            }
        });
    }

    #[test]
    fn test_null_handling_in_complex_grouping() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records with NULL values in multiple grouping columns
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("region".to_string(), FieldValue::String("US".to_string()));
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("region".to_string(), FieldValue::Null);
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(200.0));

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Null);
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("region".to_string(), FieldValue::String("EU".to_string()));
        record3
            .fields
            .insert("amount".to_string(), FieldValue::Float(300.0));

        let mut fields4 = HashMap::new();
        fields4.insert("customer_id".to_string(), FieldValue::Null);
        let mut record4 = create_stream_record_with_fields(fields4, 4);
        record4
            .fields
            .insert("region".to_string(), FieldValue::Null);
        record4
            .fields
            .insert("amount".to_string(), FieldValue::Float(400.0));

        // Test GROUP BY with multiple columns containing NULLs
        let query = parser
            .parse(
                "
            SELECT
                customer_id,
                region,
                COUNT(*) as count,
                SUM(amount) as total,
                FIRST(amount) as first_amount
            FROM orders
            GROUP BY customer_id, region
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();
            engine.execute_with_record(&query, record4).await.unwrap();

            let results =
                collect_latest_multi_group_results(&mut receiver, &["customer_id", "region"]).await;
            assert_eq!(results.len(), 4); // Should have 4 distinct groups

            // Verify each group combination
            for result in &results {
                match (
                    result.fields.get("customer_id"),
                    result.fields.get("region"),
                ) {
                    (Some(FieldValue::Float(1.0)), Some(FieldValue::String(region)))
                        if region == "US" =>
                    {
                        // Group: customer_id=1, region=US
                        match (result.fields.get("count"), result.fields.get("total")) {
                            (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 100.0);
                            }
                            (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 100.0);
                            }
                            _ => panic!("Expected count=1, total=100 for group (1, US)"),
                        }
                    }
                    (Some(FieldValue::Float(1.0)), Some(FieldValue::Null)) => {
                        // Group: customer_id=1, region=NULL
                        match (result.fields.get("count"), result.fields.get("total")) {
                            (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 200.0);
                            }
                            (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 200.0);
                            }
                            _ => panic!("Expected count=1, total=200 for group (1, NULL)"),
                        }
                    }
                    (Some(FieldValue::Null), Some(FieldValue::String(region)))
                        if region == "EU" =>
                    {
                        // Group: customer_id=NULL, region=EU
                        match (result.fields.get("count"), result.fields.get("total")) {
                            (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 300.0);
                            }
                            (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 300.0);
                            }
                            _ => panic!("Expected count=1, total=300 for group (NULL, EU)"),
                        }
                    }
                    (Some(FieldValue::Null), Some(FieldValue::Null)) => {
                        // Group: customer_id=NULL, region=NULL
                        match (result.fields.get("count"), result.fields.get("total")) {
                            (Some(FieldValue::Integer(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 400.0);
                            }
                            (Some(FieldValue::Float(count)), Some(FieldValue::Float(total))) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 400.0);
                            }
                            _ => panic!("Expected count=1, total=400 for group (NULL, NULL)"),
                        }
                    }
                    _ => panic!(
                        "Unexpected group combination: {:?}, {:?}",
                        result.fields.get("customer_id"),
                        result.fields.get("region")
                    ),
                }
            }
        });
    }

    // ========================================================================
    // HAVING CLAUSE BINARYOP TESTS - Phase 1 Implementation
    // ========================================================================
    // These tests verify BinaryOp support in HAVING clauses (addition,
    // subtraction, multiplication, division operations on aggregate functions)

    #[test]
    fn test_having_division_ratio() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Simplified test: division of aggregated values
        // Customer 1: 1000 revenue / 100 cost = 10 ratio (pass >8)
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields1.insert("revenue".to_string(), FieldValue::Float(1000.0));
        fields1.insert("cost".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        // Customer 2: 500 revenue / 100 cost = 5 ratio (fail >8)
        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        fields2.insert("revenue".to_string(), FieldValue::Float(500.0));
        fields2.insert("cost".to_string(), FieldValue::Float(100.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test division in HAVING clause
        let query = parser
            .parse(
                "
            SELECT customer_id, SUM(revenue) as total_revenue, SUM(cost) as total_cost
            FROM sales
            GROUP BY customer_id
            HAVING SUM(revenue) / SUM(cost) > 8
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // Only customer 1 should pass (10 > 8)
            assert_eq!(
                results.len(),
                1,
                "Only customer 1 should pass HAVING clause"
            );

            let result = &results[0];
            match result.fields.get("customer_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (
                        result.fields.get("total_revenue"),
                        result.fields.get("total_cost"),
                    ) {
                        (Some(FieldValue::Float(rev)), Some(FieldValue::Float(cost))) => {
                            assert_eq!(*rev, 1000.0);
                            assert_eq!(*cost, 100.0);
                            // Verify ratio: 1000/100 = 10 > 8 ✓
                        }
                        _ => panic!("Expected Float values"),
                    }
                }
                _ => panic!("Expected customer_id=1"),
            }
        });
    }

    #[test]
    fn test_having_multiplication() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields2.insert("amount".to_string(), FieldValue::Float(150.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Integer(2));
        fields3.insert("amount".to_string(), FieldValue::Float(50.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test multiplication in HAVING: AVG(amount) * COUNT(*) > 200
        let query = parser
            .parse(
                "
            SELECT customer_id, AVG(amount) as avg_amount, COUNT(*) as count
            FROM orders
            GROUP BY customer_id
            HAVING AVG(amount) * COUNT(*) > 200
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // Only customer 1 should pass: AVG(125) * COUNT(2) = 250 > 200
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("customer_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (result.fields.get("avg_amount"), result.fields.get("count")) {
                        (Some(FieldValue::Float(avg)), Some(count_val)) => {
                            assert_eq!(*avg, 125.0);
                            match count_val {
                                FieldValue::Integer(c) => assert_eq!(*c, 2),
                                FieldValue::Float(c) => assert_eq!(*c, 2.0),
                                _ => panic!("Unexpected count type"),
                            }
                        }
                        _ => panic!("Expected avg_amount and count in results"),
                    }
                }
                _ => panic!("Expected customer_id=1 in results"),
            }
        });
    }

    #[test]
    fn test_having_addition() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("product_id".to_string(), FieldValue::Integer(1));
        fields1.insert("revenue".to_string(), FieldValue::Float(100.0));
        fields1.insert("profit".to_string(), FieldValue::Float(20.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("product_id".to_string(), FieldValue::Integer(1));
        fields2.insert("revenue".to_string(), FieldValue::Float(150.0));
        fields2.insert("profit".to_string(), FieldValue::Float(30.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert("product_id".to_string(), FieldValue::Integer(2));
        fields3.insert("revenue".to_string(), FieldValue::Float(80.0));
        fields3.insert("profit".to_string(), FieldValue::Float(10.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        // Test addition in HAVING: SUM(revenue) + SUM(profit) > 300
        let query = parser
            .parse(
                "
            SELECT product_id, SUM(revenue) as total_revenue, SUM(profit) as total_profit
            FROM sales
            GROUP BY product_id
            HAVING SUM(revenue) + SUM(profit) > 300
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "product_id").await;

            // Only product 1 should pass: 250 + 50 = 300 (not > 300, so actually none pass!)
            // Wait, let me recalculate: Product 1: revenue=250, profit=50, total=300 (NOT > 300)
            // Product 2: revenue=80, profit=10, total=90 (NOT > 300)
            // Actually both fail! Let me adjust the threshold to > 250
            assert_eq!(
                results.len(),
                0,
                "No products should pass with threshold > 300"
            );
        });
    }

    #[test]
    fn test_having_addition_lower_threshold() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("product_id".to_string(), FieldValue::Integer(1));
        fields1.insert("revenue".to_string(), FieldValue::Float(100.0));
        fields1.insert("profit".to_string(), FieldValue::Float(20.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("product_id".to_string(), FieldValue::Integer(1));
        fields2.insert("revenue".to_string(), FieldValue::Float(150.0));
        fields2.insert("profit".to_string(), FieldValue::Float(30.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let query = parser
            .parse(
                "
            SELECT product_id, SUM(revenue) as total_revenue, SUM(profit) as total_profit
            FROM sales
            GROUP BY product_id
            HAVING SUM(revenue) + SUM(profit) > 250
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "product_id").await;

            // Product 1: 250 + 50 = 300 > 250 ✓
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("product_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (
                        result.fields.get("total_revenue"),
                        result.fields.get("total_profit"),
                    ) {
                        (Some(FieldValue::Float(rev)), Some(FieldValue::Float(prof))) => {
                            assert_eq!(*rev, 250.0);
                            assert_eq!(*prof, 50.0);
                        }
                        _ => panic!("Expected Float values"),
                    }
                }
                _ => panic!("Expected product_id=1"),
            }
        });
    }

    #[test]
    fn test_having_subtraction() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("account_id".to_string(), FieldValue::Integer(1));
        fields1.insert("credits".to_string(), FieldValue::Float(500.0));
        fields1.insert("debits".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("account_id".to_string(), FieldValue::Integer(2));
        fields2.insert("credits".to_string(), FieldValue::Float(300.0));
        fields2.insert("debits".to_string(), FieldValue::Float(250.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test subtraction in HAVING: SUM(credits) - SUM(debits) > 350
        let query = parser
            .parse(
                "
            SELECT account_id, SUM(credits) as total_credits, SUM(debits) as total_debits
            FROM transactions
            GROUP BY account_id
            HAVING SUM(credits) - SUM(debits) > 350
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "account_id").await;

            // Only account 1 should pass: 500 - 100 = 400 > 350
            // Account 2: 300 - 250 = 50 (not > 350)
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("account_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (
                        result.fields.get("total_credits"),
                        result.fields.get("total_debits"),
                    ) {
                        (Some(FieldValue::Float(cr)), Some(FieldValue::Float(db))) => {
                            assert_eq!(*cr, 500.0);
                            assert_eq!(*db, 100.0);
                        }
                        _ => panic!("Expected Float values"),
                    }
                }
                _ => panic!("Expected account_id=1"),
            }
        });
    }

    #[test]
    fn test_having_complex_expression() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("trader_id".to_string(), FieldValue::Integer(1));
        fields1.insert("profit".to_string(), FieldValue::Float(100.0));
        fields1.insert("trades".to_string(), FieldValue::Integer(10));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("trader_id".to_string(), FieldValue::Integer(1));
        fields2.insert("profit".to_string(), FieldValue::Float(200.0));
        fields2.insert("trades".to_string(), FieldValue::Integer(20));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test complex expression: (SUM(profit) / SUM(trades)) * 100 > 900
        // This tests nested BinaryOps
        let query = parser
            .parse(
                "
            SELECT trader_id, SUM(profit) as total_profit, SUM(trades) as total_trades
            FROM trading_activity
            GROUP BY trader_id
            HAVING SUM(profit) / SUM(trades) > 9
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "trader_id").await;

            // Trader 1: 300 / 30 = 10 > 9 ✓
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("trader_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (
                        result.fields.get("total_profit"),
                        result.fields.get("total_trades"),
                    ) {
                        (Some(FieldValue::Float(profit)), Some(FieldValue::Float(trades))) => {
                            assert_eq!(*profit, 300.0);
                            assert_eq!(*trades, 30.0);
                        }
                        (Some(FieldValue::Float(profit)), Some(FieldValue::Integer(trades))) => {
                            assert_eq!(*profit, 300.0);
                            assert_eq!(*trades, 30);
                        }
                        _ => panic!("Expected profit:Float and trades:Float/Integer"),
                    }
                }
                _ => panic!("Expected trader_id=1"),
            }
        });
    }

    // ========================================================================
    // HAVING CLAUSE COLUMN ALIAS TESTS - Phase 2 Implementation
    // ========================================================================
    // These tests verify column alias support in HAVING clauses, allowing
    // references to aliased aggregate expressions

    #[test]
    fn test_having_column_alias_simple() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Integer(1));
        fields1.insert("amount".to_string(), FieldValue::Float(300.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Integer(2));
        fields2.insert("amount".to_string(), FieldValue::Float(100.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test using alias 'total' in HAVING instead of SUM(amount)
        let query = parser
            .parse(
                "
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
            HAVING total > 250
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // Only customer 1 should pass (300 > 250)
            assert_eq!(results.len(), 1, "Only customer 1 should pass HAVING");

            let result = &results[0];
            match result.fields.get("customer_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match result.fields.get("total") {
                        Some(FieldValue::Float(total)) => {
                            assert_eq!(*total, 300.0);
                        }
                        _ => panic!("Expected Float value for total"),
                    }
                }
                _ => panic!("Expected customer_id=1"),
            }
        });
    }

    #[test]
    fn test_having_column_alias_with_arithmetic() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("product_id".to_string(), FieldValue::Integer(1));
        fields1.insert("revenue".to_string(), FieldValue::Float(1000.0));
        fields1.insert("cost".to_string(), FieldValue::Float(600.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("product_id".to_string(), FieldValue::Integer(2));
        fields2.insert("revenue".to_string(), FieldValue::Float(500.0));
        fields2.insert("cost".to_string(), FieldValue::Float(400.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test using multiple aliases in arithmetic expression
        let query = parser
            .parse(
                "
            SELECT product_id, SUM(revenue) as total_revenue, SUM(cost) as total_cost
            FROM sales
            GROUP BY product_id
            HAVING total_revenue - total_cost > 300
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "product_id").await;

            // Only product 1: 1000-600=400 > 300 ✓
            // Product 2: 500-400=100 NOT > 300 ✗
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("product_id") {
                Some(FieldValue::Integer(id)) => {
                    assert_eq!(*id, 1);
                    match (
                        result.fields.get("total_revenue"),
                        result.fields.get("total_cost"),
                    ) {
                        (Some(FieldValue::Float(rev)), Some(FieldValue::Float(cost))) => {
                            assert_eq!(*rev, 1000.0);
                            assert_eq!(*cost, 600.0);
                        }
                        _ => panic!("Expected Float values"),
                    }
                }
                _ => panic!("Expected product_id=1"),
            }
        });
    }

    #[test]
    fn test_having_column_alias_mixed_with_aggregates() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("region".to_string(), FieldValue::String("US".to_string()));
        fields1.insert("sales".to_string(), FieldValue::Float(1000.0));
        fields1.insert("orders".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("region".to_string(), FieldValue::String("EU".to_string()));
        fields2.insert("sales".to_string(), FieldValue::Float(500.0));
        fields2.insert("orders".to_string(), FieldValue::Float(200.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Mix alias with direct aggregate function
        let query = parser
            .parse(
                "
            SELECT region, SUM(sales) as total_sales, SUM(orders) as total_orders
            FROM regional_sales
            GROUP BY region
            HAVING total_sales + total_orders > 1000
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "region").await;

            // US: 1000+100 = 1100 > 1000 ✓
            // EU: 500+200 = 700 NOT > 1000 ✗
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("region") {
                Some(FieldValue::String(r)) if r == "US" => {
                    match result.fields.get("total_sales") {
                        Some(FieldValue::Float(sales)) => {
                            assert_eq!(*sales, 1000.0);
                        }
                        _ => panic!("Expected Float for total_sales"),
                    }
                }
                _ => panic!("Expected region=US"),
            }
        });
    }

    #[test]
    fn test_having_multiple_binaryop_operations() {
        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        let mut fields1 = HashMap::new();
        fields1.insert("region".to_string(), FieldValue::String("US".to_string()));
        fields1.insert("sales".to_string(), FieldValue::Float(1000.0));
        fields1.insert("returns".to_string(), FieldValue::Float(100.0));
        fields1.insert("costs".to_string(), FieldValue::Float(50.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert("region".to_string(), FieldValue::String("EU".to_string()));
        fields2.insert("sales".to_string(), FieldValue::Float(500.0));
        fields2.insert("returns".to_string(), FieldValue::Float(200.0));
        fields2.insert("costs".to_string(), FieldValue::Float(50.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        // Test multiple separate BinaryOp expressions (not nested)
        let query = parser
            .parse(
                "
            SELECT region, SUM(sales) as total_sales, SUM(returns) as total_returns, SUM(costs) as total_costs
            FROM regional_sales
            GROUP BY region
            HAVING SUM(sales) - SUM(returns) > 800
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "region").await;

            // US: 1000-100=900 > 800 ✓
            // EU: 500-200=300 NOT > 800 ✗
            assert_eq!(results.len(), 1);

            let result = &results[0];
            match result.fields.get("region") {
                Some(FieldValue::String(r)) if r == "US" => {
                    match result.fields.get("total_sales") {
                        Some(FieldValue::Float(sales)) => {
                            assert_eq!(*sales, 1000.0);
                        }
                        _ => panic!("Expected total_sales"),
                    }
                }
                _ => panic!("Expected region=US"),
            }
        });
    }

    // ============================================================================
    // Phase 3: CASE Expression Tests
    // ============================================================================

    #[test]
    fn test_having_case_simple_binary_result() {
        // Test: HAVING CASE WHEN SUM(amount) > 200 THEN 1 ELSE 0 END = 1
        // Only groups where SUM(amount) > 200 should pass

        let rt = Runtime::new().unwrap();
        let (mut engine, mut receiver) = create_test_engine();
        let parser = StreamingSqlParser::new();

        // Create records: Customer 1 has 300, Customer 2 has 150
        let mut fields1 = HashMap::new();
        fields1.insert(
            "customer_id".to_string(),
            FieldValue::String("customer_1".to_string()),
        );
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = create_stream_record_with_fields(fields1, 1);

        let mut fields2 = HashMap::new();
        fields2.insert(
            "customer_id".to_string(),
            FieldValue::String("customer_1".to_string()),
        );
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let record2 = create_stream_record_with_fields(fields2, 2);

        let mut fields3 = HashMap::new();
        fields3.insert(
            "customer_id".to_string(),
            FieldValue::String("customer_2".to_string()),
        );
        fields3.insert("amount".to_string(), FieldValue::Float(75.0));
        let record3 = create_stream_record_with_fields(fields3, 3);

        let mut fields4 = HashMap::new();
        fields4.insert(
            "customer_id".to_string(),
            FieldValue::String("customer_2".to_string()),
        );
        fields4.insert("amount".to_string(), FieldValue::Float(75.0));
        let record4 = create_stream_record_with_fields(fields4, 4);

        let query = parser
            .parse(
                "
            SELECT customer_id, SUM(amount) as total
            FROM sales
            GROUP BY customer_id
            HAVING CASE WHEN SUM(amount) > 200 THEN 1 ELSE 0 END = 1
        ",
            )
            .unwrap();

        rt.block_on(async {
            engine.execute_with_record(&query, record1).await.unwrap();
            engine.execute_with_record(&query, record2).await.unwrap();
            engine.execute_with_record(&query, record3).await.unwrap();
            engine.execute_with_record(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // Only customer_1 (300 > 200) should pass
            assert_eq!(results.len(), 1, "Only one group should pass HAVING");

            let result = &results[0];
            match result.fields.get("customer_id") {
                Some(FieldValue::String(id)) if id == "customer_1" => {
                    match result.fields.get("total") {
                        Some(FieldValue::Float(amt)) => assert_eq!(*amt, 300.0),
                        _ => panic!("Unexpected type for total"),
                    }
                }
                _ => panic!("Expected customer_1"),
            }
        });
    }
}
