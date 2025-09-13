use ferrisstreams::ferris::serialization::{JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> (StreamExecutionEngine, mpsc::UnboundedReceiver<StreamRecord>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let format: Arc<dyn SerializationFormat> = Arc::new(JsonFormat);
        let engine = StreamExecutionEngine::new(sender);
        (engine, receiver)
    }

    fn create_stream_record_with_fields(
        fields: HashMap<String, FieldValue>,
        offset: i64,
    ) -> StreamRecord {
        StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
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
                assert_eq!(group_exprs.len(), 1);
                match &group_exprs[0] {
                    Expr::Column(name) => assert_eq!(name, "customer_id"),
                    _ => panic!("Expected column expression"),
                }
            }
            _ => panic!("Expected Select query with GROUP BY"),
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
                assert_eq!(group_exprs.len(), 2);
                match (&group_exprs[0], &group_exprs[1]) {
                    (Expr::Column(name1), Expr::Column(name2)) => {
                        assert_eq!(name1, "customer_id");
                        assert_eq!(name2, "region");
                    }
                    _ => panic!("Expected column expressions"),
                }
            }
            _ => panic!("Expected Select query with multiple GROUP BY"),
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

        // Create test records
        let mut fields1 = HashMap::new();
        fields1.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields1.insert("amount".to_string(), FieldValue::Float(100.0));
        let record1 = StreamRecord {
            fields: fields1,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        fields2.insert("amount".to_string(), FieldValue::Float(200.0));
        let record2 = StreamRecord {
            fields: fields2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 2,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(2.0));
        fields3.insert("amount".to_string(), FieldValue::Float(150.0));
        let record3 = StreamRecord {
            fields: fields3,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 3,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

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
            assert!(!results.is_empty());

            // Find and verify each customer's group results
            let cust1_result = results.iter().find(|r|
                matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 1.0)
            ).expect("Should have results for customer 1");

            match cust1_result.fields.get("total") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected numeric sum for customer 1"),
            }

            let cust2_result = results.iter().find(|r|
                matches!(r.fields.get("customer_id"), Some(FieldValue::Float(id)) if *id == 2.0)
            ).expect("Should have results for customer 2");

            match cust2_result.fields.get("total") {
                Some(FieldValue::Float(sum)) => assert_eq!(*sum, 150.0),
                _ => panic!("Expected numeric sum for customer 2"),
            }
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
        let mut record1 = create_stream_record_with_fields(fields1, 1);
        record1
            .fields
            .insert("_timestamp".to_string(), FieldValue::Float(60000.0)); // 1 minute
        record1
            .fields
            .insert("amount".to_string(), FieldValue::Float(100.0));

        let mut fields2 = HashMap::new();
        fields2.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record2 = create_stream_record_with_fields(fields2, 2);
        record2
            .fields
            .insert("_timestamp".to_string(), FieldValue::Float(120000.0)); // 2 minutes
        record2
            .fields
            .insert("amount".to_string(), FieldValue::Float(200.0)); // 100+200=300 for first window

        let mut fields3 = HashMap::new();
        fields3.insert("customer_id".to_string(), FieldValue::Float(1.0));
        let mut record3 = create_stream_record_with_fields(fields3, 3);
        record3
            .fields
            .insert("_timestamp".to_string(), FieldValue::Float(360000.0)); // 6 minutes
        record3
            .fields
            .insert("amount".to_string(), FieldValue::Float(500.0)); // 500 for second window

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
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Verify we have all expected fields
            assert_eq!(result.fields.len(), 8);

            // Check basic aggregates
            match result.fields.get("total_count") {
                Some(FieldValue::Integer(count)) => assert_eq!(*count, 3),
                Some(FieldValue::Float(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected total_count to be present"),
            }

            match result.fields.get("distinct_cats") {
                Some(FieldValue::Integer(distinct)) => assert_eq!(*distinct, 2), // A and B
                Some(FieldValue::Float(distinct)) => assert_eq!(*distinct, 2.0),
                _ => panic!("Expected distinct_cats to be present"),
            }

            // Check FIRST and LAST
            assert!(result.fields.contains_key("first_cat"));
            assert!(result.fields.contains_key("last_cat"));

            // Check statistical functions
            match result.fields.get("amount_stddev") {
                Some(FieldValue::Float(stddev)) => {
                    // Standard deviation of [100, 200, 300] should be ~100
                    assert!(
                        *stddev > 80.0 && *stddev < 120.0,
                        "Expected stddev ~100, got {}",
                        stddev
                    );
                }
                _ => panic!("Expected amount_stddev to be present as Number"),
            }

            match result.fields.get("amount_variance") {
                Some(FieldValue::Float(variance)) => {
                    // Variance should be stddev squared (~10000)
                    assert!(
                        *variance > 8000.0 && *variance < 12000.0,
                        "Expected variance ~10000, got {}",
                        variance
                    );
                }
                _ => panic!("Expected amount_variance to be present as Number"),
            }

            // Check STRING_AGG
            match result.fields.get("cat_list") {
                Some(FieldValue::String(agg_result)) => {
                    assert!(
                        agg_result.contains("A") && agg_result.contains("B"),
                        "Expected both A and B in aggregated string: {}",
                        agg_result
                    );
                    assert!(
                        agg_result.contains("|"),
                        "Expected pipe separator: {}",
                        agg_result
                    );
                }
                _ => panic!("Expected cat_list to be present as String"),
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
}
