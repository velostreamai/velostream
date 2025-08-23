use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> (
        StreamExecutionEngine,
        mpsc::UnboundedReceiver<HashMap<String, InternalValue>>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let format: Arc<dyn SerializationFormat> = Arc::new(JsonFormat);
        let engine = StreamExecutionEngine::new(sender, format);
        (engine, receiver)
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
        receiver: &mut mpsc::UnboundedReceiver<HashMap<String, InternalValue>>,
    ) -> Vec<HashMap<String, InternalValue>> {
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
        receiver: &mut mpsc::UnboundedReceiver<HashMap<String, InternalValue>>,
        group_key_field: &str,
    ) -> Vec<HashMap<String, InternalValue>> {
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
            if let Some(key_value) = result.get(group_key_field) {
                let key = match key_value {
                    InternalValue::Number(n) => n.to_string(),
                    InternalValue::String(s) => s.clone(),
                    InternalValue::Integer(i) => i.to_string(),
                    InternalValue::Boolean(b) => b.to_string(),
                    _ => "null".to_string(),
                };
                latest_by_key.insert(key, result);
            }
        }

        latest_by_key.into_values().collect()
    }

    // For tests with multiple group keys
    async fn collect_latest_multi_group_results(
        receiver: &mut mpsc::UnboundedReceiver<HashMap<String, InternalValue>>,
        group_key_fields: &[&str],
    ) -> Vec<HashMap<String, InternalValue>> {
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
                if let Some(key_value) = result.get(*field) {
                    let key_part = match key_value {
                        InternalValue::Number(n) => n.to_string(),
                        InternalValue::String(s) => s.clone(),
                        InternalValue::Integer(i) => i.to_string(),
                        InternalValue::Boolean(b) => b.to_string(),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(2.0));
        record3.insert("amount".to_string(), InternalValue::Number(150.0));

        // Test query with sum aggregation
        let query = parser
            .parse("SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id")
            .unwrap();

        // Execute each record and collect results
        rt.block_on(async {
            // Execute all records first
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            // Then collect results from channel (Flink-style: latest per group)
            let results = collect_latest_group_results(&mut receiver, "customer_id").await;

            // We should get at least one result through the channel
            assert!(!results.is_empty());

            // Find and verify each customer's group results
            let cust1_result = results.iter().find(|r|
                matches!(r.get("customer_id"), Some(InternalValue::Number(id)) if *id == 1.0)
            ).expect("Should have results for customer 1");

            match cust1_result.get("total") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected numeric sum for customer 1"),
            }

            let cust2_result = results.iter().find(|r|
                matches!(r.get("customer_id"), Some(InternalValue::Number(id)) if *id == 2.0)
            ).expect("Should have results for customer 2");

            match cust2_result.get("total") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 150.0),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(2.0));
        record3.insert("amount".to_string(), InternalValue::Number(150.0));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1); // Only customer 1 should pass HAVING clause

            let result = &results[0];
            // Verify that only customer 1's group (sum = 300) is included
            match (&result.get("customer_id"), &result.get("total")) {
                (Some(InternalValue::Number(cust_id)), Some(InternalValue::Number(sum))) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("amount".to_string(), InternalValue::Number(300.0));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];
            // Verify multiple aggregation results
            assert_eq!(result.len(), 5);

            // Check count (can be Integer or Number)
            match result.get("count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present as Integer or Number"),
            }

            // Check other aggregates
            match (
                result.get("avg_amount"),
                result.get("min_amount"),
                result.get("max_amount"),
            ) {
                (
                    Some(InternalValue::Number(avg)),
                    Some(InternalValue::Number(min)),
                    Some(InternalValue::Number(max)),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));
        record1.insert("_timestamp".to_string(), InternalValue::Number(60000.0)); // 1 minute

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));
        record2.insert("_timestamp".to_string(), InternalValue::Number(120000.0)); // 2 minutes

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("amount".to_string(), InternalValue::Number(500.0));
        record3.insert("_timestamp".to_string(), InternalValue::Number(360000.0)); // 6 minutes

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            // Flush any remaining windows to ensure all results are emitted
            engine.flush_windows().await.unwrap();

            let results = collect_results(&mut receiver).await;
            assert_eq!(results.len(), 2); // Should have results for each window

            // Find first window result (0-5 minutes, sum = 300)
            let first_window = results.iter().find(|r|
                matches!(r.get("window_total"), Some(InternalValue::Number(sum)) if *sum == 300.0)
            ).expect("Should have first window results");

            // Find second window result (5-10 minutes, sum = 500)
            let second_window = results.iter().find(|r|
                matches!(r.get("window_total"), Some(InternalValue::Number(sum)) if *sum == 500.0)
            ).expect("Should have second window results");

            // Verify window results
            match first_window.get("window_total") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected numeric sum for first window"),
            }

            match second_window.get("window_total") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 500.0),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));
        record1.insert("is_prime".to_string(), InternalValue::Boolean(true));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));
        record2.insert("is_prime".to_string(), InternalValue::Boolean(false));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();

            // Collect results from channel (Flink-style: latest per group)
            let results = collect_latest_group_results(&mut receiver, "high_value").await;
            assert_eq!(results.len(), 2); // Should have two groups: true and false

            // Find low value group (amount <= 150)
            let low_value = results
                .iter()
                .find(|r| matches!(r.get("high_value"), Some(InternalValue::Boolean(false))))
                .expect("Should have results for low value group");

            // Find high value group (amount > 150)
            let high_value = results
                .iter()
                .find(|r| matches!(r.get("high_value"), Some(InternalValue::Boolean(true))))
                .expect("Should have results for high value group");

            // Verify low value group
            match (low_value.get("count"), low_value.get("total")) {
                (Some(InternalValue::Integer(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 100.0);
                }
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 100.0);
                }
                _ => panic!("Expected numeric results for low value group"),
            }

            // Verify high value group
            match (high_value.get("count"), high_value.get("total")) {
                (Some(InternalValue::Integer(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 200.0);
                }
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("score".to_string(), InternalValue::Number(10.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("score".to_string(), InternalValue::Number(20.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("score".to_string(), InternalValue::Number(30.0));

        // Test STDDEV function - use simple query for debugging
        let query = parser
            .parse("SELECT customer_id, STDDEV(score) as score_stddev, COUNT(*) as count FROM orders GROUP BY customer_id")
            .unwrap();

        rt.block_on(async {
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.get("count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check STDDEV (for values 10, 20, 30: stddev ≈ 10.0)
            match result.get("score_stddev") {
                Some(InternalValue::Number(stddev)) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("value".to_string(), InternalValue::Number(2.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("value".to_string(), InternalValue::Number(4.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("value".to_string(), InternalValue::Number(6.0));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.get("count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check VARIANCE (for values 2, 4, 6: variance should be 4.0)
            match result.get("value_variance") {
                Some(InternalValue::Number(variance)) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert(
            "product".to_string(),
            InternalValue::String("apple".to_string()),
        );

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert(
            "product".to_string(),
            InternalValue::String("banana".to_string()),
        );

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert(
            "product".to_string(),
            InternalValue::String("cherry".to_string()),
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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.get("count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check FIRST (should be "apple" - first record processed)
            match result.get("first_product") {
                Some(InternalValue::String(first)) => assert_eq!(first, "apple"),
                _ => panic!("Expected FIRST result to be present as String"),
            }

            // Check LAST (should be "cherry" - last record processed)
            match result.get("last_product") {
                Some(InternalValue::String(last)) => assert_eq!(last, "cherry"),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("tag".to_string(), InternalValue::String("red".to_string()));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert(
            "tag".to_string(),
            InternalValue::String("green".to_string()),
        );

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("tag".to_string(), InternalValue::String("blue".to_string()));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check count
            match result.get("count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected count to be present"),
            }

            // Check STRING_AGG (should contain all three colors separated by commas)
            match result.get("tag_list") {
                Some(InternalValue::String(agg_result)) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert(
            "category".to_string(),
            InternalValue::String("electronics".to_string()),
        );

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert(
            "category".to_string(),
            InternalValue::String("books".to_string()),
        );

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert(
            "category".to_string(),
            InternalValue::String("electronics".to_string()),
        ); // Duplicate

        let mut record4 = HashMap::new();
        record4.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record4.insert(
            "category".to_string(),
            InternalValue::String("clothing".to_string()),
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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();
            engine.execute(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check total count (should be 4)
            match result.get("total_count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 4),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 4.0),
                _ => panic!("Expected total_count to be present"),
            }

            // Check COUNT_DISTINCT (should be 3: electronics, books, clothing)
            match result.get("distinct_categories") {
                Some(InternalValue::Integer(distinct_count)) => assert_eq!(*distinct_count, 3),
                Some(InternalValue::Number(distinct_count)) => assert_eq!(*distinct_count, 3.0),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));
        record1.insert(
            "category".to_string(),
            InternalValue::String("A".to_string()),
        );

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Number(200.0));
        record2.insert(
            "category".to_string(),
            InternalValue::String("B".to_string()),
        );

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("amount".to_string(), InternalValue::Number(300.0));
        record3.insert(
            "category".to_string(),
            InternalValue::String("A".to_string()),
        );

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Verify we have all expected fields
            assert_eq!(result.len(), 8);

            // Check basic aggregates
            match result.get("total_count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 3),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 3.0),
                _ => panic!("Expected total_count to be present"),
            }

            match result.get("distinct_cats") {
                Some(InternalValue::Integer(distinct)) => assert_eq!(*distinct, 2), // A and B
                Some(InternalValue::Number(distinct)) => assert_eq!(*distinct, 2.0),
                _ => panic!("Expected distinct_cats to be present"),
            }

            // Check FIRST and LAST
            assert!(result.contains_key("first_cat"));
            assert!(result.contains_key("last_cat"));

            // Check statistical functions
            match result.get("amount_stddev") {
                Some(InternalValue::Number(stddev)) => {
                    // Standard deviation of [100, 200, 300] should be ~100
                    assert!(
                        *stddev > 80.0 && *stddev < 120.0,
                        "Expected stddev ~100, got {}",
                        stddev
                    );
                }
                _ => panic!("Expected amount_stddev to be present as Number"),
            }

            match result.get("amount_variance") {
                Some(InternalValue::Number(variance)) => {
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
            match result.get("cat_list") {
                Some(InternalValue::String(agg_result)) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Null);
        record2.insert("amount".to_string(), InternalValue::Number(200.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Null);
        record3.insert("amount".to_string(), InternalValue::Number(300.0));

        let mut record4 = HashMap::new();
        record4.insert("customer_id".to_string(), InternalValue::Number(2.0));
        record4.insert("amount".to_string(), InternalValue::Number(400.0));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();
            engine.execute(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 3); // Should have 3 groups: 1, NULL, 2

            // Find group for customer_id = 1
            let group1 = results.iter().find(|r|
                matches!(r.get("customer_id"), Some(InternalValue::Number(id)) if *id == 1.0)
            ).expect("Should have group for customer_id = 1");

            match (group1.get("count"), group1.get("total")) {
                (Some(InternalValue::Integer(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 100.0);
                }
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 100.0);
                }
                _ => panic!("Expected count and total for group 1"),
            }

            // Find group for customer_id = NULL
            let null_group = results
                .iter()
                .find(|r| matches!(r.get("customer_id"), Some(InternalValue::Null)))
                .expect("Should have group for customer_id = NULL");

            match (null_group.get("count"), null_group.get("total")) {
                (Some(InternalValue::Integer(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 2);
                    assert_eq!(*total, 500.0);
                }
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 2.0);
                    assert_eq!(*total, 500.0);
                }
                _ => panic!("Expected count and total for NULL group"),
            }

            // Find group for customer_id = 2
            let group2 = results.iter().find(|r|
                matches!(r.get("customer_id"), Some(InternalValue::Number(id)) if *id == 2.0)
            ).expect("Should have group for customer_id = 2");

            match (group2.get("count"), group2.get("total")) {
                (Some(InternalValue::Integer(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1);
                    assert_eq!(*total, 400.0);
                }
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert("amount".to_string(), InternalValue::Number(100.0));
        record1.insert("quantity".to_string(), InternalValue::Number(5.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("amount".to_string(), InternalValue::Null);
        record2.insert("quantity".to_string(), InternalValue::Number(3.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record3.insert("amount".to_string(), InternalValue::Number(200.0));
        record3.insert("quantity".to_string(), InternalValue::Null);

        let mut record4 = HashMap::new();
        record4.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record4.insert("amount".to_string(), InternalValue::Null);
        record4.insert("quantity".to_string(), InternalValue::Null);

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();
            engine.execute(&query, record4).await.unwrap();

            let results = collect_latest_group_results(&mut receiver, "customer_id").await;
            assert_eq!(results.len(), 1);

            let result = &results[0];

            // Check COUNT(*) - should count all records including those with NULLs
            match result.get("total_count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 4),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 4.0),
                _ => panic!("Expected total_count to be 4"),
            }

            // Check COUNT(amount) - should only count non-NULL values (2 records)
            match result.get("amount_count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 2),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 2.0),
                _ => panic!("Expected amount_count to be 2"),
            }

            // Check COUNT(quantity) - should only count non-NULL values (2 records)
            match result.get("quantity_count") {
                Some(InternalValue::Integer(count)) => assert_eq!(*count, 2),
                Some(InternalValue::Number(count)) => assert_eq!(*count, 2.0),
                _ => panic!("Expected quantity_count to be 2"),
            }

            // Check SUM(amount) - should sum only non-NULL values (100 + 200 = 300)
            match result.get("amount_sum") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 300.0),
                _ => panic!("Expected amount_sum to be 300.0"),
            }

            // Check AVG(amount) - should average only non-NULL values (300/2 = 150)
            match result.get("amount_avg") {
                Some(InternalValue::Number(avg)) => assert_eq!(*avg, 150.0),
                _ => panic!("Expected amount_avg to be 150.0"),
            }

            // Check MIN(amount) - should be minimum of non-NULL values (100)
            match result.get("amount_min") {
                Some(InternalValue::Number(min)) => assert_eq!(*min, 100.0),
                _ => panic!("Expected amount_min to be 100.0"),
            }

            // Check MAX(amount) - should be maximum of non-NULL values (200)
            match result.get("amount_max") {
                Some(InternalValue::Number(max)) => assert_eq!(*max, 200.0),
                _ => panic!("Expected amount_max to be 200.0"),
            }

            // Check SUM(quantity) - should sum only non-NULL values (5 + 3 = 8)
            match result.get("quantity_sum") {
                Some(InternalValue::Number(sum)) => assert_eq!(*sum, 8.0),
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
        let mut record1 = HashMap::new();
        record1.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record1.insert(
            "region".to_string(),
            InternalValue::String("US".to_string()),
        );
        record1.insert("amount".to_string(), InternalValue::Number(100.0));

        let mut record2 = HashMap::new();
        record2.insert("customer_id".to_string(), InternalValue::Number(1.0));
        record2.insert("region".to_string(), InternalValue::Null);
        record2.insert("amount".to_string(), InternalValue::Number(200.0));

        let mut record3 = HashMap::new();
        record3.insert("customer_id".to_string(), InternalValue::Null);
        record3.insert(
            "region".to_string(),
            InternalValue::String("EU".to_string()),
        );
        record3.insert("amount".to_string(), InternalValue::Number(300.0));

        let mut record4 = HashMap::new();
        record4.insert("customer_id".to_string(), InternalValue::Null);
        record4.insert("region".to_string(), InternalValue::Null);
        record4.insert("amount".to_string(), InternalValue::Number(400.0));

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
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();
            engine.execute(&query, record4).await.unwrap();

            let results =
                collect_latest_multi_group_results(&mut receiver, &["customer_id", "region"]).await;
            assert_eq!(results.len(), 4); // Should have 4 distinct groups

            // Verify each group combination
            for result in &results {
                match (result.get("customer_id"), result.get("region")) {
                    (Some(InternalValue::Number(1.0)), Some(InternalValue::String(region)))
                        if region == "US" =>
                    {
                        // Group: customer_id=1, region=US
                        match (result.get("count"), result.get("total")) {
                            (
                                Some(InternalValue::Integer(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 100.0);
                            }
                            (
                                Some(InternalValue::Number(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 100.0);
                            }
                            _ => panic!("Expected count=1, total=100 for group (1, US)"),
                        }
                    }
                    (Some(InternalValue::Number(1.0)), Some(InternalValue::Null)) => {
                        // Group: customer_id=1, region=NULL
                        match (result.get("count"), result.get("total")) {
                            (
                                Some(InternalValue::Integer(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 200.0);
                            }
                            (
                                Some(InternalValue::Number(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 200.0);
                            }
                            _ => panic!("Expected count=1, total=200 for group (1, NULL)"),
                        }
                    }
                    (Some(InternalValue::Null), Some(InternalValue::String(region)))
                        if region == "EU" =>
                    {
                        // Group: customer_id=NULL, region=EU
                        match (result.get("count"), result.get("total")) {
                            (
                                Some(InternalValue::Integer(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 300.0);
                            }
                            (
                                Some(InternalValue::Number(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 300.0);
                            }
                            _ => panic!("Expected count=1, total=300 for group (NULL, EU)"),
                        }
                    }
                    (Some(InternalValue::Null), Some(InternalValue::Null)) => {
                        // Group: customer_id=NULL, region=NULL
                        match (result.get("count"), result.get("total")) {
                            (
                                Some(InternalValue::Integer(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1);
                                assert_eq!(*total, 400.0);
                            }
                            (
                                Some(InternalValue::Number(count)),
                                Some(InternalValue::Number(total)),
                            ) => {
                                assert_eq!(*count, 1.0);
                                assert_eq!(*total, 400.0);
                            }
                            _ => panic!("Expected count=1, total=400 for group (NULL, NULL)"),
                        }
                    }
                    _ => panic!(
                        "Unexpected group combination: {:?}, {:?}",
                        result.get("customer_id"),
                        result.get("region")
                    ),
                }
            }
        });
    }
}
