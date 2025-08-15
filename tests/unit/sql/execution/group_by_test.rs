use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat, SerializationFormat};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::runtime::Runtime;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> (StreamExecutionEngine, mpsc::UnboundedReceiver<HashMap<String, InternalValue>>) {
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

    async fn collect_results(receiver: &mut mpsc::UnboundedReceiver<HashMap<String, InternalValue>>) -> Vec<HashMap<String, InternalValue>> {
        let mut results = Vec::new();
        while let Ok(result) = tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv()).await {
            match result {
                Some(r) => results.push(r),
                None => break
            }
        }
        results
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

            // Then collect results from channel
            let results = collect_results(&mut receiver).await;

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
        let query = parser.parse("
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
            HAVING SUM(amount) > 250
        ").unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_results(&mut receiver).await;
            assert_eq!(results.len(), 1); // Only customer 1 should pass HAVING clause

            let result = &results[0];
            // Verify that only customer 1's group (sum = 300) is included
            match (&result.get("customer_id"), &result.get("total")) {
                (Some(InternalValue::Number(cust_id)), Some(InternalValue::Number(sum))) => {
                    assert_eq!(*cust_id, 1.0);
                    assert_eq!(*sum, 300.0);
                },
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
        let query = parser.parse("
            SELECT
                customer_id,
                COUNT(*) as count,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM orders
            GROUP BY customer_id
        ").unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

            let results = collect_results(&mut receiver).await;
            assert_eq!(results.len(), 1);

            let result = &results[0];
            // Verify multiple aggregation results
            assert_eq!(result.len(), 5);
            match (
                result.get("count"),
                result.get("avg_amount"),
                result.get("min_amount"),
                result.get("max_amount")
            ) {
                (
                    Some(InternalValue::Number(count)),
                    Some(InternalValue::Number(avg)),
                    Some(InternalValue::Number(min)),
                    Some(InternalValue::Number(max))
                ) => {
                    assert_eq!(*count, 3.0);
                    assert_eq!(*avg, 200.0);
                    assert_eq!(*min, 100.0);
                    assert_eq!(*max, 300.0);
                },
                _ => panic!("Expected all aggregation results to be present"),
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
        let query = parser.parse("
            SELECT
                customer_id,
                SUM(amount) as window_total
            FROM orders
            GROUP BY customer_id
            WINDOW TUMBLING(INTERVAL 5 MINUTES)
        ").unwrap();

        // Execute each record
        rt.block_on(async {
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();
            engine.execute(&query, record3).await.unwrap();

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
        let query = parser.parse("
            SELECT
                (amount > 150) as high_value,
                COUNT(*) as count,
                SUM(amount) as total
            FROM orders
            GROUP BY amount > 150
        ").unwrap();

        rt.block_on(async {
            // Execute each record
            engine.execute(&query, record1).await.unwrap();
            engine.execute(&query, record2).await.unwrap();

            // Collect results from channel
            let results = collect_results(&mut receiver).await;
            assert_eq!(results.len(), 2); // Should have two groups: true and false

            // Find low value group (amount <= 150)
            let low_value = results.iter().find(|r|
                matches!(r.get("high_value"), Some(InternalValue::Boolean(false)))
            ).expect("Should have results for low value group");

            // Find high value group (amount > 150)
            let high_value = results.iter().find(|r|
                matches!(r.get("high_value"), Some(InternalValue::Boolean(true)))
            ).expect("Should have results for high value group");

            // Verify low value group
            match (low_value.get("count"), low_value.get("total")) {
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 100.0);
                },
                _ => panic!("Expected numeric results for low value group"),
            }

            // Verify high value group
            match (high_value.get("count"), high_value.get("total")) {
                (Some(InternalValue::Number(count)), Some(InternalValue::Number(total))) => {
                    assert_eq!(*count, 1.0);
                    assert_eq!(*total, 200.0);
                },
                _ => panic!("Expected numeric results for high value group"),
            }
        });
    }
}
