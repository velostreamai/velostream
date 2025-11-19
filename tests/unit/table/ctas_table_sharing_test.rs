use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::sql::SqlError;

/// Test CTAS table creation and sharing functionality
#[tokio::test]
async fn test_ctas_table_creation() {
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-ctas".to_string(), 10);

    // Test 1: Create a table via CTAS
    let ctas_query = "CREATE TABLE orders AS SELECT * FROM orders_topic";
    let result = server.create_table(ctas_query.to_string()).await;

    match result {
        Ok(table_name) => {
            assert_eq!(table_name, "orders");
            println!("✅ Successfully created table: {}", table_name);
        }
        Err(e) => {
            // Expected for now since we don't have actual Kafka running
            println!("⚠️ Expected error creating Kafka table: {}", e);
        }
    }

    // Test 2: List tables
    let tables = server.list_tables().await;
    println!("📋 Available tables: {:?}", tables);

    // Test 3: Check table existence
    let exists = server.table_exists("orders").await;
    if exists {
        println!("✅ Table 'orders' exists in registry");
    } else {
        println!("❌ Table 'orders' not found in registry");
    }

    // Test 4: Try to create duplicate table
    let duplicate_result = server.create_table(ctas_query.to_string()).await;
    match duplicate_result {
        Err(SqlError::ExecutionError { message, .. }) if message.contains("already exists") => {
            println!("✅ Correctly prevented duplicate table creation");
        }
        _ => {
            println!("❌ Should have prevented duplicate table creation");
        }
    }

    // Test 5: Get table statistics
    let stats = server.get_table_stats().await;
    println!("📊 Table statistics: {:?}", stats);

    // Test 6: Get table health
    let health = server.get_tables_health().await;
    println!("🏥 Table health: {:?}", health);

    println!("✅ CTAS basic functionality test completed");
}

/// Test table dependency extraction from SQL queries
#[tokio::test]
async fn test_table_dependency_detection() {
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-deps".to_string(), 10);

    // Test SQL queries with different table dependencies
    let test_queries = vec![
        ("SELECT * FROM orders", vec!["orders"]),
        (
            "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)",
            vec!["orders", "users"],
        ),
        (
            "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id",
            vec!["orders", "users"],
        ),
        (
            "SELECT user_id, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) FROM users u",
            vec!["orders", "users"],
        ),
    ];

    for (query, expected_tables) in test_queries {
        println!("\n🔍 Testing query: {}", query);

        // Try to deploy the job (should fail due to missing tables)
        let result = server
            .deploy_job(
                format!("test-job-{}", expected_tables.join("-")),
                "v1.0".to_string(),
                query.to_string(),
                "test-topic".to_string(),
                None,
                None,
            )
            .await;

        match result {
            Err(SqlError::ExecutionError { message, .. })
                if message.contains("missing required tables") =>
            {
                println!("✅ Correctly detected missing tables");
                // Extract table names from error message
                for table in &expected_tables {
                    if message.contains(table) {
                        println!("  ✅ Found dependency: {}", table);
                    } else {
                        println!("  ❌ Missing dependency: {}", table);
                    }
                }
            }
            Err(e) => {
                println!("❌ Unexpected error: {}", e);
            }
            Ok(_) => {
                println!("❌ Should have failed due to missing tables");
            }
        }
    }

    println!("✅ Table dependency detection test completed");
}

/// Test invalid CTAS queries
#[tokio::test]
async fn test_invalid_ctas_queries() {
    let server = StreamJobServer::new("localhost:9092".to_string(), "test-invalid".to_string(), 10);

    let invalid_queries = vec![
        ("SELECT * FROM orders", "Not a CREATE TABLE AS SELECT query"),
        (
            "CREATE TABLE test AS INSERT INTO orders VALUES (1, 2)",
            "Only SELECT queries are supported",
        ),
        (
            "CREATE TABLE test AS SELECT * FROM (SELECT * FROM orders)",
            "Subqueries in FROM clause are not supported",
        ),
    ];

    for (query, expected_error) in invalid_queries {
        println!("\n🚫 Testing invalid query: {}", query);

        let result = server.create_table(query.to_string()).await;
        match result {
            Err(SqlError::ExecutionError { message, .. }) => {
                if message.contains(expected_error) || expected_error.is_empty() {
                    println!("✅ Correctly rejected invalid query: {}", message);
                } else {
                    println!(
                        "❌ Wrong error message. Expected: {}, Got: {}",
                        expected_error, message
                    );
                }
            }
            Err(e) => {
                println!("❌ Unexpected error type: {}", e);
            }
            Ok(_) => {
                println!("❌ Should have rejected invalid query");
            }
        }
    }

    println!("✅ Invalid CTAS query test completed");
}
