//! Integration test for StreamExecutionEngine - proves end-to-end SQL execution works

use ferrisstreams::ferris::{
    serialization::{InternalValue, JsonFormat},
    sql::{execution::StreamExecutionEngine, parser::StreamingSqlParser},
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

/// Helper to create test market data record
fn create_market_data_record(
    symbol: &str,
    bid_price: f64,
    ask_price: f64,
    bid_size: i64,
    ask_size: i64,
) -> HashMap<String, InternalValue> {
    let mut record = HashMap::new();
    record.insert(
        "symbol".to_string(),
        InternalValue::String(symbol.to_string()),
    );
    record.insert("bid_price".to_string(), InternalValue::Number(bid_price));
    record.insert("ask_price".to_string(), InternalValue::Number(ask_price));
    record.insert("bid_size".to_string(), InternalValue::Integer(bid_size));
    record.insert("ask_size".to_string(), InternalValue::Integer(ask_size));
    record.insert(
        "timestamp".to_string(),
        InternalValue::Integer(chrono::Utc::now().timestamp_millis()),
    );
    record
}

/// Helper to execute SQL query and get results
async fn execute_sql_query(
    sql: &str,
    records: Vec<HashMap<String, InternalValue>>,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    // Create output channel
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();

    // Create serialization format
    let serialization_format = Arc::new(JsonFormat);

    // Create execution engine
    let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);

    // Parse SQL query
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql)?;

    // Execute all records
    for record in records {
        engine.execute(&query, record).await?;
    }

    // Collect all results
    let mut results = Vec::new();

    // Use try_recv to get all available results without blocking
    while let Ok(result) = output_receiver.try_recv() {
        results.push(result);
    }

    Ok(results)
}

#[tokio::test]
async fn test_simple_select_query() {
    println!("🧪 Testing simple SELECT query execution");

    let sql = "SELECT symbol, bid_price FROM market_data";
    let records = vec![
        create_market_data_record("AAPL", 150.0, 151.0, 100, 200),
        create_market_data_record("GOOGL", 2500.0, 2501.0, 50, 75),
    ];

    let results = execute_sql_query(sql, records).await.unwrap();

    assert_eq!(results.len(), 2);

    // Check first result
    assert_eq!(results[0]["symbol"], "AAPL");
    assert_eq!(results[0]["bid_price"], 150.0);
    assert!(!results[0].contains_key("ask_price")); // Should not be selected

    // Check second result
    assert_eq!(results[1]["symbol"], "GOOGL");
    assert_eq!(results[1]["bid_price"], 2500.0);

    println!("✅ Simple SELECT query works correctly");
}

#[tokio::test]
async fn test_select_with_where_clause() {
    println!("🧪 Testing SELECT with WHERE clause");

    let sql = "SELECT symbol, bid_price FROM market_data WHERE bid_price > 1000";
    let records = vec![
        create_market_data_record("AAPL", 150.0, 151.0, 100, 200), // Should be filtered out
        create_market_data_record("GOOGL", 2500.0, 2501.0, 50, 75), // Should be included
        create_market_data_record("MSFT", 300.0, 301.0, 80, 90),   // Should be filtered out
    ];

    let results = execute_sql_query(sql, records).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["symbol"], "GOOGL");
    assert_eq!(results[0]["bid_price"], 2500.0);

    println!("✅ WHERE clause filtering works correctly");
}

#[tokio::test]
async fn test_arithmetic_functions() {
    println!("🧪 Testing arithmetic functions (ABS, LEAST, GREATEST)");

    let sql = "SELECT symbol, ABS(bid_price - ask_price) as spread, LEAST(bid_size, ask_size) as min_size, GREATEST(bid_size, ask_size) as max_size FROM market_data";
    let records = vec![create_market_data_record("AAPL", 150.0, 151.0, 100, 200)];

    let results = execute_sql_query(sql, records).await.unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];

    assert_eq!(result["symbol"], "AAPL");
    assert_eq!(result["spread"], 1.0); // ABS(150.0 - 151.0) = 1.0
    assert_eq!(result["min_size"], 100); // LEAST(100, 200) = 100
    assert_eq!(result["max_size"], 200); // GREATEST(100, 200) = 200

    println!("✅ Arithmetic functions work correctly");
}

#[tokio::test]
async fn test_trading_arbitrage_query() {
    println!("🧪 Testing trading arbitrage detection query (like the demo)");

    // This mimics the actual arbitrage detection SQL from the trading demo
    let sql = r#"
        SELECT 
            symbol,
            bid_price,
            ask_price,
            LEAST(bid_size, ask_size) as available_volume,
            ABS(bid_price - ask_price) as spread
        FROM market_data 
        WHERE bid_price > ask_price
    "#;

    let records = vec![
        // Normal case - no arbitrage (bid < ask)
        create_market_data_record("AAPL", 150.0, 151.0, 100, 200),
        // Arbitrage opportunity - bid > ask (unusual but possible in different markets)
        create_market_data_record("GOOGL", 2502.0, 2501.0, 50, 75),
        // Another normal case
        create_market_data_record("MSFT", 300.0, 301.0, 80, 90),
    ];

    let results = execute_sql_query(sql, records).await.unwrap();

    // Should only return the arbitrage opportunity (GOOGL)
    assert_eq!(results.len(), 1);
    let result = &results[0];

    assert_eq!(result["symbol"], "GOOGL");
    assert_eq!(result["bid_price"], 2502.0);
    assert_eq!(result["ask_price"], 2501.0);
    assert_eq!(result["available_volume"], 50); // LEAST(50, 75) = 50
    assert_eq!(result["spread"], 1.0); // ABS(2502.0 - 2501.0) = 1.0

    println!("✅ Arbitrage detection query works correctly");
}

#[tokio::test]
async fn test_limit_functionality() {
    println!("🧪 Testing LIMIT functionality");

    let sql = "SELECT symbol, bid_price FROM market_data LIMIT 2";
    let records = vec![
        create_market_data_record("AAPL", 150.0, 151.0, 100, 200),
        create_market_data_record("GOOGL", 2500.0, 2501.0, 50, 75),
        create_market_data_record("MSFT", 300.0, 301.0, 80, 90),
        create_market_data_record("TSLA", 800.0, 801.0, 60, 70),
    ];

    let results = execute_sql_query(sql, records).await.unwrap();

    // Should only return first 2 records due to LIMIT
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["symbol"], "AAPL");
    assert_eq!(results[1]["symbol"], "GOOGL");

    println!("✅ LIMIT functionality works correctly");
}

#[tokio::test]
async fn test_string_functions() {
    println!("🧪 Testing string functions");

    let sql =
        "SELECT UPPER(symbol) as upper_symbol, LENGTH(symbol) as symbol_length FROM market_data";
    let records = vec![create_market_data_record("aapl", 150.0, 151.0, 100, 200)];

    let results = execute_sql_query(sql, records).await.unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];

    assert_eq!(result["upper_symbol"], "AAPL");
    assert_eq!(result["symbol_length"], 4);

    println!("✅ String functions work correctly");
}

#[tokio::test]
async fn test_null_handling() {
    println!("🧪 Testing NULL value handling");

    let sql = "SELECT symbol, COALESCE(bid_price, 0.0) as safe_bid_price FROM market_data";

    // Create a record with null bid_price
    let mut record = HashMap::new();
    record.insert(
        "symbol".to_string(),
        serde_json::Value::String("TEST".to_string()),
    );
    record.insert("bid_price".to_string(), serde_json::Value::Null);
    record.insert("ask_price".to_string(), serde_json::json!(100.0));

    let results = execute_sql_query(sql, vec![record]).await.unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];

    assert_eq!(result["symbol"], "TEST");
    assert_eq!(result["safe_bid_price"], 0.0); // COALESCE should return 0.0 for null

    println!("✅ NULL handling works correctly");
}

#[tokio::test]
async fn test_complex_expression() {
    println!("🧪 Testing complex expression with multiple functions");

    let sql = r#"
        SELECT 
            symbol,
            ROUND(ABS(bid_price - ask_price) * LEAST(bid_size, ask_size), 2) as total_opportunity
        FROM market_data
        WHERE bid_price > 0
    "#;

    let records = vec![create_market_data_record("AAPL", 150.5, 151.7, 100, 200)];

    let results = execute_sql_query(sql, records).await.unwrap();

    assert_eq!(results.len(), 1);
    let result = &results[0];

    assert_eq!(result["symbol"], "AAPL");
    // ROUND(ABS(150.5 - 151.7) * LEAST(100, 200), 2) = ROUND(1.2 * 100, 2) = 120.0
    assert_eq!(result["total_opportunity"], 120.0);

    println!("✅ Complex expressions work correctly");
}

#[tokio::test]
async fn test_error_handling() {
    println!("🧪 Testing error handling for invalid SQL");

    let invalid_sql = "SELECT INVALID_FUNCTION(symbol) FROM market_data";
    let records = vec![create_market_data_record("AAPL", 150.0, 151.0, 100, 200)];

    let result = execute_sql_query(invalid_sql, records).await;

    // Should return an error for invalid function
    assert!(result.is_err());

    println!("✅ Error handling works correctly");
}

#[tokio::test]
async fn test_streaming_behavior() {
    println!("🧪 Testing streaming behavior - records processed independently");

    let sql = "SELECT symbol, bid_price FROM market_data WHERE bid_price > 500";

    // Process records one by one to verify streaming behavior
    let record1 = create_market_data_record("AAPL", 150.0, 151.0, 100, 200); // Filtered out
    let record2 = create_market_data_record("GOOGL", 2500.0, 2501.0, 50, 75); // Included
    let record3 = create_market_data_record("MSFT", 300.0, 301.0, 80, 90); // Filtered out

    // Create engine
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(output_sender);
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).unwrap();

    // Process records individually
    engine.execute(&query, record1).await.unwrap();
    engine.execute(&query, record2).await.unwrap();
    engine.execute(&query, record3).await.unwrap();

    // Collect results
    let mut results = Vec::new();
    while let Ok(result) = output_receiver.try_recv() {
        results.push(result);
    }

    // Should only get one result (GOOGL)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["symbol"], "GOOGL");
    assert_eq!(results[0]["bid_price"], 2500.0);

    println!("✅ Streaming behavior works correctly - each record processed independently");
}

#[tokio::test]
async fn test_end_to_end_integration() {
    println!("🧪 Testing full end-to-end integration - simulates real trading data");

    // This test simulates the complete flow that would happen in the trading demo
    let arbitrage_sql = r#"
        SELECT 
            symbol,
            bid_price,
            ask_price,
            LEAST(bid_size, ask_size) as available_volume,
            ABS(bid_price - ask_price) as spread,
            ROUND((bid_price - ask_price) * LEAST(bid_size, ask_size), 2) as potential_profit
        FROM market_data 
        WHERE bid_price > ask_price AND LEAST(bid_size, ask_size) > 10
    "#;

    // Simulate real market data stream
    let market_data = vec![
        create_market_data_record("AAPL", 150.0, 151.0, 100, 200), // Normal - no arbitrage
        create_market_data_record("GOOGL", 2502.5, 2501.0, 50, 75), // Arbitrage opportunity!
        create_market_data_record("MSFT", 300.0, 301.0, 5, 10),    // Normal - no arbitrage
        create_market_data_record("TSLA", 801.0, 800.5, 30, 25),   // Arbitrage opportunity!
        create_market_data_record("NVDA", 500.0, 500.1, 200, 150), // Normal - no arbitrage
    ];

    let results = execute_sql_query(arbitrage_sql, market_data).await.unwrap();

    // Should find 2 arbitrage opportunities
    assert_eq!(results.len(), 2);

    // Verify GOOGL arbitrage
    let googl_result = &results[0];
    assert_eq!(googl_result["symbol"], "GOOGL");
    assert_eq!(googl_result["available_volume"], 50); // LEAST(50, 75)
    assert_eq!(googl_result["spread"], 1.5); // ABS(2502.5 - 2501.0)
    assert_eq!(googl_result["potential_profit"], 75.0); // (2502.5 - 2501.0) * 50

    // Verify TSLA arbitrage
    let tsla_result = &results[1];
    assert_eq!(tsla_result["symbol"], "TSLA");
    assert_eq!(tsla_result["available_volume"], 25); // LEAST(30, 25)
    assert_eq!(tsla_result["spread"], 0.5); // ABS(801.0 - 800.5)
    assert_eq!(tsla_result["potential_profit"], 12.5); // (801.0 - 800.5) * 25

    println!("✅ End-to-end integration test passed - arbitrage detection working perfectly!");
    println!("   Found {} arbitrage opportunities", results.len());
    println!(
        "   GOOGL: ${:.2} potential profit",
        googl_result["potential_profit"]
    );
    println!(
        "   TSLA: ${:.2} potential profit",
        tsla_result["potential_profit"]
    );
}
