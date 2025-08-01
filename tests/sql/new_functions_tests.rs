/*!
# Tests for New SQL Functions

Comprehensive test suite for newly added math, string, and date/time functions.
*/

use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use tokio::sync::mpsc;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::Float(123.456));
    fields.insert("quantity".to_string(), FieldValue::Integer(42));
    fields.insert(
        "product_name".to_string(),
        FieldValue::String("Test Product".to_string()),
    );
    fields.insert(
        "description".to_string(),
        FieldValue::String("  This is a test description  ".to_string()),
    );
    fields.insert("negative_num".to_string(), FieldValue::Integer(-15));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());
    headers.insert("version".to_string(), "1.0.0".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1734652800000, // 2024-12-20 00:00:00 UTC
        offset: 100,
        partition: 0,
    }
}

async fn execute_query(
    query: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Convert StreamRecord to HashMap<String, serde_json::Value>
    let json_record: HashMap<String, serde_json::Value> = record
        .fields
        .iter()
        .map(|(k, v)| {
            let json_val = match v {
                FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                FieldValue::Float(f) => {
                    serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))
                }
                FieldValue::String(s) => serde_json::Value::String(s.clone()),
                FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
                FieldValue::Null => serde_json::Value::Null,
                _ => serde_json::Value::String(format!("{:?}", v)),
            };
            (k.clone(), json_val)
        })
        .collect();

    // Use execute_with_metadata to preserve the original timestamp
    engine
        .execute_with_metadata(
            &parsed_query,
            json_record,
            record.headers.clone(),
            Some(record.timestamp),
            Some(record.offset),
            Some(record.partition),
        )
        .await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_math_functions() {
    // Test ABS function
    let results = execute_query("SELECT ABS(negative_num) as abs_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["abs_result"], 15);

    let results = execute_query("SELECT ABS(amount) as abs_amount FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_amount"], 123.456);

    // Test ROUND function
    let results = execute_query("SELECT ROUND(amount) as rounded FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["rounded"], 123.0);

    let results = execute_query("SELECT ROUND(amount, 2) as rounded_2 FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["rounded_2"], 123.46);

    // Test CEIL function
    let results = execute_query("SELECT CEIL(amount) as ceiling FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["ceiling"], 124);

    let results = execute_query("SELECT CEILING(amount) as ceiling2 FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["ceiling2"], 124);

    // Test FLOOR function
    let results = execute_query("SELECT FLOOR(amount) as floor_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["floor_result"], 123);

    // Test MOD function
    let results = execute_query("SELECT MOD(quantity, 10) as mod_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["mod_result"], 2);

    // Test POWER function
    let results = execute_query("SELECT POWER(2, 3) as power_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["power_result"], 8.0);

    let results = execute_query("SELECT POW(quantity, 2) as pow_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["pow_result"], 1764.0);

    // Test SQRT function
    let results = execute_query("SELECT SQRT(quantity) as sqrt_result FROM test_stream")
        .await
        .unwrap();
    assert!((results[0]["sqrt_result"].as_f64().unwrap() - 6.48074069840786).abs() < 1e-10);
}

#[tokio::test]
async fn test_string_functions() {
    // Test CONCAT function
    let results =
        execute_query("SELECT CONCAT('Hello ', 'World') as concat_result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["concat_result"], "Hello World");

    let results = execute_query(
        "SELECT CONCAT(product_name, ' - Version 2') as concat_product FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["concat_product"], "Test Product - Version 2");

    // Test LENGTH function
    let results = execute_query("SELECT LENGTH(product_name) as name_length FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["name_length"], 12);

    let results = execute_query("SELECT LEN(product_name) as name_len FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["name_len"], 12);

    // Test TRIM functions
    let results = execute_query("SELECT TRIM(description) as trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["trimmed"], "This is a test description");

    let results = execute_query("SELECT LTRIM(description) as left_trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["left_trimmed"], "This is a test description  ");

    let results = execute_query("SELECT RTRIM(description) as right_trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["right_trimmed"], "  This is a test description");

    // Test UPPER and LOWER functions
    let results = execute_query("SELECT UPPER(product_name) as upper_name FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["upper_name"], "TEST PRODUCT");

    let results = execute_query("SELECT LOWER(product_name) as lower_name FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["lower_name"], "test product");

    // Test REPLACE function
    let results =
        execute_query("SELECT REPLACE(product_name, 'Test', 'Demo') as replaced FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["replaced"], "Demo Product");

    // Test LEFT and RIGHT functions
    let results = execute_query("SELECT LEFT(product_name, 4) as left_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["left_part"], "Test");

    let results = execute_query("SELECT RIGHT(product_name, 7) as right_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["right_part"], "Product");
}

#[tokio::test]
async fn test_date_time_functions() {
    // Test NOW function
    let results = execute_query("SELECT NOW() as current_time FROM test_stream")
        .await
        .unwrap();
    let now_result = results[0]["current_time"].as_i64().unwrap();
    assert!(now_result > 1734652800000); // Should be after our test timestamp

    // Test CURRENT_TIMESTAMP function
    let results = execute_query("SELECT CURRENT_TIMESTAMP as current_ts FROM test_stream")
        .await
        .unwrap();
    let ts_result = results[0]["current_ts"].as_i64().unwrap();
    assert!(ts_result > 1734652800000);

    // Test EXTRACT function with record timestamp
    let results = execute_query("SELECT EXTRACT('YEAR', _timestamp) as year_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["year_part"], 2024);

    let results =
        execute_query("SELECT EXTRACT('MONTH', _timestamp) as month_part FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["month_part"], 12);

    let results = execute_query("SELECT EXTRACT('DAY', _timestamp) as day_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["day_part"], 20);

    let results = execute_query("SELECT EXTRACT('HOUR', _timestamp) as hour_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["hour_part"], 0);

    // Test DATE_FORMAT function
    let results = execute_query(
        "SELECT DATE_FORMAT(_timestamp, '%Y-%m-%d') as formatted_date FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["formatted_date"], "2024-12-20");

    let results = execute_query("SELECT DATE_FORMAT(_timestamp, '%Y-%m-%d %H:%M:%S') as formatted_datetime FROM test_stream").await.unwrap();
    assert_eq!(results[0]["formatted_datetime"], "2024-12-20 00:00:00");
}

#[tokio::test]
async fn test_utility_functions() {
    // Test COALESCE function
    let results = execute_query(
        "SELECT COALESCE(NULL, 'default', 'backup') as coalesce_result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["coalesce_result"], "default");

    let results = execute_query(
        "SELECT COALESCE(product_name, 'default') as coalesce_product FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["coalesce_product"], "Test Product");

    // Test NULLIF function
    let results = execute_query(
        "SELECT NULLIF(product_name, 'Test Product') as nullif_result FROM test_stream",
    )
    .await
    .unwrap();
    assert!(results[0]["nullif_result"].is_null());

    let results = execute_query(
        "SELECT NULLIF(product_name, 'Different') as nullif_different FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["nullif_different"], "Test Product");
}

#[tokio::test]
async fn test_complex_expressions() {
    // Test combining multiple functions
    let results = execute_query(
        "SELECT UPPER(LEFT(TRIM(description), 10)) as complex_string FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["complex_string"], "THIS IS A ");

    let results =
        execute_query("SELECT ROUND(ABS(negative_num) * 1.5, 1) as complex_math FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["complex_math"], 22.5);

    let results = execute_query(
        "SELECT CONCAT('Order ', quantity, ' at $', ROUND(amount, 2)) as order_summary FROM test_stream"
    ).await.unwrap();
    assert_eq!(results[0]["order_summary"], "Order 42 at $123.46");
}

#[tokio::test]
async fn test_error_handling() {
    // Test division by zero in MOD
    let result = execute_query("SELECT MOD(quantity, 0) as mod_zero FROM test_stream").await;
    assert!(result.is_err());

    // Test SQRT of negative number
    let result = execute_query("SELECT SQRT(negative_num) as sqrt_negative FROM test_stream").await;
    assert!(result.is_err());

    // Test invalid argument counts
    let result = execute_query("SELECT ABS() as abs_no_args FROM test_stream").await;
    assert!(result.is_err());

    let result = execute_query("SELECT CONCAT() as concat_no_args FROM test_stream").await;
    assert!(result.is_ok()); // CONCAT with no args should return empty string

    let result =
        execute_query("SELECT ROUND(amount, 'invalid') as round_invalid FROM test_stream").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_null_handling() {
    // Most functions should handle NULL gracefully
    let results = execute_query("SELECT LENGTH(NULL) as null_length FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_length"].is_null());

    let results = execute_query("SELECT UPPER(NULL) as null_upper FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_upper"].is_null());

    let results = execute_query("SELECT TRIM(NULL) as null_trim FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_trim"].is_null());
}

#[tokio::test]
async fn test_type_conversions() {
    // Test functions with different input types
    let results =
        execute_query("SELECT CONCAT('Value: ', quantity) as concat_int FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["concat_int"], "Value: 42");

    let results =
        execute_query("SELECT CONCAT('Amount: $', amount) as concat_float FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["concat_float"], "Amount: $123.456");

    // Test POWER with integer inputs
    let results = execute_query("SELECT POWER(quantity, 2) as power_int FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["power_int"], 1764.0);
}
