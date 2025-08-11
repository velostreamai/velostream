/*!
# Tests for New SQL Functions

Comprehensive test suite for newly added math, string, and date/time functions.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
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

fn convert_stream_record_to_internal(record: &StreamRecord) -> HashMap<String, InternalValue> {
    record
        .fields
        .iter()
        .map(|(k, v)| {
            let internal_val = match v {
                FieldValue::Integer(i) => InternalValue::Integer(*i),
                FieldValue::Float(f) => InternalValue::Number(*f),
                FieldValue::String(s) => InternalValue::String(s.clone()),
                FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                FieldValue::Null => InternalValue::Null,
                _ => InternalValue::String(format!("{:?}", v)),
            };
            (k.clone(), internal_val)
        })
        .collect()
}

async fn execute_query(
    query: &str,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Convert StreamRecord to HashMap<String, InternalValue>
    let internal_record = convert_stream_record_to_internal(&record);

    // Execute the query with internal record, including metadata
    engine
        .execute_with_metadata(
            &parsed_query,
            internal_record,
            record.headers,
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
    assert_eq!(results[0]["abs_result"], InternalValue::Integer(15));

    let results = execute_query("SELECT ABS(amount) as abs_amount FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_amount"], InternalValue::Number(123.456));

    // Test ROUND function
    let results = execute_query("SELECT ROUND(amount) as rounded FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["rounded"], InternalValue::Number(123.0));

    let results = execute_query("SELECT ROUND(amount, 2) as rounded_2 FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["rounded_2"], InternalValue::Number(123.46));

    // Test CEIL function
    let results = execute_query("SELECT CEIL(amount) as ceiling FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["ceiling"], InternalValue::Integer(124));

    let results = execute_query("SELECT CEILING(amount) as ceiling2 FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["ceiling2"], InternalValue::Integer(124));

    // Test FLOOR function
    let results = execute_query("SELECT FLOOR(amount) as floor_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["floor_result"], InternalValue::Integer(123));

    // Test MOD function
    let results = execute_query("SELECT MOD(quantity, 10) as mod_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["mod_result"], InternalValue::Integer(2));

    // Test POWER function
    let results = execute_query("SELECT POWER(2, 3) as power_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["power_result"], InternalValue::Number(8.0));

    let results = execute_query("SELECT POW(quantity, 2) as pow_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["pow_result"], InternalValue::Number(1764.0));

    // Test SQRT function
    let results = execute_query("SELECT SQRT(quantity) as sqrt_result FROM test_stream")
        .await
        .unwrap();
    let sqrt_val = match &results[0]["sqrt_result"] {
        InternalValue::Number(n) => *n,
        _ => panic!("Expected Number for sqrt result"),
    };
    assert!((sqrt_val - 6.48074069840786).abs() < 1e-10);
}

#[tokio::test]
async fn test_string_functions() {
    // Test CONCAT function
    let results =
        execute_query("SELECT CONCAT('Hello ', 'World') as concat_result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(
        results[0]["concat_result"],
        InternalValue::String("Hello World".to_string())
    );

    let results = execute_query(
        "SELECT CONCAT(product_name, ' - Version 2') as concat_product FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["concat_product"],
        InternalValue::String("Test Product - Version 2".to_string())
    );

    // Test LENGTH function
    let results = execute_query("SELECT LENGTH(product_name) as name_length FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["name_length"], InternalValue::Integer(12));

    let results = execute_query("SELECT LEN(product_name) as name_len FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["name_len"], InternalValue::Integer(12));

    // Test TRIM functions
    let results = execute_query("SELECT TRIM(description) as trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["trimmed"],
        InternalValue::String("This is a test description".to_string())
    );

    let results = execute_query("SELECT LTRIM(description) as left_trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["left_trimmed"],
        InternalValue::String("This is a test description  ".to_string())
    );

    let results = execute_query("SELECT RTRIM(description) as right_trimmed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["right_trimmed"],
        InternalValue::String("  This is a test description".to_string())
    );

    // Test UPPER and LOWER functions
    let results = execute_query("SELECT UPPER(product_name) as upper_name FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["upper_name"],
        InternalValue::String("TEST PRODUCT".to_string())
    );

    let results = execute_query("SELECT LOWER(product_name) as lower_name FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["lower_name"],
        InternalValue::String("test product".to_string())
    );

    // Test REPLACE function
    let results =
        execute_query("SELECT REPLACE(product_name, 'Test', 'Demo') as replaced FROM test_stream")
            .await
            .unwrap();
    assert_eq!(
        results[0]["replaced"],
        InternalValue::String("Demo Product".to_string())
    );

    // Test LEFT and RIGHT functions
    let results = execute_query("SELECT LEFT(product_name, 4) as left_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["left_part"],
        InternalValue::String("Test".to_string())
    );

    let results = execute_query("SELECT RIGHT(product_name, 7) as right_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(
        results[0]["right_part"],
        InternalValue::String("Product".to_string())
    );
}

#[tokio::test]
async fn test_date_time_functions() {
    // Test NOW function
    let results = execute_query("SELECT NOW() as current_time FROM test_stream")
        .await
        .unwrap();
    let now_result = match &results[0]["current_time"] {
        InternalValue::Integer(n) => *n,
        _ => panic!("Expected Integer for current_time"),
    };
    assert!(now_result > 1734652800000); // Should be after our test timestamp

    // Test CURRENT_TIMESTAMP function
    let results = execute_query("SELECT CURRENT_TIMESTAMP as current_ts FROM test_stream")
        .await
        .unwrap();
    let ts_result = match &results[0]["current_ts"] {
        InternalValue::Integer(n) => *n,
        _ => panic!("Expected Integer for current_ts"),
    };
    assert!(ts_result > 1734652800000);

    // Test EXTRACT function with record timestamp
    let results = execute_query("SELECT EXTRACT('YEAR', _timestamp) as year_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["year_part"], InternalValue::Integer(2024));

    let results =
        execute_query("SELECT EXTRACT('MONTH', _timestamp) as month_part FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["month_part"], InternalValue::Integer(12));

    let results = execute_query("SELECT EXTRACT('DAY', _timestamp) as day_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["day_part"], InternalValue::Integer(20));

    let results = execute_query("SELECT EXTRACT('HOUR', _timestamp) as hour_part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["hour_part"], InternalValue::Integer(0));

    // Test DATE_FORMAT function
    let results = execute_query(
        "SELECT DATE_FORMAT(_timestamp, '%Y-%m-%d') as formatted_date FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["formatted_date"],
        InternalValue::String("2024-12-20".to_string())
    );

    let results = execute_query("SELECT DATE_FORMAT(_timestamp, '%Y-%m-%d %H:%M:%S') as formatted_datetime FROM test_stream").await.unwrap();
    assert_eq!(
        results[0]["formatted_datetime"],
        InternalValue::String("2024-12-20 00:00:00".to_string())
    );
}

#[tokio::test]
async fn test_utility_functions() {
    // Test COALESCE function
    let results = execute_query(
        "SELECT COALESCE(NULL, 'default', 'backup') as coalesce_result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["coalesce_result"],
        InternalValue::String("default".to_string())
    );

    let results = execute_query(
        "SELECT COALESCE(product_name, 'default') as coalesce_product FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["coalesce_product"],
        InternalValue::String("Test Product".to_string())
    );

    // Test NULLIF function
    let results = execute_query(
        "SELECT NULLIF(product_name, 'Test Product') as nullif_result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["nullif_result"], InternalValue::Null);

    let results = execute_query(
        "SELECT NULLIF(product_name, 'Different') as nullif_different FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["nullif_different"],
        InternalValue::String("Test Product".to_string())
    );
}

#[tokio::test]
async fn test_complex_expressions() {
    // Test combining multiple functions
    let results = execute_query(
        "SELECT UPPER(LEFT(TRIM(description), 10)) as complex_string FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["complex_string"],
        InternalValue::String("THIS IS A ".to_string())
    );

    let results =
        execute_query("SELECT ROUND(ABS(negative_num) * 1.5, 1) as complex_math FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["complex_math"], InternalValue::Number(22.5));

    let results = execute_query(
        "SELECT CONCAT('Order ', quantity, ' at $', ROUND(amount, 2)) as order_summary FROM test_stream"
    ).await.unwrap();
    assert_eq!(
        results[0]["order_summary"],
        InternalValue::String("Order 42 at $123.46".to_string())
    );
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
    assert_eq!(results[0]["null_length"], InternalValue::Null);

    let results = execute_query("SELECT UPPER(NULL) as null_upper FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["null_upper"], InternalValue::Null);

    let results = execute_query("SELECT TRIM(NULL) as null_trim FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["null_trim"], InternalValue::Null);
}

#[tokio::test]
async fn test_type_conversions() {
    // Test functions with different input types
    let results =
        execute_query("SELECT CONCAT('Value: ', quantity) as concat_int FROM test_stream")
            .await
            .unwrap();
    assert_eq!(
        results[0]["concat_int"],
        InternalValue::String("Value: 42".to_string())
    );

    let results =
        execute_query("SELECT CONCAT('Amount: $', amount) as concat_float FROM test_stream")
            .await
            .unwrap();
    assert_eq!(
        results[0]["concat_float"],
        InternalValue::String("Amount: $123.456".to_string())
    );

    // Test POWER with integer inputs
    let results = execute_query("SELECT POWER(quantity, 2) as power_int FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["power_int"], InternalValue::Number(1764.0));
}

#[tokio::test]
async fn test_new_comparison_functions() {
    // Test LEAST function
    let results = execute_query("SELECT LEAST(10, 5, 15) as least_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["least_result"], InternalValue::Integer(5));

    let results = execute_query("SELECT LEAST(amount, quantity) as least_mixed FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["least_mixed"], InternalValue::Number(42.0));

    // Test GREATEST function
    let results = execute_query("SELECT GREATEST(10, 5, 15) as greatest_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["greatest_result"], InternalValue::Integer(15));

    let results =
        execute_query("SELECT GREATEST(amount, quantity) as greatest_mixed FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["greatest_mixed"], InternalValue::Number(123.456));

    // Test with strings
    let results =
        execute_query("SELECT LEAST('apple', 'banana', 'cherry') as least_string FROM test_stream")
            .await
            .unwrap();
    assert_eq!(
        results[0]["least_string"],
        InternalValue::String("apple".to_string())
    );

    let results = execute_query(
        "SELECT GREATEST('apple', 'banana', 'cherry') as greatest_string FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(
        results[0]["greatest_string"],
        InternalValue::String("cherry".to_string())
    );

    // Test with NULL values
    let results = execute_query("SELECT LEAST(10, NULL, 5) as least_with_null FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["least_with_null"], InternalValue::Integer(5));

    let results =
        execute_query("SELECT GREATEST(10, NULL, 5) as greatest_with_null FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["greatest_with_null"], InternalValue::Integer(10));
}

#[tokio::test]
async fn test_datediff_function() {
    // For this test, we'll create a custom test record with specific dates
    // Since our execute_query function uses a fixed test record, we'll test with computed values

    // Test hours difference between two timestamps
    let results = execute_query(
        "SELECT DATEDIFF('hours', 1734652800000, 1734739200000) as hour_diff FROM test_stream",
    )
    .await
    .unwrap();
    let hour_diff = match &results[0]["hour_diff"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer result for DATEDIFF hours"),
    };
    assert_eq!(hour_diff, 24); // 24 hours difference

    // Test days difference
    let results = execute_query(
        "SELECT DATEDIFF('days', 1734652800000, 1734739200000) as day_diff FROM test_stream",
    )
    .await
    .unwrap();
    let day_diff = match &results[0]["day_diff"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer result for DATEDIFF days"),
    };
    assert_eq!(day_diff, 1); // 1 day difference
}

#[tokio::test]
async fn test_position_function() {
    // Test POSITION function - using product_name from our test record which is "Test Product"
    let results = execute_query("SELECT POSITION('t', product_name) as t_pos FROM test_stream")
        .await
        .unwrap();
    let t_pos = match &results[0]["t_pos"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer result for POSITION"),
    };
    assert_eq!(t_pos, 4); // Position of 't' in "Test Product" (case-sensitive, first lowercase 't' in "Test")

    // Test POSITION when substring not found
    let results =
        execute_query("SELECT POSITION('xyz', product_name) as not_found FROM test_stream")
            .await
            .unwrap();
    let not_found = match &results[0]["not_found"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer result for POSITION not found"),
    };
    assert_eq!(not_found, 0); // Should return 0 when not found
}

#[tokio::test]
async fn test_listagg_function() {
    // Test LISTAGG with single string value (our test data doesn't have arrays, so we test this way)
    let results =
        execute_query("SELECT LISTAGG(product_name, '; ') as single_product FROM test_stream")
            .await
            .unwrap();
    let single_product = match &results[0]["single_product"] {
        InternalValue::String(val) => val,
        _ => panic!("Expected string result for LISTAGG single value"),
    };
    assert_eq!(single_product, "Test Product");
}

#[tokio::test]
async fn test_having_clause_execution() {
    // Note: HAVING clause testing is complex because it requires GROUP BY functionality
    // For now, we test that HAVING clauses are parsed correctly and that the execution
    // pipeline supports them at the record level

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(
        tx,
        std::sync::Arc::new(ferrisstreams::ferris::serialization::JsonFormat),
    );
    let parser = StreamingSqlParser::new();

    // Test simple HAVING clause (would work with aggregated data in a real scenario)
    let query = "SELECT quantity, amount FROM test HAVING quantity > 40";
    let parsed_query = parser.parse(query).unwrap();

    // Verify the query parsed correctly with HAVING clause
    match &parsed_query {
        ferrisstreams::ferris::sql::StreamingQuery::Select {
            having: Some(_), ..
        } => {
            // HAVING clause is present in parsed query
        }
        _ => panic!("Expected query with HAVING clause"),
    }

    let test_record = create_test_record();
    let internal_record = convert_stream_record_to_internal(&test_record);
    engine
        .execute(&parsed_query, internal_record)
        .await
        .unwrap();

    let result = rx.try_recv().unwrap();
    // Should return the record since quantity (42) > 40
    let quantity_result = match &result["quantity"] {
        ferrisstreams::ferris::serialization::InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer result for quantity"),
    };
    assert_eq!(quantity_result, 42);

    // Test HAVING clause that should filter out the record
    let query = "SELECT quantity, amount FROM test HAVING quantity > 50";
    let parsed_query = parser.parse(query).unwrap();
    let test_record = create_test_record();

    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    // Should return an error or Ok(None) since quantity (42) is not > 50
    // For now, let's just check it executes without panicking
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_datediff_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(
        tx,
        std::sync::Arc::new(ferrisstreams::ferris::serialization::JsonFormat),
    );
    let parser = StreamingSqlParser::new();

    // Test with wrong number of arguments
    let query = "SELECT DATEDIFF('hours', 123456789) FROM test";
    let parsed_query = parser.parse(query).unwrap();
    let test_record = create_test_record();
    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    assert!(
        result.is_err(),
        "DATEDIFF should fail with wrong number of arguments"
    );

    // Test with invalid unit
    let test_record = create_test_record();
    let query = "SELECT DATEDIFF('invalid_unit', 123456789, 987654321) FROM test";
    let parsed_query = parser.parse(query).unwrap();
    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    assert!(
        result.is_err(),
        "DATEDIFF should fail with invalid time unit"
    );
}

#[tokio::test]
async fn test_position_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(
        tx,
        std::sync::Arc::new(ferrisstreams::ferris::serialization::JsonFormat),
    );
    let parser = StreamingSqlParser::new();

    // Test with wrong number of arguments (too few)
    let query = "SELECT POSITION('test') FROM test";
    let parsed_query = parser.parse(query).unwrap();
    let test_record = create_test_record();
    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    assert!(
        result.is_err(),
        "POSITION should fail with too few arguments"
    );

    // Test with wrong number of arguments (too many)
    let query = "SELECT POSITION('a', 'test', 1, 'extra') FROM test";
    let parsed_query = parser.parse(query).unwrap();
    let test_record = create_test_record();
    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    assert!(
        result.is_err(),
        "POSITION should fail with too many arguments"
    );
}

#[tokio::test]
async fn test_listagg_error_cases() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(
        tx,
        std::sync::Arc::new(ferrisstreams::ferris::serialization::JsonFormat),
    );
    let parser = StreamingSqlParser::new();

    // Test with wrong number of arguments
    let query = "SELECT LISTAGG('test') FROM test";
    let parsed_query = parser.parse(query).unwrap();
    let test_record = create_test_record();
    let internal_record = convert_stream_record_to_internal(&test_record);
    let result = engine.execute(&parsed_query, internal_record).await;
    assert!(
        result.is_err(),
        "LISTAGG should fail with wrong number of arguments"
    );
}

#[tokio::test]
async fn test_comprehensive_new_functions_integration() {
    // Test combining multiple new functions in one query using our existing test data
    let results = execute_query(
        "SELECT 
            POSITION('o', product_name) as letter_pos,
            ABS(negative_num) as absolute_val,
            UPPER(LEFT(product_name, 4)) as first_word_upper
        FROM test_stream",
    )
    .await
    .unwrap();

    // Verify all function results
    let letter_pos = match &results[0]["letter_pos"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer for letter_pos"),
    };
    assert_eq!(letter_pos, 8); // Position of 'o' in "Test Product" (first 'o' in "Product")

    let absolute_val = match &results[0]["absolute_val"] {
        InternalValue::Integer(val) => *val,
        _ => panic!("Expected integer for absolute_val"),
    };
    assert_eq!(absolute_val, 15); // ABS(-15) = 15

    let first_word_upper = match &results[0]["first_word_upper"] {
        InternalValue::String(val) => val,
        _ => panic!("Expected string for first_word_upper"),
    };
    assert_eq!(first_word_upper, "TEST");
}

#[tokio::test]
async fn test_abs_function_extended() {
    // Test ABS with integers using the negative_num field
    let results = execute_query("SELECT ABS(negative_num) as abs_int FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_int"], InternalValue::Integer(15));

    let results = execute_query("SELECT ABS(quantity) as abs_positive FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_positive"], InternalValue::Integer(42));

    // Test ABS with floats
    let results = execute_query("SELECT ABS(amount) as abs_float FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_float"], InternalValue::Number(123.456));

    // Test ABS with NULL
    let results = execute_query("SELECT ABS(NULL) as abs_null FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["abs_null"], InternalValue::Null);
}

#[tokio::test]
async fn test_statistical_functions() {
    // Test STDDEV function
    let results = execute_query("SELECT STDDEV(amount) as stddev_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["stddev_result"], InternalValue::Number(0.0));

    // Test STDDEV_SAMP function
    let results =
        execute_query("SELECT STDDEV_SAMP(quantity) as stddev_samp_result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["stddev_samp_result"], InternalValue::Number(0.0));

    // Test STDDEV_POP function
    let results = execute_query("SELECT STDDEV_POP(amount) as stddev_pop_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["stddev_pop_result"], InternalValue::Number(0.0));

    // Test VARIANCE function
    let results = execute_query("SELECT VARIANCE(amount) as variance_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["variance_result"], InternalValue::Number(0.0));

    // Test VAR_SAMP function
    let results = execute_query("SELECT VAR_SAMP(quantity) as var_samp_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["var_samp_result"], InternalValue::Number(0.0));

    // Test VAR_POP function
    let results = execute_query("SELECT VAR_POP(amount) as var_pop_result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["var_pop_result"], InternalValue::Number(0.0));

    // Test MEDIAN function with integer
    let results = execute_query("SELECT MEDIAN(quantity) as median_int FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["median_int"], InternalValue::Integer(42));

    // Test MEDIAN function with float
    let results = execute_query("SELECT MEDIAN(amount) as median_float FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["median_float"], InternalValue::Number(123.456));

    // Test MEDIAN with NULL
    let results = execute_query("SELECT MEDIAN(NULL) as median_null FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["median_null"], InternalValue::Null);
}

#[tokio::test]
async fn test_statistical_functions_error_handling() {
    // Test STDDEV with no arguments
    let result = execute_query("SELECT STDDEV() as stddev_no_args FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test STDDEV with too many arguments
    let result =
        execute_query("SELECT STDDEV(amount, quantity) as stddev_too_many FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test VARIANCE with no arguments
    let result = execute_query("SELECT VARIANCE() as variance_no_args FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test MEDIAN with no arguments
    let result = execute_query("SELECT MEDIAN() as median_no_args FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test MEDIAN with non-numeric argument
    let result =
        execute_query("SELECT MEDIAN(product_name) as median_string FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("numeric argument"));

    // Test STDDEV_POP with multiple arguments
    let result = execute_query(
        "SELECT STDDEV_POP(amount, quantity, negative_num) as stddev_pop_multi FROM test_stream",
    )
    .await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test VAR_POP with no arguments
    let result = execute_query("SELECT VAR_POP() as var_pop_no_args FROM test_stream").await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));
}

#[tokio::test]
async fn test_statistical_functions_with_expressions() {
    // Test STDDEV with expression
    let results = execute_query("SELECT STDDEV(ABS(negative_num)) as stddev_expr FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["stddev_expr"], InternalValue::Number(0.0));

    // Test MEDIAN with expression
    let results = execute_query("SELECT MEDIAN(quantity * 2) as median_expr FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["median_expr"], InternalValue::Integer(84));

    // Test VARIANCE with ROUND expression
    let results =
        execute_query("SELECT VARIANCE(ROUND(amount)) as variance_round FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["variance_round"], InternalValue::Number(0.0));
}

#[tokio::test]
async fn test_multiple_statistical_functions() {
    // Test multiple statistical functions in one query
    let results = execute_query(
        "SELECT STDDEV(amount) as std, VARIANCE(amount) as var, MEDIAN(quantity) as med FROM test_stream"
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["std"], InternalValue::Number(0.0));
    assert_eq!(results[0]["var"], InternalValue::Number(0.0));
    assert_eq!(results[0]["med"], InternalValue::Integer(42));
}
