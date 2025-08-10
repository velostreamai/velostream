/*!
# Tests for Window Functions

Comprehensive test suite for window functions including LAG, LEAD, ROW_NUMBER, RANK, and DENSE_RANK.
Tests both functionality and error handling.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record(id: i64, value: i64, name: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Integer(value));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1734652800000 + (id * 1000), // Sequential timestamps
        offset: id,
        partition: 0,
    }
}

async fn execute_query_with_window(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    
    // Process each record to simulate streaming with window buffer
    for record in records {
        let internal_record: HashMap<String, InternalValue> = record
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
            .collect();

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
    }

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_row_number_function() {
    // Test ROW_NUMBER() OVER ()
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, ROW_NUMBER() OVER () as row_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    
    // Each record should have a row number
    for (i, result) in results.iter().enumerate() {
        let row_num = match &result["row_num"] {
            InternalValue::Integer(n) => *n,
            _ => panic!("Expected integer for row_num"),
        };
        assert_eq!(row_num, (i + 1) as i64);
    }
}

#[tokio::test]
async fn test_lag_function_basic() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
        create_test_record(4, 400, "fourth"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LAG(value) OVER () as prev_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 4);
    
    // First record should have NULL for LAG
    assert_eq!(results[0]["prev_value"], InternalValue::Null);
    
    // Second record should have first record's value
    if results.len() > 1 {
        assert_eq!(results[1]["prev_value"], InternalValue::Integer(100));
    }
    
    // Third record should have second record's value  
    if results.len() > 2 {
        assert_eq!(results[2]["prev_value"], InternalValue::Integer(200));
    }
}

#[tokio::test]
async fn test_lag_function_with_offset() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
        create_test_record(4, 400, "fourth"),
        create_test_record(5, 500, "fifth"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LAG(value, 2) OVER () as lag2_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 5);
    
    // First two records should have NULL for LAG(2)
    assert_eq!(results[0]["lag2_value"], InternalValue::Null);
    if results.len() > 1 {
        assert_eq!(results[1]["lag2_value"], InternalValue::Null);
    }
    
    // Third record should have first record's value
    if results.len() > 2 {
        assert_eq!(results[2]["lag2_value"], InternalValue::Integer(100));
    }
    
    // Fourth record should have second record's value
    if results.len() > 3 {
        assert_eq!(results[3]["lag2_value"], InternalValue::Integer(200));
    }
}

#[tokio::test]
async fn test_lag_function_with_default() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LAG(value, 1, -1) OVER () as lag_with_default FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);
    
    // First record should have default value -1
    assert_eq!(results[0]["lag_with_default"], InternalValue::Integer(-1));
    
    // Second record should have first record's value
    if results.len() > 1 {
        assert_eq!(results[1]["lag_with_default"], InternalValue::Integer(100));
    }
}

#[tokio::test]
async fn test_lead_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LEAD(value) OVER () as next_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    
    // All LEAD values should be NULL in streaming (cannot look forward)
    for result in &results {
        assert_eq!(result["next_value"], InternalValue::Null);
    }
}

#[tokio::test]
async fn test_lead_function_with_default() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LEAD(value, 1, -999) OVER () as lead_with_default FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);
    
    // All LEAD values should be the default value -999 in streaming
    for result in &results {
        assert_eq!(result["lead_with_default"], InternalValue::Integer(-999));
    }
}

#[tokio::test]
async fn test_rank_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, RANK() OVER () as rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    
    // In streaming, RANK behaves like ROW_NUMBER
    for (i, result) in results.iter().enumerate() {
        let rank_num = match &result["rank_num"] {
            InternalValue::Integer(n) => *n,
            _ => panic!("Expected integer for rank_num"),
        };
        assert_eq!(rank_num, (i + 1) as i64);
    }
}

#[tokio::test]
async fn test_dense_rank_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, DENSE_RANK() OVER () as dense_rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    
    // In streaming, DENSE_RANK behaves like ROW_NUMBER
    for (i, result) in results.iter().enumerate() {
        let dense_rank_num = match &result["dense_rank_num"] {
            InternalValue::Integer(n) => *n,
            _ => panic!("Expected integer for dense_rank_num"),
        };
        assert_eq!(dense_rank_num, (i + 1) as i64);
    }
}

#[tokio::test]
async fn test_multiple_window_functions() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, ROW_NUMBER() OVER () as row_num, LAG(value) OVER () as prev_value, RANK() OVER () as rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    
    for (i, result) in results.iter().enumerate() {
        // Check ROW_NUMBER
        let row_num = match &result["row_num"] {
            InternalValue::Integer(n) => *n,
            _ => panic!("Expected integer for row_num"),
        };
        assert_eq!(row_num, (i + 1) as i64);
        
        // Check RANK
        let rank_num = match &result["rank_num"] {
            InternalValue::Integer(n) => *n,
            _ => panic!("Expected integer for rank_num"),
        };
        assert_eq!(rank_num, (i + 1) as i64);
        
        // Check LAG
        if i == 0 {
            assert_eq!(result["prev_value"], InternalValue::Null);
        } else {
            let expected_prev = (i as i64) * 100; // Previous record's value
            assert_eq!(result["prev_value"], InternalValue::Integer(expected_prev));
        }
    }
}

// Error handling tests
#[tokio::test]
async fn test_row_number_with_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT ROW_NUMBER(1) OVER () as row_num FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("takes no arguments"));
}

#[tokio::test]
async fn test_lag_with_no_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT LAG() OVER () as lag_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("at least 1 argument"));
}

#[tokio::test]
async fn test_lag_with_too_many_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT LAG(value, 1, 0, 'extra') OVER () as lag_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("at most 3 arguments"));
}

#[tokio::test]
async fn test_lag_with_negative_offset_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT LAG(value, -1) OVER () as lag_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("must be non-negative"));
}

#[tokio::test]
async fn test_lead_with_no_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT LEAD() OVER () as lead_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("at least 1 argument"));
}

#[tokio::test]
async fn test_lead_with_negative_offset_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT LEAD(value, -2) OVER () as lead_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("must be non-negative"));
}

#[tokio::test]
async fn test_unsupported_window_function_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT UNKNOWN_FUNCTION() OVER () as unknown FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Unsupported window function"));
    assert!(error_msg.contains("UNKNOWN_FUNCTION"));
}

#[tokio::test]
async fn test_rank_with_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT RANK(value) OVER () as rank_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("takes no arguments"));
}

#[tokio::test]
async fn test_dense_rank_with_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];
    
    let result = execute_query_with_window(
        "SELECT DENSE_RANK(1, 2) OVER () as dense_rank_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("takes no arguments"));
}