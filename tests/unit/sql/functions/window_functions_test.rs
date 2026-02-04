/*!
# Tests for Window Functions

Comprehensive test suite for window functions including LAG, LEAD, ROW_NUMBER, RANK, and DENSE_RANK.
Tests both functionality and error handling.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

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
        event_time: None,
        topic: None,
        key: None,
    }
}

async fn execute_query_with_window(
    query: &str,
    records: Vec<StreamRecord>,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;

    // Process each record to simulate streaming with window buffer
    for record in records {
        engine.execute_with_record(&parsed_query, &record).await?;
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
        "SELECT id, ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS) as row_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // Each record should have a row number
    for (i, result) in results.iter().enumerate() {
        let row_num = match result.fields.get("row_num") {
            Some(&FieldValue::Integer(n)) => n,
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
        "SELECT id, value, LAG(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as prev_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 4);

    // First record should have NULL for LAG
    assert_eq!(results[0].fields.get("prev_value"), Some(&FieldValue::Null));

    // Second record should have first record's value
    if results.len() > 1 {
        assert_eq!(
            results[1].fields.get("prev_value"),
            Some(&FieldValue::Integer(100))
        );
    }

    // Third record should have second record's value
    if results.len() > 2 {
        assert_eq!(
            results[2].fields.get("prev_value"),
            Some(&FieldValue::Integer(200))
        );
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
        "SELECT id, value, LAG(value, 2) OVER (ROWS WINDOW BUFFER 100 ROWS) as lag2_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 5);

    // First two records should have NULL for LAG(2)
    assert_eq!(results[0].fields.get("lag2_value"), Some(&FieldValue::Null));
    if results.len() > 1 {
        assert_eq!(results[1].fields.get("lag2_value"), Some(&FieldValue::Null));
    }

    // Third record should have first record's value
    if results.len() > 2 {
        assert_eq!(
            results[2].fields.get("lag2_value"),
            Some(&FieldValue::Integer(100))
        );
    }

    // Fourth record should have second record's value
    if results.len() > 3 {
        assert_eq!(
            results[3].fields.get("lag2_value"),
            Some(&FieldValue::Integer(200))
        );
    }
}

#[tokio::test]
async fn test_lag_function_with_default() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LAG(value, 1, -1) OVER (ROWS WINDOW BUFFER 100 ROWS) as lag_with_default FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);

    // First record should have default value -1
    assert_eq!(
        results[0].fields.get("lag_with_default"),
        Some(&FieldValue::Integer(-1))
    );

    // Second record should have first record's value
    if results.len() > 1 {
        assert_eq!(
            results[1].fields.get("lag_with_default"),
            Some(&FieldValue::Integer(100))
        );
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
        "SELECT id, value, LEAD(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as next_value FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // All LEAD values should be NULL in streaming (cannot look forward)
    for result in &results {
        assert_eq!(result.fields.get("next_value"), Some(&FieldValue::Null));
    }
}

#[tokio::test]
async fn test_lead_function_with_default() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
    ];

    let results = execute_query_with_window(
        "SELECT id, value, LEAD(value, 1, -999) OVER (ROWS WINDOW BUFFER 100 ROWS) as lead_with_default FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);

    // All LEAD values should be the default value -999 in streaming
    for result in &results {
        assert_eq!(
            result.fields.get("lead_with_default"),
            Some(&FieldValue::Integer(-999))
        );
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
        "SELECT id, RANK() OVER (ROWS WINDOW BUFFER 100 ROWS) as rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // Without ORDER BY in OVER clause, all records are equal → all rank 1
    for result in results.iter() {
        let rank_num = match result.fields.get("rank_num") {
            Some(&FieldValue::Integer(n)) => n,
            _ => panic!("Expected integer for rank_num"),
        };
        assert_eq!(rank_num, 1);
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
        "SELECT id, DENSE_RANK() OVER (ROWS WINDOW BUFFER 100 ROWS) as dense_rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // Without ORDER BY in OVER clause, all records are equal → all dense_rank 1
    for result in results.iter() {
        let dense_rank_num = match result.fields.get("dense_rank_num") {
            Some(&FieldValue::Integer(n)) => n,
            _ => panic!("Expected integer for dense_rank_num"),
        };
        assert_eq!(dense_rank_num, 1);
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
        "SELECT id, ROW_NUMBER() OVER (ROWS WINDOW BUFFER 100 ROWS) as row_num, LAG(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as prev_value, RANK() OVER (ROWS WINDOW BUFFER 100 ROWS) as rank_num FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    for (i, result) in results.iter().enumerate() {
        // Check ROW_NUMBER
        let row_num = match result.fields.get("row_num") {
            Some(&FieldValue::Integer(n)) => n,
            _ => panic!("Expected integer for row_num"),
        };
        assert_eq!(row_num, (i + 1) as i64);

        // Check RANK (without ORDER BY, all records are equal → rank 1)
        let rank_num = match result.fields.get("rank_num") {
            Some(&FieldValue::Integer(n)) => n,
            _ => panic!("Expected integer for rank_num"),
        };
        assert_eq!(rank_num, 1);

        // Check LAG
        if i == 0 {
            assert_eq!(result.fields.get("prev_value"), Some(&FieldValue::Null));
        } else {
            let expected_prev = (i as i64) * 100; // Previous record's value
            assert_eq!(
                result.fields.get("prev_value"),
                Some(&FieldValue::Integer(expected_prev))
            );
        }
    }
}

#[tokio::test]
async fn test_first_value_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, FIRST_VALUE(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as first_val FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // All records should return the first value (100)
    for result in &results {
        assert_eq!(
            result.fields.get("first_val"),
            Some(&FieldValue::Integer(100))
        );
    }
}

#[tokio::test]
async fn test_last_value_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, LAST_VALUE(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as last_val FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // Each record should return its own value as the "last" value in streaming
    assert_eq!(
        results[0].fields.get("last_val"),
        Some(&FieldValue::Integer(100))
    );
    assert_eq!(
        results[1].fields.get("last_val"),
        Some(&FieldValue::Integer(200))
    );
    assert_eq!(
        results[2].fields.get("last_val"),
        Some(&FieldValue::Integer(300))
    );
}

#[tokio::test]
async fn test_nth_value_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
        create_test_record(4, 400, "fourth"),
    ];

    // Test NTH_VALUE(value, 2) - should return the 2nd value
    let results = execute_query_with_window(
        "SELECT id, NTH_VALUE(value, 2) OVER (ROWS WINDOW BUFFER 100 ROWS) as nth_val FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 4);

    // First record: only 1 record exists, so NULL
    assert_eq!(results[0].fields.get("nth_val"), Some(&FieldValue::Null));

    // Second record onwards: should return the 2nd value (200)
    for i in 1..results.len() {
        assert_eq!(
            results[i].fields.get("nth_val"),
            Some(&FieldValue::Integer(200))
        );
    }
}

#[tokio::test]
async fn test_percent_rank_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, PERCENT_RANK() OVER (ROWS WINDOW BUFFER 100 ROWS) as percent_rank FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // Without ORDER BY, all records are equal → rank 1 → percent_rank = (1-1)/(n-1) = 0.0
    for result in &results {
        assert_eq!(
            result.fields.get("percent_rank"),
            Some(&FieldValue::Float(0.0))
        );
    }
}

#[tokio::test]
async fn test_cume_dist_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
    ];

    let results = execute_query_with_window(
        "SELECT id, CUME_DIST() OVER (ROWS WINDOW BUFFER 100 ROWS) as cume_dist FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);

    // In streaming context, each record sees current_position / total_known_rows
    assert_eq!(
        results[0].fields.get("cume_dist"),
        Some(&FieldValue::Float(1.0))
    ); // 1/1
    assert_eq!(
        results[1].fields.get("cume_dist"),
        Some(&FieldValue::Float(1.0))
    ); // 2/2
    assert_eq!(
        results[2].fields.get("cume_dist"),
        Some(&FieldValue::Float(1.0))
    ); // 3/3
}

#[tokio::test]
async fn test_ntile_function() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
        create_test_record(4, 400, "fourth"),
    ];

    // Test NTILE(2) - split into 2 tiles
    let results = execute_query_with_window(
        "SELECT id, NTILE(2) OVER (ROWS WINDOW BUFFER 100 ROWS) as tile FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 4);

    // NTILE(2) distributes records into 2 tiles based on position in growing buffer
    // Each record is at the last position, so tile assignment depends on buffer size
    for result in &results {
        let tile = match result.fields.get("tile") {
            Some(&FieldValue::Integer(n)) => n,
            other => panic!("Expected integer for tile, got {:?}", other),
        };
        assert!(
            tile >= 1 && tile <= 2,
            "tile should be 1 or 2, got {}",
            tile
        );
    }
}

#[tokio::test]
async fn test_ntile_quartiles() {
    let records = vec![
        create_test_record(1, 100, "first"),
        create_test_record(2, 200, "second"),
        create_test_record(3, 300, "third"),
        create_test_record(4, 400, "fourth"),
    ];

    // Test NTILE(4) - split into quartiles
    let results = execute_query_with_window(
        "SELECT id, NTILE(4) OVER (ROWS WINDOW BUFFER 100 ROWS) as quartile FROM test_stream",
        records,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 4);

    // NTILE(4) distributes records into 4 quartiles based on position in growing buffer
    for result in &results {
        let quartile = match result.fields.get("quartile") {
            Some(&FieldValue::Integer(n)) => n,
            other => panic!("Expected integer for quartile, got {:?}", other),
        };
        assert!(
            quartile >= 1 && quartile <= 4,
            "quartile should be between 1 and 4, got {}",
            quartile
        );
    }
}

// Error handling tests
#[tokio::test]
async fn test_row_number_with_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];

    let result = execute_query_with_window(
        "SELECT ROW_NUMBER(1) OVER (ROWS WINDOW BUFFER 100 ROWS) as row_num FROM test_stream",
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
        "SELECT LAG() OVER (ROWS WINDOW BUFFER 100 ROWS) as lag_val FROM test_stream",
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
        "SELECT LAG(value, 1, 0, 'extra') OVER (ROWS WINDOW BUFFER 100 ROWS) as lag_val FROM test_stream",
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
        "SELECT LAG(value, -1) OVER (ROWS WINDOW BUFFER 100 ROWS) as lag_val FROM test_stream",
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
        "SELECT LEAD() OVER (ROWS WINDOW BUFFER 100 ROWS) as lead_val FROM test_stream",
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
        "SELECT LEAD(value, -2) OVER (ROWS WINDOW BUFFER 100 ROWS) as lead_val FROM test_stream",
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
        "SELECT UNKNOWN_FUNCTION() OVER (ROWS WINDOW BUFFER 100 ROWS) as unknown FROM test_stream",
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
        "SELECT RANK(value) OVER (ROWS WINDOW BUFFER 100 ROWS) as rank_val FROM test_stream",
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
        "SELECT DENSE_RANK(1, 2) OVER (ROWS WINDOW BUFFER 100 ROWS) as dense_rank_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("takes no arguments"));
}

#[tokio::test]
async fn test_first_value_with_no_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];

    let result = execute_query_with_window(
        "SELECT FIRST_VALUE() OVER (ROWS WINDOW BUFFER 100 ROWS) as first_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("requires exactly 1 argument"));
}

#[tokio::test]
async fn test_nth_value_with_invalid_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];

    // Test with no arguments
    let result = execute_query_with_window(
        "SELECT NTH_VALUE() OVER (ROWS WINDOW BUFFER 100 ROWS) as nth_val FROM test_stream",
        records.clone(),
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("requires exactly 2 arguments"));

    // Test with invalid position
    let result = execute_query_with_window(
        "SELECT NTH_VALUE(value, 0) OVER (ROWS WINDOW BUFFER 100 ROWS) as nth_val FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("must be positive"));
}

#[tokio::test]
async fn test_ntile_with_invalid_arguments_error() {
    let records = vec![create_test_record(1, 100, "first")];

    // Test with no arguments
    let result = execute_query_with_window(
        "SELECT NTILE() OVER (ROWS WINDOW BUFFER 100 ROWS) as tile FROM test_stream",
        records.clone(),
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("requires exactly 1 argument"));

    // Test with invalid tiles count
    let result = execute_query_with_window(
        "SELECT NTILE(0) OVER (ROWS WINDOW BUFFER 100 ROWS) as tile FROM test_stream",
        records,
    )
    .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("must be positive"));
}
