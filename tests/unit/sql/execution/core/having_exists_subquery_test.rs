/*!
# HAVING Clause with EXISTS Subquery Tests

Tests for EXISTS subquery support in HAVING clauses, particularly for GROUP BY aggregations.
This functionality was implemented to support queries like:

```sql
SELECT symbol, COUNT(*) as spike_count
FROM market_data_ts
GROUP BY symbol
HAVING EXISTS (
    SELECT 1 FROM market_data_ts m2
    WHERE m2.symbol = market_data_ts.symbol
    AND m2.volume > 10000
)
AND COUNT(*) >= 5
```
*/

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::streaming::{
    RecordBatch, RecordStream, SimpleStreamRecord as StreamingRecord, StreamResult,
};
use velostream::velostream::table::unified_table::{TableResult, UnifiedTable};

/// Create test records for market data
fn create_market_data_records() -> Vec<StreamRecord> {
    let symbols = vec!["AAPL", "GOOGL", "MSFT"];
    let mut records = Vec::new();

    for (i, symbol) in symbols.iter().enumerate() {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + i as f64 * 10.0),
        );
        fields.insert(
            "volume".to_string(),
            FieldValue::Integer(if i == 0 { 15000 } else { 5000 }),
        );
        fields.insert(
            "event_time".to_string(),
            FieldValue::Integer(1640995200000 + i as i64 * 1000),
        );

        records.push(StreamRecord {
            fields,
            headers: HashMap::new(),
            timestamp: 1640995200000 + i as i64 * 1000,
            offset: i as i64,
            partition: 0,
            event_time: None,
            topic: None,
            key: None,
        });
    }

    records
}

/// Mock table that simulates market_data_ts with volume filtering
#[derive(Debug)]
struct MockMarketDataTable {
    name: String,
    records: Vec<HashMap<String, FieldValue>>,
}

impl MockMarketDataTable {
    fn new(name: &str) -> Self {
        let mut records = Vec::new();

        // AAPL with high volume (>10000)
        let mut record1 = HashMap::new();
        record1.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        record1.insert("price".to_string(), FieldValue::Float(150.0));
        record1.insert("volume".to_string(), FieldValue::Integer(15000));
        record1.insert("event_time".to_string(), FieldValue::Integer(1640995200000));
        records.push(record1);

        // AAPL - another high volume record
        let mut record2 = HashMap::new();
        record2.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        record2.insert("price".to_string(), FieldValue::Float(151.0));
        record2.insert("volume".to_string(), FieldValue::Integer(12000));
        record2.insert("event_time".to_string(), FieldValue::Integer(1640995210000));
        records.push(record2);

        // GOOGL with low volume (<10000)
        let mut record3 = HashMap::new();
        record3.insert(
            "symbol".to_string(),
            FieldValue::String("GOOGL".to_string()),
        );
        record3.insert("price".to_string(), FieldValue::Float(2800.0));
        record3.insert("volume".to_string(), FieldValue::Integer(5000));
        record3.insert("event_time".to_string(), FieldValue::Integer(1640995220000));
        records.push(record3);

        // MSFT with medium volume (exactly 10000)
        let mut record4 = HashMap::new();
        record4.insert("symbol".to_string(), FieldValue::String("MSFT".to_string()));
        record4.insert("price".to_string(), FieldValue::Float(300.0));
        record4.insert("volume".to_string(), FieldValue::Integer(10000));
        record4.insert("event_time".to_string(), FieldValue::Integer(1640995230000));
        records.push(record4);

        MockMarketDataTable {
            name: name.to_string(),
            records,
        }
    }
}

#[async_trait]
impl UnifiedTable for MockMarketDataTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        if let Some(index_str) = key.strip_prefix(&format!("{}_", self.name)) {
            if let Ok(index) = index_str.parse::<usize>() {
                if let Some(record) = self.records.get(index) {
                    return Ok(Some(record.clone()));
                }
            }
        }
        Ok(None)
    }

    fn contains_key(&self, key: &str) -> bool {
        if let Some(index_str) = key.strip_prefix(&format!("{}_", self.name)) {
            if let Ok(index) = index_str.parse::<usize>() {
                return index < self.records.len();
            }
        }
        false
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        Box::new(
            self.records
                .iter()
                .enumerate()
                .map(|(i, record)| (format!("{}_{}", self.name, i), record.clone())),
        )
    }

    fn sql_column_values(&self, column: &str, _where_clause: &str) -> TableResult<Vec<FieldValue>> {
        let mut values = Vec::new();
        for record in &self.records {
            if let Some(value) = record.get(column) {
                values.push(value.clone());
            }
        }
        Ok(values)
    }

    fn sql_scalar(&self, _select_expr: &str, _where_clause: &str) -> TableResult<FieldValue> {
        // For EXISTS checks, return the count of matching records
        Ok(FieldValue::Integer(self.records.len() as i64))
    }

    fn sql_exists(&self, where_clause: &str) -> TableResult<bool> {
        // Simplified EXISTS implementation for testing
        // Check if any record matches the where clause conditions

        // Parse simple volume comparison patterns
        if where_clause.contains("volume") && where_clause.contains(">") {
            // Extract the threshold value
            let threshold = if where_clause.contains("> 100000") {
                100000
            } else if where_clause.contains("> 50000") {
                50000
            } else if where_clause.contains("> 10000") {
                10000
            } else {
                // Default to 0 if we can't parse the threshold
                0
            };

            // Check if any record has volume > threshold
            for record in &self.records {
                if let Some(FieldValue::Integer(volume)) = record.get("volume") {
                    if *volume > threshold {
                        return Ok(true);
                    }
                }
            }
            return Ok(false);
        }

        // Default: return true if table has records
        Ok(!self.records.is_empty())
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = mpsc::unbounded_channel();

        for (i, record) in self.records.iter().enumerate() {
            let streaming_record = StreamingRecord {
                key: format!("{}_{}", self.name, i),
                fields: record.clone(),
            };
            let _ = tx.send(Ok(streaming_record));
        }

        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        _batch_size: usize,
        _offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        let mut records = Vec::new();

        for (i, record) in self.records.iter().enumerate() {
            records.push(StreamingRecord {
                key: format!("{}_{}", self.name, i),
                fields: record.clone(),
            });
        }

        Ok(RecordBatch {
            records,
            has_more: false,
        })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(self.record_count())
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        Ok(FieldValue::Integer(self.records.len() as i64))
    }
}

async fn execute_group_by_having_test(
    query: &str,
    input_records: Vec<StreamRecord>,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;

    // Create processor context with mock table
    let query_id = "test_having_exists";
    let mut context =
        velostream::velostream::sql::execution::processors::context::ProcessorContext::new(
            query_id,
        );

    // Load mock market_data_ts table
    context.load_reference_table(
        "market_data_ts",
        Arc::new(MockMarketDataTable::new("market_data_ts")),
    );

    // Process all input records through GROUP BY
    let mut output_records = Vec::new();

    for record in input_records {
        let result =
            velostream::velostream::sql::execution::processors::QueryProcessor::process_query(
                &parsed_query,
                &record,
                &mut context,
            )?;

        if let Some(output_record) = result.record {
            output_records.push(output_record);
        }
    }

    Ok(output_records)
}

#[tokio::test]
async fn test_having_exists_subquery_basic() {
    // Test basic EXISTS subquery in HAVING clause
    let query = r#"
        SELECT symbol, COUNT(*) as count
        FROM test_stream
        GROUP BY symbol
        HAVING EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 10000)
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(
        result.is_ok(),
        "HAVING EXISTS subquery should execute successfully"
    );

    let results = result.unwrap();
    println!("Results: {:?}", results);

    // Since EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 10000) returns true,
    // all groups should pass the HAVING filter
    assert!(
        !results.is_empty(),
        "Should have at least one group passing HAVING EXISTS"
    );
}

#[tokio::test]
async fn test_having_exists_with_count_condition() {
    // Test EXISTS combined with COUNT condition - similar to the actual use case
    let query = r#"
        SELECT symbol, COUNT(*) as spike_count
        FROM test_stream
        GROUP BY symbol
        HAVING EXISTS (
            SELECT 1 FROM market_data_ts m2
            WHERE m2.volume > 10000
        )
        AND COUNT(*) >= 1
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(
        result.is_ok(),
        "HAVING EXISTS with COUNT should execute successfully"
    );

    let results = result.unwrap();

    // Both conditions must be satisfied:
    // 1. EXISTS check passes (there are records with volume > 10000)
    // 2. COUNT(*) >= 1 (all groups have at least 1 record)
    assert!(
        !results.is_empty(),
        "Groups satisfying both conditions should pass"
    );

    // Verify results have the expected fields
    for record in &results {
        assert!(record.fields.contains_key("symbol"));
        assert!(record.fields.contains_key("spike_count"));
    }
}

#[tokio::test]
async fn test_having_not_exists_subquery() {
    // Test NOT EXISTS subquery in HAVING clause
    let query = r#"
        SELECT symbol, COUNT(*) as count
        FROM test_stream
        GROUP BY symbol
        HAVING NOT EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 50000)
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(
        result.is_ok(),
        "HAVING NOT EXISTS subquery should execute successfully"
    );

    let results = result.unwrap();

    // NOT EXISTS (volume > 50000) should return true (no records have volume > 50000)
    // So all groups should pass
    assert!(
        !results.is_empty(),
        "Groups should pass NOT EXISTS condition"
    );
}

#[tokio::test]
async fn test_having_exists_with_complex_conditions() {
    // Test EXISTS with multiple AND conditions in HAVING
    let query = r#"
        SELECT symbol, COUNT(*) as count
        FROM test_stream
        GROUP BY symbol
        HAVING EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 10000)
           AND COUNT(*) >= 1
           AND COUNT(*) <= 10
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(
        result.is_ok(),
        "HAVING with complex conditions should execute successfully"
    );

    let results = result.unwrap();

    // All conditions should be evaluated:
    // - EXISTS returns true
    // - COUNT(*) >= 1 and <= 10
    assert!(
        !results.is_empty(),
        "Groups satisfying all conditions should pass"
    );
}

#[tokio::test]
async fn test_having_exists_no_false_positives() {
    // Test that HAVING EXISTS correctly filters when condition is false
    let query = r#"
        SELECT symbol, COUNT(*) as count
        FROM test_stream
        GROUP BY symbol
        HAVING EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 100000)
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(
        result.is_ok(),
        "HAVING EXISTS with false condition should execute successfully"
    );

    let results = result.unwrap();

    // Since no records have volume > 100000, EXISTS should return false
    // and HAVING should filter out all groups
    assert_eq!(
        results.len(),
        0,
        "No groups should pass when EXISTS condition is false"
    );
}

#[tokio::test]
async fn test_having_exists_parsing() {
    // Test that the parser correctly handles EXISTS in HAVING clause
    let parser = StreamingSqlParser::new();

    let queries = vec![
        "SELECT symbol, COUNT(*) FROM test GROUP BY symbol HAVING EXISTS (SELECT 1 FROM table)",
        "SELECT a, SUM(b) FROM test GROUP BY a HAVING NOT EXISTS (SELECT 1 FROM table WHERE c > 10)",
        "SELECT x, COUNT(*) FROM test GROUP BY x HAVING EXISTS (SELECT 1 FROM t1) AND COUNT(*) > 5",
    ];

    for query in queries {
        let result = parser.parse(query);
        if let Err(ref e) = result {
            eprintln!("Parse error for query: {}", query);
            eprintln!("Error: {:?}", e);
        }
        assert!(result.is_ok(), "Query should parse successfully: {}", query);
    }
}

#[tokio::test]
async fn test_having_exists_preserves_group_by_semantics() {
    // Verify that EXISTS in HAVING doesn't break GROUP BY semantics
    let query = r#"
        SELECT symbol, COUNT(*) as count
        FROM test_stream
        GROUP BY symbol
        HAVING COUNT(*) >= 1
           AND EXISTS (SELECT 1 FROM market_data_ts WHERE volume > 10000)
    "#;

    let input_records = create_market_data_records();
    let result = execute_group_by_having_test(query, input_records).await;

    assert!(result.is_ok(), "Query should execute successfully");

    let results = result.unwrap();

    // Verify grouping is preserved - each result should have a unique symbol
    let mut seen_symbols = std::collections::HashSet::new();
    for record in &results {
        if let Some(FieldValue::String(symbol)) = record.fields.get("symbol") {
            assert!(
                !seen_symbols.contains(symbol),
                "Each symbol should appear only once (proper grouping)"
            );
            seen_symbols.insert(symbol.clone());
        }
    }
}
