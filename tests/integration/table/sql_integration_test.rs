/*!
# Table SQL Integration Tests

End-to-end integration tests for Table SQL functionality with real Kafka topics.
These tests verify the complete flow from Kafka messages through Table to SQL queries.
*/

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time::sleep;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use velostream::velostream::kafka::serialization::{JsonSerializer, StringSerializer};
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{KafkaDataSource, SqlDataSource, SqlQueryable};
use velostream::velostream::table::Table;

const TEST_KAFKA_BROKERS: &str = "localhost:9092";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UserProfile {
    id: String,
    name: String,
    email: String,
    tier: String,
    active: bool,
    score: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ConfigEntry {
    key: String,
    value: String,
    enabled: bool,
    priority: i32,
}

#[tokio::test]
async fn test_table_sql_with_real_kafka() {
    // Skip if Kafka is not available
    if std::env::var("SKIP_KAFKA_TESTS").is_ok() {
        println!("Skipping Kafka integration test");
        return;
    }

    let config = ConsumerConfig::new(TEST_KAFKA_BROKERS, "table-sql-test-group")
        .auto_offset_reset(OffsetReset::Earliest);

    match Table::<String, _, _>::new(
        config,
        "test-user-profiles".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await
    {
        Ok(table) => {
            // Create data source from Table
            let datasource = KafkaDataSource::from_table(table.clone());

            // Start Table consumption in background
            let table_clone = table.clone();
            let consume_handle = tokio::spawn(async move {
                let _ = table_clone.start().await;
            });

            // Give it time to consume any existing messages
            sleep(Duration::from_millis(500)).await;

            // Test SQL operations
            test_sql_operations_on_table(&datasource).await;

            // Stop Table
            table.stop();
            let _ = consume_handle.await;
        }
        Err(e) => {
            println!("⚠️ Kafka not available for integration test: {:?}", e);
            println!(
                "   Ensure Kafka is running at {} to run this test",
                TEST_KAFKA_BROKERS
            );
        }
    }
}

async fn test_sql_operations_on_table(datasource: &KafkaDataSource) {
    // Test basic queries even if no data is present
    let all_records = datasource.get_all_records().unwrap();
    println!("Table has {} records", all_records.len());

    // Test EXISTS query
    let exists = datasource.sql_exists("true").unwrap();
    assert_eq!(exists, !datasource.is_empty());

    // Test filter query
    let filtered = datasource.sql_filter("true").unwrap();
    assert_eq!(filtered.len(), datasource.record_count());

    // If there's data, test more complex queries
    if !datasource.is_empty() {
        // Test filtering by specific fields (assuming user profile structure)
        let premium_users = datasource.sql_filter("tier = 'premium'");
        if premium_users.is_ok() {
            println!("Found {} premium users", premium_users.unwrap().len());
        }

        // Test column value extraction
        let user_ids = datasource.sql_column_values("id", "active = true");
        if user_ids.is_ok() {
            println!("Found {} active user IDs", user_ids.unwrap().len());
        }
    }
}

#[tokio::test]
async fn test_table_sql_with_mock_data() {
    // This test doesn't require Kafka, uses a mock Table-like structure
    use std::sync::{Arc, RwLock};

    // Create a mock table with test data
    let mut table_data = HashMap::new();

    table_data.insert(
        "config1".to_string(),
        json!({
            "key": "max_connections",
            "value": "100",
            "enabled": true,
            "priority": 1
        }),
    );

    table_data.insert(
        "config2".to_string(),
        json!({
            "key": "timeout_seconds",
            "value": "30",
            "enabled": false,
            "priority": 2
        }),
    );

    table_data.insert(
        "config3".to_string(),
        json!({
            "key": "retry_limit",
            "value": "3",
            "enabled": true,
            "priority": 3
        }),
    );

    // Create a mock data source
    let mock_source = MockTableDataSource {
        data: Arc::new(RwLock::new(table_data)),
    };

    // Test SQL operations
    test_config_queries(&mock_source).await;
}

// Mock implementation for testing without Kafka
struct MockTableDataSource {
    data: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl SqlDataSource for MockTableDataSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        let data = self.data.read().unwrap();
        let mut records = HashMap::new();

        for (key, value) in data.iter() {
            records.insert(key.clone(), json_to_field_value(value));
        }

        Ok(records)
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).map(json_to_field_value))
    }

    fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    fn record_count(&self) -> usize {
        self.data.read().unwrap().len()
    }
}

async fn test_config_queries(datasource: &impl SqlQueryable) {
    // Test filtering enabled configs
    let enabled = datasource.sql_filter("enabled = true").unwrap();
    assert_eq!(enabled.len(), 2); // config1 and config3

    // Test EXISTS for disabled configs
    let has_disabled = datasource.sql_exists("enabled = false").unwrap();
    assert!(has_disabled);

    // Test extracting config keys
    let keys = datasource.sql_column_values("key", "priority = 1").unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], FieldValue::String("max_connections".to_string()));

    // Test scalar query
    let value = datasource
        .sql_scalar("value", "key = 'timeout_seconds'")
        .unwrap();
    assert_eq!(value, FieldValue::String("30".to_string()));
}

// Helper function to convert JSON to FieldValue
fn json_to_field_value(value: &serde_json::Value) -> FieldValue {
    match value {
        serde_json::Value::Null => FieldValue::Null,
        serde_json::Value::Bool(b) => FieldValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => FieldValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let field_values: Vec<FieldValue> = arr.iter().map(json_to_field_value).collect();
            FieldValue::Array(field_values)
        }
        serde_json::Value::Object(obj) => {
            let mut fields = HashMap::new();
            for (key, val) in obj {
                fields.insert(key.clone(), json_to_field_value(val));
            }
            FieldValue::Struct(fields)
        }
    }
}

#[tokio::test]
async fn test_subquery_patterns() {
    // Test common subquery patterns that would be used in real SQL
    let mock_source = create_test_reference_data();

    // Pattern 1: EXISTS subquery for validation
    // SQL: WHERE EXISTS (SELECT 1 FROM users WHERE tier = 'premium')
    let has_premium_users = mock_source.sql_exists("tier = 'premium'").unwrap();
    assert!(has_premium_users);

    // Pattern 2: IN subquery for membership testing
    // SQL: WHERE user_id IN (SELECT id FROM users WHERE active = true)
    let active_ids = mock_source
        .sql_column_values("id", "active = true")
        .unwrap();
    assert!(!active_ids.is_empty());

    // Pattern 3: Scalar subquery for configuration
    // SQL: WHERE amount > (SELECT max_limit FROM config WHERE type = 'transaction')
    let limit = mock_source
        .sql_scalar("max_limit", "type = 'transaction'")
        .unwrap();
    match limit {
        FieldValue::Integer(val) => assert!(val > 0),
        FieldValue::Float(val) => assert!(val > 0.0),
        _ => {}
    }

    // Pattern 4: NOT EXISTS for exclusion
    // SQL: WHERE NOT EXISTS (SELECT 1 FROM blocked_users WHERE id = user_id)
    let no_blocked = !mock_source.sql_exists("status = 'blocked'").unwrap();
    assert!(no_blocked);
}

fn create_test_reference_data() -> MockTableDataSource {
    let mut data = HashMap::new();

    // User reference data
    data.insert(
        "user1".to_string(),
        json!({
            "id": "user1",
            "tier": "premium",
            "active": true,
            "credit_limit": 10000
        }),
    );

    data.insert(
        "user2".to_string(),
        json!({
            "id": "user2",
            "tier": "basic",
            "active": true,
            "credit_limit": 1000
        }),
    );

    // Config reference data
    data.insert(
        "config_tx_limit".to_string(),
        json!({
            "type": "transaction",
            "max_limit": 50000,
            "currency": "USD"
        }),
    );

    MockTableDataSource {
        data: Arc::new(RwLock::new(data)),
    }
}

#[tokio::test]
async fn test_performance_with_large_dataset() {
    use std::time::Instant;

    let mut data = HashMap::new();

    // Create 1000 records for performance testing
    for i in 0..1000 {
        data.insert(
            format!("record{}", i),
            json!({
                "id": format!("record{}", i),
                "value": i,
                "category": if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" },
                "active": i % 2 == 0
            }),
        );
    }

    let mock_source = MockTableDataSource {
        data: Arc::new(RwLock::new(data)),
    };

    // Test filter performance
    let start = Instant::now();
    let filtered = mock_source.sql_filter("category = 'A'").unwrap();
    let filter_duration = start.elapsed();

    assert!(filtered.len() > 300); // Should have ~333 records
    assert!(filter_duration.as_millis() < 50); // Should be fast for 1000 records

    // Test EXISTS performance (should terminate early)
    let start = Instant::now();
    let exists = mock_source.sql_exists("category = 'A'").unwrap();
    let exists_duration = start.elapsed();

    assert!(exists);
    assert!(exists_duration.as_micros() < 5000); // Should be under 5ms

    // Test column extraction performance
    let start = Instant::now();
    let values = mock_source
        .sql_column_values("id", "active = true")
        .unwrap();
    let extract_duration = start.elapsed();

    assert_eq!(values.len(), 500); // Half should be active
    assert!(extract_duration.as_millis() < 50); // Should be fast for 1000 records

    println!("Performance results for 1000 records:");
    println!("  Filter: {:?}", filter_duration);
    println!("  EXISTS: {:?}", exists_duration);
    println!("  Column extraction: {:?}", extract_duration);
}
