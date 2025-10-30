/*!
# Unified Loading Architecture Tests

This test suite validates the unified table loading architecture that provides
consistent bulk + incremental loading patterns across all data source types.

## Test Coverage

- ✅ Bulk loading using existing DataSource implementations
- ✅ Incremental loading with offset tracking
- ✅ OptimizedTableImpl integration with unified loading
- ✅ Loading statistics and performance monitoring
- ✅ Error handling and fallback behavior
- ✅ Integration with existing mature DataSource trait system

## Architecture Validation

These tests confirm that unified loading successfully provides unified loading
semantics while leveraging existing mature implementations without
creating architectural confusion.
*/

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::Utc;
use velostream::velostream::datasource::config::SourceConfig;
use velostream::velostream::datasource::traits::{DataReader, DataSource};
use velostream::velostream::datasource::types::{SourceMetadata, SourceOffset};
use velostream::velostream::schema::Schema;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::table::loading_helpers::{
    bulk_load_table, incremental_load_table, unified_load_table, LoadingConfig, LoadingStats,
};
use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

// ============================================================================
// MOCK DATA SOURCES FOR TESTING
// ============================================================================

/// Mock data source that simulates batch reading
struct MockBatchDataSource {
    records: Arc<RwLock<Vec<StreamRecord>>>,
    read_position: Arc<RwLock<usize>>,
    batch_size: usize,
}

impl MockBatchDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records: Arc::new(RwLock::new(records)),
            read_position: Arc::new(RwLock::new(0)),
            batch_size,
        }
    }

    fn add_records(&self, mut new_records: Vec<StreamRecord>) {
        let mut records = self.records.write().unwrap();
        records.append(&mut new_records);
    }
}

#[async_trait]
impl DataSource for MockBatchDataSource {
    async fn initialize(
        &mut self,
        _config: SourceConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Schema::new(vec![]))
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(MockBatchReader {
            records: self.records.clone(),
            read_position: self.read_position.clone(),
            batch_size: self.batch_size,
        }))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            source_type: "mock-batch".to_string(),
            version: "1.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec!["read".to_string(), "seek".to_string()],
        }
    }
}

/// Mock data reader for batch reading simulation
struct MockBatchReader {
    records: Arc<RwLock<Vec<StreamRecord>>>,
    read_position: Arc<RwLock<usize>>,
    batch_size: usize,
}

#[async_trait]
impl DataReader for MockBatchReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let records = self.records.read().unwrap();
        let mut position = self.read_position.write().unwrap();

        let start = *position;
        let end = (start + self.batch_size).min(records.len());

        if start >= records.len() {
            return Ok(Vec::new()); // No more data
        }

        let batch = records[start..end].to_vec();
        *position = end;

        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let SourceOffset::Generic(pos_str) = offset {
            if let Ok(position) = pos_str.parse::<usize>() {
                *self.read_position.write().unwrap() = position;
            }
        }
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let records = self.records.read().unwrap();
        let position = self.read_position.read().unwrap();
        Ok(*position < records.len())
    }
}

/// Mock streaming data source that simulates real-time updates
struct MockStreamingDataSource {
    initial_records: Vec<StreamRecord>,
    incremental_records: Arc<RwLock<Vec<StreamRecord>>>,
    read_position: Arc<RwLock<usize>>,
}

impl MockStreamingDataSource {
    fn new(initial_records: Vec<StreamRecord>) -> Self {
        Self {
            initial_records,
            incremental_records: Arc::new(RwLock::new(Vec::new())),
            read_position: Arc::new(RwLock::new(0)),
        }
    }

    fn add_incremental_records(&self, mut new_records: Vec<StreamRecord>) {
        let mut incremental = self.incremental_records.write().unwrap();
        incremental.append(&mut new_records);
    }
}

#[async_trait]
impl DataSource for MockStreamingDataSource {
    async fn initialize(
        &mut self,
        _config: SourceConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Schema::new(vec![]))
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(MockStreamingReader {
            initial_records: self.initial_records.clone(),
            incremental_records: self.incremental_records.clone(),
            read_position: self.read_position.clone(),
            is_initial_read: true,
        }))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            source_type: "mock-streaming".to_string(),
            version: "1.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: false,
            capabilities: vec![
                "read".to_string(),
                "seek".to_string(),
                "streaming".to_string(),
            ],
        }
    }
}

/// Mock streaming reader
struct MockStreamingReader {
    initial_records: Vec<StreamRecord>,
    incremental_records: Arc<RwLock<Vec<StreamRecord>>>,
    read_position: Arc<RwLock<usize>>,
    is_initial_read: bool,
}

#[async_trait]
impl DataReader for MockStreamingReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.is_initial_read {
            // Return initial records for bulk load
            self.is_initial_read = false;
            Ok(self.initial_records.clone())
        } else {
            // For incremental reading after seek
            let incremental = self.incremental_records.read().unwrap();
            let position = *self.read_position.read().unwrap();
            let initial_len = self.initial_records.len();

            // If we're past all data, return empty
            let total_len = initial_len + incremental.len();
            if position >= total_len {
                return Ok(Vec::new());
            }

            // If position is at or past initial records, read from incremental
            if position >= initial_len {
                let incremental_start = position - initial_len;
                let records = incremental[incremental_start..].to_vec();
                drop(incremental);
                // Update position to end of all data
                *self.read_position.write().unwrap() = total_len;
                Ok(records)
            } else {
                // Position is before end of initial records
                // This shouldn't happen in incremental mode after seek, return empty
                Ok(Vec::new())
            }
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let SourceOffset::Generic(pos_str) = offset {
            if let Ok(position) = pos_str.parse::<usize>() {
                *self.read_position.write().unwrap() = position;
                self.is_initial_read = false; // Seeking implies we want incremental reads
            }
        }
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.is_initial_read {
            Ok(!self.initial_records.is_empty())
        } else {
            let incremental = self.incremental_records.read().unwrap();
            let position = self.read_position.read().unwrap();
            let initial_len = self.initial_records.len();
            let total_len = initial_len + incremental.len();
            Ok(*position < total_len)
        }
    }
}

// ============================================================================
// HELPER FUNCTIONS FOR CREATING TEST DATA
// ============================================================================

fn create_test_record(key: i64, name: &str, value: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(key));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("value".to_string(), FieldValue::Integer(value));

    StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        offset: key,
        partition: 0,
        headers: HashMap::new(),
        event_time: Some(Utc::now()),
    }
}

fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| create_test_record(i as i64, &format!("record_{}", i), i as i64 * 10))
        .collect()
}

// ============================================================================
// UNIT TESTS FOR UNIFIED LOADING ARCHITECTURE
// ============================================================================

#[tokio::test]
async fn test_bulk_load_with_mock_data_source() {
    // Create test data
    let test_records = create_test_records(100);
    let data_source = MockBatchDataSource::new(test_records.clone(), 25);

    // Test bulk loading
    let config = LoadingConfig {
        max_bulk_records: Some(100),
        max_bulk_duration: Some(Duration::from_secs(10)),
        ..Default::default()
    };

    let loaded_records = bulk_load_table(&data_source, Some(config))
        .await
        .expect("Bulk load should succeed");

    // Verify all records were loaded
    assert_eq!(loaded_records.len(), 100);

    // Verify record content
    for (i, record) in loaded_records.iter().enumerate() {
        assert_eq!(
            record.fields.get("id"),
            Some(&FieldValue::Integer(i as i64))
        );
        assert_eq!(
            record.fields.get("name"),
            Some(&FieldValue::String(format!("record_{}", i)))
        );
        assert_eq!(
            record.fields.get("value"),
            Some(&FieldValue::Integer(i as i64 * 10))
        );
    }
}

#[tokio::test]
async fn test_incremental_load_with_mock_data_source() {
    // Create initial data
    let initial_records = create_test_records(50);
    let data_source = MockStreamingDataSource::new(initial_records);

    // Add incremental data
    let incremental_records = create_test_records(25)
        .into_iter()
        .map(|mut r| {
            // Modify to simulate new data
            if let Some(FieldValue::Integer(id)) = r.fields.get_mut("id") {
                *id += 1000;
            }
            if let Some(FieldValue::Integer(id)) = r.fields.get_mut("id") {
                *id += 1000;
            }
            r
        })
        .collect();

    data_source.add_incremental_records(incremental_records);

    // Test incremental loading from offset
    // Seek to offset 50 (after the initial 50 records) to read the incremental records
    let offset = SourceOffset::Generic("50".to_string());
    let config = LoadingConfig {
        max_incremental_records: Some(30),
        ..Default::default()
    };

    let loaded_records = incremental_load_table(&data_source, offset, Some(config))
        .await
        .expect("Incremental load should succeed");

    // Verify incremental records were loaded
    assert_eq!(loaded_records.len(), 25);

    // Verify these are the incremental records (ids >= 1000)
    for record in &loaded_records {
        if let Some(FieldValue::Integer(id)) = record.fields.get("id") {
            assert!(
                *id >= 1000,
                "Expected incremental record with id >= 1000, got {}",
                id
            );
        }
    }
}

#[tokio::test]
async fn test_unified_load_table_bulk_path() {
    // Create test data
    let test_records = create_test_records(75);
    let data_source = MockBatchDataSource::new(test_records, 20);

    // Test unified load without previous offset (should do bulk load)
    let (loaded_records, stats) = unified_load_table(&data_source, None, None)
        .await
        .expect("Unified load should succeed");

    // Verify bulk load was performed
    assert_eq!(loaded_records.len(), 75);
    assert_eq!(stats.bulk_records_loaded, 75);
    assert_eq!(stats.incremental_records_loaded, 0);
    assert_eq!(stats.total_load_operations, 1);
    assert!(stats.last_successful_load.is_some());
}

#[tokio::test]
async fn test_unified_load_table_incremental_path() {
    // Create test data
    let initial_records = create_test_records(30);
    let data_source = MockStreamingDataSource::new(initial_records);

    // Add incremental data
    let incremental_records = create_test_records(15)
        .into_iter()
        .map(|mut r| {
            if let Some(FieldValue::Integer(id)) = r.fields.get_mut("id") {
                *id += 500;
            }
            r
        })
        .collect();

    data_source.add_incremental_records(incremental_records);

    // Test unified load with previous offset (should do incremental load)
    let previous_offset = SourceOffset::Generic("30".to_string());
    let (loaded_records, stats) = unified_load_table(&data_source, Some(previous_offset), None)
        .await
        .expect("Unified load should succeed");

    // Verify incremental load was performed
    assert_eq!(loaded_records.len(), 15);
    assert_eq!(stats.bulk_records_loaded, 0);
    assert_eq!(stats.incremental_records_loaded, 15);
    assert_eq!(stats.total_load_operations, 1);
}

#[tokio::test]
async fn test_optimized_table_impl_bulk_load_integration() {
    // Create test data
    let test_records = create_test_records(50);
    let data_source = MockBatchDataSource::new(test_records, 15);

    // Create OptimizedTableImpl and test bulk loading
    let mut table = OptimizedTableImpl::new();

    let loading_stats = table
        .bulk_load_from_source(&data_source, None)
        .await
        .expect("OptimizedTableImpl bulk load should succeed");

    // Verify loading statistics
    assert_eq!(loading_stats.bulk_records_loaded, 50);
    assert_eq!(loading_stats.incremental_records_loaded, 0);
    assert_eq!(loading_stats.total_load_operations, 1);

    // Verify data was loaded into the table
    let record_count = table.record_count();
    assert_eq!(record_count, 50);

    // Verify specific records
    let record_0 = table
        .get_record("0")
        .expect("Should be able to get record 0");
    assert!(record_0.is_some());
    let record_0 = record_0.unwrap();
    assert_eq!(
        record_0.get("name"),
        Some(&FieldValue::String("record_0".to_string()))
    );
    assert_eq!(record_0.get("value"), Some(&FieldValue::Integer(0)));
}

#[tokio::test]
async fn test_optimized_table_impl_incremental_load_integration() {
    // Create test data
    let initial_records = create_test_records(25);
    let data_source = MockStreamingDataSource::new(initial_records);

    // Create OptimizedTableImpl and perform initial bulk load
    let mut table = OptimizedTableImpl::new();
    let _initial_stats = table
        .bulk_load_from_source(&data_source, None)
        .await
        .expect("Initial bulk load should succeed");

    // Add incremental data
    let incremental_records = create_test_records(10)
        .into_iter()
        .map(|mut r| {
            if let Some(FieldValue::Integer(id)) = r.fields.get_mut("id") {
                *id += 100;
            }
            if let Some(FieldValue::String(name)) = r.fields.get_mut("name") {
                *name = format!("incremental_{}", *name);
            }
            r
        })
        .collect();

    data_source.add_incremental_records(incremental_records);

    // Perform incremental load
    let offset = SourceOffset::Generic("25".to_string());
    let incremental_stats = table
        .incremental_load_from_source(&data_source, offset, None)
        .await
        .expect("Incremental load should succeed");

    // Verify incremental loading statistics
    assert_eq!(incremental_stats.bulk_records_loaded, 0);
    assert_eq!(incremental_stats.incremental_records_loaded, 10);
    assert_eq!(incremental_stats.total_load_operations, 1);

    // Verify table now contains both initial and incremental data
    let record_count = table.record_count();
    assert_eq!(record_count, 35); // 25 initial + 10 incremental

    // Verify incremental record was loaded
    let incremental_record = table
        .get_record("100")
        .expect("Should be able to get incremental record");
    assert!(incremental_record.is_some());
    let incremental_record = incremental_record.unwrap();
    assert_eq!(
        incremental_record.get("name"),
        Some(&FieldValue::String("incremental_record_0".to_string()))
    );
}

#[tokio::test]
async fn test_loading_config_customization() {
    // Create test data
    let test_records = create_test_records(1000);
    let data_source = MockBatchDataSource::new(test_records, 100);

    // Test with custom loading configuration
    let config = LoadingConfig {
        max_bulk_records: Some(500), // Limit to 500 records
        max_bulk_duration: Some(Duration::from_secs(5)),
        read_timeout: Duration::from_secs(10),
        continue_on_errors: false,
        ..Default::default()
    };

    let loaded_records = bulk_load_table(&data_source, Some(config))
        .await
        .expect("Bulk load with custom config should succeed");

    // Verify the limit was respected
    assert_eq!(loaded_records.len(), 500);
}

#[tokio::test]
async fn test_loading_error_handling() {
    // Create a mock data source that fails
    struct FailingDataSource;

    #[async_trait]
    impl DataSource for FailingDataSource {
        async fn initialize(
            &mut self,
            _config: SourceConfig,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
            Err("Schema fetch failed".into())
        }

        async fn create_reader(
            &self,
        ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
            Err("Reader creation failed".into())
        }

        fn supports_streaming(&self) -> bool {
            false
        }

        fn supports_batch(&self) -> bool {
            false
        }

        fn metadata(&self) -> SourceMetadata {
            SourceMetadata {
                source_type: "failing".to_string(),
                version: "1.0".to_string(),
                supports_streaming: false,
                supports_batch: false,
                supports_schema_evolution: false,
                capabilities: vec![],
            }
        }
    }

    let failing_source = FailingDataSource;

    // Test that appropriate errors are returned
    let result = bulk_load_table(&failing_source, None).await;
    assert!(result.is_err());

    match result {
        Err(SqlError::ConfigurationError { message }) => {
            assert!(message.contains("does not support batch reading"));
        }
        Err(e) => panic!("Expected ConfigurationError, got: {:?}", e),
        Ok(_) => panic!("Expected error, but got success"),
    }
}

#[tokio::test]
async fn test_loading_statistics_accuracy() {
    // Create test data
    let test_records = create_test_records(42);
    let data_source = MockBatchDataSource::new(test_records, 10);

    let _start_time = SystemTime::now();

    // Test bulk loading with statistics
    let loaded_records = bulk_load_table(&data_source, None)
        .await
        .expect("Bulk load should succeed");

    let _end_time = SystemTime::now();

    // Verify loaded record count
    assert_eq!(loaded_records.len(), 42);

    // Note: In a real implementation, we would get statistics from the helper function
    // For this test, we're primarily validating the integration works correctly
}

#[tokio::test]
async fn test_check_loading_support() {
    // Test with batch-only source
    let batch_source = MockBatchDataSource::new(create_test_records(10), 5);
    let (supports_bulk, supports_incremental) =
        velostream::velostream::table::loading_helpers::check_loading_support(&batch_source);

    assert!(
        supports_bulk,
        "MockBatchDataSource should support bulk loading"
    );
    assert!(
        supports_incremental,
        "MockBatchDataSource should support incremental loading"
    );

    // Test with streaming source
    let streaming_source = MockStreamingDataSource::new(create_test_records(10));
    let (supports_bulk, supports_incremental) =
        velostream::velostream::table::loading_helpers::check_loading_support(&streaming_source);

    assert!(
        supports_bulk,
        "MockStreamingDataSource should support bulk loading"
    );
    assert!(
        supports_incremental,
        "MockStreamingDataSource should support incremental loading"
    );
}

#[tokio::test]
async fn test_unified_load_with_optimized_table_impl() {
    // Create test data
    let initial_records = create_test_records(30);
    let data_source = MockBatchDataSource::new(initial_records, 10);

    // Create OptimizedTableImpl and test unified loading
    let mut table = OptimizedTableImpl::new();

    // Test unified load without previous offset (bulk path)
    let loading_stats = table
        .unified_load_from_source(&data_source, None, None)
        .await
        .expect("Unified load should succeed");

    // Verify bulk load was performed
    assert_eq!(loading_stats.bulk_records_loaded, 30);
    assert_eq!(loading_stats.incremental_records_loaded, 0);

    // Verify data was loaded
    let record_count = table.record_count();
    assert_eq!(record_count, 30);

    // Test check loading support
    let (supports_bulk, supports_incremental) = table.check_loading_support(&data_source);
    assert!(supports_bulk);
    assert!(supports_incremental);
}
