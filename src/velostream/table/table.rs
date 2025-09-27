use crate::velostream::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use crate::velostream::kafka::kafka_error::ConsumerError;
use crate::velostream::kafka::serialization::Serializer;
use crate::velostream::kafka::{KafkaConsumer, Message};
use crate::velostream::serialization::{FieldValue, SerializationFormat};
use crate::velostream::table::streaming::{
    RecordBatch, RecordStream, SimpleStreamRecord, StreamResult,
};
use crate::velostream::table::unified_table::{TableResult, UnifiedTable};
use futures::StreamExt;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

/// A Table represents a materialized view of a Kafka topic where each record
/// represents the latest state for a given key. Tables are ideal for:
///
/// - User profiles, configuration data, reference data
/// - Event sourcing with state snapshots
/// - Stream-table joins for enrichment
/// - Changelog-based state recovery
///
/// Tables now work with FieldValue records and pluggable serialization formats,
/// supporting JSON, Avro, Protobuf, and the full SQL type system.
///
/// # Examples
///
/// ```rust,no_run
/// use velostream::velostream::table::Table;
/// use velostream::velostream::kafka::consumer_config::{ConsumerConfig, OffsetReset};
/// use velostream::velostream::kafka::serialization::JsonSerializer;
/// use velostream::velostream::serialization::JsonFormat;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create a Table with FieldValue support
///     let user_table = Table::new(
///         ConsumerConfig::new("localhost:9092", "user-table-group")
///             .auto_offset_reset(OffsetReset::Earliest),
///         "users".to_string(),
///         JsonSerializer,
///         JsonFormat,
///     ).await?;
///
///     // Start consuming and building state
///     let user_table_clone = user_table.clone();
///     tokio::spawn(async move {
///         let _ = user_table_clone.start().await;
///     });
///
///     // Query current state (returns FieldValue records)
///     let user_record = user_table.get(&"user-123".to_string());
///
///     // Get full snapshot with rich type system
///     let all_users = user_table.snapshot();
///
///     Ok(())
/// }
/// ```
pub struct Table<K, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + std::fmt::Debug + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    consumer: Arc<
        KafkaConsumer<K, Vec<u8>, KS, crate::velostream::kafka::serialization::BytesSerializer>,
    >,
    value_format: Arc<VS>,
    state: Arc<RwLock<HashMap<K, HashMap<String, FieldValue>>>>,
    topic: String,
    group_id: String,
    running: Arc<AtomicBool>,
    last_updated: Arc<RwLock<Option<SystemTime>>>,
}

/// Statistics about the Table state
#[derive(Debug, Clone)]
pub struct TableStats {
    pub key_count: usize,
    pub last_updated: Option<SystemTime>,
    pub topic: String,
    pub group_id: String,
}

/// Change event representing a state update in the Table
#[derive(Debug, Clone)]
pub struct ChangeEvent<K> {
    pub key: K,
    pub old_value: Option<HashMap<String, FieldValue>>,
    pub new_value: Option<HashMap<String, FieldValue>>,
    pub timestamp: SystemTime,
}

impl<K, KS, VS> Table<K, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + std::fmt::Debug + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    /// Creates a new Table from a Kafka topic
    ///
    /// The consumer will be configured to start from the earliest offset
    /// to rebuild the complete state from the topic. Values are deserialized
    /// using the provided SerializationFormat into FieldValue records.
    pub async fn new(
        mut consumer_config: ConsumerConfig,
        topic: String,
        key_serializer: KS,
        value_format: VS,
    ) -> Result<Self, ConsumerError> {
        // Ensure we start from earliest to rebuild full state
        consumer_config = consumer_config
            .auto_offset_reset(OffsetReset::Earliest)
            .auto_commit(false, Duration::from_secs(5))
            .isolation_level(IsolationLevel::ReadCommitted);

        let group_id = consumer_config.group_id.clone();

        // Use BytesSerializer for values since we'll handle deserialization ourselves
        let consumer = KafkaConsumer::with_config(
            consumer_config,
            key_serializer,
            crate::velostream::kafka::serialization::BytesSerializer,
        )?;

        consumer.subscribe(&[&topic])?;

        Ok(Table {
            consumer: Arc::new(consumer),
            value_format: Arc::new(value_format),
            state: Arc::new(RwLock::new(HashMap::new())),
            topic: topic.clone(),
            group_id,
            running: Arc::new(AtomicBool::new(false)),
            last_updated: Arc::new(RwLock::new(None)),
        })
    }

    /// Creates a Table with an existing consumer and SerializationFormat
    pub fn from_consumer(
        consumer: KafkaConsumer<
            K,
            Vec<u8>,
            KS,
            crate::velostream::kafka::serialization::BytesSerializer,
        >,
        topic: String,
        value_format: VS,
    ) -> Self {
        let group_id = consumer.group_id().to_string();

        Table {
            consumer: Arc::new(consumer),
            value_format: Arc::new(value_format),
            state: Arc::new(RwLock::new(HashMap::new())),
            topic,
            group_id,
            running: Arc::new(AtomicBool::new(false)),
            last_updated: Arc::new(RwLock::new(None)),
        }
    }

    /// Starts consuming from the topic and building/maintaining the table state
    ///
    /// This method should be called in a background task as it will run continuously
    /// until `stop()` is called.
    pub async fn start(&self) -> Result<(), ConsumerError> {
        self.running.store(true, Ordering::Relaxed);

        let mut stream = self.consumer.stream();

        while self.running.load(Ordering::Relaxed) {
            match stream.next().await {
                Some(Ok(message)) => {
                    self.process_message(message).await;
                }
                Some(Err(e)) => {
                    println!("Table error processing message: {:?}", e);
                    // Continue processing despite errors
                }
                None => {
                    // Stream ended, wait briefly and continue
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Ok(())
    }

    /// Stops the table updates
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Checks if the table is currently running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Processes a single message and updates the table state
    async fn process_message(&self, message: Message<K, Vec<u8>>) {
        if let Some(key) = message.key() {
            let _old_value = {
                let state = self.state.read().unwrap();
                state.get(key).cloned()
            };

            // Deserialize the value bytes using the SerializationFormat
            if let Ok(field_value_record) = self.value_format.deserialize_record(message.value()) {
                // Update state
                {
                    let mut state = self.state.write().unwrap();
                    state.insert(key.clone(), field_value_record);
                }

                // Update last modified time
                {
                    let mut last_updated = self.last_updated.write().unwrap();
                    *last_updated = Some(SystemTime::now());
                }
            } else {
                eprintln!("Failed to deserialize message for key: {:?}", key);
            }
        }
    }

    /// Gets the current value for a key as a FieldValue record
    pub fn get(&self, key: &K) -> Option<HashMap<String, FieldValue>> {
        self.state.read().unwrap().get(key).cloned()
    }

    /// Checks if a key exists in the table
    pub fn contains_key(&self, key: &K) -> bool {
        self.state.read().unwrap().contains_key(key)
    }

    /// Gets all keys currently in the table
    pub fn keys(&self) -> Vec<K> {
        self.state.read().unwrap().keys().cloned().collect()
    }

    /// Gets the number of keys in the table
    pub fn len(&self) -> usize {
        self.state.read().unwrap().len()
    }

    /// Checks if the table is empty
    pub fn is_empty(&self) -> bool {
        self.state.read().unwrap().is_empty()
    }

    /// Creates a snapshot of the current state
    ///
    /// This returns a clone of the entire state as FieldValue records.
    /// Use with caution for large tables.
    pub fn snapshot(&self) -> HashMap<K, HashMap<String, FieldValue>> {
        self.state.read().unwrap().clone()
    }

    /// Gets statistics about the table
    pub fn stats(&self) -> TableStats {
        TableStats {
            key_count: self.len(),
            last_updated: *self.last_updated.read().unwrap(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
        }
    }

    /// Waits for the table to have at least the specified number of keys
    ///
    /// Useful for testing and ensuring the table has been populated
    pub async fn wait_for_keys(&self, min_keys: usize, timeout: Duration) -> bool {
        let start = SystemTime::now();

        while start.elapsed().unwrap_or(timeout) < timeout {
            if self.len() >= min_keys {
                return true;
            }
            sleep(Duration::from_millis(50)).await;
        }

        false
    }

    /// Gets the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Gets the consumer group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Creates a derived table by applying a function to each FieldValue record
    ///
    /// Returns a HashMap snapshot with transformed values.
    /// For real-time transformations, consider using a separate Table.
    pub fn map_values<V2, F>(&self, mapper: F) -> HashMap<K, V2>
    where
        V2: Clone,
        F: Fn(&HashMap<String, FieldValue>) -> V2,
    {
        self.snapshot()
            .into_iter()
            .map(|(k, v)| (k, mapper(&v)))
            .collect()
    }

    /// Creates a filtered snapshot of the table
    ///
    /// Returns a HashMap with only entries that pass the predicate.
    /// For real-time filtering, consider using a separate Table.
    pub fn filter<F>(&self, predicate: F) -> HashMap<K, HashMap<String, FieldValue>>
    where
        F: Fn(&K, &HashMap<String, FieldValue>) -> bool,
    {
        self.snapshot()
            .into_iter()
            .filter(|(k, v)| predicate(k, v))
            .collect()
    }
}

/// UnifiedTable implementation for Table
/// Provides fast HashMap-based table operations
#[async_trait::async_trait]
impl<K, KS, VS> UnifiedTable for Table<K, KS, VS>
where
    K: Clone + std::hash::Hash + Eq + ToString + Send + Sync + 'static + std::fmt::Debug,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    /// Enable downcasting (returns self)
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        // For simplicity, assume K is String (most common case)
        // A real implementation would need proper key type conversion
        if std::any::type_name::<K>() == std::any::type_name::<String>() {
            let key_string = key.to_string();
            // Safe transmute since we verified the type
            let key_k = unsafe { std::ptr::read(&key_string as *const _ as *const K) };
            std::mem::forget(key_string);
            Ok(self.get(&key_k))
        } else {
            Ok(None)
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        if std::any::type_name::<K>() == std::any::type_name::<String>() {
            let key_string = key.to_string();
            let key_k = unsafe { std::ptr::read(&key_string as *const _ as *const K) };
            let result = self.contains_key(&key_k);
            std::mem::forget(key_string);
            std::mem::forget(key_k);
            result
        } else {
            false
        }
    }

    fn record_count(&self) -> usize {
        self.len()
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        let state = self.state.read().unwrap();
        let records: Vec<_> = state
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        Box::new(records.into_iter())
    }

    // Note: iter_filtered removed from trait for dyn compatibility

    fn sql_column_values(&self, column: &str, where_clause: &str) -> TableResult<Vec<FieldValue>> {
        let filtered = self.sql_filter(where_clause)?;
        let mut values = Vec::new();

        for (_key, record) in filtered {
            if let FieldValue::Struct(map) = record {
                if let Some(value) = map.get(column) {
                    values.push(value.clone());
                }
            }
        }

        Ok(values)
    }

    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> TableResult<FieldValue> {
        let filtered = self.sql_filter(where_clause)?;

        if filtered.is_empty() {
            return Ok(FieldValue::Null);
        }

        // For simple field selection, return the field from the first record
        if !select_expr.contains('(') {
            if let Some((_key, FieldValue::Struct(record))) = filtered.into_iter().next() {
                return Ok(record.get(select_expr).cloned().unwrap_or(FieldValue::Null));
            }
        }

        // Handle aggregates - simplified for now
        Ok(FieldValue::Null)
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        // Create a simple stream from the current state
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let state = self.state.clone();

        tokio::spawn(async move {
            let state_guard = state.read().unwrap();
            for (key, record) in state_guard.iter() {
                let stream_record = SimpleStreamRecord {
                    key: key.to_string(),
                    fields: record.clone(),
                };
                if tx.send(Ok(stream_record)).is_err() {
                    break;
                }
            }
        });

        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        // Simplified implementation - delegates to stream_all
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        let state = self.state.read().unwrap();
        let offset = offset.unwrap_or(0);

        let records: Vec<SimpleStreamRecord> = state
            .iter()
            .skip(offset)
            .take(batch_size)
            .map(|(key, fields)| SimpleStreamRecord {
                key: key.to_string(),
                fields: fields.clone(),
            })
            .collect();

        let has_more = offset + records.len() < state.len();

        Ok(RecordBatch { records, has_more })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(self.record_count())
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        // Simplified implementation
        Ok(FieldValue::Null)
    }
}

// We need to implement Clone for Table to enable transformations
impl<K, KS, VS> Clone for Table<K, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + std::fmt::Debug + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Table {
            consumer: self.consumer.clone(),
            value_format: self.value_format.clone(),
            state: self.state.clone(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
            running: self.running.clone(),
            last_updated: self.last_updated.clone(),
        }
    }
}
