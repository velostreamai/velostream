use crate::ferris::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use crate::ferris::kafka::kafka_error::ConsumerError;
use crate::ferris::kafka::serialization::Serializer;
use crate::ferris::kafka::{KafkaConsumer, Message};
use futures::StreamExt;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

/// A KTable represents a materialized view of a Kafka topic where each record
/// represents the latest state for a given key. KTables are ideal for:
///
/// - User profiles, configuration data, reference data
/// - Event sourcing with state snapshots
/// - Stream-table joins for enrichment
/// - Changelog-based state recovery
///
/// # Examples
///
/// ```rust,no_run
/// use ferrisstreams::ferris::kafka::*;
///
/// // Create a KTable from a compacted topic
/// let user_table = KTable::new(
///     ConsumerConfig::new("localhost:9092", "user-table-group")
///         .auto_offset_reset(OffsetReset::Earliest),
///     "users",
///     JsonSerializer,
///     JsonSerializer,
/// ).await?;
///
/// // Start consuming and building state
/// tokio::spawn(async move {
///     user_table.start().await
/// });
///
/// // Query current state
/// let user = user_table.get(&"user-123".to_string());
///
/// // Get full snapshot
/// let all_users = user_table.snapshot();
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct KTable<K, V, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: Serializer<V> + Send + Sync + 'static,
{
    consumer: Arc<KafkaConsumer<K, V, KS, VS>>,
    state: Arc<RwLock<HashMap<K, V>>>,
    topic: String,
    group_id: String,
    running: Arc<AtomicBool>,
    last_updated: Arc<RwLock<Option<SystemTime>>>,
}

/// Statistics about the KTable state
#[derive(Debug, Clone)]
pub struct KTableStats {
    pub key_count: usize,
    pub last_updated: Option<SystemTime>,
    pub topic: String,
    pub group_id: String,
}

/// Change event representing a state update in the KTable
#[derive(Debug, Clone)]
pub struct ChangeEvent<K, V> {
    pub key: K,
    pub old_value: Option<V>,
    pub new_value: Option<V>,
    pub timestamp: SystemTime,
}

impl<K, V, KS, VS> KTable<K, V, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: Serializer<V> + Send + Sync + 'static,
{
    /// Creates a new KTable from a Kafka topic
    ///
    /// The consumer will be configured to start from the earliest offset
    /// to rebuild the complete state from the topic.
    pub async fn new(
        mut consumer_config: ConsumerConfig,
        topic: String,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Self, ConsumerError> {
        // Ensure we start from earliest to rebuild full state
        consumer_config = consumer_config
            .auto_offset_reset(OffsetReset::Earliest)
            .auto_commit(false, Duration::from_secs(5))
            .isolation_level(IsolationLevel::ReadCommitted);

        let group_id = consumer_config.group_id.clone();

        let consumer =
            KafkaConsumer::with_config(consumer_config, key_serializer, value_serializer)?;

        consumer.subscribe(&[&topic])?;

        Ok(KTable {
            consumer: Arc::new(consumer),
            state: Arc::new(RwLock::new(HashMap::new())),
            topic: topic.clone(),
            group_id,
            running: Arc::new(AtomicBool::new(false)),
            last_updated: Arc::new(RwLock::new(None)),
        })
    }

    /// Creates a KTable with an existing consumer
    pub fn from_consumer(consumer: KafkaConsumer<K, V, KS, VS>, topic: String) -> Self {
        let group_id = consumer.group_id().to_string();

        KTable {
            consumer: Arc::new(consumer),
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
                    println!("KTable error processing message: {:?}", e);
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
    async fn process_message(&self, message: Message<K, V>) {
        if let Some(key) = message.key() {
            let _old_value = {
                let state = self.state.read().unwrap();
                state.get(&key).cloned()
            };

            // Update state
            {
                let mut state = self.state.write().unwrap();
                state.insert(key.clone(), message.value().clone());
            }

            // Update last modified time
            {
                let mut last_updated = self.last_updated.write().unwrap();
                *last_updated = Some(SystemTime::now());
            }
        }
    }

    /// Gets the current value for a key
    pub fn get(&self, key: &K) -> Option<V> {
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
    /// This returns a clone of the entire state, so use with caution
    /// for large tables.
    pub fn snapshot(&self) -> HashMap<K, V> {
        self.state.read().unwrap().clone()
    }

    /// Gets statistics about the table
    pub fn stats(&self) -> KTableStats {
        KTableStats {
            key_count: self.len(),
            last_updated: self.last_updated.read().unwrap().clone(),
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

    /// Creates a derived table by applying a function to each value
    ///
    /// Returns a HashMap snapshot with transformed values.
    /// For real-time transformations, consider using a separate KTable.
    pub fn map_values<V2, F>(&self, mapper: F) -> HashMap<K, V2>
    where
        V2: Clone,
        F: Fn(&V) -> V2,
    {
        self.snapshot()
            .into_iter()
            .map(|(k, v)| (k, mapper(&v)))
            .collect()
    }

    /// Creates a filtered snapshot of the table
    ///
    /// Returns a HashMap with only entries that pass the predicate.
    /// For real-time filtering, consider using a separate KTable.
    pub fn filter<F>(&self, predicate: F) -> HashMap<K, V>
    where
        F: Fn(&K, &V) -> bool,
    {
        self.snapshot()
            .into_iter()
            .filter(|(k, v)| predicate(k, v))
            .collect()
    }
}

// We need to implement Clone for KTable to enable transformations
impl<K, V, KS, VS> Clone for KTable<K, V, KS, VS>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    KS: Serializer<K> + Send + Sync + 'static,
    VS: Serializer<V> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        KTable {
            consumer: self.consumer.clone(),
            state: self.state.clone(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
            running: self.running.clone(),
            last_updated: self.last_updated.clone(),
        }
    }
}
