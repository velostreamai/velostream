//! Join State Store
//!
//! Windowed state store for buffering records on one side of a stream-stream join.
//! Records are stored by join key with time-based expiration.
//!
//! ## Time-Indexed Lookups
//!
//! Records are stored in a two-level structure:
//! - Outer: `HashMap<JoinKey, TimeIndex>` for O(1) key lookup
//! - Inner: `BTreeMap<EventTime, VecDeque<Entries>>` for O(log n) time range queries
//!
//! This enables efficient interval join lookups when per-key cardinality is high.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use crate::velostream::sql::execution::StreamRecord;

/// Entry in the join buffer containing a record and its timing metadata
#[derive(Debug, Clone)]
pub struct JoinBufferEntry {
    /// The buffered record
    pub record: StreamRecord,
    /// Event time of the record (milliseconds since epoch)
    pub event_time: i64,
    /// Expiration time (milliseconds since epoch)
    pub expire_at: i64,
}

impl JoinBufferEntry {
    /// Create a new buffer entry
    pub fn new(record: StreamRecord, event_time: i64, retention_ms: i64) -> Self {
        Self {
            record,
            event_time,
            expire_at: event_time + retention_ms,
        }
    }

    /// Check if this entry has expired based on the given watermark
    pub fn is_expired(&self, watermark: i64) -> bool {
        watermark > self.expire_at
    }
}

/// Statistics for monitoring join state store performance
#[derive(Debug, Default, Clone)]
pub struct JoinStateStats {
    /// Total records stored (lifetime)
    pub records_stored: u64,
    /// Total records expired (lifetime)
    pub records_expired: u64,
    /// Total records evicted due to memory limits (lifetime)
    pub records_evicted: u64,
    /// Records evicted due to memory pressure specifically
    pub records_evicted_memory: u64,
    /// Records evicted due to record count limit
    pub records_evicted_count: u64,
    /// Total lookup operations
    pub lookups: u64,
    /// Total matches found across all lookups
    pub matches_found: u64,
    /// Current number of records in the store
    pub current_size: usize,
    /// Peak number of records observed
    pub peak_size: usize,
    /// Current number of unique keys
    pub current_keys: usize,
    /// Number of times max_records limit was hit
    pub limit_hits: u64,
    /// Number of times max_memory limit was hit
    pub memory_limit_hits: u64,
    /// Current estimated memory usage in bytes
    pub current_memory_bytes: usize,
    /// Peak memory usage observed
    pub peak_memory_bytes: usize,
}

impl JoinStateStats {
    /// Record that a record was stored
    pub fn record_store(&mut self, new_size: usize, memory_bytes: usize) {
        self.records_stored += 1;
        self.current_size = new_size;
        self.current_memory_bytes = memory_bytes;
        if new_size > self.peak_size {
            self.peak_size = new_size;
        }
        if memory_bytes > self.peak_memory_bytes {
            self.peak_memory_bytes = memory_bytes;
        }
    }

    /// Record that records were expired
    pub fn record_expiration(
        &mut self,
        count: usize,
        new_size: usize,
        new_keys: usize,
        memory_bytes: usize,
    ) {
        self.records_expired += count as u64;
        self.current_size = new_size;
        self.current_keys = new_keys;
        self.current_memory_bytes = memory_bytes;
    }

    /// Record a lookup operation
    pub fn record_lookup(&mut self, matches: usize) {
        self.lookups += 1;
        self.matches_found += matches as u64;
    }

    /// Record memory-based eviction
    pub fn record_memory_eviction(&mut self, count: usize) {
        self.records_evicted += count as u64;
        self.records_evicted_memory += count as u64;
        self.memory_limit_hits += 1;
    }

    /// Record count-based eviction
    pub fn record_count_eviction(&mut self, count: usize) {
        self.records_evicted += count as u64;
        self.records_evicted_count += count as u64;
    }
}

/// Eviction policy when memory or record limits are reached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Evict oldest records by event time (default, current behavior)
    #[default]
    Fifo,
    /// Evict least recently accessed records (requires access tracking)
    Lru,
}

/// Configuration for state store memory limits
#[derive(Debug, Clone)]
pub struct JoinStateStoreConfig {
    /// Maximum number of records to store (0 = unlimited)
    pub max_records: usize,
    /// Maximum records per key (0 = unlimited)
    pub max_records_per_key: usize,
    /// Maximum memory usage in bytes (0 = unlimited)
    pub max_memory_bytes: usize,
    /// Warning threshold as percentage of limits (0-100)
    pub warning_threshold_pct: u8,
    /// Eviction policy when limits are reached
    pub eviction_policy: EvictionPolicy,
}

impl Default for JoinStateStoreConfig {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,          // 1M records default limit
            max_records_per_key: 10_000,     // 10K records per key
            max_memory_bytes: 1_073_741_824, // 1GB default memory limit
            warning_threshold_pct: 80,       // Warn at 80% capacity
            eviction_policy: EvictionPolicy::Fifo,
        }
    }
}

impl JoinStateStoreConfig {
    /// Create unlimited config (use with caution!)
    pub fn unlimited() -> Self {
        Self {
            max_records: 0,
            max_records_per_key: 0,
            max_memory_bytes: 0,
            warning_threshold_pct: 0,
            eviction_policy: EvictionPolicy::Fifo,
        }
    }

    /// Create config with specific record limits
    pub fn with_limits(max_records: usize, max_per_key: usize) -> Self {
        Self {
            max_records,
            max_records_per_key: max_per_key,
            max_memory_bytes: 0, // No memory limit
            warning_threshold_pct: 80,
            eviction_policy: EvictionPolicy::Fifo,
        }
    }

    /// Create config with memory limit
    pub fn with_memory_limit(max_memory_bytes: usize) -> Self {
        Self {
            max_records: 0, // No record limit
            max_records_per_key: 0,
            max_memory_bytes,
            warning_threshold_pct: 80,
            eviction_policy: EvictionPolicy::Fifo,
        }
    }

    /// Create config with both record and memory limits
    pub fn with_all_limits(
        max_records: usize,
        max_per_key: usize,
        max_memory_bytes: usize,
    ) -> Self {
        Self {
            max_records,
            max_records_per_key: max_per_key,
            max_memory_bytes,
            warning_threshold_pct: 80,
            eviction_policy: EvictionPolicy::Fifo,
        }
    }

    /// Set eviction policy
    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }
}

/// Time-indexed store for records at a single join key
///
/// Uses BTreeMap for O(log n) time range queries.
/// Each event_time can have multiple records (VecDeque for FIFO order).
type TimeIndex = BTreeMap<i64, VecDeque<JoinBufferEntry>>;

/// Windowed state store for one side of a stream-stream join
///
/// Records are stored indexed by join key. Each key can have multiple records
/// (for cases where the same key appears multiple times in the stream).
/// Records expire based on the retention period and watermark advancement.
///
/// ## Time-Indexed Lookups
///
/// Uses a two-level indexing structure:
/// - `HashMap<JoinKey, BTreeMap<EventTime, VecDeque<Entries>>>`
///
/// This enables O(log n + m) time range queries where n is records per key
/// and m is matching records (vs O(n) linear scan with VecDeque).
///
/// ## Memory Management
///
/// The store enforces configurable memory limits:
/// - `max_records`: Maximum total records across all keys
/// - `max_records_per_key`: Maximum records for any single key
/// - `max_memory_bytes`: Maximum estimated memory usage
///
/// When limits are hit, records are evicted based on the configured eviction policy.
#[derive(Debug)]
pub struct JoinStateStore {
    /// Records indexed by join key, then by event time
    /// Outer: join key (String) -> O(1) lookup
    /// Inner: event_time (i64) -> O(log n) range queries
    records: HashMap<String, TimeIndex>,

    /// Current watermark for this side (milliseconds since epoch)
    watermark: i64,

    /// Retention period in milliseconds
    retention_ms: i64,

    /// Statistics for monitoring
    stats: JoinStateStats,

    /// Running count of total records (maintained incrementally for O(1) lookup)
    record_count: usize,

    /// Estimated memory usage in bytes (tracked incrementally)
    estimated_memory: usize,

    /// Memory limit configuration
    config: JoinStateStoreConfig,

    /// Flag indicating if we've logged a capacity warning
    capacity_warning_logged: bool,

    /// Flag indicating if we've logged a memory warning
    memory_warning_logged: bool,

    /// Global access counter for LRU tracking (incremented on each access)
    access_counter: u64,

    /// Last access time per key (maps key -> access_counter value)
    /// Used for LRU eviction policy
    key_access_times: HashMap<String, u64>,
}

// Constants for memory estimation
const ENTRY_OVERHEAD: usize = 16; // event_time + expire_at
const HASHMAP_ENTRY_OVERHEAD: usize = 72; // HashMap entry overhead per field
const HASHMAP_BASE_SIZE: usize = 48; // Empty HashMap
const BTREEMAP_NODE_OVERHEAD: usize = 48; // BTreeMap node overhead
const VECDEQUE_OVERHEAD: usize = 24; // VecDeque base size
const STRING_OVERHEAD: usize = 24; // String struct overhead

impl JoinStateStore {
    /// Create a new state store with the given retention period
    ///
    /// # Arguments
    /// * `retention` - How long to keep records before they expire
    ///
    /// # Example
    /// ```
    /// use velostream::velostream::sql::execution::join::JoinStateStore;
    /// use std::time::Duration;
    ///
    /// let store = JoinStateStore::new(Duration::from_secs(3600)); // 1 hour retention
    /// ```
    pub fn new(retention: Duration) -> Self {
        Self::with_config(retention, JoinStateStoreConfig::default())
    }

    /// Create a new state store with retention specified in milliseconds
    pub fn with_retention_ms(retention_ms: i64) -> Self {
        Self {
            records: HashMap::new(),
            watermark: 0,
            retention_ms,
            stats: JoinStateStats::default(),
            record_count: 0,
            estimated_memory: 0,
            config: JoinStateStoreConfig::default(),
            capacity_warning_logged: false,
            memory_warning_logged: false,
            access_counter: 0,
            key_access_times: HashMap::new(),
        }
    }

    /// Count records for a specific key (O(n) where n is distinct event times)
    fn count_records_for_key(time_index: &TimeIndex) -> usize {
        time_index.values().map(|entries| entries.len()).sum()
    }

    /// Estimate memory size of a StreamRecord
    ///
    /// This provides a conservative estimate of heap memory usage.
    /// The actual memory may vary based on allocator behavior.
    pub fn estimate_record_size(record: &StreamRecord) -> usize {
        use crate::velostream::sql::execution::types::FieldValue;

        let mut size = ENTRY_OVERHEAD; // JoinBufferEntry overhead
        size += std::mem::size_of::<StreamRecord>(); // Base struct size

        // Fields HashMap
        size += HASHMAP_BASE_SIZE;
        for (key, value) in &record.fields {
            size += HASHMAP_ENTRY_OVERHEAD;
            size += STRING_OVERHEAD + key.len();
            size += Self::estimate_field_value_size(value);
        }

        // Headers HashMap
        size += HASHMAP_BASE_SIZE;
        for (key, value) in &record.headers {
            size += HASHMAP_ENTRY_OVERHEAD;
            size += STRING_OVERHEAD + key.len();
            size += STRING_OVERHEAD + value.len();
        }

        // Optional fields
        if let Some(topic) = &record.topic {
            size += Self::estimate_field_value_size(topic);
        }
        if let Some(key) = &record.key {
            size += Self::estimate_field_value_size(key);
        }

        size
    }

    /// Estimate memory size of a FieldValue
    fn estimate_field_value_size(
        value: &crate::velostream::sql::execution::types::FieldValue,
    ) -> usize {
        use crate::velostream::sql::execution::types::FieldValue;

        match value {
            FieldValue::Null => 1,
            FieldValue::Boolean(_) => 1,
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::String(s) => STRING_OVERHEAD + s.len(),
            FieldValue::Date(_) => 4,
            FieldValue::Timestamp(_) => 12,
            FieldValue::Decimal(_) => 16,
            FieldValue::ScaledInteger(_, _) => 9,
            FieldValue::Array(arr) => {
                24 + arr
                    .iter()
                    .map(Self::estimate_field_value_size)
                    .sum::<usize>()
            }
            FieldValue::Map(map) | FieldValue::Struct(map) => {
                HASHMAP_BASE_SIZE
                    + map
                        .iter()
                        .map(|(k, v)| {
                            HASHMAP_ENTRY_OVERHEAD
                                + STRING_OVERHEAD
                                + k.len()
                                + Self::estimate_field_value_size(v)
                        })
                        .sum::<usize>()
            }
            FieldValue::Interval { .. } => 16,
        }
    }

    /// Create a new state store with custom configuration
    ///
    /// # Panics
    /// Panics if the retention duration exceeds `i64::MAX` milliseconds (~292 million years).
    pub fn with_config(retention: Duration, config: JoinStateStoreConfig) -> Self {
        let retention_ms = i64::try_from(retention.as_millis()).unwrap_or_else(|_| {
            panic!(
                "Retention duration {} ms exceeds i64::MAX",
                retention.as_millis()
            )
        });
        Self {
            records: HashMap::new(),
            watermark: 0,
            retention_ms,
            stats: JoinStateStats::default(),
            record_count: 0,
            estimated_memory: 0,
            config,
            capacity_warning_logged: false,
            memory_warning_logged: false,
            access_counter: 0,
            key_access_times: HashMap::new(),
        }
    }

    /// Store a record, returning its expiration time
    ///
    /// # Arguments
    /// * `key` - The join key for this record
    /// * `record` - The record to store
    /// * `event_time` - The event time of the record (milliseconds since epoch)
    ///
    /// # Returns
    /// The expiration time of the stored record
    ///
    /// # Memory Management
    /// If limits are configured, oldest records may be evicted to make room.
    /// Both record count and memory limits are enforced.
    pub fn store(&mut self, key: &str, record: StreamRecord, event_time: i64) -> i64 {
        // Estimate memory for this record
        let record_memory = Self::estimate_record_size(&record)
            + STRING_OVERHEAD
            + key.len() // Key string
            + BTREEMAP_NODE_OVERHEAD // Amortized BTreeMap overhead
            + VECDEQUE_OVERHEAD; // Amortized VecDeque overhead

        // Check memory limit and evict if needed
        if self.config.max_memory_bytes > 0 {
            while self.estimated_memory + record_memory > self.config.max_memory_bytes
                && self.record_count > 0
            {
                self.evict_oldest_records(1);
                self.stats.record_memory_eviction(1);
            }

            // Check memory warning threshold
            if !self.memory_warning_logged {
                let threshold = (self.config.max_memory_bytes
                    * self.config.warning_threshold_pct as usize)
                    / 100;
                if self.estimated_memory >= threshold {
                    log::warn!(
                        "JoinStateStore: Approaching memory limit ({}/{} bytes, {}%)",
                        self.estimated_memory,
                        self.config.max_memory_bytes,
                        (self.estimated_memory * 100) / self.config.max_memory_bytes
                    );
                    self.memory_warning_logged = true;
                }
            }
        }

        // Check global record count limit and evict if needed
        if self.config.max_records > 0 && self.record_count >= self.config.max_records {
            self.evict_oldest_records(1);
            self.stats.limit_hits += 1;
            self.stats.record_count_eviction(1);
        }

        // Check capacity warning threshold
        if self.config.max_records > 0 && !self.capacity_warning_logged {
            let threshold =
                (self.config.max_records * self.config.warning_threshold_pct as usize) / 100;
            if self.record_count >= threshold {
                log::warn!(
                    "JoinStateStore: Approaching capacity limit ({}/{} records, {}%)",
                    self.record_count,
                    self.config.max_records,
                    (self.record_count * 100) / self.config.max_records
                );
                self.capacity_warning_logged = true;
            }
        }

        let entry = JoinBufferEntry::new(record, event_time, self.retention_ms);
        let expire_at = entry.expire_at;

        // Get or create the time index for this key
        let time_index = self.records.entry(key.to_string()).or_default();

        // Check per-key limit and evict oldest from this key if needed
        if self.config.max_records_per_key > 0 {
            let key_record_count = Self::count_records_for_key(time_index);
            if key_record_count >= self.config.max_records_per_key {
                // Remove oldest entry for this key (smallest event_time)
                if let Some((&oldest_time, _)) = time_index.first_key_value() {
                    if let Some(entries) = time_index.get_mut(&oldest_time) {
                        if let Some(evicted) = entries.pop_front() {
                            let evicted_size = Self::estimate_record_size(&evicted.record);
                            self.estimated_memory =
                                self.estimated_memory.saturating_sub(evicted_size);
                        }
                        self.record_count = self.record_count.saturating_sub(1);
                        self.stats.records_evicted += 1;
                        // Remove empty time slot
                        if entries.is_empty() {
                            time_index.remove(&oldest_time);
                        }
                    }
                }
            }
        }

        // Insert into the time index
        let entries = time_index.entry(event_time).or_default();
        entries.push_back(entry);

        // Update running counts (O(1) instead of O(n))
        self.record_count += 1;
        self.estimated_memory += record_memory;
        self.stats
            .record_store(self.record_count, self.estimated_memory);
        self.stats.current_keys = self.records.len();

        // Track access for LRU
        self.touch_key(key);

        expire_at
    }

    /// Update access time for a key (for LRU tracking)
    fn touch_key(&mut self, key: &str) {
        self.access_counter += 1;
        self.key_access_times
            .insert(key.to_string(), self.access_counter);
    }

    /// Find the key with the most records (for FIFO eviction targeting)
    ///
    /// Returns None if the store is empty.
    fn find_largest_key(&self) -> Option<&str> {
        self.records
            .iter()
            .max_by_key(|(_, time_index)| Self::count_records_for_key(time_index))
            .map(|(key, _)| key.as_str())
    }

    /// Find the least recently used key (for LRU eviction)
    ///
    /// Returns None if the store is empty.
    fn find_lru_key(&self) -> Option<&str> {
        // Find the key with the lowest access time
        // Keys not in access_times are treated as oldest (access_time = 0)
        self.records
            .keys()
            .min_by_key(|key| self.key_access_times.get(*key).unwrap_or(&0))
            .map(|s| s.as_str())
    }

    /// Find the key to evict based on the configured policy
    fn find_eviction_target(&self) -> Option<&str> {
        match self.config.eviction_policy {
            EvictionPolicy::Fifo => self.find_largest_key(),
            EvictionPolicy::Lru => self.find_lru_key(),
        }
    }

    /// Evict records to free space
    ///
    /// Strategy depends on eviction policy:
    /// - FIFO: evict from keys with most records first (balances load)
    /// - LRU: evict from least recently accessed keys first
    fn evict_oldest_records(&mut self, count: usize) {
        let mut evicted = 0;

        while evicted < count {
            // Find the key to evict from based on policy
            let target_key = match self.find_eviction_target() {
                Some(key) => key.to_string(), // Clone only the target key
                None => break,                // Store is empty
            };

            // Now we can mutate
            if let Some(time_index) = self.records.get_mut(&target_key) {
                // Get the oldest time slot using BTreeMap's first entry
                if let Some((&oldest_time, _)) = time_index.first_key_value() {
                    if let Some(entries) = time_index.get_mut(&oldest_time) {
                        if let Some(evicted_entry) = entries.pop_front() {
                            // Track memory freed
                            let evicted_size = Self::estimate_record_size(&evicted_entry.record);
                            self.estimated_memory =
                                self.estimated_memory.saturating_sub(evicted_size);
                            evicted += 1;
                            self.record_count = self.record_count.saturating_sub(1);
                            self.stats.records_evicted += 1;
                        }
                        // Remove empty time slot
                        if entries.is_empty() {
                            time_index.remove(&oldest_time);
                        }
                    }
                }
                // Remove empty keys and their access tracking
                if time_index.is_empty() {
                    self.records.remove(&target_key);
                    self.key_access_times.remove(&target_key);
                }
            }
        }
    }

    /// Lookup all records for a key that match the time constraint
    ///
    /// Uses BTreeMap range queries for O(log n + m) complexity where n is
    /// the number of distinct event times and m is matching records.
    ///
    /// # Arguments
    /// * `key` - The join key to lookup
    /// * `time_lower` - Lower bound of the time window (inclusive)
    /// * `time_upper` - Upper bound of the time window (inclusive)
    ///
    /// # Returns
    /// Vector of references to matching records
    pub fn lookup(&mut self, key: &str, time_lower: i64, time_upper: i64) -> Vec<&StreamRecord> {
        // Track access for LRU first (before borrowing records)
        if self.records.contains_key(key) {
            self.access_counter += 1;
            self.key_access_times
                .insert(key.to_string(), self.access_counter);
        }

        let watermark = self.watermark;
        let matches: Vec<&StreamRecord> = self
            .records
            .get(key)
            .map(|time_index| {
                // Use BTreeMap range for O(log n) lookup to the start point
                time_index
                    .range(time_lower..=time_upper)
                    .flat_map(|(_, entries)| entries.iter())
                    .filter(|e| !e.is_expired(watermark))
                    .map(|e| &e.record)
                    .collect()
            })
            .unwrap_or_default();

        self.stats.record_lookup(matches.len());
        matches
    }

    /// Lookup all records for a key (no time constraint)
    ///
    /// Returns all non-expired records for the given key.
    pub fn lookup_all(&mut self, key: &str) -> Vec<&StreamRecord> {
        // Track access for LRU first (before borrowing records)
        if self.records.contains_key(key) {
            self.access_counter += 1;
            self.key_access_times
                .insert(key.to_string(), self.access_counter);
        }

        let watermark = self.watermark;
        let matches: Vec<&StreamRecord> = self
            .records
            .get(key)
            .map(|time_index| {
                time_index
                    .values()
                    .flat_map(|entries| entries.iter())
                    .filter(|e| !e.is_expired(watermark))
                    .map(|e| &e.record)
                    .collect()
            })
            .unwrap_or_default();

        self.stats.record_lookup(matches.len());
        matches
    }

    /// Advance watermark and expire old records
    ///
    /// Uses BTreeMap's ordered structure to efficiently find and remove
    /// expired records without scanning all entries.
    ///
    /// # Arguments
    /// * `new_watermark` - The new watermark value (milliseconds since epoch)
    ///
    /// # Returns
    /// Number of records expired
    pub fn advance_watermark(&mut self, new_watermark: i64) -> usize {
        if new_watermark <= self.watermark {
            return 0;
        }

        self.watermark = new_watermark;
        let mut expired_count = 0;
        let mut memory_freed = 0usize;
        let mut removed_keys = Vec::new();

        // Remove expired entries from each key's time index
        self.records.retain(|key, time_index| {
            // For each time slot, remove expired entries
            time_index.retain(|_event_time, entries| {
                let before_len = entries.len();
                // Track memory of expired entries before removing
                for entry in entries.iter() {
                    if entry.is_expired(new_watermark) {
                        memory_freed += Self::estimate_record_size(&entry.record);
                    }
                }
                entries.retain(|e| !e.is_expired(new_watermark));
                expired_count += before_len - entries.len();
                !entries.is_empty()
            });
            let keep = !time_index.is_empty();
            if !keep {
                removed_keys.push(key.clone());
            }
            keep
        });

        // Clean up access tracking for removed keys
        for key in removed_keys {
            self.key_access_times.remove(&key);
        }

        // Update running counts
        self.record_count = self.record_count.saturating_sub(expired_count);
        self.estimated_memory = self.estimated_memory.saturating_sub(memory_freed);
        self.stats.record_expiration(
            expired_count,
            self.record_count,
            self.records.len(),
            self.estimated_memory,
        );

        // Reset warning flags if we're below threshold again
        if self.config.max_memory_bytes > 0 {
            let threshold =
                (self.config.max_memory_bytes * self.config.warning_threshold_pct as usize) / 100;
            if self.estimated_memory < threshold {
                self.memory_warning_logged = false;
            }
        }
        if self.config.max_records > 0 {
            let threshold =
                (self.config.max_records * self.config.warning_threshold_pct as usize) / 100;
            if self.record_count < threshold {
                self.capacity_warning_logged = false;
            }
        }

        expired_count
    }

    /// Get current watermark
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Get retention period in milliseconds
    pub fn retention_ms(&self) -> i64 {
        self.retention_ms
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinStateStats {
        &self.stats
    }

    /// Get the number of unique keys currently stored
    pub fn key_count(&self) -> usize {
        self.records.len()
    }

    /// Get the total number of records currently stored (O(1))
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Clear all records from the store
    pub fn clear(&mut self) {
        let expired = self.record_count();
        self.records.clear();
        self.record_count = 0;
        self.estimated_memory = 0;
        self.stats.record_expiration(expired, 0, 0, 0);
        self.capacity_warning_logged = false;
        self.memory_warning_logged = false;
        // Reset LRU tracking
        self.access_counter = 0;
        self.key_access_times.clear();
    }

    /// Get all keys currently in the store
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.records.keys()
    }

    /// Get the current configuration
    pub fn config(&self) -> &JoinStateStoreConfig {
        &self.config
    }

    /// Check if the store is at or near capacity
    #[must_use]
    pub fn is_near_capacity(&self) -> bool {
        if self.config.max_records == 0 {
            return false; // Unlimited
        }
        let threshold =
            (self.config.max_records * self.config.warning_threshold_pct as usize) / 100;
        self.record_count >= threshold
    }

    /// Check if the store is at full capacity
    #[must_use]
    pub fn is_at_capacity(&self) -> bool {
        if self.config.max_records == 0 {
            return false; // Unlimited
        }
        self.record_count >= self.config.max_records
    }

    /// Get capacity usage as a percentage (0.0 - 100.0)
    /// Returns 0.0 if unlimited
    #[must_use]
    pub fn capacity_usage_pct(&self) -> f64 {
        if self.config.max_records == 0 {
            return 0.0;
        }
        (self.record_count as f64 / self.config.max_records as f64) * 100.0
    }

    /// Get remaining capacity (records that can be added before limit)
    /// Returns usize::MAX if unlimited
    #[must_use]
    pub fn remaining_capacity(&self) -> usize {
        if self.config.max_records == 0 {
            return usize::MAX;
        }
        self.config.max_records.saturating_sub(self.record_count)
    }

    /// Get current estimated memory usage in bytes
    #[must_use]
    pub fn estimated_memory(&self) -> usize {
        self.estimated_memory
    }

    /// Get memory usage as a percentage (0.0 - 100.0)
    /// Returns 0.0 if unlimited
    #[must_use]
    pub fn memory_usage_pct(&self) -> f64 {
        if self.config.max_memory_bytes == 0 {
            return 0.0;
        }
        (self.estimated_memory as f64 / self.config.max_memory_bytes as f64) * 100.0
    }

    /// Get remaining memory capacity in bytes
    /// Returns usize::MAX if unlimited
    #[must_use]
    pub fn remaining_memory(&self) -> usize {
        if self.config.max_memory_bytes == 0 {
            return usize::MAX;
        }
        self.config
            .max_memory_bytes
            .saturating_sub(self.estimated_memory)
    }

    /// Check if the store is at or near memory limit
    #[must_use]
    pub fn is_near_memory_limit(&self) -> bool {
        if self.config.max_memory_bytes == 0 {
            return false; // Unlimited
        }
        let threshold =
            (self.config.max_memory_bytes * self.config.warning_threshold_pct as usize) / 100;
        self.estimated_memory >= threshold
    }

    /// Check if the store is at memory limit
    #[must_use]
    pub fn is_at_memory_limit(&self) -> bool {
        if self.config.max_memory_bytes == 0 {
            return false; // Unlimited
        }
        self.estimated_memory >= self.config.max_memory_bytes
    }

    /// Get the average memory per record (useful for capacity planning)
    #[must_use]
    pub fn avg_record_memory(&self) -> usize {
        if self.record_count == 0 {
            return 0;
        }
        self.estimated_memory / self.record_count
    }
}
