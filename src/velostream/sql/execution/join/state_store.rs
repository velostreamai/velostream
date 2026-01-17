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
}

impl JoinStateStats {
    /// Record that a record was stored
    pub fn record_store(&mut self, new_size: usize) {
        self.records_stored += 1;
        self.current_size = new_size;
        if new_size > self.peak_size {
            self.peak_size = new_size;
        }
    }

    /// Record that records were expired
    pub fn record_expiration(&mut self, count: usize, new_size: usize, new_keys: usize) {
        self.records_expired += count as u64;
        self.current_size = new_size;
        self.current_keys = new_keys;
    }

    /// Record a lookup operation
    pub fn record_lookup(&mut self, matches: usize) {
        self.lookups += 1;
        self.matches_found += matches as u64;
    }
}

/// Configuration for state store memory limits
#[derive(Debug, Clone)]
pub struct JoinStateStoreConfig {
    /// Maximum number of records to store (0 = unlimited)
    pub max_records: usize,
    /// Maximum records per key (0 = unlimited)
    pub max_records_per_key: usize,
    /// Warning threshold as percentage of max_records (0-100)
    pub warning_threshold_pct: u8,
}

impl Default for JoinStateStoreConfig {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,      // 1M records default limit
            max_records_per_key: 10_000, // 10K records per key
            warning_threshold_pct: 80,   // Warn at 80% capacity
        }
    }
}

impl JoinStateStoreConfig {
    /// Create unlimited config (use with caution!)
    pub fn unlimited() -> Self {
        Self {
            max_records: 0,
            max_records_per_key: 0,
            warning_threshold_pct: 0,
        }
    }

    /// Create config with specific limits
    pub fn with_limits(max_records: usize, max_per_key: usize) -> Self {
        Self {
            max_records,
            max_records_per_key: max_per_key,
            warning_threshold_pct: 80,
        }
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
///
/// When limits are hit, oldest records are evicted (FIFO within each key).
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

    /// Memory limit configuration
    config: JoinStateStoreConfig,

    /// Flag indicating if we've logged a capacity warning
    capacity_warning_logged: bool,
}

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
            config: JoinStateStoreConfig::default(),
            capacity_warning_logged: false,
        }
    }

    /// Count records for a specific key (O(n) where n is distinct event times)
    fn count_records_for_key(time_index: &TimeIndex) -> usize {
        time_index.values().map(|entries| entries.len()).sum()
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
            config,
            capacity_warning_logged: false,
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
    pub fn store(&mut self, key: &str, record: StreamRecord, event_time: i64) -> i64 {
        // Check global limit and evict if needed
        if self.config.max_records > 0 && self.record_count >= self.config.max_records {
            self.evict_oldest_records(1);
            self.stats.limit_hits += 1;
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
                        entries.pop_front();
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

        // Update running count (O(1) instead of O(n))
        self.record_count += 1;
        self.stats.record_store(self.record_count);
        self.stats.current_keys = self.records.len();

        expire_at
    }

    /// Find the key with the most records (for eviction targeting)
    ///
    /// Returns None if the store is empty.
    fn find_largest_key(&self) -> Option<&str> {
        self.records
            .iter()
            .max_by_key(|(_, time_index)| Self::count_records_for_key(time_index))
            .map(|(key, _)| key.as_str())
    }

    /// Evict oldest records to free space
    ///
    /// Strategy: evict from keys with most records first to balance load.
    /// Optimized to avoid cloning keys - uses reference-based lookup.
    fn evict_oldest_records(&mut self, count: usize) {
        let mut evicted = 0;

        while evicted < count {
            // Find the key with the most records (without cloning)
            let target_key = match self.find_largest_key() {
                Some(key) => key.to_string(), // Clone only the target key
                None => break,                // Store is empty
            };

            // Now we can mutate
            if let Some(time_index) = self.records.get_mut(&target_key) {
                // Get the oldest time slot using BTreeMap's first entry
                if let Some((&oldest_time, _)) = time_index.first_key_value() {
                    if let Some(entries) = time_index.get_mut(&oldest_time) {
                        if !entries.is_empty() {
                            entries.pop_front();
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
                // Remove empty keys
                if time_index.is_empty() {
                    self.records.remove(&target_key);
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

        // Remove expired entries from each key's time index
        self.records.retain(|_key, time_index| {
            // For each time slot, remove expired entries
            time_index.retain(|_event_time, entries| {
                let before_len = entries.len();
                entries.retain(|e| !e.is_expired(new_watermark));
                expired_count += before_len - entries.len();
                !entries.is_empty()
            });
            !time_index.is_empty()
        });

        // Update running count
        self.record_count = self.record_count.saturating_sub(expired_count);
        self.stats
            .record_expiration(expired_count, self.record_count, self.records.len());

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
        self.stats.record_expiration(expired, 0, 0);
        self.capacity_warning_logged = false;
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
}
