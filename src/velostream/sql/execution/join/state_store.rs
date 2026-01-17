//! Join State Store
//!
//! Windowed state store for buffering records on one side of a stream-stream join.
//! Records are stored by join key with time-based expiration.

use std::collections::HashMap;
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

/// Windowed state store for one side of a stream-stream join
///
/// Records are stored indexed by join key. Each key can have multiple records
/// (for cases where the same key appears multiple times in the stream).
/// Records expire based on the retention period and watermark advancement.
#[derive(Debug)]
pub struct JoinStateStore {
    /// Records indexed by join key
    /// Key: join key (String)
    /// Value: Vec of buffered entries for that key
    records: HashMap<String, Vec<JoinBufferEntry>>,

    /// Current watermark for this side (milliseconds since epoch)
    watermark: i64,

    /// Retention period in milliseconds
    retention_ms: i64,

    /// Statistics for monitoring
    stats: JoinStateStats,
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
        Self {
            records: HashMap::new(),
            watermark: 0,
            retention_ms: retention.as_millis() as i64,
            stats: JoinStateStats::default(),
        }
    }

    /// Create a new state store with retention specified in milliseconds
    pub fn with_retention_ms(retention_ms: i64) -> Self {
        Self {
            records: HashMap::new(),
            watermark: 0,
            retention_ms,
            stats: JoinStateStats::default(),
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
    pub fn store(&mut self, key: &str, record: StreamRecord, event_time: i64) -> i64 {
        let entry = JoinBufferEntry::new(record, event_time, self.retention_ms);
        let expire_at = entry.expire_at;

        self.records
            .entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(entry);

        // Update stats
        let total_size: usize = self.records.values().map(|v| v.len()).sum();
        self.stats.record_store(total_size);
        self.stats.current_keys = self.records.len();

        expire_at
    }

    /// Lookup all records for a key that match the time constraint
    ///
    /// # Arguments
    /// * `key` - The join key to lookup
    /// * `time_lower` - Lower bound of the time window (inclusive)
    /// * `time_upper` - Upper bound of the time window (inclusive)
    ///
    /// # Returns
    /// Vector of references to matching records
    pub fn lookup(&mut self, key: &str, time_lower: i64, time_upper: i64) -> Vec<&StreamRecord> {
        let matches: Vec<&StreamRecord> = self
            .records
            .get(key)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.event_time >= time_lower && e.event_time <= time_upper)
                    .filter(|e| !e.is_expired(self.watermark))
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
        let matches: Vec<&StreamRecord> = self
            .records
            .get(key)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| !e.is_expired(self.watermark))
                    .map(|e| &e.record)
                    .collect()
            })
            .unwrap_or_default();

        self.stats.record_lookup(matches.len());
        matches
    }

    /// Advance watermark and expire old records
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

        // Remove expired entries from each key
        self.records.retain(|_key, entries| {
            let before_len = entries.len();
            entries.retain(|e| !e.is_expired(new_watermark));
            expired_count += before_len - entries.len();
            !entries.is_empty()
        });

        // Update stats
        let total_size: usize = self.records.values().map(|v| v.len()).sum();
        self.stats
            .record_expiration(expired_count, total_size, self.records.len());

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

    /// Get the total number of records currently stored
    pub fn record_count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Clear all records from the store
    pub fn clear(&mut self) {
        let expired = self.record_count();
        self.records.clear();
        self.stats.record_expiration(expired, 0, 0);
    }

    /// Get all keys currently in the store
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.records.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn make_test_record(id: i64) -> StreamRecord {
        let mut fields = StdHashMap::new();
        fields.insert(
            "id".to_string(),
            crate::velostream::sql::execution::FieldValue::Integer(id),
        );
        StreamRecord::new(fields)
    }

    #[test]
    fn test_store_and_lookup() {
        let mut store = JoinStateStore::new(Duration::from_secs(3600));

        store.store("key1", make_test_record(1), 1000);
        store.store("key1", make_test_record(2), 2000);
        store.store("key2", make_test_record(3), 1500);

        // Lookup all for key1
        let matches = store.lookup_all("key1");
        assert_eq!(matches.len(), 2);

        // Lookup with time window
        let matches = store.lookup("key1", 1500, 2500);
        assert_eq!(matches.len(), 1);

        // Lookup non-existent key
        let matches = store.lookup_all("key3");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_watermark_expiration() {
        let mut store = JoinStateStore::with_retention_ms(1000); // 1 second retention

        store.store("key", make_test_record(1), 1000);
        store.store("key", make_test_record(2), 2000);
        store.store("key", make_test_record(3), 3000);

        assert_eq!(store.record_count(), 3);

        // Advance watermark - should expire record 1 (expire_at = 2000)
        let expired = store.advance_watermark(2500);
        assert_eq!(expired, 1);
        assert_eq!(store.record_count(), 2);

        // Advance further - should expire record 2 (expire_at = 3000)
        let expired = store.advance_watermark(3500);
        assert_eq!(expired, 1);
        assert_eq!(store.record_count(), 1);
    }

    #[test]
    fn test_time_constrained_lookup() {
        let mut store = JoinStateStore::new(Duration::from_secs(3600));

        store.store("key", make_test_record(1), 1000);
        store.store("key", make_test_record(2), 2000);
        store.store("key", make_test_record(3), 3000);
        store.store("key", make_test_record(4), 4000);

        // Lookup window [1500, 3500]
        let matches = store.lookup("key", 1500, 3500);
        assert_eq!(matches.len(), 2); // records 2 and 3

        // Lookup window [0, 1500]
        let matches = store.lookup("key", 0, 1500);
        assert_eq!(matches.len(), 1); // record 1

        // Lookup window [5000, 6000]
        let matches = store.lookup("key", 5000, 6000);
        assert_eq!(matches.len(), 0); // none
    }

    #[test]
    fn test_stats_tracking() {
        let mut store = JoinStateStore::with_retention_ms(1000);

        store.store("key1", make_test_record(1), 1000);
        store.store("key2", make_test_record(2), 2000);
        store.lookup_all("key1");
        store.lookup_all("key3"); // non-existent

        let stats = store.stats();
        assert_eq!(stats.records_stored, 2);
        assert_eq!(stats.lookups, 2);
        assert_eq!(stats.matches_found, 1);
        assert_eq!(stats.current_size, 2);
        assert_eq!(stats.current_keys, 2);

        store.advance_watermark(2500);
        let stats = store.stats();
        assert_eq!(stats.records_expired, 1);
    }

    #[test]
    fn test_empty_store() {
        let mut store = JoinStateStore::new(Duration::from_secs(3600));

        assert!(store.is_empty());
        assert_eq!(store.key_count(), 0);
        assert_eq!(store.record_count(), 0);

        store.store("key", make_test_record(1), 1000);
        assert!(!store.is_empty());

        store.clear();
        assert!(store.is_empty());
    }
}
