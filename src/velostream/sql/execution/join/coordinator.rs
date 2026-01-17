//! Join Coordinator
//!
//! Coordinates stream-stream join processing by managing dual state stores,
//! routing records to the appropriate side, and emitting joined results.

use std::collections::HashMap;
use std::time::Duration;

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::join::key_extractor::JoinKeyExtractorPair;
use crate::velostream::sql::execution::join::state_store::JoinStateStore;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};

/// Which side of the join a record belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    /// Left side (typically the "driving" table)
    Left,
    /// Right side (typically the "joined" table)
    Right,
}

impl JoinSide {
    /// Get the opposite side
    pub fn opposite(&self) -> Self {
        match self {
            JoinSide::Left => JoinSide::Right,
            JoinSide::Right => JoinSide::Left,
        }
    }
}

/// Type of join to perform
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join - only matching records from both sides
    Inner,
    /// Left outer join - all left records, matching right records (or NULL)
    LeftOuter,
    /// Right outer join - all right records, matching left records (or NULL)
    RightOuter,
    /// Full outer join - all records from both sides
    FullOuter,
}

impl Default for JoinType {
    fn default() -> Self {
        JoinType::Inner
    }
}

/// Configuration for a stream-stream join
#[derive(Debug, Clone)]
pub struct JoinConfig {
    /// Join type (Inner, Left, Right, Full)
    pub join_type: JoinType,

    /// Lower bound for interval join (milliseconds, can be negative)
    /// right.time >= left.time + lower_bound
    pub lower_bound_ms: i64,

    /// Upper bound for interval join (milliseconds)
    /// right.time <= left.time + upper_bound
    pub upper_bound_ms: i64,

    /// Retention period for state (milliseconds)
    /// Records older than this are expired regardless of watermark
    pub retention_ms: i64,

    /// Left source name (for logging/debugging)
    pub left_source: String,

    /// Right source name (for logging/debugging)
    pub right_source: String,

    /// Join key columns: (left_column, right_column) pairs
    pub join_keys: Vec<(String, String)>,

    /// Field name containing event time (common to both sides)
    pub event_time_field: String,
}

impl JoinConfig {
    /// Create a new interval join configuration
    ///
    /// # Arguments
    /// * `left_source` - Name of the left source
    /// * `right_source` - Name of the right source
    /// * `join_keys` - Pairs of (left_column, right_column) for join condition
    /// * `lower_bound` - Lower bound of time interval
    /// * `upper_bound` - Upper bound of time interval
    pub fn interval(
        left_source: &str,
        right_source: &str,
        join_keys: Vec<(String, String)>,
        lower_bound: Duration,
        upper_bound: Duration,
    ) -> Self {
        Self {
            join_type: JoinType::Inner,
            lower_bound_ms: lower_bound.as_millis() as i64,
            upper_bound_ms: upper_bound.as_millis() as i64,
            retention_ms: upper_bound.as_millis() as i64 * 2, // 2x upper bound as default retention
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            join_keys,
            event_time_field: "event_time".to_string(),
        }
    }

    /// Create a simple equi-join configuration (no time bounds)
    pub fn equi_join(
        left_source: &str,
        right_source: &str,
        join_keys: Vec<(String, String)>,
        retention: Duration,
    ) -> Self {
        Self {
            join_type: JoinType::Inner,
            lower_bound_ms: i64::MIN,
            upper_bound_ms: i64::MAX,
            retention_ms: retention.as_millis() as i64,
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            join_keys,
            event_time_field: "event_time".to_string(),
        }
    }

    /// Set the join type
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Set the retention period
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention_ms = retention.as_millis() as i64;
        self
    }

    /// Set the event time field name
    pub fn with_event_time_field(mut self, field: &str) -> Self {
        self.event_time_field = field.to_string();
        self
    }

    /// Check if this is an interval join (has time bounds)
    pub fn is_interval_join(&self) -> bool {
        self.lower_bound_ms != i64::MIN || self.upper_bound_ms != i64::MAX
    }
}

/// Statistics for monitoring join coordinator performance
#[derive(Debug, Default, Clone)]
pub struct JoinCoordinatorStats {
    /// Records processed from left side
    pub left_records_processed: u64,
    /// Records processed from right side
    pub right_records_processed: u64,
    /// Total join matches emitted
    pub matches_emitted: u64,
    /// Records with missing join keys
    pub missing_key_count: u64,
    /// Records with missing event time
    pub missing_time_count: u64,
    /// Current left state store size
    pub left_store_size: usize,
    /// Current right state store size
    pub right_store_size: usize,
}

/// Coordinates stream-stream join processing
///
/// The coordinator manages two windowed state stores (one per side) and
/// processes records by:
/// 1. Extracting the join key
/// 2. Storing the record in the appropriate side's buffer
/// 3. Looking up matches in the opposite side's buffer
/// 4. Emitting joined records for all matches within time constraints
#[derive(Debug)]
pub struct JoinCoordinator {
    /// Join configuration
    config: JoinConfig,

    /// State store for left side records
    left_store: JoinStateStore,

    /// State store for right side records
    right_store: JoinStateStore,

    /// Key extractors for both sides
    key_extractors: JoinKeyExtractorPair,

    /// Statistics
    stats: JoinCoordinatorStats,
}

impl JoinCoordinator {
    /// Create a new join coordinator with the given configuration
    pub fn new(config: JoinConfig) -> Self {
        let retention_ms = config.retention_ms;
        let key_extractors = JoinKeyExtractorPair::from_pairs(config.join_keys.clone());

        Self {
            config,
            left_store: JoinStateStore::with_retention_ms(retention_ms),
            right_store: JoinStateStore::with_retention_ms(retention_ms),
            key_extractors,
            stats: JoinCoordinatorStats::default(),
        }
    }

    /// Process a record from the specified side
    ///
    /// Returns joined records if matches are found.
    pub fn process(
        &mut self,
        side: JoinSide,
        record: StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        match side {
            JoinSide::Left => self.process_left(record),
            JoinSide::Right => self.process_right(record),
        }
    }

    /// Process a record from the left side
    pub fn process_left(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError> {
        self.stats.left_records_processed += 1;

        // Extract join key
        let key = match self.key_extractors.left.extract(&record) {
            Some(k) => k,
            None => {
                self.stats.missing_key_count += 1;
                return Ok(vec![]); // Skip records with missing keys
            }
        };

        // Extract event time
        let event_time = self.extract_event_time(&record)?;

        // Store in left buffer
        self.left_store.store(&key, record.clone(), event_time);

        // Lookup matches in right buffer
        let (time_lower, time_upper) = self.compute_lookup_bounds_for_left(event_time);
        let matches: Vec<StreamRecord> = self
            .right_store
            .lookup(&key, time_lower, time_upper)
            .into_iter()
            .cloned()
            .collect();

        // Emit joined records
        let joined: Vec<StreamRecord> = matches
            .iter()
            .map(|right| self.merge_records(&record, right))
            .collect();

        self.stats.matches_emitted += joined.len() as u64;
        Ok(joined)
    }

    /// Process a record from the right side
    pub fn process_right(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError> {
        self.stats.right_records_processed += 1;

        // Extract join key
        let key = match self.key_extractors.right.extract(&record) {
            Some(k) => k,
            None => {
                self.stats.missing_key_count += 1;
                return Ok(vec![]); // Skip records with missing keys
            }
        };

        // Extract event time
        let event_time = self.extract_event_time(&record)?;

        // Store in right buffer
        self.right_store.store(&key, record.clone(), event_time);

        // Lookup matches in left buffer
        let (time_lower, time_upper) = self.compute_lookup_bounds_for_right(event_time);
        let matches: Vec<StreamRecord> = self
            .left_store
            .lookup(&key, time_lower, time_upper)
            .into_iter()
            .cloned()
            .collect();

        // Emit joined records
        let joined: Vec<StreamRecord> = matches
            .iter()
            .map(|left| self.merge_records(left, &record))
            .collect();

        self.stats.matches_emitted += joined.len() as u64;
        Ok(joined)
    }

    /// Extract event time from a record
    fn extract_event_time(&mut self, record: &StreamRecord) -> Result<i64, SqlError> {
        // First try the configured event time field
        if let Some(value) = record.fields.get(&self.config.event_time_field) {
            if let Some(ts) = self.field_value_to_timestamp(value) {
                return Ok(ts);
            }
        }

        // Fall back to record's event_time metadata if available
        if let Some(event_time) = record.event_time {
            return Ok(event_time.timestamp_millis());
        }

        // Fall back to record timestamp
        if record.timestamp > 0 {
            return Ok(record.timestamp);
        }

        self.stats.missing_time_count += 1;
        // Use current time as last resort
        Ok(chrono::Utc::now().timestamp_millis())
    }

    /// Convert a FieldValue to a timestamp in milliseconds
    fn field_value_to_timestamp(&self, value: &FieldValue) -> Option<i64> {
        match value {
            FieldValue::Integer(i) => Some(*i),
            FieldValue::Timestamp(ts) => Some(ts.and_utc().timestamp_millis()),
            FieldValue::String(s) => {
                // Try to parse as ISO timestamp
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.timestamp_millis())
                    .or_else(|| {
                        // Try parsing as milliseconds
                        s.parse::<i64>().ok()
                    })
            }
            _ => None,
        }
    }

    /// Compute lookup bounds when processing a LEFT record
    ///
    /// For interval join: right.time ∈ [left.time + lower, left.time + upper]
    /// So when looking up right records for a left record at time T:
    /// - right.time >= T + lower_bound
    /// - right.time <= T + upper_bound
    fn compute_lookup_bounds_for_left(&self, left_time: i64) -> (i64, i64) {
        let lower = left_time.saturating_add(self.config.lower_bound_ms);
        let upper = left_time.saturating_add(self.config.upper_bound_ms);
        (lower, upper)
    }

    /// Compute lookup bounds when processing a RIGHT record
    ///
    /// For interval join: right.time ∈ [left.time + lower, left.time + upper]
    /// Rearranging: left.time ∈ [right.time - upper, right.time - lower]
    fn compute_lookup_bounds_for_right(&self, right_time: i64) -> (i64, i64) {
        let lower = right_time.saturating_sub(self.config.upper_bound_ms);
        let upper = right_time.saturating_sub(self.config.lower_bound_ms);
        (lower, upper)
    }

    /// Merge left and right records into a joined record
    ///
    /// Fields from both records are combined with prefixes to avoid collision:
    /// - Left fields: `{left_source}.{field_name}`
    /// - Right fields: `{right_source}.{field_name}`
    fn merge_records(&self, left: &StreamRecord, right: &StreamRecord) -> StreamRecord {
        let mut merged_fields = HashMap::new();

        // Add left fields with prefix
        for (key, value) in &left.fields {
            merged_fields.insert(
                format!("{}.{}", self.config.left_source, key),
                value.clone(),
            );
            // Also add without prefix for convenience (may be overwritten by right)
            merged_fields.insert(key.clone(), value.clone());
        }

        // Add right fields with prefix
        for (key, value) in &right.fields {
            merged_fields.insert(
                format!("{}.{}", self.config.right_source, key),
                value.clone(),
            );
            // Also add without prefix (overwrites left if same name)
            merged_fields.insert(key.clone(), value.clone());
        }

        // Use the later timestamp of the two records
        let merged_timestamp = left.timestamp.max(right.timestamp);
        let merged_event_time = match (left.event_time, right.event_time) {
            (Some(l), Some(r)) => Some(l.max(r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };

        StreamRecord {
            fields: merged_fields,
            timestamp: merged_timestamp,
            offset: left.offset, // Use left offset as reference
            partition: left.partition,
            headers: left.headers.clone(), // Preserve left headers
            event_time: merged_event_time,
            topic: left.topic.clone(), // Preserve left topic
            key: left.key.clone(),     // Preserve left key
        }
    }

    /// Advance watermark for a side and expire old records
    ///
    /// Returns the number of records expired from each store.
    pub fn advance_watermark(&mut self, side: JoinSide, watermark: i64) -> (usize, usize) {
        let (left_expired, right_expired) = match side {
            JoinSide::Left => {
                let left = self.left_store.advance_watermark(watermark);
                // Also advance right store with minimum watermark
                let min_wm = self
                    .left_store
                    .watermark()
                    .min(self.right_store.watermark());
                let right = self.right_store.advance_watermark(min_wm);
                (left, right)
            }
            JoinSide::Right => {
                let right = self.right_store.advance_watermark(watermark);
                // Also advance left store with minimum watermark
                let min_wm = self
                    .left_store
                    .watermark()
                    .min(self.right_store.watermark());
                let left = self.left_store.advance_watermark(min_wm);
                (left, right)
            }
        };

        self.stats.left_store_size = self.left_store.record_count();
        self.stats.right_store_size = self.right_store.record_count();

        (left_expired, right_expired)
    }

    /// Get the configuration
    pub fn config(&self) -> &JoinConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinCoordinatorStats {
        &self.stats
    }

    /// Get left store reference (for testing/monitoring)
    pub fn left_store(&self) -> &JoinStateStore {
        &self.left_store
    }

    /// Get right store reference (for testing/monitoring)
    pub fn right_store(&self) -> &JoinStateStore {
        &self.right_store
    }

    /// Check if both stores are empty
    pub fn is_empty(&self) -> bool {
        self.left_store.is_empty() && self.right_store.is_empty()
    }

    /// Get total record count across both stores
    pub fn total_records(&self) -> usize {
        self.left_store.record_count() + self.right_store.record_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn make_test_record(fields: Vec<(&str, FieldValue)>, timestamp: i64) -> StreamRecord {
        let field_map: StdHashMap<String, FieldValue> = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let mut record = StreamRecord::new(field_map);
        record.timestamp = timestamp;
        record
    }

    #[test]
    fn test_inner_join_matching_records() {
        let config = JoinConfig::interval(
            "orders",
            "shipments",
            vec![("order_id".to_string(), "order_id".to_string())],
            Duration::ZERO,
            Duration::from_secs(3600), // 1 hour
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process an order
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("amount", FieldValue::Float(100.0)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        let results = coordinator.process_left(order).unwrap();
        assert!(results.is_empty()); // No shipments yet

        // Process a matching shipment within time window
        let shipment = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("tracking", FieldValue::String("TRACK001".to_string())),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = coordinator.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        // Verify merged record has fields from both
        let joined = &results[0];
        assert!(joined.fields.contains_key("orders.order_id"));
        assert!(joined.fields.contains_key("shipments.tracking"));
    }

    #[test]
    fn test_no_match_different_keys() {
        let config = JoinConfig::interval(
            "orders",
            "shipments",
            vec![("order_id".to_string(), "order_id".to_string())],
            Duration::ZERO,
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process an order
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        coordinator.process_left(order).unwrap();

        // Process a shipment with different order_id
        let shipment = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(456)), // Different!
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = coordinator.process_right(shipment).unwrap();
        assert!(results.is_empty()); // No match
    }

    #[test]
    fn test_interval_time_bounds() {
        let config = JoinConfig::interval(
            "orders",
            "shipments",
            vec![("order_id".to_string(), "order_id".to_string())],
            Duration::ZERO,
            Duration::from_secs(1), // Only 1 second window
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process an order at time 1000
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        coordinator.process_left(order).unwrap();

        // Shipment at 1500ms (within 1s window) - should match
        let shipment1 = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("event_time", FieldValue::Integer(1500)),
            ],
            1500,
        );
        let results = coordinator.process_right(shipment1).unwrap();
        assert_eq!(results.len(), 1);

        // Shipment at 3000ms (outside 1s window) - should not match
        let shipment2 = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(123)),
                ("event_time", FieldValue::Integer(3000)),
            ],
            3000,
        );
        let results = coordinator.process_right(shipment2).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_bidirectional_matching() {
        let config = JoinConfig::equi_join(
            "stream_a",
            "stream_b",
            vec![("key".to_string(), "key".to_string())],
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process from right side first
        let right_record = make_test_record(
            vec![
                ("key", FieldValue::Integer(1)),
                ("right_value", FieldValue::String("B".to_string())),
            ],
            1000,
        );
        let results = coordinator.process_right(right_record).unwrap();
        assert!(results.is_empty()); // No left records yet

        // Now process from left side - should match the buffered right record
        let left_record = make_test_record(
            vec![
                ("key", FieldValue::Integer(1)),
                ("left_value", FieldValue::String("A".to_string())),
            ],
            2000,
        );
        let results = coordinator.process_left(left_record).unwrap();
        assert_eq!(results.len(), 1);

        // Verify both values present
        let joined = &results[0];
        assert!(joined.fields.contains_key("stream_a.left_value"));
        assert!(joined.fields.contains_key("stream_b.right_value"));
    }

    #[test]
    fn test_multiple_matches() {
        let config = JoinConfig::equi_join(
            "orders",
            "items",
            vec![("order_id".to_string(), "order_id".to_string())],
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process multiple items for the same order
        for i in 0..3 {
            let item = make_test_record(
                vec![
                    ("order_id", FieldValue::Integer(100)),
                    ("item_id", FieldValue::Integer(i)),
                ],
                1000 + i,
            );
            coordinator.process_right(item).unwrap();
        }

        // Process order - should match all 3 items
        let order = make_test_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
            ],
            2000,
        );
        let results = coordinator.process_left(order).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_watermark_expiration() {
        let config = JoinConfig::equi_join(
            "left",
            "right",
            vec![("key".to_string(), "key".to_string())],
            Duration::from_millis(1000), // 1 second retention
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Add a record at time 1000
        let record = make_test_record(
            vec![
                ("key", FieldValue::Integer(1)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        coordinator.process_left(record).unwrap();

        assert_eq!(coordinator.left_store().record_count(), 1);

        // Advance watermark past expiration (1000 + 1000 = 2000)
        coordinator.advance_watermark(JoinSide::Left, 2500);

        // Record should be expired
        assert_eq!(coordinator.left_store().record_count(), 0);
    }

    #[test]
    fn test_missing_key_handling() {
        let config = JoinConfig::equi_join(
            "left",
            "right",
            vec![("missing_col".to_string(), "missing_col".to_string())],
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Record without the join key column
        let record = make_test_record(vec![("other_field", FieldValue::Integer(123))], 1000);

        let results = coordinator.process_left(record).unwrap();
        assert!(results.is_empty());
        assert_eq!(coordinator.stats().missing_key_count, 1);
    }

    #[test]
    fn test_composite_key_join() {
        let config = JoinConfig::equi_join(
            "left",
            "right",
            vec![
                ("region".to_string(), "region".to_string()),
                ("customer_id".to_string(), "customer_id".to_string()),
            ],
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Left record
        let left = make_test_record(
            vec![
                ("region", FieldValue::String("US".to_string())),
                ("customer_id", FieldValue::Integer(42)),
                ("left_data", FieldValue::String("L".to_string())),
            ],
            1000,
        );
        coordinator.process_left(left).unwrap();

        // Right record with same composite key
        let right = make_test_record(
            vec![
                ("region", FieldValue::String("US".to_string())),
                ("customer_id", FieldValue::Integer(42)),
                ("right_data", FieldValue::String("R".to_string())),
            ],
            2000,
        );
        let results = coordinator.process_right(right).unwrap();
        assert_eq!(results.len(), 1);

        // Right record with different composite key
        let right_diff = make_test_record(
            vec![
                ("region", FieldValue::String("EU".to_string())), // Different region
                ("customer_id", FieldValue::Integer(42)),
                ("right_data", FieldValue::String("R2".to_string())),
            ],
            3000,
        );
        let results = coordinator.process_right(right_diff).unwrap();
        assert!(results.is_empty()); // No match
    }

    #[test]
    fn test_stats_tracking() {
        let config = JoinConfig::equi_join(
            "left",
            "right",
            vec![("key".to_string(), "key".to_string())],
            Duration::from_secs(3600),
        );

        let mut coordinator = JoinCoordinator::new(config);

        // Process some records
        for i in 0..5 {
            let record = make_test_record(vec![("key", FieldValue::Integer(i))], 1000 + i);
            coordinator.process_left(record).unwrap();
        }

        for i in 0..3 {
            let record = make_test_record(vec![("key", FieldValue::Integer(i))], 2000 + i);
            coordinator.process_right(record).unwrap();
        }

        let stats = coordinator.stats();
        assert_eq!(stats.left_records_processed, 5);
        assert_eq!(stats.right_records_processed, 3);
        assert_eq!(stats.matches_emitted, 3); // Keys 0, 1, 2 matched
    }
}
