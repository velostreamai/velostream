//! Join Coordinator
//!
//! Coordinates stream-stream join processing by managing dual state stores,
//! routing records to the appropriate side, and emitting joined results.

use std::collections::HashMap;
use std::time::Duration;

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::join::key_extractor::JoinKeyExtractorPair;
use crate::velostream::sql::execution::join::state_store::{JoinStateStore, JoinStateStoreConfig};
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

/// Behavior when event time is missing from a record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingEventTimeBehavior {
    /// Use wall-clock time as fallback (default, may cause incorrect joins)
    UseWallClock,
    /// Skip the record (record won't participate in joins)
    SkipRecord,
    /// Return an error (strict mode)
    Error,
}

impl Default for MissingEventTimeBehavior {
    fn default() -> Self {
        MissingEventTimeBehavior::UseWallClock
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

    /// Behavior when event time is missing from a record
    pub missing_event_time: MissingEventTimeBehavior,
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
    ///
    /// # Panics
    /// Panics if the duration exceeds `i64::MAX` milliseconds (~292 million years).
    pub fn interval(
        left_source: &str,
        right_source: &str,
        join_keys: Vec<(String, String)>,
        lower_bound: Duration,
        upper_bound: Duration,
    ) -> Self {
        let lower_ms = Self::duration_to_i64_ms(lower_bound);
        let upper_ms = Self::duration_to_i64_ms(upper_bound);
        Self {
            join_type: JoinType::Inner,
            lower_bound_ms: lower_ms,
            upper_bound_ms: upper_ms,
            retention_ms: upper_ms.saturating_mul(2), // 2x upper bound as default retention
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            join_keys,
            event_time_field: "event_time".to_string(),
            missing_event_time: MissingEventTimeBehavior::default(),
        }
    }

    /// Safely convert Duration to i64 milliseconds
    fn duration_to_i64_ms(duration: Duration) -> i64 {
        let millis = duration.as_millis();
        i64::try_from(millis).unwrap_or_else(|_| {
            panic!(
                "Duration {} ms exceeds i64::MAX; use a smaller duration",
                millis
            )
        })
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
            retention_ms: Self::duration_to_i64_ms(retention),
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            join_keys,
            event_time_field: "event_time".to_string(),
            missing_event_time: MissingEventTimeBehavior::default(),
        }
    }

    /// Set the join type
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Set the retention period
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention_ms = Self::duration_to_i64_ms(retention);
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

    /// Create an interval join with millisecond bounds (supports negative values)
    ///
    /// Use this when you need negative lower bounds, e.g., "right event can
    /// occur up to 1 hour BEFORE left event":
    ///
    /// ```
    /// use velostream::velostream::sql::execution::join::JoinConfig;
    ///
    /// // Shipment can arrive 1 hour before to 24 hours after order
    /// let config = JoinConfig::interval_ms(
    ///     "orders", "shipments",
    ///     vec![("order_id".to_string(), "order_id".to_string())],
    ///     -3600_000,  // 1 hour before
    ///     86400_000,  // 24 hours after
    /// );
    /// ```
    pub fn interval_ms(
        left_source: &str,
        right_source: &str,
        join_keys: Vec<(String, String)>,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> Self {
        // Calculate retention: max of |lower_bound| and |upper_bound|, times 2
        let max_bound = lower_bound_ms.abs().max(upper_bound_ms.abs());
        let retention = max_bound.saturating_mul(2);

        Self {
            join_type: JoinType::Inner,
            lower_bound_ms,
            upper_bound_ms,
            retention_ms: retention,
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            join_keys,
            event_time_field: "event_time".to_string(),
            missing_event_time: MissingEventTimeBehavior::default(),
        }
    }

    /// Set behavior when event time is missing
    ///
    /// - `UseWallClock`: Use current time (default, may cause incorrect joins)
    /// - `SkipRecord`: Skip records without event time
    /// - `Error`: Return an error for strict event-time processing
    pub fn with_missing_event_time(mut self, behavior: MissingEventTimeBehavior) -> Self {
        self.missing_event_time = behavior;
        self
    }
}

/// Extended configuration including state store memory limits
#[derive(Debug, Clone)]
pub struct JoinCoordinatorConfig {
    /// Base join configuration
    pub join_config: JoinConfig,
    /// State store config for left side (None = default)
    pub left_store_config: Option<JoinStateStoreConfig>,
    /// State store config for right side (None = default)
    pub right_store_config: Option<JoinStateStoreConfig>,
}

impl JoinCoordinatorConfig {
    /// Create from a JoinConfig with default state store configs
    pub fn new(join_config: JoinConfig) -> Self {
        Self {
            join_config,
            left_store_config: None,
            right_store_config: None,
        }
    }

    /// Set state store config for both sides
    pub fn with_store_config(mut self, config: JoinStateStoreConfig) -> Self {
        self.left_store_config = Some(config.clone());
        self.right_store_config = Some(config);
        self
    }

    /// Set state store configs separately for left and right
    pub fn with_store_configs(
        mut self,
        left_config: JoinStateStoreConfig,
        right_config: JoinStateStoreConfig,
    ) -> Self {
        self.left_store_config = Some(left_config);
        self.right_store_config = Some(right_config);
        self
    }

    /// Set maximum records for both stores
    pub fn with_max_records(mut self, max_records: usize) -> Self {
        let config = JoinStateStoreConfig::with_limits(max_records, 0);
        self.left_store_config = Some(config.clone());
        self.right_store_config = Some(config);
        self
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
    /// Records evicted from left store due to limits
    pub left_evictions: u64,
    /// Records evicted from right store due to limits
    pub right_evictions: u64,
}

/// Memory pressure status for backpressure signaling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// Both stores have plenty of capacity
    Normal,
    /// One or both stores are approaching limits (>80% capacity)
    Warning,
    /// One or both stores are at capacity, evictions occurring
    Critical,
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

    /// Create a new join coordinator with full configuration including memory limits
    pub fn with_config(config: JoinCoordinatorConfig) -> Self {
        // Convert retention_ms to Duration safely (negative values become 0)
        let retention_ms = config.join_config.retention_ms.max(0) as u64;
        let retention = Duration::from_millis(retention_ms);
        let key_extractors = JoinKeyExtractorPair::from_pairs(config.join_config.join_keys.clone());

        let left_store = match config.left_store_config {
            Some(store_config) => JoinStateStore::with_config(retention, store_config),
            None => JoinStateStore::new(retention),
        };

        let right_store = match config.right_store_config {
            Some(store_config) => JoinStateStore::with_config(retention, store_config),
            None => JoinStateStore::new(retention),
        };

        Self {
            config: config.join_config,
            left_store,
            right_store,
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
        let event_time = match self.extract_event_time(&record)? {
            Some(ts) => ts,
            None => return Ok(vec![]), // Skip record (SkipRecord behavior)
        };

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
        self.update_eviction_stats();
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
        let event_time = match self.extract_event_time(&record)? {
            Some(ts) => ts,
            None => return Ok(vec![]), // Skip record (SkipRecord behavior)
        };

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
        self.update_eviction_stats();
        Ok(joined)
    }

    /// Extract event time from a record
    ///
    /// Returns:
    /// - `Ok(Some(timestamp))` - Event time was found
    /// - `Ok(None)` - Event time missing and `SkipRecord` behavior configured
    /// - `Err(SqlError)` - Event time missing and `Error` behavior configured
    fn extract_event_time(&mut self, record: &StreamRecord) -> Result<Option<i64>, SqlError> {
        // First try the configured event time field
        if let Some(value) = record.fields.get(&self.config.event_time_field) {
            if let Some(ts) = self.field_value_to_timestamp(value) {
                return Ok(Some(ts));
            }
        }

        // Fall back to record's event_time metadata if available
        if let Some(event_time) = record.event_time {
            return Ok(Some(event_time.timestamp_millis()));
        }

        // Fall back to record timestamp
        if record.timestamp > 0 {
            return Ok(Some(record.timestamp));
        }

        // Event time is missing - apply configured behavior
        self.stats.missing_time_count += 1;

        match self.config.missing_event_time {
            MissingEventTimeBehavior::UseWallClock => {
                log::debug!(
                    "JoinCoordinator: Missing event time, using wall-clock time (consider using SkipRecord or Error mode)"
                );
                Ok(Some(chrono::Utc::now().timestamp_millis()))
            }
            MissingEventTimeBehavior::SkipRecord => {
                log::debug!("JoinCoordinator: Skipping record with missing event time");
                Ok(None)
            }
            MissingEventTimeBehavior::Error => Err(SqlError::ExecutionError {
                message: format!(
                    "Record missing event time field '{}' and no fallback configured",
                    self.config.event_time_field
                ),
                query: None,
            }),
        }
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
    ///
    /// Note: Unprefixed field names from the right side will overwrite left side
    /// values when both sides have the same field name. Use prefixed field names
    /// (e.g., `orders.amount` vs `shipments.amount`) to access both values.
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
            // Check for field collision before overwriting
            if left.fields.contains_key(key) {
                log::debug!(
                    "JoinCoordinator: Field '{}' exists in both sides; unprefixed value will be from right ({}). Use '{}.{}' or '{}.{}' for explicit access.",
                    key,
                    self.config.right_source,
                    self.config.left_source,
                    key,
                    self.config.right_source,
                    key
                );
            }
            // Add without prefix (overwrites left if same name)
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

    /// Check memory pressure across both state stores
    ///
    /// Returns the worst pressure level between the two stores:
    /// - `Critical`: One or both stores are at capacity (evictions occurring)
    /// - `Warning`: One or both stores are approaching limits (>80% by default)
    /// - `Normal`: Both stores have plenty of capacity
    #[must_use]
    pub fn memory_pressure(&self) -> MemoryPressure {
        let left_at_capacity = self.left_store.is_at_capacity();
        let right_at_capacity = self.right_store.is_at_capacity();

        if left_at_capacity || right_at_capacity {
            return MemoryPressure::Critical;
        }

        let left_near_capacity = self.left_store.is_near_capacity();
        let right_near_capacity = self.right_store.is_near_capacity();

        if left_near_capacity || right_near_capacity {
            return MemoryPressure::Warning;
        }

        MemoryPressure::Normal
    }

    /// Check if backpressure should be applied to slow down ingestion
    ///
    /// Returns true if either store is at Warning or Critical pressure level.
    /// Use this to implement flow control in upstream processing.
    #[must_use]
    pub fn should_apply_backpressure(&self) -> bool {
        self.memory_pressure() != MemoryPressure::Normal
    }

    /// Get combined capacity usage as a percentage
    ///
    /// Returns the higher of the two stores' usage percentages.
    /// Returns 0.0 if both stores are unlimited.
    #[must_use]
    pub fn combined_capacity_usage_pct(&self) -> f64 {
        self.left_store
            .capacity_usage_pct()
            .max(self.right_store.capacity_usage_pct())
    }

    /// Get remaining capacity across both stores
    ///
    /// Returns the minimum remaining capacity between stores.
    /// Returns usize::MAX if both stores are unlimited.
    pub fn remaining_capacity(&self) -> usize {
        self.left_store
            .remaining_capacity()
            .min(self.right_store.remaining_capacity())
    }

    /// Get eviction counts from both stores
    pub fn eviction_counts(&self) -> (u64, u64) {
        (
            self.left_store.stats().records_evicted,
            self.right_store.stats().records_evicted,
        )
    }

    /// Update stats with current eviction counts from stores
    fn update_eviction_stats(&mut self) {
        self.stats.left_evictions = self.left_store.stats().records_evicted;
        self.stats.right_evictions = self.right_store.stats().records_evicted;
    }
}
