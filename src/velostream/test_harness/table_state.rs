//! Table state management for CTAS (CREATE TABLE AS SELECT) testing
//!
//! Provides capabilities for:
//! - Materialized table state tracking
//! - Table snapshot and checkpoint management
//! - Table freshness assertions
//! - Key-based table lookups

use super::error::{TestHarnessError, TestHarnessResult};
use super::utils::field_value_to_string;
use crate::velostream::sql::execution::types::FieldValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Configuration for table state management
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStateConfig {
    /// Enable table state tracking
    #[serde(default)]
    pub enabled: bool,

    /// Key fields for the table (for upsert semantics)
    #[serde(default)]
    pub key_fields: Vec<String>,

    /// Enable changelog tracking
    #[serde(default)]
    pub track_changelog: bool,

    /// Maximum changelog entries to keep
    #[serde(default = "default_max_changelog")]
    pub max_changelog_entries: usize,

    /// Enable snapshot on completion
    #[serde(default)]
    pub snapshot_on_complete: bool,

    /// Freshness window (milliseconds) - table should have updates within this window
    #[serde(default)]
    pub freshness_window_ms: Option<u64>,
}

fn default_max_changelog() -> usize {
    10000
}

/// Represents the current state of a materialized table
#[derive(Debug, Clone)]
pub struct TableState {
    /// Table name
    pub name: String,

    /// Current state (key -> record)
    pub records: HashMap<String, HashMap<String, FieldValue>>,

    /// Key fields used for record identification
    pub key_fields: Vec<String>,

    /// Changelog entries (if tracking enabled)
    pub changelog: Vec<ChangelogEntry>,

    /// Statistics
    pub stats: TableStateStats,

    /// Last update timestamp
    pub last_update: Option<Instant>,

    /// Creation timestamp
    pub created_at: Instant,
}

impl Default for TableState {
    fn default() -> Self {
        Self::new("", vec![])
    }
}

impl TableState {
    /// Create a new table state
    pub fn new(name: &str, key_fields: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            records: HashMap::new(),
            key_fields,
            changelog: Vec::new(),
            stats: TableStateStats::default(),
            last_update: None,
            created_at: Instant::now(),
        }
    }
}

/// A changelog entry tracking state changes
#[derive(Debug, Clone)]
pub struct ChangelogEntry {
    /// Type of change
    pub change_type: ChangeType,

    /// Record key
    pub key: String,

    /// Record value (after change)
    pub value: Option<HashMap<String, FieldValue>>,

    /// Previous value (for updates)
    pub previous_value: Option<HashMap<String, FieldValue>>,

    /// Timestamp of change
    pub timestamp_ms: u64,
}

/// Type of change in changelog
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    /// New record inserted
    Insert,
    /// Existing record updated
    Update,
    /// Record deleted
    Delete,
    /// Tombstone (null value)
    Tombstone,
}

/// Statistics about table state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStateStats {
    /// Total records currently in table
    pub record_count: usize,

    /// Total inserts processed
    pub total_inserts: usize,

    /// Total updates processed
    pub total_updates: usize,

    /// Total deletes processed
    pub total_deletes: usize,

    /// Total tombstones processed
    pub total_tombstones: usize,

    /// Maximum record count seen
    pub peak_record_count: usize,
}

impl TableState {
    /// Generate key from record using key fields
    pub fn generate_key(&self, record: &HashMap<String, FieldValue>) -> String {
        if self.key_fields.is_empty() {
            // Use all fields for key if no key fields specified
            let mut parts: Vec<String> = record
                .iter()
                .map(|(k, v)| format!("{}={}", k, field_value_to_string(v)))
                .collect();
            parts.sort();
            parts.join("|")
        } else {
            self.key_fields
                .iter()
                .map(|f| {
                    record
                        .get(f)
                        .map(field_value_to_string)
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect::<Vec<_>>()
                .join("|")
        }
    }

    /// Upsert a record into the table
    pub fn upsert(&mut self, record: HashMap<String, FieldValue>, track_changelog: bool) {
        let key = self.generate_key(&record);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let previous = self.records.get(&key).cloned();
        let change_type = if previous.is_some() {
            ChangeType::Update
        } else {
            ChangeType::Insert
        };

        if track_changelog {
            self.changelog.push(ChangelogEntry {
                change_type: change_type.clone(),
                key: key.clone(),
                value: Some(record.clone()),
                previous_value: previous,
                timestamp_ms: now_ms,
            });
        }

        // Update stats
        match change_type {
            ChangeType::Insert => self.stats.total_inserts += 1,
            ChangeType::Update => self.stats.total_updates += 1,
            _ => {}
        }

        self.records.insert(key, record);
        self.stats.record_count = self.records.len();
        self.stats.peak_record_count = self.stats.peak_record_count.max(self.records.len());
        self.last_update = Some(Instant::now());
    }

    /// Delete a record from the table
    pub fn delete(&mut self, key: &str, track_changelog: bool) {
        let previous = self.records.remove(key);

        if previous.is_some() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            if track_changelog {
                self.changelog.push(ChangelogEntry {
                    change_type: ChangeType::Delete,
                    key: key.to_string(),
                    value: None,
                    previous_value: previous,
                    timestamp_ms: now_ms,
                });
            }

            self.stats.total_deletes += 1;
            self.stats.record_count = self.records.len();
            self.last_update = Some(Instant::now());
        }
    }

    /// Process a tombstone (null value for key)
    pub fn tombstone(&mut self, key: &str, track_changelog: bool) {
        let previous = self.records.remove(key);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if track_changelog {
            self.changelog.push(ChangelogEntry {
                change_type: ChangeType::Tombstone,
                key: key.to_string(),
                value: None,
                previous_value: previous,
                timestamp_ms: now_ms,
            });
        }

        self.stats.total_tombstones += 1;
        self.stats.record_count = self.records.len();
        self.last_update = Some(Instant::now());
    }

    /// Get a record by key
    pub fn get(&self, key: &str) -> Option<&HashMap<String, FieldValue>> {
        self.records.get(key)
    }

    /// Get all records
    pub fn all_records(&self) -> Vec<&HashMap<String, FieldValue>> {
        self.records.values().collect()
    }

    /// Get record count
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Check if table has been updated within the freshness window
    pub fn is_fresh(&self, freshness_window: Duration) -> bool {
        self.last_update
            .map(|t| t.elapsed() < freshness_window)
            .unwrap_or(false)
    }

    /// Get time since last update
    pub fn time_since_last_update(&self) -> Option<Duration> {
        self.last_update.map(|t| t.elapsed())
    }

    /// Get the changelog
    pub fn changelog(&self) -> &[ChangelogEntry] {
        &self.changelog
    }

    /// Trim changelog to max entries
    pub fn trim_changelog(&mut self, max_entries: usize) {
        if self.changelog.len() > max_entries {
            let to_remove = self.changelog.len() - max_entries;
            self.changelog.drain(0..to_remove);
        }
    }

    /// Create a snapshot of current state
    pub fn snapshot(&self) -> TableSnapshot {
        TableSnapshot {
            name: self.name.clone(),
            records: self.records.clone(),
            key_fields: self.key_fields.clone(),
            stats: self.stats.clone(),
            snapshot_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Restore from a snapshot
    pub fn restore_from_snapshot(&mut self, snapshot: TableSnapshot) {
        self.records = snapshot.records;
        self.stats.record_count = self.records.len();
        self.last_update = Some(Instant::now());
    }

    /// Query records matching a predicate
    pub fn query<F>(&self, predicate: F) -> Vec<&HashMap<String, FieldValue>>
    where
        F: Fn(&HashMap<String, FieldValue>) -> bool,
    {
        self.records.values().filter(|r| predicate(r)).collect()
    }

    /// Get distinct values for a field
    pub fn distinct_values(&self, field: &str) -> Vec<FieldValue> {
        let mut values: Vec<_> = self
            .records
            .values()
            .filter_map(|r| r.get(field).cloned())
            .collect();
        values.sort_by(|a, b| {
            field_value_to_string(a)
                .partial_cmp(&field_value_to_string(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        values.dedup_by(|a, b| field_value_to_string(a) == field_value_to_string(b));
        values
    }
}

/// A snapshot of table state at a point in time
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Table name
    pub name: String,

    /// Records at snapshot time
    pub records: HashMap<String, HashMap<String, FieldValue>>,

    /// Key fields
    pub key_fields: Vec<String>,

    /// Statistics at snapshot time
    pub stats: TableStateStats,

    /// Snapshot timestamp (ms since epoch)
    pub snapshot_time: u64,
}

impl TableSnapshot {
    /// Save snapshot to file as JSON
    /// Note: FieldValue is serialized to a string representation
    pub fn save_to_file(&self, path: &std::path::Path) -> TestHarnessResult<()> {
        // Convert records to a JSON-serializable format
        let records_json: HashMap<String, HashMap<String, String>> = self
            .records
            .iter()
            .map(|(k, v)| {
                let values: HashMap<String, String> = v
                    .iter()
                    .map(|(field, fv)| (field.clone(), field_value_to_string(fv)))
                    .collect();
                (k.clone(), values)
            })
            .collect();

        let json_obj = serde_json::json!({
            "name": self.name,
            "records": records_json,
            "key_fields": self.key_fields,
            "stats": self.stats,
            "snapshot_time": self.snapshot_time,
        });

        let content =
            serde_json::to_string_pretty(&json_obj).map_err(|e| TestHarnessError::IoError {
                message: format!("Failed to serialize snapshot: {}", e),
                path: path.display().to_string(),
            })?;

        std::fs::write(path, content).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        Ok(())
    }

    /// Get record count
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &str) -> bool {
        self.records.contains_key(key)
    }
}

/// Manager for multiple table states
#[derive(Debug, Default)]
pub struct TableStateManager {
    /// Table states by name
    tables: HashMap<String, TableState>,

    /// Snapshots by name
    snapshots: HashMap<String, Vec<TableSnapshot>>,
}

impl TableStateManager {
    /// Create a new table state manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Create or get a table state
    pub fn get_or_create(&mut self, name: &str, key_fields: Vec<String>) -> &mut TableState {
        self.tables
            .entry(name.to_string())
            .or_insert_with(|| TableState::new(name, key_fields))
    }

    /// Get a table state
    pub fn get(&self, name: &str) -> Option<&TableState> {
        self.tables.get(name)
    }

    /// Get mutable table state
    pub fn get_mut(&mut self, name: &str) -> Option<&mut TableState> {
        self.tables.get_mut(name)
    }

    /// Remove a table state
    pub fn remove(&mut self, name: &str) -> Option<TableState> {
        self.tables.remove(name)
    }

    /// List all table names
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Take a snapshot of a table
    pub fn snapshot_table(&mut self, name: &str) -> Option<TableSnapshot> {
        let table = self.tables.get(name)?;
        let snapshot = table.snapshot();

        self.snapshots
            .entry(name.to_string())
            .or_default()
            .push(snapshot.clone());

        Some(snapshot)
    }

    /// Get latest snapshot for a table
    pub fn latest_snapshot(&self, name: &str) -> Option<&TableSnapshot> {
        self.snapshots.get(name)?.last()
    }

    /// Get all snapshots for a table
    pub fn snapshots(&self, name: &str) -> Option<&[TableSnapshot]> {
        self.snapshots.get(name).map(|v| v.as_slice())
    }

    /// Clear all table states
    pub fn clear(&mut self) {
        self.tables.clear();
        self.snapshots.clear();
    }

    /// Get summary of all tables
    pub fn summary(&self) -> HashMap<String, TableStateStats> {
        self.tables
            .iter()
            .map(|(name, state)| (name.clone(), state.stats.clone()))
            .collect()
    }
}

// Note: field_value_to_string is now imported from utils module

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_state_insert_update() {
        let mut state = TableState::new("test_table", vec!["id".to_string()]);

        // Insert
        let mut record1 = HashMap::new();
        record1.insert("id".to_string(), FieldValue::Integer(1));
        record1.insert("value".to_string(), FieldValue::String("first".to_string()));
        state.upsert(record1, true);

        assert_eq!(state.count(), 1);
        assert_eq!(state.stats.total_inserts, 1);
        assert_eq!(state.stats.total_updates, 0);

        // Update
        let mut record2 = HashMap::new();
        record2.insert("id".to_string(), FieldValue::Integer(1));
        record2.insert(
            "value".to_string(),
            FieldValue::String("updated".to_string()),
        );
        state.upsert(record2, true);

        assert_eq!(state.count(), 1);
        assert_eq!(state.stats.total_inserts, 1);
        assert_eq!(state.stats.total_updates, 1);

        // Check changelog
        assert_eq!(state.changelog().len(), 2);
        assert_eq!(state.changelog()[0].change_type, ChangeType::Insert);
        assert_eq!(state.changelog()[1].change_type, ChangeType::Update);
    }

    #[test]
    fn test_table_state_delete() {
        let mut state = TableState::new("test_table", vec!["id".to_string()]);

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(1));
        record.insert("value".to_string(), FieldValue::String("test".to_string()));
        state.upsert(record, true);

        assert_eq!(state.count(), 1);

        state.delete("1", true);

        assert_eq!(state.count(), 0);
        assert_eq!(state.stats.total_deletes, 1);
    }

    #[test]
    fn test_table_state_query() {
        let mut state = TableState::new("test_table", vec!["id".to_string()]);

        for i in 1..=10 {
            let mut record = HashMap::new();
            record.insert("id".to_string(), FieldValue::Integer(i));
            record.insert("value".to_string(), FieldValue::Integer(i * 10));
            state.upsert(record, false);
        }

        let results =
            state.query(|r| matches!(r.get("value"), Some(FieldValue::Integer(v)) if *v > 50));

        assert_eq!(results.len(), 5); // Values 60, 70, 80, 90, 100
    }

    #[test]
    fn test_table_snapshot() {
        let mut state = TableState::new("test_table", vec!["id".to_string()]);

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(1));
        record.insert("value".to_string(), FieldValue::String("test".to_string()));
        state.upsert(record, false);

        let snapshot = state.snapshot();

        assert_eq!(snapshot.name, "test_table");
        assert_eq!(snapshot.records.len(), 1);
        assert!(snapshot.snapshot_time > 0);
    }

    #[test]
    fn test_table_state_manager() {
        let mut manager = TableStateManager::new();

        let table = manager.get_or_create("orders", vec!["order_id".to_string()]);

        let mut record = HashMap::new();
        record.insert(
            "order_id".to_string(),
            FieldValue::String("ORD001".to_string()),
        );
        record.insert("amount".to_string(), FieldValue::Float(100.0));
        table.upsert(record, false);

        assert!(manager.get("orders").is_some());
        assert_eq!(manager.table_names(), vec!["orders"]);

        // Snapshot
        let snapshot = manager.snapshot_table("orders");
        assert!(snapshot.is_some());
        assert!(manager.latest_snapshot("orders").is_some());
    }

    #[test]
    fn test_distinct_values() {
        let mut state = TableState::new("test_table", vec!["id".to_string()]);

        for i in 1..=5 {
            let mut record = HashMap::new();
            record.insert("id".to_string(), FieldValue::Integer(i));
            record.insert(
                "category".to_string(),
                FieldValue::String(if i % 2 == 0 { "even" } else { "odd" }.to_string()),
            );
            state.upsert(record, false);
        }

        let distinct = state.distinct_values("category");
        assert_eq!(distinct.len(), 2);
    }
}
