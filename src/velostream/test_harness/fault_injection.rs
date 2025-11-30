//! Fault injection framework for chaos testing
//!
//! Provides capabilities for:
//! - Malformed record injection
//! - Duplicate message injection
//! - Out-of-order event simulation
//! - Network partition simulation
//! - Slow processing simulation

use crate::velostream::sql::execution::types::FieldValue;
use rand::Rng;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for fault injection during test execution
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FaultInjectionConfig {
    /// Malformed record injection settings
    #[serde(default)]
    pub malformed_records: Option<MalformedRecordConfig>,

    /// Duplicate message injection settings
    #[serde(default)]
    pub duplicates: Option<DuplicateConfig>,

    /// Out-of-order event settings
    #[serde(default)]
    pub out_of_order: Option<OutOfOrderConfig>,

    /// Slow processing simulation
    #[serde(default)]
    pub slow_processing: Option<SlowProcessingConfig>,

    /// Field corruption settings
    #[serde(default)]
    pub field_corruption: Option<FieldCorruptionConfig>,

    /// Random seed for reproducibility
    #[serde(default)]
    pub seed: Option<u64>,
}

impl FaultInjectionConfig {
    /// Check if any fault injection is enabled
    pub fn is_enabled(&self) -> bool {
        self.malformed_records.as_ref().is_some_and(|c| c.enabled)
            || self.duplicates.as_ref().is_some_and(|c| c.enabled)
            || self.out_of_order.as_ref().is_some_and(|c| c.enabled)
            || self.slow_processing.as_ref().is_some_and(|c| c.enabled)
            || self.field_corruption.as_ref().is_some_and(|c| c.enabled)
    }
}

/// Configuration for injecting malformed records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MalformedRecordConfig {
    /// Enable malformed record injection
    #[serde(default)]
    pub enabled: bool,

    /// Probability of malforming a record (0.0 to 1.0)
    #[serde(default = "default_rate")]
    pub rate: f64,

    /// Types of malformations to inject
    #[serde(default)]
    pub types: Vec<MalformationType>,
}

fn default_rate() -> f64 {
    0.05 // 5% default
}

impl Default for MalformedRecordConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: default_rate(),
            types: vec![
                MalformationType::MissingField,
                MalformationType::WrongType,
                MalformationType::InvalidJson,
            ],
        }
    }
}

/// Types of record malformations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MalformationType {
    /// Remove a required field
    MissingField,
    /// Use wrong data type for a field
    WrongType,
    /// Produce invalid JSON/format
    InvalidJson,
    /// Exceed field length limits
    Overflow,
    /// Empty record
    EmptyRecord,
    /// Null where not allowed
    UnexpectedNull,
    /// Invalid UTF-8 encoding
    InvalidEncoding,
    /// Truncated record
    Truncated,
}

/// Configuration for duplicate message injection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateConfig {
    /// Enable duplicate injection
    #[serde(default)]
    pub enabled: bool,

    /// Probability of duplicating a record (0.0 to 1.0)
    #[serde(default = "default_duplicate_rate")]
    pub rate: f64,

    /// Maximum number of duplicates per record
    #[serde(default = "default_max_duplicates")]
    pub max_duplicates: usize,

    /// Delay between duplicates (milliseconds)
    #[serde(default)]
    pub delay_ms: Option<u64>,
}

fn default_duplicate_rate() -> f64 {
    0.01 // 1% default
}

fn default_max_duplicates() -> usize {
    3
}

impl Default for DuplicateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: default_duplicate_rate(),
            max_duplicates: default_max_duplicates(),
            delay_ms: None,
        }
    }
}

/// Configuration for out-of-order event injection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutOfOrderConfig {
    /// Enable out-of-order injection
    #[serde(default)]
    pub enabled: bool,

    /// Probability of reordering (0.0 to 1.0)
    #[serde(default = "default_ooo_rate")]
    pub rate: f64,

    /// Maximum delay in milliseconds for late events
    #[serde(default = "default_max_delay")]
    pub max_delay_ms: u64,

    /// Window size for reordering (number of records to buffer)
    #[serde(default = "default_window_size")]
    pub window_size: usize,
}

fn default_ooo_rate() -> f64 {
    0.1 // 10% default
}

fn default_max_delay() -> u64 {
    5000 // 5 seconds
}

fn default_window_size() -> usize {
    10
}

impl Default for OutOfOrderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: default_ooo_rate(),
            max_delay_ms: default_max_delay(),
            window_size: default_window_size(),
        }
    }
}

/// Configuration for slow processing simulation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowProcessingConfig {
    /// Enable slow processing simulation
    #[serde(default)]
    pub enabled: bool,

    /// Probability of slow processing (0.0 to 1.0)
    #[serde(default = "default_slow_rate")]
    pub rate: f64,

    /// Minimum delay in milliseconds
    #[serde(default = "default_min_delay")]
    pub min_delay_ms: u64,

    /// Maximum delay in milliseconds
    #[serde(default = "default_max_slow_delay")]
    pub max_delay_ms: u64,
}

fn default_slow_rate() -> f64 {
    0.05 // 5% default
}

fn default_min_delay() -> u64 {
    100
}

fn default_max_slow_delay() -> u64 {
    1000
}

impl Default for SlowProcessingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: default_slow_rate(),
            min_delay_ms: default_min_delay(),
            max_delay_ms: default_max_slow_delay(),
        }
    }
}

/// Configuration for field corruption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCorruptionConfig {
    /// Enable field corruption
    #[serde(default)]
    pub enabled: bool,

    /// Probability of corrupting a field (0.0 to 1.0)
    #[serde(default = "default_corruption_rate")]
    pub rate: f64,

    /// Fields to corrupt (empty = any field)
    #[serde(default)]
    pub target_fields: Vec<String>,

    /// Types of corruption
    #[serde(default)]
    pub corruption_types: Vec<CorruptionType>,
}

fn default_corruption_rate() -> f64 {
    0.02 // 2% default
}

impl Default for FieldCorruptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: default_corruption_rate(),
            target_fields: Vec::new(),
            corruption_types: vec![CorruptionType::RandomValue, CorruptionType::BoundaryValue],
        }
    }
}

/// Types of field corruption
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CorruptionType {
    /// Replace with random value of same type
    RandomValue,
    /// Use boundary values (min/max)
    BoundaryValue,
    /// Flip bits in numeric values
    BitFlip,
    /// String truncation
    Truncate,
    /// Case change for strings
    CaseChange,
}

/// Fault injector that modifies records during generation
pub struct FaultInjector {
    config: FaultInjectionConfig,
    rng: StdRng,
    /// Buffer for out-of-order simulation
    ooo_buffer: Vec<HashMap<String, FieldValue>>,
    /// Statistics
    stats: FaultInjectionStats,
}

/// Statistics about injected faults
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FaultInjectionStats {
    /// Total records processed
    pub records_processed: usize,
    /// Malformed records injected
    pub malformed_injected: usize,
    /// Duplicates injected
    pub duplicates_injected: usize,
    /// Records reordered
    pub records_reordered: usize,
    /// Fields corrupted
    pub fields_corrupted: usize,
    /// Slow processing delays added
    pub slow_delays_added: usize,
}

impl FaultInjector {
    /// Create a new fault injector
    pub fn new(config: FaultInjectionConfig) -> Self {
        let seed = config.seed.unwrap_or_else(|| rand::thread_rng().r#gen());
        Self {
            config,
            rng: StdRng::seed_from_u64(seed),
            ooo_buffer: Vec::new(),
            stats: FaultInjectionStats::default(),
        }
    }

    /// Process a record, potentially injecting faults
    /// Returns a list of records (may be 0, 1, or more due to duplicates/reordering)
    pub fn process_record(
        &mut self,
        mut record: HashMap<String, FieldValue>,
    ) -> Vec<HashMap<String, FieldValue>> {
        self.stats.records_processed += 1;
        let mut results = Vec::new();

        // Apply field corruption - clone config to avoid borrow issues
        if let Some(corruption_config) = self.config.field_corruption.clone() {
            if corruption_config.enabled && self.rng.r#gen::<f64>() < corruption_config.rate {
                self.corrupt_fields(&mut record, &corruption_config);
                self.stats.fields_corrupted += 1;
            }
        }

        // Handle out-of-order - clone config to avoid borrow issues
        if let Some(ooo_config) = self.config.out_of_order.clone() {
            if ooo_config.enabled {
                return self.handle_out_of_order(record, &ooo_config);
            }
        }

        // Handle duplicates
        if let Some(ref dup_config) = self.config.duplicates {
            if dup_config.enabled && self.rng.r#gen::<f64>() < dup_config.rate {
                let num_dups = self.rng.gen_range(1..=dup_config.max_duplicates);
                for _ in 0..num_dups {
                    results.push(record.clone());
                    self.stats.duplicates_injected += 1;
                }
            }
        }

        results.push(record);
        results
    }

    /// Generate a malformed record based on a valid one
    pub fn generate_malformed(
        &mut self,
        record: &HashMap<String, FieldValue>,
    ) -> Option<HashMap<String, FieldValue>> {
        let config = self.config.malformed_records.as_ref()?;
        if !config.enabled || self.rng.r#gen::<f64>() >= config.rate {
            return None;
        }

        if config.types.is_empty() {
            return None;
        }

        let malformation_type = config.types.choose(&mut self.rng)?;
        let mut malformed = record.clone();

        match malformation_type {
            MalformationType::MissingField => {
                // Remove a random field
                let keys: Vec<_> = malformed.keys().cloned().collect();
                if let Some(key) = keys.choose(&mut self.rng) {
                    malformed.remove(key);
                }
            }
            MalformationType::WrongType => {
                // Change type of a random field
                let keys: Vec<_> = malformed.keys().cloned().collect();
                if let Some(key) = keys.choose(&mut self.rng) {
                    let new_value = match malformed.get(key) {
                        Some(FieldValue::Integer(_)) => {
                            FieldValue::String("not_an_int".to_string())
                        }
                        Some(FieldValue::Float(_)) => FieldValue::String("not_a_float".to_string()),
                        Some(FieldValue::String(_)) => FieldValue::Integer(999),
                        Some(FieldValue::Boolean(_)) => {
                            FieldValue::String("not_a_bool".to_string())
                        }
                        _ => FieldValue::String("corrupted".to_string()),
                    };
                    malformed.insert(key.clone(), new_value);
                }
            }
            MalformationType::UnexpectedNull => {
                // Set a random field to null
                let keys: Vec<_> = malformed.keys().cloned().collect();
                if let Some(key) = keys.choose(&mut self.rng) {
                    malformed.insert(key.clone(), FieldValue::Null);
                }
            }
            MalformationType::EmptyRecord => {
                malformed.clear();
            }
            MalformationType::Overflow => {
                // Add a very long string
                let keys: Vec<_> = malformed.keys().cloned().collect();
                if let Some(key) = keys.choose(&mut self.rng) {
                    let long_string = "x".repeat(100000);
                    malformed.insert(key.clone(), FieldValue::String(long_string));
                }
            }
            MalformationType::InvalidJson => {
                // Insert a string that looks like invalid JSON (for raw payload corruption)
                // Note: Since we're working with FieldValue maps, we can't produce truly invalid JSON
                // Instead, we insert a field that contains what would be invalid JSON if serialized raw
                malformed.insert(
                    "__malformed_json__".to_string(),
                    FieldValue::String("{invalid: json, missing: quotes}".to_string()),
                );
            }
            MalformationType::InvalidEncoding => {
                // Insert invalid UTF-8 sequence representation
                // Since FieldValue::String must be valid UTF-8, we represent it as escaped bytes
                let keys: Vec<_> = malformed.keys().cloned().collect();
                if let Some(key) = keys.choose(&mut self.rng) {
                    // Represent invalid UTF-8 as a string with replacement character sequences
                    malformed.insert(
                        key.clone(),
                        FieldValue::String("\u{FFFD}\u{FFFD}\u{FFFD}invalid_encoding".to_string()),
                    );
                }
            }
            MalformationType::Truncated => {
                // Truncate string fields to simulate truncated records
                let keys: Vec<_> = malformed.keys().cloned().collect();
                for key in keys {
                    if let Some(FieldValue::String(s)) = malformed.get(&key) {
                        if s.len() > 3 {
                            // Truncate to first 3 chars
                            let truncated = s.chars().take(3).collect::<String>();
                            malformed.insert(key, FieldValue::String(truncated));
                        }
                    }
                }
            }
        }

        self.stats.malformed_injected += 1;
        Some(malformed)
    }

    /// Handle out-of-order record buffering
    fn handle_out_of_order(
        &mut self,
        record: HashMap<String, FieldValue>,
        config: &OutOfOrderConfig,
    ) -> Vec<HashMap<String, FieldValue>> {
        self.ooo_buffer.push(record);

        if self.ooo_buffer.len() >= config.window_size {
            // Shuffle the buffer based on rate
            if self.rng.r#gen::<f64>() < config.rate {
                self.ooo_buffer.shuffle(&mut self.rng);
                self.stats.records_reordered += self.ooo_buffer.len();
            }
            std::mem::take(&mut self.ooo_buffer)
        } else {
            Vec::new()
        }
    }

    /// Flush remaining buffered records
    pub fn flush(&mut self) -> Vec<HashMap<String, FieldValue>> {
        std::mem::take(&mut self.ooo_buffer)
    }

    /// Corrupt fields in a record
    fn corrupt_fields(
        &mut self,
        record: &mut HashMap<String, FieldValue>,
        config: &FieldCorruptionConfig,
    ) {
        let keys: Vec<_> = if config.target_fields.is_empty() {
            record.keys().cloned().collect()
        } else {
            config
                .target_fields
                .iter()
                .filter(|k| record.contains_key(*k))
                .cloned()
                .collect()
        };

        if let Some(key) = keys.choose(&mut self.rng) {
            if let Some(corruption_type) = config.corruption_types.choose(&mut self.rng) {
                let corrupted = self.corrupt_value(record.get(key), corruption_type);
                record.insert(key.clone(), corrupted);
            }
        }
    }

    /// Corrupt a single value
    fn corrupt_value(
        &mut self,
        value: Option<&FieldValue>,
        corruption_type: &CorruptionType,
    ) -> FieldValue {
        match (value, corruption_type) {
            (Some(FieldValue::Integer(_n)), CorruptionType::RandomValue) => {
                FieldValue::Integer(self.rng.r#gen())
            }
            (Some(FieldValue::Integer(_)), CorruptionType::BoundaryValue) => {
                if self.rng.r#gen::<bool>() {
                    FieldValue::Integer(i64::MAX)
                } else {
                    FieldValue::Integer(i64::MIN)
                }
            }
            (Some(FieldValue::Integer(n)), CorruptionType::BitFlip) => {
                let bit = 1i64 << self.rng.gen_range(0..64);
                FieldValue::Integer(n ^ bit)
            }
            (Some(FieldValue::Float(_f)), CorruptionType::RandomValue) => {
                FieldValue::Float(self.rng.r#gen::<f64>() * 1000.0)
            }
            (Some(FieldValue::Float(_)), CorruptionType::BoundaryValue) => {
                if self.rng.r#gen::<bool>() {
                    FieldValue::Float(f64::MAX)
                } else {
                    FieldValue::Float(f64::MIN)
                }
            }
            (Some(FieldValue::String(s)), CorruptionType::Truncate) => {
                let len = s.len();
                if len > 1 {
                    FieldValue::String(s[..len / 2].to_string())
                } else {
                    FieldValue::String(String::new())
                }
            }
            (Some(FieldValue::String(s)), CorruptionType::CaseChange) => {
                if self.rng.r#gen::<bool>() {
                    FieldValue::String(s.to_uppercase())
                } else {
                    FieldValue::String(s.to_lowercase())
                }
            }
            _ => FieldValue::Null,
        }
    }

    /// Get statistics about injected faults
    pub fn statistics(&self) -> &FaultInjectionStats {
        &self.stats
    }

    /// Check if a delay should be added (for slow processing simulation)
    pub fn should_delay(&mut self) -> Option<std::time::Duration> {
        let config = self.config.slow_processing.as_ref()?;
        if !config.enabled || self.rng.r#gen::<f64>() >= config.rate {
            return None;
        }

        let delay_ms = self
            .rng
            .gen_range(config.min_delay_ms..=config.max_delay_ms);
        self.stats.slow_delays_added += 1;
        Some(std::time::Duration::from_millis(delay_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fault_injection_disabled() {
        let config = FaultInjectionConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_malformed_injection() {
        let config = FaultInjectionConfig {
            malformed_records: Some(MalformedRecordConfig {
                enabled: true,
                rate: 1.0, // Always inject
                types: vec![MalformationType::MissingField],
            }),
            seed: Some(42),
            ..Default::default()
        };

        let mut injector = FaultInjector::new(config);
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(1));
        record.insert("name".to_string(), FieldValue::String("test".to_string()));

        let malformed = injector.generate_malformed(&record);
        assert!(malformed.is_some());
        let malformed = malformed.unwrap();
        assert!(malformed.len() < record.len()); // One field removed
    }

    #[test]
    fn test_duplicate_injection() {
        let config = FaultInjectionConfig {
            duplicates: Some(DuplicateConfig {
                enabled: true,
                rate: 1.0, // Always duplicate
                max_duplicates: 2,
                delay_ms: None,
            }),
            seed: Some(42),
            ..Default::default()
        };

        let mut injector = FaultInjector::new(config);
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(1));

        let results = injector.process_record(record);
        assert!(results.len() > 1); // At least one duplicate
    }

    #[test]
    fn test_field_corruption() {
        let config = FaultInjectionConfig {
            field_corruption: Some(FieldCorruptionConfig {
                enabled: true,
                rate: 1.0, // Always corrupt
                target_fields: vec!["value".to_string()],
                corruption_types: vec![CorruptionType::BoundaryValue],
            }),
            seed: Some(42),
            ..Default::default()
        };

        let mut injector = FaultInjector::new(config);
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(1));
        record.insert("value".to_string(), FieldValue::Integer(100));

        let results = injector.process_record(record);
        assert_eq!(results.len(), 1);

        let processed = &results[0];
        let value = processed.get("value").unwrap();
        match value {
            FieldValue::Integer(n) => {
                assert!(*n == i64::MAX || *n == i64::MIN);
            }
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_statistics() {
        let config = FaultInjectionConfig {
            duplicates: Some(DuplicateConfig {
                enabled: true,
                rate: 1.0,
                max_duplicates: 1,
                delay_ms: None,
            }),
            seed: Some(42),
            ..Default::default()
        };

        let mut injector = FaultInjector::new(config);

        for i in 0..10 {
            let mut record = HashMap::new();
            record.insert("id".to_string(), FieldValue::Integer(i));
            injector.process_record(record);
        }

        let stats = injector.statistics();
        assert_eq!(stats.records_processed, 10);
        assert!(stats.duplicates_injected > 0);
    }
}
