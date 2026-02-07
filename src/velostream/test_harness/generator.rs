//! Test data generation from schemas
//!
//! Generates test data based on schema definitions including:
//! - Type-appropriate value generation
//! - Distribution-based sampling
//! - Derived field calculation
//! - Foreign key relationships
//! - Time simulation for sequential timestamp generation

use super::error::{TestHarnessError, TestHarnessResult};
use super::schema::{
    Distribution, FieldConstraints, FieldDefinition, FieldType, LogNormalConfig, NormalConfig,
    Schema, SchemaRegistry, SimpleFieldType, ZipfConfig,
};
use super::spec::TimeSimulationConfig;
use crate::velostream::sql::execution::types::FieldValue;
use chrono::{DateTime, Duration, Utc};
use rand::distributions::{Distribution as RandDistribution, WeightedIndex};
use rand::prelude::*;
use rand_distr::StandardNormal;
use std::collections::HashMap;

/// Conventional field name for wall-clock production time in generated records.
const WALL_CLOCK_FIELD_NAME: &str = "timestamp";

// =============================================================================
// Time Simulation State
// =============================================================================

/// Tracks state during time-simulated generation
///
/// Maintains the current simulated time and calculates sequential timestamps
/// for each generated record.
#[derive(Debug, Clone)]
pub struct TimeSimulationState {
    /// Current simulated time (progresses per record)
    current_time: DateTime<Utc>,

    /// Start time of the simulation range
    start_time: DateTime<Utc>,

    /// End time of the simulation range
    end_time: DateTime<Utc>,

    /// Simulated time increment per record
    time_per_record: Duration,

    /// Records generated so far
    records_generated: usize,

    /// Total records to generate
    total_records: usize,

    /// RNG for jitter (if enabled)
    jitter_rng: StdRng,

    /// Jitter range in milliseconds
    jitter_ms: Option<u64>,

    /// Whether sequential mode is enabled
    sequential: bool,
}

impl TimeSimulationState {
    /// Create a new time simulation state
    ///
    /// # Arguments
    /// * `config` - Time simulation configuration
    /// * `total_records` - Total number of records to generate
    /// * `seed` - Optional seed for jitter RNG
    pub fn new(
        config: &TimeSimulationConfig,
        total_records: usize,
        seed: Option<u64>,
    ) -> TestHarnessResult<Self> {
        let now = Utc::now();

        // Parse start time
        let start_time = match &config.start_time {
            Some(s) => parse_time_spec_with_reference(s, now)?,
            None => now - Duration::hours(1), // Default: 1 hour ago
        };

        // Parse end time
        let end_time = match &config.end_time {
            Some(s) => parse_time_spec_with_reference(s, now)?,
            None => now, // Default: now
        };

        // Ensure start < end
        if start_time >= end_time {
            return Err(TestHarnessError::ConfigError {
                message: format!(
                    "start_time ({}) must be before end_time ({})",
                    start_time, end_time
                ),
            });
        }

        // Calculate time per record
        let total_duration = end_time - start_time;
        let time_per_record = if total_records > 1 {
            total_duration / (total_records as i32 - 1)
        } else {
            Duration::zero()
        };

        let jitter_rng = match seed {
            Some(s) => StdRng::seed_from_u64(s.wrapping_add(12345)), // Different seed for jitter
            None => StdRng::from_entropy(),
        };

        log::debug!(
            "Time simulation initialized: {} to {} ({} records, {:?} per record)",
            start_time,
            end_time,
            total_records,
            time_per_record
        );

        Ok(Self {
            current_time: start_time,
            start_time,
            end_time,
            time_per_record,
            records_generated: 0,
            total_records,
            jitter_rng,
            jitter_ms: config.jitter_ms,
            sequential: config.sequential,
        })
    }

    /// Get the next timestamp in the simulation
    pub fn next_timestamp(&mut self) -> DateTime<Utc> {
        let base_time = if self.sequential {
            // Sequential mode: timestamps progress linearly
            let t = self.current_time;
            self.current_time += self.time_per_record;
            self.records_generated += 1;
            t
        } else {
            // Random mode: pick random time in range (legacy behavior)
            let range_ms = (self.end_time - self.start_time).num_milliseconds();
            let offset_ms = self.jitter_rng.gen_range(0..range_ms.max(1));
            self.records_generated += 1;
            self.start_time + Duration::milliseconds(offset_ms)
        };

        // Apply jitter if configured
        if let Some(jitter_ms) = self.jitter_ms {
            let jitter = self
                .jitter_rng
                .gen_range(-(jitter_ms as i64)..=(jitter_ms as i64));
            base_time + Duration::milliseconds(jitter)
        } else {
            base_time
        }
    }

    /// Get the number of records generated
    pub fn records_generated(&self) -> usize {
        self.records_generated
    }

    /// Get the current simulated time
    pub fn current_time(&self) -> DateTime<Utc> {
        self.current_time
    }

    /// Check if all records have been generated
    pub fn is_complete(&self) -> bool {
        self.records_generated >= self.total_records
    }

    /// Reset the simulation state to start over
    pub fn reset(&mut self) {
        self.current_time = self.start_time;
        self.records_generated = 0;
    }
}

/// Parse a time specification string into a DateTime
///
/// Supports:
/// - Relative: "-4h", "-30m", "-1d", "now", "+1h"
/// - Absolute: ISO 8601 format "2024-01-15T09:30:00Z"
///
/// # Arguments
/// * `spec` - Time specification string
///
/// # Returns
/// DateTime in UTC
pub fn parse_time_spec(spec: &str) -> TestHarnessResult<DateTime<Utc>> {
    parse_time_spec_with_reference(spec, Utc::now())
}

/// Parse a time specification string into a DateTime using a reference time
fn parse_time_spec_with_reference(
    spec: &str,
    reference: DateTime<Utc>,
) -> TestHarnessResult<DateTime<Utc>> {
    let spec = spec.trim();

    // Check for "now"
    if spec.eq_ignore_ascii_case("now") {
        return Ok(reference);
    }

    // Check for relative time
    if spec.starts_with('-') || spec.starts_with('+') {
        let offset_secs = parse_relative_time(spec);
        return Ok(reference + Duration::seconds(offset_secs));
    }

    // Try ISO 8601 parsing
    DateTime::parse_from_rfc3339(spec)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| TestHarnessError::ConfigError {
            message: format!("Invalid time specification '{}': {}", spec, e),
        })
}

/// Generates test data from schema definitions
pub struct SchemaDataGenerator {
    /// Random number generator
    rng: StdRng,

    /// Schema registry for foreign key lookups
    registry: SchemaRegistry,

    /// Cached reference data for foreign keys (table.field -> values)
    reference_cache: HashMap<String, Vec<HashMap<String, FieldValue>>>,

    /// Direct reference data storage (table.field -> list of values)
    /// Used for sampling foreign key values
    reference_data: HashMap<String, Vec<FieldValue>>,

    /// Time simulation state (for sequential timestamp generation)
    time_state: Option<TimeSimulationState>,

    /// Random walk state: (schema_name, field_name, group_key) -> last_value
    /// Used for GBM-style price path generation where each value depends on previous
    random_walk_state: HashMap<(String, String, String), f64>,

    /// Current record being generated (used for group_by lookups)
    current_record: Option<HashMap<String, FieldValue>>,

    /// Current schema name being generated
    current_schema: Option<String>,

    /// Simulated event times (one per record) — used as Kafka message timestamps.
    /// Populated when time simulation is active; the executor drains these after
    /// `generate()` and sets them as Kafka header timestamps, keeping event time
    /// separate from the `timestamp` payload field (wall-clock production time).
    event_times: Vec<i64>,
}

impl SchemaDataGenerator {
    /// Create new generator with optional seed
    pub fn new(seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };

        Self {
            rng,
            registry: SchemaRegistry::new(),
            reference_cache: HashMap::new(),
            reference_data: HashMap::new(),
            time_state: None,
            random_walk_state: HashMap::new(),
            current_record: None,
            current_schema: None,
            event_times: Vec::new(),
        }
    }

    /// Reset random walk state (e.g., between test runs)
    ///
    /// This clears all tracked price paths so they start fresh from initial values.
    pub fn reset_random_walk_state(&mut self) {
        self.random_walk_state.clear();
    }

    /// Get the current random walk value for a field (for debugging/testing)
    pub fn get_random_walk_value(&self, schema: &str, field: &str, group_key: &str) -> Option<f64> {
        self.random_walk_state
            .get(&(schema.to_string(), field.to_string(), group_key.to_string()))
            .copied()
    }

    /// Set schema registry for foreign key resolution
    pub fn with_registry(mut self, registry: SchemaRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Initialize time simulation for sequential timestamp generation
    ///
    /// # Arguments
    /// * `config` - Time simulation configuration
    /// * `total_records` - Total number of records that will be generated
    pub fn with_time_simulation(
        mut self,
        config: &TimeSimulationConfig,
        total_records: usize,
    ) -> TestHarnessResult<Self> {
        let seed = Some(self.rng.next_u64()); // Generate seed from current RNG
        self.time_state = Some(TimeSimulationState::new(config, total_records, seed)?);
        Ok(self)
    }

    /// Set time simulation state directly
    pub fn set_time_simulation(
        &mut self,
        config: &TimeSimulationConfig,
        total_records: usize,
    ) -> TestHarnessResult<()> {
        let seed = Some(self.rng.next_u64());
        self.time_state = Some(TimeSimulationState::new(config, total_records, seed)?);
        Ok(())
    }

    /// Clear time simulation state
    pub fn clear_time_simulation(&mut self) {
        self.time_state = None;
        self.event_times.clear();
    }

    /// Drain the accumulated simulated event times (one per generated record).
    ///
    /// The executor calls this after `generate()` to get the per-record event times
    /// that should be set as Kafka message timestamps (header), keeping event time
    /// separate from the `timestamp` payload field.
    pub fn take_event_times(&mut self) -> Vec<i64> {
        std::mem::take(&mut self.event_times)
    }

    /// Check if time simulation is active
    pub fn has_time_simulation(&self) -> bool {
        self.time_state.is_some()
    }

    /// Get the current time simulation state (if any)
    pub fn time_simulation_state(&self) -> Option<&TimeSimulationState> {
        self.time_state.as_ref()
    }

    /// Get the next simulated timestamp (for testing)
    ///
    /// Returns None if time simulation is not active.
    pub fn next_simulated_timestamp(&mut self) -> Option<chrono::NaiveDateTime> {
        self.time_state
            .as_mut()
            .map(|state| state.next_timestamp().naive_utc())
    }

    /// Load reference data for foreign key generation
    ///
    /// Reference data can be loaded from:
    /// - Pre-generated data from another schema
    /// - Explicit lists of valid values
    ///
    /// # Arguments
    /// * `table` - The referenced table name
    /// * `field` - The referenced field name
    /// * `values` - List of valid values for this reference
    ///
    /// # Example
    /// ```ignore
    /// let mut generator = SchemaDataGenerator::new(Some(42));
    ///
    /// // Load customer IDs for order.customer_id foreign key
    /// generator.load_reference_data(
    ///     "customers",
    ///     "id",
    ///     vec![
    ///         FieldValue::String("CUST001".to_string()),
    ///         FieldValue::String("CUST002".to_string()),
    ///         FieldValue::String("CUST003".to_string()),
    ///     ]
    /// );
    /// ```
    pub fn load_reference_data(&mut self, table: &str, field: &str, values: Vec<FieldValue>) {
        let key = format!("{}.{}", table, field);
        log::debug!("Loading {} reference values for {}", values.len(), key);
        self.reference_data.insert(key, values);
    }

    /// Load reference data from generated records
    ///
    /// Extracts a specific field from generated records to use as reference data.
    ///
    /// # Arguments
    /// * `table` - The table name for this reference
    /// * `field` - The field name to extract
    /// * `records` - Records from which to extract values
    pub fn load_reference_data_from_records(
        &mut self,
        table: &str,
        field: &str,
        records: &[HashMap<String, FieldValue>],
    ) {
        let values: Vec<FieldValue> = records
            .iter()
            .filter_map(|r| r.get(field).cloned())
            .collect();
        self.load_reference_data(table, field, values);
    }

    /// Check if reference data is loaded for a given table.field
    pub fn has_reference_data(&self, table: &str, field: &str) -> bool {
        let key = format!("{}.{}", table, field);
        self.reference_data.contains_key(&key)
    }

    /// Get the count of reference values for a given table.field
    pub fn reference_data_count(&self, table: &str, field: &str) -> usize {
        let key = format!("{}.{}", table, field);
        self.reference_data.get(&key).map(|v| v.len()).unwrap_or(0)
    }

    /// Generate records for a schema
    pub fn generate(
        &mut self,
        schema: &Schema,
        count: usize,
    ) -> TestHarnessResult<Vec<HashMap<String, FieldValue>>> {
        self.event_times.clear();
        if self.time_state.is_some() {
            self.event_times.reserve(count);
        }
        let mut records = Vec::with_capacity(count);

        for _ in 0..count {
            let record = self.generate_record(schema)?;
            records.push(record);
        }

        Ok(records)
    }

    /// Generate a single record
    fn generate_record(
        &mut self,
        schema: &Schema,
    ) -> TestHarnessResult<HashMap<String, FieldValue>> {
        let mut record = HashMap::new();

        // If time simulation is active, advance to the next event time.
        // This goes to the Kafka message header (_event_time), NOT into the JSON payload.
        // The `timestamp` field in the payload will be wall-clock production time.
        if let Some(ref mut time_state) = self.time_state {
            let event_time = time_state.next_timestamp();
            self.event_times.push(event_time.timestamp_millis());
        }

        // Set current schema for random walk state tracking
        self.current_schema = Some(schema.name.clone());

        // First pass: generate non-derived fields (except those with random_walk that need group_by)
        // We need to generate group_by fields first, then random_walk fields
        let mut random_walk_fields = Vec::new();

        for field in &schema.fields {
            if field.constraints.derived.is_none() {
                // Check if this field has random_walk with group_by - defer until group_by field is generated
                if let Some(Distribution::RandomWalk { random_walk }) =
                    &field.constraints.distribution
                    && random_walk.group_by.is_some()
                {
                    random_walk_fields.push(field.clone());
                    continue;
                }
                let value = self.generate_field_value(field, &record)?;
                record.insert(field.name.clone(), value);
            }
        }

        // Update current_record for group_by lookups
        self.current_record = Some(record.clone());

        // Now generate random_walk fields that depend on group_by
        for field in random_walk_fields {
            let value = self.generate_field_value(&field, &record)?;
            record.insert(field.name.clone(), value);
        }

        // Update current_record again with all fields
        self.current_record = Some(record.clone());

        // Second pass: generate derived fields
        for field in &schema.fields {
            if field.constraints.derived.is_some() {
                let value = self.generate_derived_value(field, &record)?;
                record.insert(field.name.clone(), value);
            }
        }

        // Clear current record/schema
        self.current_record = None;
        self.current_schema = None;

        Ok(record)
    }

    /// Generate value for a field
    fn generate_field_value(
        &mut self,
        field: &FieldDefinition,
        current_record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<FieldValue> {
        // Handle nullable fields
        if field.nullable && self.rng.gen_bool(0.1) {
            return Ok(FieldValue::Null);
        }

        // Check for enum constraint first
        if let Some(ref enum_constraint) = field.constraints.enum_values {
            return self.generate_enum_value(enum_constraint);
        }

        // Check for reference constraint
        if let Some(ref reference) = field.constraints.references {
            return self.generate_reference_value(reference, &field.name);
        }

        // Check for random walk distribution (needs special handling)
        if let Some(Distribution::RandomWalk { ref random_walk }) = field.constraints.distribution {
            return self.generate_random_walk_value(field, random_walk, current_record);
        }

        // Generate based on type
        match &field.field_type {
            FieldType::DecimalType { decimal } => {
                self.generate_decimal_value(&field.constraints, decimal.precision)
            }
            FieldType::Simple(simple) => match simple {
                SimpleFieldType::String => self.generate_string_value(&field.constraints),
                SimpleFieldType::Integer => {
                    self.generate_integer_value(&field.name, &field.constraints)
                }
                SimpleFieldType::Float => self.generate_float_value(&field.constraints),
                SimpleFieldType::Boolean => Ok(FieldValue::Boolean(self.rng.gen_bool(0.5))),
                SimpleFieldType::Timestamp => self.generate_timestamp_value(&field.constraints),
                SimpleFieldType::Date => self.generate_date_value(&field.constraints),
                SimpleFieldType::Uuid => Ok(FieldValue::String(uuid::Uuid::new_v4().to_string())),
            },
        }
    }

    /// Generate a random walk (GBM-style) value
    ///
    /// Uses the formula: S(t+1) = S(t) * (1 + drift + volatility * Z)
    /// where Z ~ N(0,1)
    fn generate_random_walk_value(
        &mut self,
        field: &FieldDefinition,
        config: &super::schema::RandomWalkConfig,
        current_record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<FieldValue> {
        let schema_name = self
            .current_schema
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        // Get group key if group_by is specified
        let group_key = if let Some(ref group_by_field) = config.group_by {
            current_record
                .get(group_by_field)
                .map(|v| match v {
                    FieldValue::String(s) => s.clone(),
                    other => format!("{:?}", other),
                })
                .unwrap_or_else(|| "_default".to_string())
        } else {
            "_default".to_string()
        };

        let state_key = (schema_name.clone(), field.name.clone(), group_key.clone());

        // Get previous value or initialize from range.min
        let (min_value, max_value) = if let Some(ref range) = field.constraints.range {
            (range.min, range.max)
        } else {
            (100.0, 1000.0) // Default range
        };

        let previous_value = self
            .random_walk_state
            .get(&state_key)
            .copied()
            .unwrap_or(min_value);

        // Generate GBM step: S(t+1) = S(t) * (1 + drift + volatility * Z)
        let z: f64 = rand_distr::StandardNormal.sample(&mut self.rng);
        let mut new_value = previous_value * (1.0 + config.drift + config.volatility * z);

        // Apply bounds if configured
        if !config.allow_below_min {
            new_value = new_value.max(min_value);
        }
        if !config.allow_above_max {
            new_value = new_value.min(max_value);
        }

        // Store the new value for next iteration
        self.random_walk_state.insert(state_key, new_value);

        // Return appropriate FieldValue type
        match &field.field_type {
            FieldType::DecimalType { decimal } => {
                let scale = 10_i64.pow(decimal.precision as u32);
                let scaled = (new_value * scale as f64).round() as i64;
                Ok(FieldValue::ScaledInteger(scaled, decimal.precision))
            }
            FieldType::Simple(SimpleFieldType::Float) => Ok(FieldValue::Float(new_value)),
            FieldType::Simple(SimpleFieldType::Integer) => {
                Ok(FieldValue::Integer(new_value.round() as i64))
            }
            _ => {
                // Fallback to float for unsupported types
                Ok(FieldValue::Float(new_value))
            }
        }
    }

    /// Generate enum value with optional weights
    fn generate_enum_value(
        &mut self,
        constraint: &super::schema::EnumConstraint,
    ) -> TestHarnessResult<FieldValue> {
        if constraint.values.is_empty() {
            return Err(TestHarnessError::GeneratorError {
                message: "Empty enum values".to_string(),
                schema: "unknown".to_string(),
            });
        }

        let value = if let Some(ref weights) = constraint.weights {
            let dist =
                WeightedIndex::new(weights).map_err(|e| TestHarnessError::GeneratorError {
                    message: format!("Invalid weights: {}", e),
                    schema: "unknown".to_string(),
                })?;
            &constraint.values[dist.sample(&mut self.rng)]
        } else {
            constraint.values.choose(&mut self.rng).unwrap()
        };

        Ok(FieldValue::String(value.clone()))
    }

    /// Generate string value
    fn generate_string_value(
        &mut self,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        let length = if let Some(ref len) = constraints.length {
            let min = len.min.unwrap_or(5);
            let max = len.max.unwrap_or(20);
            self.rng.gen_range(min..=max)
        } else {
            self.rng.gen_range(5..20)
        };

        // Generate random alphanumeric string
        let value: String = (0..length)
            .map(|_| self.rng.sample(rand::distributions::Alphanumeric) as char)
            .collect();

        Ok(FieldValue::String(value))
    }

    /// Generate integer value
    ///
    /// Handles both regular integer ranges and timestamp_epoch_ms constraints.
    /// When timestamp_epoch_ms is specified, generates epoch milliseconds as Integer,
    /// which survives JSON serialization round-trips (unlike FieldValue::Timestamp).
    fn generate_integer_value(
        &mut self,
        field_name: &str,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        // Check for timestamp_epoch_ms constraint first - this generates epoch millis as Integer
        if let Some(ref range) = constraints.timestamp_epoch_ms {
            return self.generate_epoch_millis_value(field_name, range);
        }

        let value = if let Some(ref range) = constraints.range {
            self.generate_with_distribution(range.min, range.max, &constraints.distribution) as i64
        } else if field_name == WALL_CLOCK_FIELD_NAME {
            // The `timestamp` field is wall-clock production time by convention
            Utc::now().timestamp_millis()
        } else {
            self.rng.gen_range(0..1000)
        };

        Ok(FieldValue::Integer(value))
    }

    /// Generate epoch milliseconds value as Integer
    ///
    /// This is the preferred format for event-time fields because:
    /// - JSON serializes as number (not string), so round-trips correctly
    /// - Matches Kafka message timestamp format
    /// - Avoids timezone ambiguity (always UTC)
    /// - Simple arithmetic: NOW() - event_time just works
    fn generate_epoch_millis_value(
        &mut self,
        field_name: &str,
        range: &super::schema::TimestampRange,
    ) -> TestHarnessResult<FieldValue> {
        // When time simulation is active, separate the two time paths:
        // - `timestamp` payload field → wall-clock "now" (production time)
        // - Other epoch_millis fields → the simulated event time (already advanced in generate_record)
        if self.time_state.is_some() {
            if field_name == WALL_CLOCK_FIELD_NAME {
                return Ok(FieldValue::Integer(Utc::now().timestamp_millis()));
            }
            if let Some(&event_time) = self.event_times.last() {
                return Ok(FieldValue::Integer(event_time));
            }
        }

        // Without time simulation, generate from the range constraint
        let timestamp = self.generate_random_timestamp_in_range(range);
        Ok(FieldValue::Integer(timestamp.timestamp_millis()))
    }

    /// Generate a random timestamp within the given range
    ///
    /// Shared helper used by both `generate_timestamp_value` and `generate_epoch_millis_value`
    /// to avoid code duplication.
    fn generate_random_timestamp_in_range(
        &mut self,
        range: &super::schema::TimestampRange,
    ) -> DateTime<Utc> {
        let now = Utc::now();

        let (start, end) = match range {
            super::schema::TimestampRange::Relative { start, end } => {
                let start_offset = parse_relative_time(start);
                let end_offset = parse_relative_time(end);
                (
                    now + Duration::seconds(start_offset),
                    now + Duration::seconds(end_offset),
                )
            }
            super::schema::TimestampRange::Absolute { start, end } => {
                let start_dt = DateTime::parse_from_rfc3339(start)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|e| {
                        log::warn!(
                            "Failed to parse absolute timestamp start '{}': {}. Using default (now - 1h)",
                            start, e
                        );
                        now - Duration::hours(1)
                    });
                let end_dt = DateTime::parse_from_rfc3339(end)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|e| {
                        log::warn!(
                            "Failed to parse absolute timestamp end '{}': {}. Using default (now)",
                            end,
                            e
                        );
                        now
                    });
                (start_dt, end_dt)
            }
        };

        let range_ms = (end - start).num_milliseconds();
        if range_ms <= 0 {
            log::warn!(
                "Timestamp range is zero or negative (start >= end). Using start timestamp."
            );
            return start;
        }

        let offset_ms = self.rng.gen_range(0..range_ms);
        start + Duration::milliseconds(offset_ms)
    }

    /// Generate float value
    fn generate_float_value(
        &mut self,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        let value = if let Some(ref range) = constraints.range {
            self.generate_with_distribution(range.min, range.max, &constraints.distribution)
        } else {
            self.rng.gen_range(0.0..1000.0)
        };

        Ok(FieldValue::Float(value))
    }

    /// Generate decimal value (as ScaledInteger)
    fn generate_decimal_value(
        &mut self,
        constraints: &FieldConstraints,
        precision: u8,
    ) -> TestHarnessResult<FieldValue> {
        let float_value = if let Some(ref range) = constraints.range {
            self.generate_with_distribution(range.min, range.max, &constraints.distribution)
        } else {
            self.rng.gen_range(0.0..1000.0)
        };

        // Convert to scaled integer
        let scale = 10_i64.pow(precision as u32);
        let scaled = (float_value * scale as f64).round() as i64;

        Ok(FieldValue::ScaledInteger(scaled, precision))
    }

    /// Generate timestamp value
    ///
    /// If time simulation is active, uses the time simulation state for
    /// sequential/controlled timestamp generation. Otherwise falls back
    /// to random timestamps within the configured range.
    ///
    /// Note: For event-time fields that need to survive JSON serialization,
    /// use `type: integer` with `timestamp_epoch_ms` constraint instead.
    fn generate_timestamp_value(
        &mut self,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        // If time simulation is active, use it for timestamp generation
        if let Some(ref mut time_state) = self.time_state {
            let timestamp = time_state.next_timestamp();
            return Ok(FieldValue::Timestamp(timestamp.naive_utc()));
        }

        // Use timestamp_range if provided, otherwise default range
        let default_range = super::schema::TimestampRange::Relative {
            start: "-1h".to_string(),
            end: "now".to_string(),
        };
        let range = constraints
            .timestamp_range
            .as_ref()
            .unwrap_or(&default_range);
        let timestamp = self.generate_random_timestamp_in_range(range);

        Ok(FieldValue::Timestamp(timestamp.naive_utc()))
    }

    /// Generate date value
    fn generate_date_value(
        &mut self,
        _constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        let now = Utc::now();
        let days_offset = self.rng.gen_range(-30..1);
        let date = now + Duration::days(days_offset);
        Ok(FieldValue::String(date.format("%Y-%m-%d").to_string()))
    }

    /// Generate reference value (foreign key)
    ///
    /// Samples from loaded reference data if available, otherwise generates
    /// a placeholder value.
    fn generate_reference_value(
        &mut self,
        reference: &super::schema::ReferenceConstraint,
        field_name: &str,
    ) -> TestHarnessResult<FieldValue> {
        // Build the reference key: schema.field
        let key = format!("{}.{}", reference.schema, reference.field);

        // Check if we have reference data loaded
        if let Some(values) = self.reference_data.get(&key).filter(|v| !v.is_empty()) {
            // Sample a random value from the reference data
            let idx = self.rng.gen_range(0..values.len());
            let sampled = values[idx].clone();
            log::trace!(
                "Sampled reference value for {}.{} from {}: {:?}",
                field_name,
                key,
                values.len(),
                sampled
            );
            return Ok(sampled);
        }

        // Check reference cache (for records loaded via generate_for_reference)
        let cache_key = format!("{}.{}", reference.schema, reference.field);
        if let Some(records) = self
            .reference_cache
            .get(&cache_key)
            .filter(|v| !v.is_empty())
        {
            let idx = self.rng.gen_range(0..records.len());
            if let Some(value) = records[idx].get(&reference.field) {
                return Ok(value.clone());
            }
        }

        // If no reference data is loaded, generate a placeholder
        // This allows tests to run even without full referential setup
        log::warn!(
            "No reference data loaded for {} -> {}.{}, generating placeholder",
            field_name,
            reference.schema,
            reference.field
        );
        Ok(FieldValue::String(format!(
            "REF_{}_{}",
            reference.schema.to_uppercase(),
            self.rng.gen_range(1..1000)
        )))
    }

    /// Generate derived value from expression
    ///
    /// Supports:
    /// - Field references: `price`, `quantity`
    /// - Binary operators: `+`, `-`, `*`, `/`
    /// - Comparisons: `>`, `<`, `>=`, `<=`, `==`, `!=`
    /// - Functions: `random()`, `abs(x)`, `min(a, b)`, `max(a, b)`
    /// - Numeric literals: `100`, `3.14`
    /// - Concatenation: `concat(a, b)` for strings
    fn generate_derived_value(
        &mut self,
        field: &FieldDefinition,
        record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<FieldValue> {
        let derived = field.constraints.derived.as_ref().unwrap();
        let expression = derived.expression.trim();

        self.evaluate_expression(expression, record)
    }

    /// Evaluate a derived expression
    fn evaluate_expression(
        &mut self,
        expression: &str,
        record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<FieldValue> {
        let expression = expression.trim();

        // Handle function calls first
        if let Some(result) = self.try_evaluate_function(expression, record)? {
            return Ok(result);
        }

        // Try to parse as a binary operation
        if let Some(result) = self.try_evaluate_binary_op(expression, record)? {
            return Ok(result);
        }

        // Try to resolve as a field reference
        if let Some(value) = record.get(expression) {
            return Ok(value.clone());
        }

        // Try to parse as a numeric literal
        if let Ok(i) = expression.parse::<i64>() {
            return Ok(FieldValue::Integer(i));
        }
        if let Ok(f) = expression.parse::<f64>() {
            return Ok(FieldValue::Float(f));
        }

        // Try to parse as a string literal (quoted)
        if (expression.starts_with('"') && expression.ends_with('"'))
            || (expression.starts_with('\'') && expression.ends_with('\''))
        {
            let s = &expression[1..expression.len() - 1];
            return Ok(FieldValue::String(s.to_string()));
        }

        // Unknown expression
        Ok(FieldValue::Null)
    }

    /// Try to evaluate function calls like random(), abs(), min(), max(), concat()
    fn try_evaluate_function(
        &mut self,
        expression: &str,
        record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<Option<FieldValue>> {
        // random() - returns random float 0.0-1.0
        if expression == "random()" {
            return Ok(Some(FieldValue::Float(self.rng.gen_range(0.0..1.0))));
        }

        // random(min, max) - returns random float in range
        if expression.starts_with("random(") && expression.ends_with(')') {
            let args = &expression[7..expression.len() - 1];
            let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2
                && let (Ok(min), Ok(max)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>())
            {
                return Ok(Some(FieldValue::Float(self.rng.gen_range(min..=max))));
            }
        }

        // abs(x) - absolute value
        if expression.starts_with("abs(") && expression.ends_with(')') {
            let inner = &expression[4..expression.len() - 1];
            let value = self.evaluate_expression(inner, record)?;
            return Ok(Some(apply_abs(&value)?));
        }

        // min(a, b) - minimum of two values
        if expression.starts_with("min(") && expression.ends_with(')') {
            let args = &expression[4..expression.len() - 1];
            if let Some((a, b)) = split_function_args(args) {
                let left = self.evaluate_expression(a, record)?;
                let right = self.evaluate_expression(b, record)?;
                return Ok(Some(apply_min(&left, &right)?));
            }
        }

        // max(a, b) - maximum of two values
        if expression.starts_with("max(") && expression.ends_with(')') {
            let args = &expression[4..expression.len() - 1];
            if let Some((a, b)) = split_function_args(args) {
                let left = self.evaluate_expression(a, record)?;
                let right = self.evaluate_expression(b, record)?;
                return Ok(Some(apply_max(&left, &right)?));
            }
        }

        // concat(a, b) - string concatenation
        if expression.starts_with("concat(") && expression.ends_with(')') {
            let args = &expression[7..expression.len() - 1];
            if let Some((a, b)) = split_function_args(args) {
                let left = self.evaluate_expression(a, record)?;
                let right = self.evaluate_expression(b, record)?;
                return Ok(Some(apply_concat(&left, &right)));
            }
        }

        // round(x) or round(x, precision)
        if expression.starts_with("round(") && expression.ends_with(')') {
            let args = &expression[6..expression.len() - 1];
            let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
            if parts.len() == 1 {
                let value = self.evaluate_expression(parts[0], record)?;
                return Ok(Some(apply_round(&value, 0)?));
            } else if parts.len() == 2 {
                let value = self.evaluate_expression(parts[0], record)?;
                if let Ok(precision) = parts[1].parse::<i32>() {
                    return Ok(Some(apply_round(&value, precision)?));
                }
            }
        }

        // floor(x) - floor value
        if expression.starts_with("floor(") && expression.ends_with(')') {
            let inner = &expression[6..expression.len() - 1];
            let value = self.evaluate_expression(inner, record)?;
            return Ok(Some(apply_floor(&value)?));
        }

        // ceil(x) - ceiling value
        if expression.starts_with("ceil(") && expression.ends_with(')') {
            let inner = &expression[5..expression.len() - 1];
            let value = self.evaluate_expression(inner, record)?;
            return Ok(Some(apply_ceil(&value)?));
        }

        Ok(None)
    }

    /// Try to evaluate binary operations
    fn try_evaluate_binary_op(
        &mut self,
        expression: &str,
        record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<Option<FieldValue>> {
        // Order matters - check longer operators first
        let operators = [
            (">=", BinaryOp::Gte),
            ("<=", BinaryOp::Lte),
            ("==", BinaryOp::Eq),
            ("!=", BinaryOp::Neq),
            (">", BinaryOp::Gt),
            ("<", BinaryOp::Lt),
            ("+", BinaryOp::Add),
            ("-", BinaryOp::Sub),
            ("*", BinaryOp::Mul),
            ("/", BinaryOp::Div),
            ("%", BinaryOp::Mod),
        ];

        for (op_str, op) in operators {
            if let Some(pos) = find_operator_position(expression, op_str) {
                let left_expr = expression[..pos].trim();
                let right_expr = expression[pos + op_str.len()..].trim();

                let left = self.evaluate_expression(left_expr, record)?;
                let right = self.evaluate_expression(right_expr, record)?;

                return Ok(Some(apply_binary_op(&left, &right, op)?));
            }
        }

        Ok(None)
    }

    /// Generate value with distribution
    fn generate_with_distribution(
        &mut self,
        min: f64,
        max: f64,
        dist: &Option<Distribution>,
    ) -> f64 {
        match dist {
            Some(Distribution::Normal { normal }) => {
                // Box-Muller transform for normal distribution
                let u1: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let u2: f64 = self.rng.gen_range(0.0_f64..1.0_f64);
                let z0 =
                    ((-2.0_f64 * u1.ln()).sqrt()) * ((2.0_f64 * std::f64::consts::PI * u2).cos());
                let value = normal.mean + normal.std_dev * z0;
                value.clamp(min, max)
            }
            Some(Distribution::LogNormal { log_normal }) => {
                // Log-normal: exp(normal distribution)
                let u1: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let u2: f64 = self.rng.gen_range(0.0_f64..1.0_f64);
                let z0 =
                    ((-2.0_f64 * u1.ln()).sqrt()) * ((2.0_f64 * std::f64::consts::PI * u2).cos());
                let normal_value = log_normal.mean + log_normal.std_dev * z0;
                let value = normal_value.exp();
                value.clamp(min, max)
            }
            Some(Distribution::Zipf { zipf }) => {
                // Simplified Zipf approximation using inverse transform
                let u: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let range = max - min;
                // Power law distribution approximation
                let value = min + range * (1.0 - u.powf(1.0 / zipf.exponent));
                value.clamp(min, max)
            }
            // RandomWalk is handled separately in generate_random_walk_value
            // This is a fallback that shouldn't normally be reached
            Some(Distribution::RandomWalk { .. }) => self.rng.gen_range(min..=max),
            Some(Distribution::Uniform(_)) | None => self.rng.gen_range(min..=max),
        }
    }
}

/// Binary operation types
#[derive(Debug, Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    Neq,
}

/// Find byte position of operator in expression (respecting parentheses)
fn find_operator_position(expression: &str, op: &str) -> Option<usize> {
    let mut depth = 0;

    for (byte_idx, c) in expression.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if depth == 0 => {
                // Check if op matches at this byte position
                if expression[byte_idx..].starts_with(op) {
                    return Some(byte_idx);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split function arguments (handles nested function calls)
fn split_function_args(args: &str) -> Option<(&str, &str)> {
    let mut depth = 0;
    for (byte_idx, c) in args.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                // byte_idx is the byte position of the comma
                // c.len_utf8() gives the byte length of the comma character (1 for ASCII)
                return Some((
                    args[..byte_idx].trim(),
                    args[byte_idx + c.len_utf8()..].trim(),
                ));
            }
            _ => {}
        }
    }
    None
}

/// Apply binary operation to two field values
fn apply_binary_op(
    left: &FieldValue,
    right: &FieldValue,
    op: BinaryOp,
) -> TestHarnessResult<FieldValue> {
    // Convert both to f64 for numeric operations
    let (l_val, r_val) = match (field_to_f64(left), field_to_f64(right)) {
        (Some(l), Some(r)) => (l, r),
        _ => {
            // For string comparisons
            if matches!(op, BinaryOp::Eq | BinaryOp::Neq) {
                let l_str = field_to_string(left);
                let r_str = field_to_string(right);
                return Ok(FieldValue::Boolean(match op {
                    BinaryOp::Eq => l_str == r_str,
                    BinaryOp::Neq => l_str != r_str,
                    _ => false,
                }));
            }
            return Err(TestHarnessError::GeneratorError {
                message: format!("Cannot apply {:?} to {:?} and {:?}", op, left, right),
                schema: "derived".to_string(),
            });
        }
    };

    Ok(match op {
        BinaryOp::Add => FieldValue::Float(l_val + r_val),
        BinaryOp::Sub => FieldValue::Float(l_val - r_val),
        BinaryOp::Mul => multiply_field_values(left, right)?,
        BinaryOp::Div => {
            if r_val == 0.0 {
                FieldValue::Null
            } else {
                FieldValue::Float(l_val / r_val)
            }
        }
        BinaryOp::Mod => {
            if r_val == 0.0 {
                FieldValue::Null
            } else {
                FieldValue::Float(l_val % r_val)
            }
        }
        BinaryOp::Gt => FieldValue::Boolean(l_val > r_val),
        BinaryOp::Lt => FieldValue::Boolean(l_val < r_val),
        BinaryOp::Gte => FieldValue::Boolean(l_val >= r_val),
        BinaryOp::Lte => FieldValue::Boolean(l_val <= r_val),
        BinaryOp::Eq => FieldValue::Boolean((l_val - r_val).abs() < 1e-10),
        BinaryOp::Neq => FieldValue::Boolean((l_val - r_val).abs() >= 1e-10),
    })
}

/// Apply abs() function
fn apply_abs(value: &FieldValue) -> TestHarnessResult<FieldValue> {
    match value {
        FieldValue::Integer(i) => Ok(FieldValue::Integer(i.abs())),
        FieldValue::Float(f) => Ok(FieldValue::Float(f.abs())),
        FieldValue::ScaledInteger(v, s) => Ok(FieldValue::ScaledInteger(v.abs(), *s)),
        _ => Err(TestHarnessError::GeneratorError {
            message: format!("Cannot apply abs to {:?}", value),
            schema: "derived".to_string(),
        }),
    }
}

/// Apply min() function
fn apply_min(left: &FieldValue, right: &FieldValue) -> TestHarnessResult<FieldValue> {
    let l = field_to_f64(left).unwrap_or(f64::MAX);
    let r = field_to_f64(right).unwrap_or(f64::MAX);
    if l <= r {
        Ok(left.clone())
    } else {
        Ok(right.clone())
    }
}

/// Apply max() function
fn apply_max(left: &FieldValue, right: &FieldValue) -> TestHarnessResult<FieldValue> {
    let l = field_to_f64(left).unwrap_or(f64::MIN);
    let r = field_to_f64(right).unwrap_or(f64::MIN);
    if l >= r {
        Ok(left.clone())
    } else {
        Ok(right.clone())
    }
}

/// Apply concat() function
fn apply_concat(left: &FieldValue, right: &FieldValue) -> FieldValue {
    let l = field_to_string(left);
    let r = field_to_string(right);
    FieldValue::String(format!("{}{}", l, r))
}

/// Apply round() function
fn apply_round(value: &FieldValue, precision: i32) -> TestHarnessResult<FieldValue> {
    match value {
        FieldValue::Float(f) => {
            let factor = 10_f64.powi(precision);
            Ok(FieldValue::Float((f * factor).round() / factor))
        }
        FieldValue::Integer(i) => Ok(FieldValue::Integer(*i)),
        FieldValue::ScaledInteger(v, s) => {
            if precision >= *s as i32 {
                Ok(value.clone())
            } else {
                let scale_diff = *s as i32 - precision;
                let factor = 10_i64.pow(scale_diff as u32);
                let rounded = (v + factor / 2) / factor * factor;
                Ok(FieldValue::ScaledInteger(rounded, *s))
            }
        }
        _ => Err(TestHarnessError::GeneratorError {
            message: format!("Cannot apply round to {:?}", value),
            schema: "derived".to_string(),
        }),
    }
}

/// Apply floor() function
fn apply_floor(value: &FieldValue) -> TestHarnessResult<FieldValue> {
    match value {
        FieldValue::Float(f) => Ok(FieldValue::Integer(f.floor() as i64)),
        FieldValue::Integer(i) => Ok(FieldValue::Integer(*i)),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            Ok(FieldValue::Integer(v / scale))
        }
        _ => Err(TestHarnessError::GeneratorError {
            message: format!("Cannot apply floor to {:?}", value),
            schema: "derived".to_string(),
        }),
    }
}

/// Apply ceil() function
fn apply_ceil(value: &FieldValue) -> TestHarnessResult<FieldValue> {
    match value {
        FieldValue::Float(f) => Ok(FieldValue::Integer(f.ceil() as i64)),
        FieldValue::Integer(i) => Ok(FieldValue::Integer(*i)),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            let remainder = v % scale;
            if remainder > 0 {
                Ok(FieldValue::Integer(v / scale + 1))
            } else {
                Ok(FieldValue::Integer(v / scale))
            }
        }
        _ => Err(TestHarnessError::GeneratorError {
            message: format!("Cannot apply ceil to {:?}", value),
            schema: "derived".to_string(),
        }),
    }
}

/// Convert field value to f64
fn field_to_f64(value: &FieldValue) -> Option<f64> {
    match value {
        FieldValue::Integer(i) => Some(*i as f64),
        FieldValue::Float(f) => Some(*f),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            Some(*v as f64 / scale as f64)
        }
        _ => None,
    }
}

/// Convert field value to string
fn field_to_string(value: &FieldValue) -> String {
    match value {
        FieldValue::String(s) => s.clone(),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Null => "null".to_string(),
        FieldValue::Timestamp(ts) => ts.to_string(),
        FieldValue::ScaledInteger(v, s) => {
            let scale = 10_i64.pow(*s as u32);
            format!("{:.prec$}", *v as f64 / scale as f64, prec = *s as usize)
        }
        _ => format!("{:?}", value),
    }
}

/// Parse relative time string (e.g., "-1h", "now", "-30m")
///
/// Supported formats:
/// - "now" - current time (0 offset)
/// - "-1h", "1h" - hours
/// - "-30m", "30m" - minutes
/// - "-60s", "60s" - seconds
/// - "-1d", "1d" - days
///
/// Returns seconds offset from now. Logs a warning if parsing fails.
fn parse_relative_time(s: &str) -> i64 {
    let original = s;
    let s = s.trim().to_lowercase();

    if s == "now" {
        return 0;
    }

    let negative = s.starts_with('-');
    let s = s.trim_start_matches('-');

    let (num, unit) = if s.ends_with('h') {
        let num_str = s.trim_end_matches('h');
        match num_str.parse::<i64>() {
            Ok(n) => (n, 3600),
            Err(e) => {
                log::warn!(
                    "Failed to parse relative time '{}': invalid number '{}': {}. Using 0.",
                    original,
                    num_str,
                    e
                );
                (0, 3600)
            }
        }
    } else if s.ends_with('m') {
        let num_str = s.trim_end_matches('m');
        match num_str.parse::<i64>() {
            Ok(n) => (n, 60),
            Err(e) => {
                log::warn!(
                    "Failed to parse relative time '{}': invalid number '{}': {}. Using 0.",
                    original,
                    num_str,
                    e
                );
                (0, 60)
            }
        }
    } else if s.ends_with('s') {
        let num_str = s.trim_end_matches('s');
        match num_str.parse::<i64>() {
            Ok(n) => (n, 1),
            Err(e) => {
                log::warn!(
                    "Failed to parse relative time '{}': invalid number '{}': {}. Using 0.",
                    original,
                    num_str,
                    e
                );
                (0, 1)
            }
        }
    } else if s.ends_with('d') {
        let num_str = s.trim_end_matches('d');
        match num_str.parse::<i64>() {
            Ok(n) => (n, 86400),
            Err(e) => {
                log::warn!(
                    "Failed to parse relative time '{}': invalid number '{}': {}. Using 0.",
                    original,
                    num_str,
                    e
                );
                (0, 86400)
            }
        }
    } else {
        match s.parse::<i64>() {
            Ok(n) => (n, 1),
            Err(e) => {
                log::warn!(
                    "Failed to parse relative time '{}': unrecognized format: {}. Using 0.",
                    original,
                    e
                );
                (0, 1)
            }
        }
    };

    let seconds = num * unit;
    if negative { -seconds } else { seconds }
}

/// Multiply two field values
fn multiply_field_values(left: &FieldValue, right: &FieldValue) -> TestHarnessResult<FieldValue> {
    match (left, right) {
        (FieldValue::Integer(l), FieldValue::Integer(r)) => Ok(FieldValue::Integer(l * r)),
        (FieldValue::Float(l), FieldValue::Float(r)) => Ok(FieldValue::Float(l * r)),
        (FieldValue::Integer(l), FieldValue::Float(r)) => Ok(FieldValue::Float(*l as f64 * r)),
        (FieldValue::Float(l), FieldValue::Integer(r)) => Ok(FieldValue::Float(l * *r as f64)),
        (FieldValue::ScaledInteger(l, ls), FieldValue::ScaledInteger(r, rs)) => {
            // Multiply scaled integers, result scale is sum of scales
            let result = l * r;
            let result_scale = ls + rs;
            Ok(FieldValue::ScaledInteger(result, result_scale))
        }
        // ScaledInteger × Float -> ScaledInteger (preserves precision)
        (FieldValue::ScaledInteger(l, ls), FieldValue::Float(r)) => {
            // Convert scaled integer to float, multiply, convert back
            let scale_factor = 10_i64.pow(*ls as u32) as f64;
            let left_float = *l as f64 / scale_factor;
            let result = left_float * r;
            // Convert back to scaled integer with same scale
            let result_scaled = (result * scale_factor).round() as i64;
            Ok(FieldValue::ScaledInteger(result_scaled, *ls))
        }
        // Float × ScaledInteger -> ScaledInteger (preserves precision)
        (FieldValue::Float(l), FieldValue::ScaledInteger(r, rs)) => {
            let scale_factor = 10_i64.pow(*rs as u32) as f64;
            let right_float = *r as f64 / scale_factor;
            let result = l * right_float;
            let result_scaled = (result * scale_factor).round() as i64;
            Ok(FieldValue::ScaledInteger(result_scaled, *rs))
        }
        // ScaledInteger × Integer -> ScaledInteger
        (FieldValue::ScaledInteger(l, ls), FieldValue::Integer(r)) => {
            Ok(FieldValue::ScaledInteger(l * r, *ls))
        }
        // Integer × ScaledInteger -> ScaledInteger
        (FieldValue::Integer(l), FieldValue::ScaledInteger(r, rs)) => {
            Ok(FieldValue::ScaledInteger(l * r, *rs))
        }
        _ => Err(TestHarnessError::GeneratorError {
            message: format!("Cannot multiply {:?} and {:?}", left, right),
            schema: "derived".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema::{
        DerivedConstraint, Distribution, EnumConstraint, FieldConstraints, FieldDefinition,
        FieldType, LengthConstraint, RangeConstraint, Schema, SchemaRegistry, TimestampRange,
    };
    use super::*;

    // =====================================================================
    // Helper Functions
    // =====================================================================

    fn create_simple_schema(name: &str, fields: Vec<FieldDefinition>) -> Schema {
        Schema {
            name: name.to_string(),
            description: None,
            fields,
            record_count: 1000,
            key_field: None,
            source_path: None,
        }
    }

    fn string_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::string(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        }
    }

    fn integer_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        }
    }

    fn float_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        }
    }

    // =====================================================================
    // SchemaDataGenerator Basic Tests
    // =====================================================================

    #[test]
    fn test_generator_with_seed_deterministic() {
        let schema = create_simple_schema("test", vec![integer_field("id")]);

        // Same seed should produce same results
        let mut gen1 = SchemaDataGenerator::new(Some(42));
        let mut gen2 = SchemaDataGenerator::new(Some(42));

        let records1 = gen1.generate(&schema, 5).unwrap();
        let records2 = gen2.generate(&schema, 5).unwrap();

        assert_eq!(records1.len(), records2.len());
        for (r1, r2) in records1.iter().zip(records2.iter()) {
            assert_eq!(r1.get("id"), r2.get("id"));
        }
    }

    #[test]
    fn test_generator_without_seed_varies() {
        let schema = create_simple_schema("test", vec![integer_field("id")]);

        // Without seed, different generators should (usually) produce different results
        let mut gen1 = SchemaDataGenerator::new(None);
        let mut gen2 = SchemaDataGenerator::new(None);

        let records1 = gen1.generate(&schema, 100).unwrap();
        let records2 = gen2.generate(&schema, 100).unwrap();

        // Very unlikely to be identical without seed
        let mut different = false;
        for (r1, r2) in records1.iter().zip(records2.iter()) {
            if r1.get("id") != r2.get("id") {
                different = true;
                break;
            }
        }
        assert!(
            different,
            "Unseeded generators should produce different results"
        );
    }

    #[test]
    fn test_generate_correct_count() {
        let schema = create_simple_schema("test", vec![string_field("name")]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 50).unwrap();
        assert_eq!(records.len(), 50);
    }

    #[test]
    fn test_generate_zero_records() {
        let schema = create_simple_schema("test", vec![string_field("name")]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 0).unwrap();
        assert!(records.is_empty());
    }

    // =====================================================================
    // String Field Tests
    // =====================================================================

    #[test]
    fn test_generate_string_default() {
        let schema = create_simple_schema("test", vec![string_field("text")]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("text") {
                assert!(!s.is_empty());
                assert!(s.len() >= 5 && s.len() < 20);
                assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
            } else {
                panic!("Expected string field");
            }
        }
    }

    #[test]
    fn test_generate_string_with_length_constraint() {
        let mut field = string_field("text");
        field.constraints.length = Some(LengthConstraint {
            min: Some(10),
            max: Some(15),
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("text") {
                assert!(
                    s.len() >= 10 && s.len() <= 15,
                    "Length {} not in range 10-15",
                    s.len()
                );
            } else {
                panic!("Expected string field");
            }
        }
    }

    // =====================================================================
    // Integer Field Tests
    // =====================================================================

    #[test]
    fn test_generate_integer_default() {
        let schema = create_simple_schema("test", vec![integer_field("count")]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            if let Some(FieldValue::Integer(n)) = record.get("count") {
                assert!(*n >= 0 && *n < 1000);
            } else {
                panic!("Expected integer field");
            }
        }
    }

    #[test]
    fn test_generate_integer_with_range() {
        let mut field = integer_field("value");
        field.constraints.range = Some(RangeConstraint {
            min: 100.0,
            max: 200.0,
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 50).unwrap();

        for record in &records {
            if let Some(FieldValue::Integer(n)) = record.get("value") {
                assert!(*n >= 100 && *n <= 200, "Value {} not in range 100-200", n);
            } else {
                panic!("Expected integer field");
            }
        }
    }

    // =====================================================================
    // Float Field Tests
    // =====================================================================

    #[test]
    fn test_generate_float_default() {
        let schema = create_simple_schema("test", vec![float_field("price")]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            if let Some(FieldValue::Float(f)) = record.get("price") {
                assert!(*f >= 0.0 && *f < 1000.0);
            } else {
                panic!("Expected float field");
            }
        }
    }

    #[test]
    fn test_generate_float_with_range() {
        let mut field = float_field("rate");
        field.constraints.range = Some(RangeConstraint {
            min: 0.01,
            max: 0.10,
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 50).unwrap();

        for record in &records {
            if let Some(FieldValue::Float(f)) = record.get("rate") {
                assert!(
                    *f >= 0.01 && *f <= 0.10,
                    "Value {} not in range 0.01-0.10",
                    f
                );
            } else {
                panic!("Expected float field");
            }
        }
    }

    // =====================================================================
    // Decimal Field Tests
    // =====================================================================

    #[test]
    fn test_generate_decimal_field() {
        let field = FieldDefinition {
            name: "amount".to_string(),
            field_type: FieldType::decimal(2),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 10.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            if let Some(FieldValue::ScaledInteger(scaled, precision)) = record.get("amount") {
                assert_eq!(*precision, 2);
                let float_value = *scaled as f64 / 100.0;
                assert!(
                    float_value >= 10.0 && float_value <= 100.0,
                    "Decimal value {} not in range 10-100",
                    float_value
                );
            } else {
                panic!(
                    "Expected ScaledInteger field, got {:?}",
                    record.get("amount")
                );
            }
        }
    }

    // =====================================================================
    // Boolean Field Tests
    // =====================================================================

    #[test]
    fn test_generate_boolean_field() {
        let field = FieldDefinition {
            name: "active".to_string(),
            field_type: FieldType::boolean(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 100).unwrap();

        let mut has_true = false;
        let mut has_false = false;

        for record in &records {
            match record.get("active") {
                Some(FieldValue::Boolean(true)) => has_true = true,
                Some(FieldValue::Boolean(false)) => has_false = true,
                other => panic!("Expected boolean field, got {:?}", other),
            }
        }

        assert!(
            has_true && has_false,
            "Should have both true and false values"
        );
    }

    // =====================================================================
    // Timestamp Field Tests
    // =====================================================================

    #[test]
    fn test_generate_timestamp_default() {
        let field = FieldDefinition {
            name: "created_at".to_string(),
            field_type: FieldType::timestamp(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        let now = Utc::now().naive_utc();
        let one_hour_ago = now - Duration::hours(1);

        for record in &records {
            if let Some(FieldValue::Timestamp(ts)) = record.get("created_at") {
                // Should be within last hour by default
                assert!(
                    *ts >= one_hour_ago && *ts <= now + Duration::seconds(1),
                    "Timestamp {:?} not in expected range",
                    ts
                );
            } else {
                panic!("Expected timestamp field");
            }
        }
    }

    #[test]
    fn test_generate_timestamp_relative_range() {
        let mut field = FieldDefinition {
            name: "event_time".to_string(),
            field_type: FieldType::timestamp(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };
        field.constraints.timestamp_range = Some(TimestampRange::Relative {
            start: "-30m".to_string(),
            end: "now".to_string(),
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        let now = Utc::now().naive_utc();
        let thirty_min_ago = now - Duration::minutes(30);

        for record in &records {
            if let Some(FieldValue::Timestamp(ts)) = record.get("event_time") {
                assert!(
                    *ts >= thirty_min_ago - Duration::seconds(1)
                        && *ts <= now + Duration::seconds(1),
                    "Timestamp {:?} not in expected 30min range",
                    ts
                );
            } else {
                panic!("Expected timestamp field");
            }
        }
    }

    // =====================================================================
    // UUID Field Tests
    // =====================================================================

    #[test]
    fn test_generate_uuid_field() {
        let field = FieldDefinition {
            name: "id".to_string(),
            field_type: FieldType::uuid(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        let mut uuids = std::collections::HashSet::new();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("id") {
                // UUID format: 8-4-4-4-12 hex chars
                assert!(uuid::Uuid::parse_str(s).is_ok(), "Invalid UUID: {}", s);
                uuids.insert(s.clone());
            } else {
                panic!("Expected string (UUID) field");
            }
        }

        // All UUIDs should be unique
        assert_eq!(uuids.len(), 10, "UUIDs should be unique");
    }

    // =====================================================================
    // Enum Field Tests
    // =====================================================================

    #[test]
    fn test_generate_enum_values() {
        let mut field = string_field("status");
        field.constraints.enum_values = Some(EnumConstraint {
            values: vec![
                "ACTIVE".to_string(),
                "INACTIVE".to_string(),
                "PENDING".to_string(),
            ],
            weights: None,
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 100).unwrap();

        let valid_values: std::collections::HashSet<_> = ["ACTIVE", "INACTIVE", "PENDING"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut seen_values = std::collections::HashSet::new();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("status") {
                assert!(valid_values.contains(s), "Invalid enum value: {}", s);
                seen_values.insert(s.clone());
            } else {
                panic!("Expected string field");
            }
        }

        // Should have seen all values with 100 records
        assert_eq!(seen_values.len(), 3, "Should see all enum values");
    }

    #[test]
    fn test_generate_enum_with_weights() {
        let mut field = string_field("priority");
        field.constraints.enum_values = Some(EnumConstraint {
            values: vec!["HIGH".to_string(), "MEDIUM".to_string(), "LOW".to_string()],
            weights: Some(vec![0.1, 0.3, 0.6]), // LOW should be most common
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 1000).unwrap();

        let mut counts = HashMap::new();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("priority") {
                *counts.entry(s.clone()).or_insert(0) += 1;
            }
        }

        // LOW should be most common (weighted 0.6)
        let low_count = *counts.get("LOW").unwrap_or(&0);
        let high_count = *counts.get("HIGH").unwrap_or(&0);

        assert!(
            low_count > high_count * 2,
            "LOW ({}) should be significantly more common than HIGH ({})",
            low_count,
            high_count
        );
    }

    #[test]
    fn test_generate_enum_empty_error() {
        let mut field = string_field("empty");
        field.constraints.enum_values = Some(EnumConstraint {
            values: vec![],
            weights: None,
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let result = generator.generate(&schema, 1);
        assert!(result.is_err());

        if let Err(TestHarnessError::GeneratorError { message, .. }) = result {
            assert!(message.contains("Empty enum"));
        } else {
            panic!("Expected GeneratorError");
        }
    }

    // =====================================================================
    // Nullable Field Tests
    // =====================================================================

    #[test]
    fn test_generate_nullable_field() {
        let mut field = string_field("optional_name");
        field.nullable = true;

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 200).unwrap();

        let mut null_count = 0;
        let mut non_null_count = 0;

        for record in &records {
            match record.get("optional_name") {
                Some(FieldValue::Null) => null_count += 1,
                Some(FieldValue::String(_)) => non_null_count += 1,
                other => panic!("Unexpected value: {:?}", other),
            }
        }

        // With 10% null probability, we should see some nulls in 200 records
        assert!(null_count > 0, "Should have some null values");
        assert!(non_null_count > 0, "Should have some non-null values");
    }

    // =====================================================================
    // Distribution Tests
    // =====================================================================

    #[test]
    fn test_generate_normal_distribution() {
        let mut field = float_field("value");
        field.constraints.range = Some(RangeConstraint {
            min: 0.0,
            max: 100.0,
        });
        field.constraints.distribution = Some(Distribution::Normal {
            normal: NormalConfig {
                mean: 50.0,
                std_dev: 10.0,
            },
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 1000).unwrap();

        let values: Vec<f64> = records
            .iter()
            .filter_map(|r| {
                if let Some(FieldValue::Float(f)) = r.get("value") {
                    Some(*f)
                } else {
                    None
                }
            })
            .collect();

        let sum: f64 = values.iter().sum();
        let mean = sum / values.len() as f64;

        // Mean should be close to 50
        assert!(
            (mean - 50.0).abs() < 5.0,
            "Mean {} should be close to 50",
            mean
        );
    }

    #[test]
    fn test_generate_zipf_distribution() {
        let mut field = float_field("rank_score");
        field.constraints.range = Some(RangeConstraint {
            min: 1.0,
            max: 100.0,
        });
        field.constraints.distribution = Some(Distribution::Zipf {
            zipf: ZipfConfig { exponent: 2.0 },
        });

        let schema = create_simple_schema("test", vec![field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 500).unwrap();

        // All values should be in range
        for record in &records {
            if let Some(FieldValue::Float(f)) = record.get("rank_score") {
                assert!(*f >= 1.0 && *f <= 100.0, "Value {} not in range", f);
            }
        }
    }

    // =====================================================================
    // Derived Field Tests
    // =====================================================================

    #[test]
    fn test_generate_derived_multiplication_integers() {
        let price_field = FieldDefinition {
            name: "price".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 10.0,
                    max: 20.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let quantity_field = FieldDefinition {
            name: "quantity".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint { min: 1.0, max: 5.0 }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let total_field = FieldDefinition {
            name: "total".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "price * quantity".to_string(),
                    depends_on: vec!["price".to_string(), "quantity".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![price_field, quantity_field, total_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            let price = match record.get("price") {
                Some(FieldValue::Integer(p)) => *p,
                _ => panic!("Expected price"),
            };
            let quantity = match record.get("quantity") {
                Some(FieldValue::Integer(q)) => *q,
                _ => panic!("Expected quantity"),
            };
            let total = match record.get("total") {
                Some(FieldValue::Integer(t)) => *t,
                _ => panic!("Expected total as integer"),
            };

            assert_eq!(total, price * quantity, "total should be price * quantity");
        }
    }

    #[test]
    fn test_generate_derived_multiplication_floats() {
        let rate_field = FieldDefinition {
            name: "rate".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint { min: 0.1, max: 0.5 }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let amount_field = FieldDefinition {
            name: "amount".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 100.0,
                    max: 200.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let interest_field = FieldDefinition {
            name: "interest".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "rate * amount".to_string(),
                    depends_on: vec!["rate".to_string(), "amount".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![rate_field, amount_field, interest_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            let rate = match record.get("rate") {
                Some(FieldValue::Float(r)) => *r,
                _ => panic!("Expected rate"),
            };
            let amount = match record.get("amount") {
                Some(FieldValue::Float(a)) => *a,
                _ => panic!("Expected amount"),
            };
            let interest = match record.get("interest") {
                Some(FieldValue::Float(i)) => *i,
                _ => panic!("Expected interest as float"),
            };

            let expected = rate * amount;
            assert!(
                (interest - expected).abs() < 0.0001,
                "interest {} should equal rate * amount {}",
                interest,
                expected
            );
        }
    }

    // =====================================================================
    // Multiple Fields Tests
    // =====================================================================

    #[test]
    fn test_generate_multiple_fields() {
        let schema = create_simple_schema(
            "test",
            vec![
                string_field("name"),
                integer_field("age"),
                float_field("score"),
                FieldDefinition {
                    name: "active".to_string(),
                    field_type: FieldType::boolean(),
                    constraints: FieldConstraints::default(),
                    nullable: false,
                    description: None,
                },
            ],
        );

        let mut generator = SchemaDataGenerator::new(Some(42));
        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            assert!(record.contains_key("name"));
            assert!(record.contains_key("age"));
            assert!(record.contains_key("score"));
            assert!(record.contains_key("active"));

            assert!(matches!(record.get("name"), Some(FieldValue::String(_))));
            assert!(matches!(record.get("age"), Some(FieldValue::Integer(_))));
            assert!(matches!(record.get("score"), Some(FieldValue::Float(_))));
            assert!(matches!(record.get("active"), Some(FieldValue::Boolean(_))));
        }
    }

    // =====================================================================
    // parse_relative_time Tests
    // =====================================================================

    #[test]
    fn test_parse_relative_time_now() {
        assert_eq!(parse_relative_time("now"), 0);
        assert_eq!(parse_relative_time("NOW"), 0);
    }

    #[test]
    fn test_parse_relative_time_hours() {
        assert_eq!(parse_relative_time("-1h"), -3600);
        assert_eq!(parse_relative_time("-2h"), -7200);
        assert_eq!(parse_relative_time("1h"), 3600);
    }

    #[test]
    fn test_parse_relative_time_minutes() {
        assert_eq!(parse_relative_time("-30m"), -1800);
        assert_eq!(parse_relative_time("-5m"), -300);
        assert_eq!(parse_relative_time("10m"), 600);
    }

    #[test]
    fn test_parse_relative_time_seconds() {
        assert_eq!(parse_relative_time("-60s"), -60);
        assert_eq!(parse_relative_time("30s"), 30);
    }

    #[test]
    fn test_parse_relative_time_days() {
        assert_eq!(parse_relative_time("-1d"), -86400);
        assert_eq!(parse_relative_time("2d"), 172800);
    }

    // =====================================================================
    // multiply_field_values Tests
    // =====================================================================

    #[test]
    fn test_multiply_integers() {
        let result =
            multiply_field_values(&FieldValue::Integer(10), &FieldValue::Integer(5)).unwrap();

        assert_eq!(result, FieldValue::Integer(50));
    }

    #[test]
    fn test_multiply_floats() {
        let result =
            multiply_field_values(&FieldValue::Float(2.5), &FieldValue::Float(4.0)).unwrap();

        if let FieldValue::Float(f) = result {
            assert!((f - 10.0).abs() < 0.0001);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_multiply_integer_float() {
        let result =
            multiply_field_values(&FieldValue::Integer(5), &FieldValue::Float(2.5)).unwrap();

        if let FieldValue::Float(f) = result {
            assert!((f - 12.5).abs() < 0.0001);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_multiply_scaled_integers() {
        let result = multiply_field_values(
            &FieldValue::ScaledInteger(1000, 2), // 10.00
            &FieldValue::ScaledInteger(500, 2),  // 5.00
        )
        .unwrap();

        // 10.00 * 5.00 = 50.0000 (scale 4)
        if let FieldValue::ScaledInteger(value, scale) = result {
            assert_eq!(value, 500000); // 1000 * 500
            assert_eq!(scale, 4); // 2 + 2
        } else {
            panic!("Expected ScaledInteger");
        }
    }

    #[test]
    fn test_multiply_incompatible_types() {
        let result = multiply_field_values(
            &FieldValue::String("hello".to_string()),
            &FieldValue::Integer(5),
        );

        assert!(result.is_err());
        if let Err(TestHarnessError::GeneratorError { message, .. }) = result {
            assert!(message.contains("Cannot multiply"));
        }
    }

    // =====================================================================
    // SchemaRegistry Integration Tests
    // =====================================================================

    #[test]
    fn test_generator_with_registry() {
        let registry = SchemaRegistry::new();
        let generator = SchemaDataGenerator::new(Some(42)).with_registry(registry);

        // Just verify the builder pattern works
        assert!(generator.reference_cache.is_empty());
    }

    // =====================================================================
    // Advanced Derived Expression Tests
    // =====================================================================

    #[test]
    fn test_derived_addition() {
        let a_field = FieldDefinition {
            name: "a".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 10.0,
                    max: 20.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let b_field = FieldDefinition {
            name: "b".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 5.0,
                    max: 10.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let sum_field = FieldDefinition {
            name: "sum".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "a + b".to_string(),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![a_field, b_field, sum_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            let a = match record.get("a") {
                Some(FieldValue::Integer(v)) => *v as f64,
                _ => panic!("Expected a"),
            };
            let b = match record.get("b") {
                Some(FieldValue::Integer(v)) => *v as f64,
                _ => panic!("Expected b"),
            };
            let sum = match record.get("sum") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected sum as float"),
            };

            assert!(
                (sum - (a + b)).abs() < 0.001,
                "sum {} should equal a + b = {}",
                sum,
                a + b
            );
        }
    }

    #[test]
    fn test_derived_subtraction() {
        let a_field = FieldDefinition {
            name: "a".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 50.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let b_field = FieldDefinition {
            name: "b".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 10.0,
                    max: 30.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let diff_field = FieldDefinition {
            name: "diff".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "a - b".to_string(),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![a_field, b_field, diff_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            let a = match record.get("a") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected a"),
            };
            let b = match record.get("b") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected b"),
            };
            let diff = match record.get("diff") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected diff as float"),
            };

            assert!(
                (diff - (a - b)).abs() < 0.001,
                "diff {} should equal a - b = {}",
                diff,
                a - b
            );
        }
    }

    #[test]
    fn test_derived_division() {
        let total_field = FieldDefinition {
            name: "total".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 100.0,
                    max: 1000.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let count_field = FieldDefinition {
            name: "count".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 1.0,
                    max: 10.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let avg_field = FieldDefinition {
            name: "avg".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "total / count".to_string(),
                    depends_on: vec!["total".to_string(), "count".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![total_field, count_field, avg_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            let total = match record.get("total") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected total"),
            };
            let count = match record.get("count") {
                Some(FieldValue::Integer(v)) => *v as f64,
                _ => panic!("Expected count"),
            };
            let avg = match record.get("avg") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected avg as float"),
            };

            assert!(
                (avg - (total / count)).abs() < 0.001,
                "avg {} should equal total / count = {}",
                avg,
                total / count
            );
        }
    }

    #[test]
    fn test_derived_random() {
        let random_field = FieldDefinition {
            name: "rand".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "random()".to_string(),
                    depends_on: vec![],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![random_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 100).unwrap();

        let mut values = Vec::new();
        for record in &records {
            if let Some(FieldValue::Float(v)) = record.get("rand") {
                assert!(*v >= 0.0 && *v <= 1.0, "random() should be in [0, 1]");
                values.push(*v);
            } else {
                panic!("Expected float for random()");
            }
        }

        // Check that we get some variety
        let unique: std::collections::HashSet<_> =
            values.iter().map(|v| (*v * 1000.0) as i64).collect();
        assert!(unique.len() > 50, "random() should produce varied values");
    }

    #[test]
    fn test_derived_random_range() {
        let random_field = FieldDefinition {
            name: "rand".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "random(10, 20)".to_string(),
                    depends_on: vec![],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![random_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 50).unwrap();

        for record in &records {
            if let Some(FieldValue::Float(v)) = record.get("rand") {
                assert!(
                    *v >= 10.0 && *v <= 20.0,
                    "random(10, 20) should be in [10, 20], got {}",
                    v
                );
            } else {
                panic!("Expected float for random()");
            }
        }
    }

    #[test]
    fn test_derived_abs() {
        let value_field = FieldDefinition {
            name: "value".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: -100.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let abs_field = FieldDefinition {
            name: "abs_value".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "abs(value)".to_string(),
                    depends_on: vec!["value".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![value_field, abs_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            let value = match record.get("value") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected value"),
            };
            let abs_value = match record.get("abs_value") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected abs_value as integer"),
            };

            assert_eq!(
                abs_value,
                value.abs(),
                "abs({}) should be {}",
                value,
                value.abs()
            );
        }
    }

    #[test]
    fn test_derived_min_max() {
        let a_field = FieldDefinition {
            name: "a".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 0.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let b_field = FieldDefinition {
            name: "b".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 0.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let min_field = FieldDefinition {
            name: "minimum".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "min(a, b)".to_string(),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let max_field = FieldDefinition {
            name: "maximum".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "max(a, b)".to_string(),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![a_field, b_field, min_field, max_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            let a = match record.get("a") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected a"),
            };
            let b = match record.get("b") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected b"),
            };
            let minimum = match record.get("minimum") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected minimum"),
            };
            let maximum = match record.get("maximum") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected maximum"),
            };

            assert_eq!(
                minimum,
                a.min(b),
                "min({}, {}) should be {}",
                a,
                b,
                a.min(b)
            );
            assert_eq!(
                maximum,
                a.max(b),
                "max({}, {}) should be {}",
                a,
                b,
                a.max(b)
            );
        }
    }

    #[test]
    fn test_derived_concat() {
        let first_field = FieldDefinition {
            name: "first".to_string(),
            field_type: FieldType::string(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };

        let second_field = FieldDefinition {
            name: "second".to_string(),
            field_type: FieldType::string(),
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        };

        let concat_field = FieldDefinition {
            name: "combined".to_string(),
            field_type: FieldType::string(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "concat(first, second)".to_string(),
                    depends_on: vec!["first".to_string(), "second".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![first_field, second_field, concat_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            let first = match record.get("first") {
                Some(FieldValue::String(s)) => s.clone(),
                _ => panic!("Expected first"),
            };
            let second = match record.get("second") {
                Some(FieldValue::String(s)) => s.clone(),
                _ => panic!("Expected second"),
            };
            let combined = match record.get("combined") {
                Some(FieldValue::String(s)) => s.clone(),
                _ => panic!("Expected combined"),
            };

            assert_eq!(
                combined,
                format!("{}{}", first, second),
                "concat should concatenate"
            );
        }
    }

    #[test]
    fn test_derived_comparison() {
        let a_field = FieldDefinition {
            name: "a".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 0.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let b_field = FieldDefinition {
            name: "b".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 0.0,
                    max: 100.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let gt_field = FieldDefinition {
            name: "a_gt_b".to_string(),
            field_type: FieldType::boolean(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "a > b".to_string(),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![a_field, b_field, gt_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            let a = match record.get("a") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected a"),
            };
            let b = match record.get("b") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected b"),
            };
            let a_gt_b = match record.get("a_gt_b") {
                Some(FieldValue::Boolean(v)) => *v,
                _ => panic!("Expected a_gt_b as boolean"),
            };

            assert_eq!(a_gt_b, a > b, "{} > {} should be {}", a, b, a > b);
        }
    }

    #[test]
    fn test_derived_floor_ceil() {
        let value_field = FieldDefinition {
            name: "value".to_string(),
            field_type: FieldType::float(),
            constraints: FieldConstraints {
                range: Some(RangeConstraint {
                    min: 0.0,
                    max: 10.0,
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let floor_field = FieldDefinition {
            name: "floored".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "floor(value)".to_string(),
                    depends_on: vec!["value".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let ceil_field = FieldDefinition {
            name: "ceiled".to_string(),
            field_type: FieldType::integer(),
            constraints: FieldConstraints {
                derived: Some(DerivedConstraint {
                    expression: "ceil(value)".to_string(),
                    depends_on: vec!["value".to_string()],
                }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let schema = create_simple_schema("test", vec![value_field, floor_field, ceil_field]);
        let mut generator = SchemaDataGenerator::new(Some(42));

        let records = generator.generate(&schema, 20).unwrap();

        for record in &records {
            let value = match record.get("value") {
                Some(FieldValue::Float(v)) => *v,
                _ => panic!("Expected value"),
            };
            let floored = match record.get("floored") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected floored as integer"),
            };
            let ceiled = match record.get("ceiled") {
                Some(FieldValue::Integer(v)) => *v,
                _ => panic!("Expected ceiled as integer"),
            };

            assert_eq!(
                floored,
                value.floor() as i64,
                "floor({}) should be {}",
                value,
                value.floor()
            );
            assert_eq!(
                ceiled,
                value.ceil() as i64,
                "ceil({}) should be {}",
                value,
                value.ceil()
            );
        }
    }

    // =====================================================================
    // Helper Function Tests
    // =====================================================================

    #[test]
    fn test_find_operator_position() {
        assert_eq!(find_operator_position("a + b", "+"), Some(2));
        assert_eq!(find_operator_position("a * b", "*"), Some(2));
        assert_eq!(find_operator_position("abs(a + b) * c", "*"), Some(11));
        assert_eq!(find_operator_position("min(a, b)", "+"), None);
    }

    #[test]
    fn test_split_function_args() {
        assert_eq!(split_function_args("a, b"), Some(("a", "b")));
        assert_eq!(
            split_function_args("min(x, y), z"),
            Some(("min(x, y)", "z"))
        );
        assert_eq!(split_function_args("x"), None);
    }

    #[test]
    fn test_field_to_f64() {
        assert_eq!(field_to_f64(&FieldValue::Integer(42)), Some(42.0));
        assert_eq!(field_to_f64(&FieldValue::Float(2.5)), Some(2.5));
        assert_eq!(
            field_to_f64(&FieldValue::ScaledInteger(1234, 2)),
            Some(12.34)
        );
        assert_eq!(field_to_f64(&FieldValue::String("test".to_string())), None);
    }

    #[test]
    fn test_field_to_string() {
        assert_eq!(
            field_to_string(&FieldValue::String("hello".to_string())),
            "hello"
        );
        assert_eq!(field_to_string(&FieldValue::Integer(42)), "42");
        assert_eq!(field_to_string(&FieldValue::Boolean(true)), "true");
    }

    // =====================================================================
    // Reference Data Tests (Phase 9: Foreign Key Relationships)
    // =====================================================================

    #[test]
    fn test_load_reference_data() {
        let mut generator = SchemaDataGenerator::new(Some(42));

        // Load some reference data
        generator.load_reference_data(
            "customers",
            "id",
            vec![
                FieldValue::String("CUST001".to_string()),
                FieldValue::String("CUST002".to_string()),
                FieldValue::String("CUST003".to_string()),
            ],
        );

        assert!(generator.has_reference_data("customers", "id"));
        assert_eq!(generator.reference_data_count("customers", "id"), 3);

        assert!(!generator.has_reference_data("orders", "id"));
        assert_eq!(generator.reference_data_count("orders", "id"), 0);
    }

    #[test]
    fn test_load_reference_data_from_records() {
        let mut generator = SchemaDataGenerator::new(Some(42));

        // Create some "customer" records
        let customer_records = vec![
            {
                let mut r = HashMap::new();
                r.insert("id".to_string(), FieldValue::String("C001".to_string()));
                r.insert("name".to_string(), FieldValue::String("Alice".to_string()));
                r
            },
            {
                let mut r = HashMap::new();
                r.insert("id".to_string(), FieldValue::String("C002".to_string()));
                r.insert("name".to_string(), FieldValue::String("Bob".to_string()));
                r
            },
            {
                let mut r = HashMap::new();
                r.insert("id".to_string(), FieldValue::String("C003".to_string()));
                r.insert(
                    "name".to_string(),
                    FieldValue::String("Charlie".to_string()),
                );
                r
            },
        ];

        // Load from records
        generator.load_reference_data_from_records("customers", "id", &customer_records);

        assert!(generator.has_reference_data("customers", "id"));
        assert_eq!(generator.reference_data_count("customers", "id"), 3);
    }

    #[test]
    fn test_generate_reference_value_from_loaded_data() {
        use super::super::schema::ReferenceConstraint;

        let mut generator = SchemaDataGenerator::new(Some(42));

        // Load reference data for customers.id
        let customer_ids = vec![
            FieldValue::String("CUST001".to_string()),
            FieldValue::String("CUST002".to_string()),
            FieldValue::String("CUST003".to_string()),
        ];
        generator.load_reference_data("customers", "id", customer_ids.clone());

        // Create a field with reference constraint
        let mut field = string_field("customer_id");
        field.constraints.references = Some(ReferenceConstraint {
            schema: "customers".to_string(),
            field: "id".to_string(),
            file: None,
        });

        let schema = create_simple_schema("orders", vec![field]);

        // Generate records
        let records = generator.generate(&schema, 100).unwrap();

        // Verify all generated values are from the reference data
        let valid_ids: std::collections::HashSet<_> = customer_ids
            .iter()
            .filter_map(|v| {
                if let FieldValue::String(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("customer_id") {
                assert!(
                    valid_ids.contains(s),
                    "customer_id '{}' should be from reference data",
                    s
                );
            } else {
                panic!("Expected string customer_id");
            }
        }
    }

    #[test]
    fn test_generate_reference_value_placeholder_when_no_data() {
        use super::super::schema::ReferenceConstraint;

        let mut generator = SchemaDataGenerator::new(Some(42));

        // Don't load any reference data

        // Create a field with reference constraint
        let mut field = string_field("product_id");
        field.constraints.references = Some(ReferenceConstraint {
            schema: "products".to_string(),
            field: "id".to_string(),
            file: None,
        });

        let schema = create_simple_schema("order_items", vec![field]);

        // Generate records - should use placeholder values
        let records = generator.generate(&schema, 10).unwrap();

        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("product_id") {
                // Should be placeholder format: REF_PRODUCTS_<number>
                assert!(
                    s.starts_with("REF_PRODUCTS_"),
                    "product_id '{}' should be a placeholder",
                    s
                );
            } else {
                panic!("Expected string product_id");
            }
        }
    }

    #[test]
    fn test_generate_reference_value_deterministic() {
        use super::super::schema::ReferenceConstraint;

        // Generate with same seed twice
        let customer_ids = vec![
            FieldValue::String("A".to_string()),
            FieldValue::String("B".to_string()),
            FieldValue::String("C".to_string()),
            FieldValue::String("D".to_string()),
            FieldValue::String("E".to_string()),
        ];

        let mut field = string_field("customer_id");
        field.constraints.references = Some(ReferenceConstraint {
            schema: "customers".to_string(),
            field: "id".to_string(),
            file: None,
        });
        let schema = create_simple_schema("orders", vec![field]);

        let mut gen1 = SchemaDataGenerator::new(Some(42));
        gen1.load_reference_data("customers", "id", customer_ids.clone());

        let mut gen2 = SchemaDataGenerator::new(Some(42));
        gen2.load_reference_data("customers", "id", customer_ids);

        let records1 = gen1.generate(&schema, 20).unwrap();
        let records2 = gen2.generate(&schema, 20).unwrap();

        // With same seed, should produce identical results
        for (r1, r2) in records1.iter().zip(records2.iter()) {
            assert_eq!(
                r1.get("customer_id"),
                r2.get("customer_id"),
                "Same seed should produce same reference values"
            );
        }
    }

    #[test]
    fn test_reference_data_integer_values() {
        use super::super::schema::ReferenceConstraint;

        let mut generator = SchemaDataGenerator::new(Some(42));

        // Load integer reference data
        generator.load_reference_data(
            "categories",
            "id",
            vec![
                FieldValue::Integer(1),
                FieldValue::Integer(2),
                FieldValue::Integer(3),
                FieldValue::Integer(4),
                FieldValue::Integer(5),
            ],
        );

        // Create a field referencing integers
        let mut field = integer_field("category_id");
        field.constraints.references = Some(ReferenceConstraint {
            schema: "categories".to_string(),
            field: "id".to_string(),
            file: None,
        });

        let schema = create_simple_schema("products", vec![field]);

        let records = generator.generate(&schema, 50).unwrap();

        let valid_ids = vec![1, 2, 3, 4, 5];
        for record in &records {
            if let Some(FieldValue::Integer(i)) = record.get("category_id") {
                assert!(
                    valid_ids.contains(i),
                    "category_id {} should be from reference data",
                    i
                );
            } else {
                panic!(
                    "Expected integer category_id, got {:?}",
                    record.get("category_id")
                );
            }
        }
    }

    #[test]
    fn test_reference_data_distribution() {
        use super::super::schema::ReferenceConstraint;

        let mut generator = SchemaDataGenerator::new(Some(42));

        // Load just 3 reference values
        generator.load_reference_data(
            "regions",
            "code",
            vec![
                FieldValue::String("US".to_string()),
                FieldValue::String("EU".to_string()),
                FieldValue::String("APAC".to_string()),
            ],
        );

        let mut field = string_field("region");
        field.constraints.references = Some(ReferenceConstraint {
            schema: "regions".to_string(),
            field: "code".to_string(),
            file: None,
        });

        let schema = create_simple_schema("offices", vec![field]);

        let records = generator.generate(&schema, 300).unwrap();

        // Count distribution
        let mut counts = HashMap::new();
        for record in &records {
            if let Some(FieldValue::String(s)) = record.get("region") {
                *counts.entry(s.clone()).or_insert(0) += 1;
            }
        }

        // Should see all 3 regions with roughly uniform distribution
        assert_eq!(counts.len(), 3, "Should have all 3 regions");

        for (region, count) in &counts {
            // With 300 records and 3 values, expect ~100 each
            // Allow significant variance due to randomness
            assert!(
                *count >= 50 && *count <= 150,
                "Region {} has {} occurrences, expected roughly 100",
                region,
                count
            );
        }
    }

    #[test]
    fn test_multiple_reference_fields() {
        use super::super::schema::ReferenceConstraint;

        let mut generator = SchemaDataGenerator::new(Some(42));

        // Load reference data for multiple tables
        generator.load_reference_data(
            "customers",
            "id",
            vec![
                FieldValue::String("C1".to_string()),
                FieldValue::String("C2".to_string()),
            ],
        );

        generator.load_reference_data(
            "products",
            "sku",
            vec![
                FieldValue::String("SKU-A".to_string()),
                FieldValue::String("SKU-B".to_string()),
                FieldValue::String("SKU-C".to_string()),
            ],
        );

        // Create schema with multiple reference fields
        let mut customer_field = string_field("customer_id");
        customer_field.constraints.references = Some(ReferenceConstraint {
            schema: "customers".to_string(),
            field: "id".to_string(),
            file: None,
        });

        let mut product_field = string_field("product_sku");
        product_field.constraints.references = Some(ReferenceConstraint {
            schema: "products".to_string(),
            field: "sku".to_string(),
            file: None,
        });

        let quantity_field = integer_field("quantity");

        let schema = create_simple_schema(
            "orders",
            vec![customer_field, product_field, quantity_field],
        );

        let records = generator.generate(&schema, 50).unwrap();

        let valid_customers: std::collections::HashSet<_> =
            ["C1", "C2"].iter().map(|s| s.to_string()).collect();
        let valid_products: std::collections::HashSet<_> = ["SKU-A", "SKU-B", "SKU-C"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        for record in &records {
            // Verify customer_id
            if let Some(FieldValue::String(s)) = record.get("customer_id") {
                assert!(valid_customers.contains(s), "Invalid customer_id: {}", s);
            }

            // Verify product_sku
            if let Some(FieldValue::String(s)) = record.get("product_sku") {
                assert!(valid_products.contains(s), "Invalid product_sku: {}", s);
            }

            // Verify quantity is generated normally
            assert!(matches!(
                record.get("quantity"),
                Some(FieldValue::Integer(_))
            ));
        }
    }
}
