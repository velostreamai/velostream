//! Test data generation from schemas
//!
//! Generates test data based on schema definitions including:
//! - Type-appropriate value generation
//! - Distribution-based sampling
//! - Derived field calculation
//! - Foreign key relationships

use super::error::{TestHarnessError, TestHarnessResult};
use super::schema::{
    Distribution, FieldConstraints, FieldDefinition, FieldType, Schema, SchemaRegistry,
};
use crate::velostream::sql::execution::types::FieldValue;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rand::distributions::{Distribution as RandDistribution, WeightedIndex};
use rand::prelude::*;
use std::collections::HashMap;

/// Generates test data from schema definitions
pub struct SchemaDataGenerator {
    /// Random number generator
    rng: StdRng,

    /// Schema registry for foreign key lookups
    registry: SchemaRegistry,

    /// Cached reference data for foreign keys
    reference_cache: HashMap<String, Vec<HashMap<String, FieldValue>>>,
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
        }
    }

    /// Set schema registry for foreign key resolution
    pub fn with_registry(mut self, registry: SchemaRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Generate records for a schema
    pub fn generate(
        &mut self,
        schema: &Schema,
        count: usize,
    ) -> TestHarnessResult<Vec<HashMap<String, FieldValue>>> {
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

        // First pass: generate non-derived fields
        for field in &schema.fields {
            if field.constraints.derived.is_none() {
                let value = self.generate_field_value(field)?;
                record.insert(field.name.clone(), value);
            }
        }

        // Second pass: generate derived fields
        for field in &schema.fields {
            if field.constraints.derived.is_some() {
                let value = self.generate_derived_value(field, &record)?;
                record.insert(field.name.clone(), value);
            }
        }

        Ok(record)
    }

    /// Generate value for a field
    fn generate_field_value(&mut self, field: &FieldDefinition) -> TestHarnessResult<FieldValue> {
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

        // Generate based on type
        match &field.field_type {
            FieldType::String => self.generate_string_value(&field.constraints),
            FieldType::Integer => self.generate_integer_value(&field.constraints),
            FieldType::Float => self.generate_float_value(&field.constraints),
            FieldType::Decimal { precision } => {
                self.generate_decimal_value(&field.constraints, *precision)
            }
            FieldType::Boolean => Ok(FieldValue::Boolean(self.rng.gen_bool(0.5))),
            FieldType::Timestamp => self.generate_timestamp_value(&field.constraints),
            FieldType::Date => self.generate_date_value(&field.constraints),
            FieldType::Uuid => Ok(FieldValue::String(uuid::Uuid::new_v4().to_string())),
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
    fn generate_integer_value(
        &mut self,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        let value = if let Some(ref range) = constraints.range {
            self.generate_with_distribution(range.min, range.max, &constraints.distribution) as i64
        } else {
            self.rng.gen_range(0..1000)
        };

        Ok(FieldValue::Integer(value))
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
    fn generate_timestamp_value(
        &mut self,
        constraints: &FieldConstraints,
    ) -> TestHarnessResult<FieldValue> {
        let now = Utc::now();

        let (start, end) = if let Some(ref range) = constraints.timestamp_range {
            match range {
                super::schema::TimestampRange::Relative { start, end } => {
                    let start_offset = parse_relative_time(start);
                    let end_offset = parse_relative_time(end);
                    (
                        now + Duration::seconds(start_offset),
                        now + Duration::seconds(end_offset),
                    )
                }
                super::schema::TimestampRange::Absolute { start, end } => {
                    // Parse ISO 8601 timestamps
                    let start_dt = DateTime::parse_from_rfc3339(start)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or(now - Duration::hours(1));
                    let end_dt = DateTime::parse_from_rfc3339(end)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or(now);
                    (start_dt, end_dt)
                }
            }
        } else {
            // Default: last hour
            (now - Duration::hours(1), now)
        };

        let range_ms = (end - start).num_milliseconds();
        let offset_ms = self.rng.gen_range(0..range_ms.max(1));
        let timestamp = start + Duration::milliseconds(offset_ms);

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
    fn generate_reference_value(
        &mut self,
        reference: &super::schema::ReferenceConstraint,
        _field_name: &str,
    ) -> TestHarnessResult<FieldValue> {
        // TODO: Phase 1.5 - Implement reference resolution
        // 1. Check cache for reference data
        // 2. Load from file if specified
        // 3. Sample from referenced schema

        // Placeholder: generate random value
        Ok(FieldValue::String(format!(
            "REF_{}",
            self.rng.gen_range(1..100)
        )))
    }

    /// Generate derived value from expression
    fn generate_derived_value(
        &mut self,
        field: &FieldDefinition,
        record: &HashMap<String, FieldValue>,
    ) -> TestHarnessResult<FieldValue> {
        let derived = field.constraints.derived.as_ref().unwrap();

        // TODO: Phase 1.5 - Implement expression evaluation
        // For now, handle simple multiplication: "field1 * field2"
        let expression = &derived.expression;

        if expression.contains('*') {
            let parts: Vec<&str> = expression.split('*').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let left = record.get(parts[0]);
                let right = record.get(parts[1]);

                if let (Some(l), Some(r)) = (left, right) {
                    return multiply_field_values(l, r);
                }
            }
        }

        // Default: return null for unimplemented expressions
        Ok(FieldValue::Null)
    }

    /// Generate value with distribution
    fn generate_with_distribution(
        &mut self,
        min: f64,
        max: f64,
        dist: &Option<Distribution>,
    ) -> f64 {
        match dist {
            Some(Distribution::Normal { mean, std_dev }) => {
                // Box-Muller transform for normal distribution
                let u1: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let u2: f64 = self.rng.gen_range(0.0_f64..1.0_f64);
                let z0 =
                    ((-2.0_f64 * u1.ln()).sqrt()) * ((2.0_f64 * std::f64::consts::PI * u2).cos());
                let value = mean + std_dev * z0;
                value.clamp(min, max)
            }
            Some(Distribution::LogNormal { mean, std_dev }) => {
                // Log-normal: exp(normal distribution)
                let u1: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let u2: f64 = self.rng.gen_range(0.0_f64..1.0_f64);
                let z0 =
                    ((-2.0_f64 * u1.ln()).sqrt()) * ((2.0_f64 * std::f64::consts::PI * u2).cos());
                let normal_value = mean + std_dev * z0;
                let value = normal_value.exp();
                value.clamp(min, max)
            }
            Some(Distribution::Zipf { exponent }) => {
                // Simplified Zipf approximation using inverse transform
                let u: f64 = self.rng.gen_range(0.0001_f64..1.0_f64);
                let range = max - min;
                // Power law distribution approximation
                let value = min + range * (1.0 - u.powf(1.0 / exponent));
                value.clamp(min, max)
            }
            _ => self.rng.gen_range(min..=max),
        }
    }
}

/// Parse relative time string (e.g., "-1h", "now", "-30m")
fn parse_relative_time(s: &str) -> i64 {
    let s = s.trim().to_lowercase();

    if s == "now" {
        return 0;
    }

    let negative = s.starts_with('-');
    let s = s.trim_start_matches('-');

    let (num, unit) = if s.ends_with('h') {
        (s.trim_end_matches('h').parse::<i64>().unwrap_or(0), 3600)
    } else if s.ends_with('m') {
        (s.trim_end_matches('m').parse::<i64>().unwrap_or(0), 60)
    } else if s.ends_with('s') {
        (s.trim_end_matches('s').parse::<i64>().unwrap_or(0), 1)
    } else if s.ends_with('d') {
        (s.trim_end_matches('d').parse::<i64>().unwrap_or(0), 86400)
    } else {
        (s.parse::<i64>().unwrap_or(0), 1)
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
        }
    }

    fn string_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::String,
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        }
    }

    fn integer_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::Integer,
            constraints: FieldConstraints::default(),
            nullable: false,
            description: None,
        }
    }

    fn float_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::Float,
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
            field_type: FieldType::Decimal { precision: 2 },
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
            field_type: FieldType::Boolean,
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
            field_type: FieldType::Timestamp,
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
            field_type: FieldType::Timestamp,
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
            field_type: FieldType::Uuid,
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
            mean: 50.0,
            std_dev: 10.0,
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
        field.constraints.distribution = Some(Distribution::Zipf { exponent: 2.0 });

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
            field_type: FieldType::Integer,
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
            field_type: FieldType::Integer,
            constraints: FieldConstraints {
                range: Some(RangeConstraint { min: 1.0, max: 5.0 }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let total_field = FieldDefinition {
            name: "total".to_string(),
            field_type: FieldType::Integer,
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
            field_type: FieldType::Float,
            constraints: FieldConstraints {
                range: Some(RangeConstraint { min: 0.1, max: 0.5 }),
                ..Default::default()
            },
            nullable: false,
            description: None,
        };

        let amount_field = FieldDefinition {
            name: "amount".to_string(),
            field_type: FieldType::Float,
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
            field_type: FieldType::Float,
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
                    field_type: FieldType::Boolean,
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
}
