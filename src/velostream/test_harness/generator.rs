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
