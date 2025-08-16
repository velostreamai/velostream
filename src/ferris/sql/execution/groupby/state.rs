//! GROUP BY state management for streaming SQL aggregation operations.

use std::collections::{HashMap, HashSet};

use crate::ferris::sql::ast::{Expr, SelectField};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};

/// State for GROUP BY operations
#[derive(Debug)]
pub struct GroupByState {
    /// Map of group keys to their accumulated state
    pub groups: HashMap<Vec<String>, GroupAccumulator>,
    /// The GROUP BY expressions for this state
    pub group_expressions: Vec<Expr>,
    /// The SELECT fields to compute for each group
    pub select_fields: Vec<SelectField>,
    /// Optional HAVING clause
    pub having_clause: Option<Expr>,
}

/// Accumulator for a single group's aggregate state
#[derive(Debug, Clone)]
pub struct GroupAccumulator {
    /// Count of records in this group
    pub count: u64,
    /// Count of non-NULL values for COUNT(column) aggregates (field_name -> count)
    pub non_null_counts: HashMap<String, u64>,
    /// Sum values for SUM() aggregates (field_name -> sum)
    pub sums: HashMap<String, f64>,
    /// Values for MIN() aggregates (field_name -> min_value)
    pub mins: HashMap<String, FieldValue>,
    /// Values for MAX() aggregates (field_name -> max_value)
    pub maxs: HashMap<String, FieldValue>,
    /// Values for statistical aggregates (field_name -> [values])
    pub numeric_values: HashMap<String, Vec<f64>>,
    /// First values for FIRST() aggregates
    pub first_values: HashMap<String, FieldValue>,
    /// Last values for LAST() aggregates (updated on each record)
    pub last_values: HashMap<String, FieldValue>,
    /// String values for STRING_AGG
    pub string_values: HashMap<String, Vec<String>>,
    /// Distinct values for COUNT_DISTINCT
    pub distinct_values: HashMap<String, HashSet<String>>,
    /// Sample record for non-aggregate fields (takes first record's values)
    pub sample_record: Option<StreamRecord>,
}

impl GroupByState {
    /// Create a new GROUP BY state
    pub fn new(
        group_expressions: Vec<Expr>,
        select_fields: Vec<SelectField>,
        having_clause: Option<Expr>,
    ) -> Self {
        Self {
            groups: HashMap::new(),
            group_expressions,
            select_fields,
            having_clause,
        }
    }

    /// Get or create a group accumulator for the given key
    pub fn get_or_create_accumulator(&mut self, group_key: Vec<String>) -> &mut GroupAccumulator {
        self.groups
            .entry(group_key)
            .or_insert_with(|| GroupAccumulator {
                count: 0,
                non_null_counts: HashMap::new(),
                sums: HashMap::new(),
                mins: HashMap::new(),
                maxs: HashMap::new(),
                numeric_values: HashMap::new(),
                first_values: HashMap::new(),
                last_values: HashMap::new(),
                string_values: HashMap::new(),
                distinct_values: HashMap::new(),
                sample_record: None,
            })
    }

    /// Get all group keys
    pub fn group_keys(&self) -> impl Iterator<Item = &Vec<String>> {
        self.groups.keys()
    }

    /// Get accumulator for a specific group
    pub fn get_accumulator(&self, group_key: &[String]) -> Option<&GroupAccumulator> {
        self.groups.get(group_key)
    }

    /// Get mutable accumulator for a specific group
    pub fn get_accumulator_mut(&mut self, group_key: &[String]) -> Option<&mut GroupAccumulator> {
        self.groups.get_mut(group_key)
    }

    /// Clear all groups (useful for windowed operations)
    pub fn clear(&mut self) {
        self.groups.clear();
    }

    /// Get the number of groups
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

impl GroupAccumulator {
    /// Create a new empty accumulator
    pub fn new() -> Self {
        Self {
            count: 0,
            non_null_counts: HashMap::new(),
            sums: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
            numeric_values: HashMap::new(),
            first_values: HashMap::new(),
            last_values: HashMap::new(),
            string_values: HashMap::new(),
            distinct_values: HashMap::new(),
            sample_record: None,
        }
    }

    /// Increment the record count
    pub fn increment_count(&mut self) {
        self.count += 1;
    }

    /// Set the sample record if it's the first one
    pub fn set_sample_record_if_first(&mut self, record: &StreamRecord) {
        if self.sample_record.is_none() {
            self.sample_record = Some(record.clone());
        }
    }

    /// Update the non-null count for a field
    pub fn update_non_null_count(&mut self, field_name: &str) {
        *self
            .non_null_counts
            .entry(field_name.to_string())
            .or_insert(0) += 1;
    }

    /// Update the sum for a field
    pub fn update_sum(&mut self, field_name: &str, value: f64) {
        *self.sums.entry(field_name.to_string()).or_insert(0.0) += value;
    }

    /// Update the minimum value for a field
    pub fn update_min(&mut self, field_name: &str, value: FieldValue) {
        let entry = self.mins.entry(field_name.to_string());
        match entry {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(value);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if Self::is_less_than(&value, e.get()) {
                    e.insert(value);
                }
            }
        }
    }

    /// Update the maximum value for a field
    pub fn update_max(&mut self, field_name: &str, value: FieldValue) {
        let entry = self.maxs.entry(field_name.to_string());
        match entry {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(value);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if Self::is_greater_than(&value, e.get()) {
                    e.insert(value);
                }
            }
        }
    }

    /// Add a numeric value for statistical aggregates
    pub fn add_numeric_value(&mut self, field_name: &str, value: f64) {
        self.numeric_values
            .entry(field_name.to_string())
            .or_insert_with(Vec::new)
            .push(value);
    }

    /// Set the first value for a field (only if not already set)
    pub fn set_first_value(&mut self, field_name: &str, value: FieldValue) {
        self.first_values
            .entry(field_name.to_string())
            .or_insert(value);
    }

    /// Update the last value for a field (always overwrites)
    pub fn update_last_value(&mut self, field_name: &str, value: FieldValue) {
        self.last_values.insert(field_name.to_string(), value);
    }

    /// Add a string value for STRING_AGG
    pub fn add_string_value(&mut self, field_name: &str, value: String) {
        self.string_values
            .entry(field_name.to_string())
            .or_insert_with(Vec::new)
            .push(value);
    }

    /// Add a distinct value for COUNT_DISTINCT
    pub fn add_distinct_value(&mut self, field_name: &str, value: String) {
        self.distinct_values
            .entry(field_name.to_string())
            .or_insert_with(HashSet::new)
            .insert(value);
    }

    /// Helper function to compare FieldValues for MIN operations
    fn is_less_than(a: &FieldValue, b: &FieldValue) -> bool {
        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a < b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a < b,
            (FieldValue::String(a), FieldValue::String(b)) => a < b,
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64) < *b,
            (FieldValue::Float(a), FieldValue::Integer(b)) => *a < (*b as f64),
            _ => false,
        }
    }

    /// Helper function to compare FieldValues for MAX operations
    fn is_greater_than(a: &FieldValue, b: &FieldValue) -> bool {
        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a > b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a > b,
            (FieldValue::String(a), FieldValue::String(b)) => a > b,
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64) > *b,
            (FieldValue::Float(a), FieldValue::Integer(b)) => *a > (*b as f64),
            _ => false,
        }
    }
}

impl Default for GroupAccumulator {
    fn default() -> Self {
        Self::new()
    }
}
