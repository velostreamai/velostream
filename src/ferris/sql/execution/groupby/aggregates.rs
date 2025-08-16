//! Aggregate helper functions for GROUP BY operations in streaming SQL.

use std::collections::{HashMap, HashSet};

use crate::ferris::sql::execution::types::FieldValue;

/// Helper functions for aggregate computations
pub struct AggregateHelper;

impl AggregateHelper {
    /// Compute average from numeric values
    pub fn compute_average(values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }

    /// Compute standard deviation from numeric values
    pub fn compute_stddev(values: &[f64]) -> Option<f64> {
        if values.len() < 2 {
            None
        } else {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Some(variance.sqrt())
        }
    }

    /// Compute variance from numeric values
    pub fn compute_variance(values: &[f64]) -> Option<f64> {
        if values.len() < 2 {
            None
        } else {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Some(variance)
        }
    }

    /// Compute minimum value from a collection of FieldValues
    pub fn compute_min(values: &[&FieldValue]) -> Option<FieldValue> {
        values
            .iter()
            .filter(|v| !matches!(***v, FieldValue::Null))
            .min_by(|a, b| Self::compare_field_values(a, b))
            .map(|v| (*v).clone())
    }

    /// Compute maximum value from a collection of FieldValues
    pub fn compute_max(values: &[&FieldValue]) -> Option<FieldValue> {
        values
            .iter()
            .filter(|v| !matches!(***v, FieldValue::Null))
            .max_by(|a, b| Self::compare_field_values(a, b))
            .map(|v| (*v).clone())
    }

    /// Compare two FieldValues for ordering
    pub fn compare_field_values(a: &FieldValue, b: &FieldValue) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a.cmp(b),
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (FieldValue::String(a), FieldValue::String(b)) => a.cmp(b),
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a.cmp(b),
            (FieldValue::Date(a), FieldValue::Date(b)) => a.cmp(b),
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a.cmp(b),
            (FieldValue::Decimal(a), FieldValue::Decimal(b)) => a.cmp(b),

            // Cross-type numeric comparisons
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }

            // NULL handling - NULLs are considered equal to each other and less than everything else
            (FieldValue::Null, FieldValue::Null) => Ordering::Equal,
            (FieldValue::Null, _) => Ordering::Less,
            (_, FieldValue::Null) => Ordering::Greater,

            // Different types - use string representation for comparison
            _ => a.to_display_string().cmp(&b.to_display_string()),
        }
    }

    /// Join string values with a delimiter
    pub fn join_strings(values: &[String], delimiter: &str) -> String {
        values.join(delimiter)
    }

    /// Count distinct values
    pub fn count_distinct(distinct_set: &HashSet<String>) -> usize {
        distinct_set.len()
    }

    /// Extract numeric value from FieldValue for aggregations
    pub fn extract_numeric_value(value: &FieldValue) -> Option<f64> {
        match value {
            FieldValue::Integer(i) => Some(*i as f64),
            FieldValue::Float(f) => Some(*f),
            FieldValue::Decimal(d) => d.to_string().parse().ok(),
            _ => None,
        }
    }

    /// Merge two accumulator values for distributed aggregation
    pub fn merge_sums(sum1: f64, sum2: f64) -> f64 {
        sum1 + sum2
    }

    /// Merge two count values
    pub fn merge_counts(count1: u64, count2: u64) -> u64 {
        count1 + count2
    }

    /// Merge two min values
    pub fn merge_mins(min1: &Option<FieldValue>, min2: &Option<FieldValue>) -> Option<FieldValue> {
        match (min1, min2) {
            (Some(a), Some(b)) => {
                if Self::compare_field_values(a, b) == std::cmp::Ordering::Less {
                    Some(a.clone())
                } else {
                    Some(b.clone())
                }
            }
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }

    /// Merge two max values
    pub fn merge_maxs(max1: &Option<FieldValue>, max2: &Option<FieldValue>) -> Option<FieldValue> {
        match (max1, max2) {
            (Some(a), Some(b)) => {
                if Self::compare_field_values(a, b) == std::cmp::Ordering::Greater {
                    Some(a.clone())
                } else {
                    Some(b.clone())
                }
            }
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }

    /// Merge two sets of numeric values for statistical functions
    pub fn merge_numeric_values(values1: &[f64], values2: &[f64]) -> Vec<f64> {
        let mut merged = Vec::with_capacity(values1.len() + values2.len());
        merged.extend_from_slice(values1);
        merged.extend_from_slice(values2);
        merged
    }

    /// Merge two distinct value sets
    pub fn merge_distinct_sets(set1: &HashSet<String>, set2: &HashSet<String>) -> HashSet<String> {
        let mut merged = set1.clone();
        merged.extend(set2.iter().cloned());
        merged
    }

    /// Compute percentile from sorted numeric values
    pub fn compute_percentile(values: &mut [f64], percentile: f64) -> Option<f64> {
        if values.is_empty() || percentile < 0.0 || percentile > 100.0 {
            return None;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let index = (percentile / 100.0) * (values.len() - 1) as f64;
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            Some(values[lower_index])
        } else if upper_index < values.len() {
            let weight = index - lower_index as f64;
            Some(values[lower_index] * (1.0 - weight) + values[upper_index] * weight)
        } else {
            Some(values[lower_index])
        }
    }

    /// Compute median (50th percentile)
    pub fn compute_median(values: &mut [f64]) -> Option<f64> {
        Self::compute_percentile(values, 50.0)
    }

    /// Check if a function is an aggregate function
    pub fn is_aggregate_function(function_name: &str) -> bool {
        matches!(
            function_name.to_uppercase().as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "STDDEV"
                | "VARIANCE"
                | "FIRST"
                | "LAST"
                | "STRING_AGG"
                | "GROUP_CONCAT"
                | "COUNT_DISTINCT"
                | "MEDIAN"
                | "PERCENTILE"
        )
    }

    /// Get list of all supported aggregate functions
    pub fn supported_aggregate_functions() -> Vec<&'static str> {
        vec![
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "STDDEV",
            "VARIANCE",
            "FIRST",
            "LAST",
            "STRING_AGG",
            "GROUP_CONCAT",
            "COUNT_DISTINCT",
            "MEDIAN",
            "PERCENTILE",
        ]
    }
}
