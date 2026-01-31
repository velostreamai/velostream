//! Shared pure-function computation module for aggregate math.
//!
//! This module is the single source of truth for aggregate computation logic,
//! including Welford's online algorithm for numerically stable running statistics
//! and shared helpers for variance, stddev, median, and type conversion.

use crate::velostream::sql::execution::types::FieldValue;

/// Welford's online algorithm state for numerically stable running mean/variance.
///
/// This replaces O(n) memory storage of all values for AVG, STDDEV, and VARIANCE
/// with O(1) state that is updated incrementally per record.
///
/// Reference: Welford, B.P. (1962). "Note on a method for calculating corrected
/// sums of squares and products". Technometrics. 4 (3): 419–420.
#[derive(Debug, Clone)]
pub struct WelfordState {
    pub count: u64,
    pub mean: f64,
    pub m2: f64,
}

impl WelfordState {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Incorporate a new value using Welford's online update.
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }
}

impl Default for WelfordState {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a `FieldValue` to `f64` for numeric aggregation.
pub fn field_value_to_f64(val: &FieldValue) -> Option<f64> {
    match val {
        FieldValue::Integer(i) => Some(*i as f64),
        FieldValue::ScaledInteger(v, scale) => Some(*v as f64 / 10f64.powi(*scale as i32)),
        FieldValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// Compute the average from a Welford state.
pub fn compute_avg_from_welford(state: &WelfordState) -> Option<f64> {
    if state.count == 0 {
        None
    } else {
        Some(state.mean)
    }
}

/// Compute variance from a Welford state.
///
/// `sample=true` gives sample variance (N-1 divisor), `sample=false` gives population variance (N divisor).
pub fn compute_variance_from_welford(state: &WelfordState, sample: bool) -> Option<f64> {
    if state.count == 0 {
        return None;
    }
    if sample {
        if state.count < 2 {
            return None;
        }
        Some(state.m2 / (state.count - 1) as f64)
    } else {
        if state.count == 1 {
            return Some(0.0);
        }
        Some(state.m2 / state.count as f64)
    }
}

/// Compute standard deviation from a Welford state.
pub fn compute_stddev_from_welford(state: &WelfordState, sample: bool) -> Option<f64> {
    compute_variance_from_welford(state, sample).map(|v| v.sqrt())
}

/// Compute variance from a slice of f64 values (two-pass).
///
/// Used when all values are already collected (e.g., unified_table path).
pub fn compute_variance_from_values(values: &[f64], sample: bool) -> Option<f64> {
    let n = values.len();
    if n == 0 || (sample && n < 2) {
        return None;
    }
    if !sample && n == 1 {
        return Some(0.0);
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let sum_sq: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
    let divisor = if sample { (n - 1) as f64 } else { n as f64 };
    Some(sum_sq / divisor)
}

/// Compute standard deviation from a slice of f64 values (two-pass).
pub fn compute_stddev_from_values(values: &[f64], sample: bool) -> Option<f64> {
    compute_variance_from_values(values, sample).map(|v| v.sqrt())
}

/// Compute median from a slice of f64 values.
pub fn compute_median_from_values(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = sorted.len();
    if len % 2 == 0 {
        Some((sorted[len / 2 - 1] + sorted[len / 2]) / 2.0)
    } else {
        Some(sorted[len / 2])
    }
}

/// Compute the SUM result, returning Integer when all inputs were integer.
pub fn compute_sum_result(sum: f64, all_integer: bool, has_values: bool) -> FieldValue {
    if !has_values {
        return FieldValue::Null;
    }
    if all_integer && sum.fract() == 0.0 {
        FieldValue::Integer(sum as i64)
    } else {
        FieldValue::Float(sum)
    }
}
