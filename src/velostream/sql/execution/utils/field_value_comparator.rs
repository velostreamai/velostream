use crate::velostream::sql::SqlError;
/// Field Value Comparison Utilities
///
/// Handles comparison operations between FieldValue instances.
/// These are pure comparison functions with no engine state dependency.
use crate::velostream::sql::ast::BinaryOperator;
use crate::velostream::sql::execution::FieldValue;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Utility class for comparing FieldValue instances
pub struct FieldValueComparator;

impl FieldValueComparator {
    /// Compare two FieldValues for equality with exact type matching
    pub fn values_equal(left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                // Exact ScaledInteger comparison - both value and scale must match
                a == b && scale_a == scale_b
            }
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Date(a), FieldValue::Date(b)) => a == b,
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a == b,
            (FieldValue::Decimal(a), FieldValue::Decimal(b)) => a == b,
            (
                FieldValue::Interval {
                    value: a,
                    unit: unit_a,
                },
                FieldValue::Interval {
                    value: b,
                    unit: unit_b,
                },
            ) => a == b && unit_a == unit_b,
            (FieldValue::Null, FieldValue::Null) => true,
            (FieldValue::Array(a), FieldValue::Array(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| Self::values_equal(x, y))
            }
            (FieldValue::Map(a), FieldValue::Map(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).is_some_and(|bv| Self::values_equal(v, bv)))
            }
            (FieldValue::Struct(a), FieldValue::Struct(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).is_some_and(|bv| Self::values_equal(v, bv)))
            }
            _ => false,
        }
    }

    /// Compare values with numeric type coercion for IN operations
    pub fn values_equal_with_coercion(left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            // Exact type matches
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                Self::scaled_integers_equal(*a, *scale_a, *b, *scale_b)
            }
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Date(a), FieldValue::Date(b)) => a == b,
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a == b,
            (FieldValue::Decimal(a), FieldValue::Decimal(b)) => a == b,
            (
                FieldValue::Interval {
                    value: a,
                    unit: unit_a,
                },
                FieldValue::Interval {
                    value: b,
                    unit: unit_b,
                },
            ) => a == b && unit_a == unit_b,

            // Numeric coercion: Integer, Float, and ScaledInteger should be comparable
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (FieldValue::Float(a), FieldValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,

            // ScaledInteger to Integer coercion
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                let normalized_a = Self::scaled_to_f64(*a, *scale);
                (normalized_a - *b as f64).abs() < f64::EPSILON
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                let normalized_b = Self::scaled_to_f64(*b, *scale);
                (*a as f64 - normalized_b).abs() < f64::EPSILON
            }

            // ScaledInteger to Float coercion
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                let normalized_a = Self::scaled_to_f64(*a, *scale);
                (normalized_a - b).abs() < f64::EPSILON
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                let normalized_b = Self::scaled_to_f64(*b, *scale);
                (a - normalized_b).abs() < f64::EPSILON
            }

            // Decimal numeric coercion
            (FieldValue::Decimal(a), FieldValue::Integer(b)) => {
                let decimal_b = Decimal::from(*b);
                *a == decimal_b
            }
            (FieldValue::Integer(a), FieldValue::Decimal(b)) => {
                let decimal_a = Decimal::from(*a);
                decimal_a == *b
            }
            (FieldValue::Decimal(a), FieldValue::Float(b)) => {
                let float_a = a.to_f64().unwrap_or(0.0);
                (float_a - b).abs() < f64::EPSILON
            }
            (FieldValue::Float(a), FieldValue::Decimal(b)) => {
                let float_b = b.to_f64().unwrap_or(0.0);
                (a - float_b).abs() < f64::EPSILON
            }
            (FieldValue::Decimal(a), FieldValue::ScaledInteger(b, scale)) => {
                let normalized_b = Self::scaled_to_f64(*b, *scale);
                let float_a = a.to_f64().unwrap_or(0.0);
                (float_a - normalized_b).abs() < f64::EPSILON
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Decimal(b)) => {
                let normalized_a = Self::scaled_to_f64(*a, *scale);
                let float_b = b.to_f64().unwrap_or(0.0);
                (normalized_a - float_b).abs() < f64::EPSILON
            }

            // Complex types (same as values_equal)
            (FieldValue::Array(a), FieldValue::Array(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| Self::values_equal_with_coercion(x, y))
            }
            (FieldValue::Map(a), FieldValue::Map(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(k, v)| {
                        b.get(k)
                            .is_some_and(|bv| Self::values_equal_with_coercion(v, bv))
                    })
            }
            (FieldValue::Struct(a), FieldValue::Struct(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(k, v)| {
                        b.get(k)
                            .is_some_and(|bv| Self::values_equal_with_coercion(v, bv))
                    })
            }

            // NULL values should not match anything in IN context
            (FieldValue::Null, _) | (_, FieldValue::Null) => false,

            // All other combinations don't match
            _ => false,
        }
    }

    /// Compare two FieldValues using a binary operator and return boolean result
    ///
    /// SQL semantics for NULL comparisons (three-valued logic):
    /// - NULL = anything → NULL (treated as false in boolean context)
    /// - NULL != anything → NULL (treated as false in boolean context)
    /// - NULL <op> anything → NULL (treated as false) for all comparison operators
    pub fn compare_values_for_boolean(
        left: &FieldValue,
        right: &FieldValue,
        op: &BinaryOperator,
    ) -> Result<bool, SqlError> {
        // SQL three-valued logic: Any comparison involving NULL returns NULL
        // In boolean context, NULL is treated as false
        if matches!(left, FieldValue::Null) || matches!(right, FieldValue::Null) {
            return Ok(false);
        }

        match op {
            BinaryOperator::Equal => Ok(Self::values_equal_with_coercion(left, right)),
            BinaryOperator::NotEqual => Ok(!Self::values_equal_with_coercion(left, right)),
            BinaryOperator::GreaterThan => Self::compare_numeric_values(left, right, |a, b| a > b),
            BinaryOperator::LessThan => Self::compare_numeric_values(left, right, |a, b| a < b),
            BinaryOperator::GreaterThanOrEqual => {
                Self::compare_numeric_values(left, right, |a, b| a >= b)
            }
            BinaryOperator::LessThanOrEqual => {
                Self::compare_numeric_values(left, right, |a, b| a <= b)
            }
            _ => Err(SqlError::ExecutionError {
                message: "Invalid comparison operator".to_string(),
                query: None,
            }),
        }
    }

    /// Compare numeric values with a comparison function
    ///
    /// SQL semantics: Comparisons involving NULL return false (NULL comparison result
    /// treated as false in boolean context). This follows standard SQL three-valued logic.
    fn compare_numeric_values<F>(
        left: &FieldValue,
        right: &FieldValue,
        op: F,
    ) -> Result<bool, SqlError>
    where
        F: Fn(f64, f64) -> bool,
    {
        // SQL semantics: NULL compared with anything returns NULL (false in boolean context)
        // This handles cases like MAX(volume) > 2000 when accumulator is empty
        if matches!(left, FieldValue::Null) || matches!(right, FieldValue::Null) {
            return Ok(false);
        }

        let (left_num, right_num) = match (left, right) {
            // Pure numeric types
            (FieldValue::Integer(a), FieldValue::Integer(b)) => (*a as f64, *b as f64),
            (FieldValue::Float(a), FieldValue::Float(b)) => (*a, *b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64, *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => (*a, *b as f64),

            // ScaledInteger comparisons
            (FieldValue::ScaledInteger(a, scale_a), FieldValue::ScaledInteger(b, scale_b)) => {
                // Use scaled integer comparison for exact precision
                return Ok(Self::compare_scaled_integers(
                    *a, *scale_a, *b, *scale_b, &op,
                ));
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Integer(b)) => {
                (Self::scaled_to_f64(*a, *scale), *b as f64)
            }
            (FieldValue::Integer(a), FieldValue::ScaledInteger(b, scale)) => {
                (*a as f64, Self::scaled_to_f64(*b, *scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Float(b)) => {
                (Self::scaled_to_f64(*a, *scale), *b)
            }
            (FieldValue::Float(a), FieldValue::ScaledInteger(b, scale)) => {
                (*a, Self::scaled_to_f64(*b, *scale))
            }

            // Decimal comparisons
            (FieldValue::Decimal(a), FieldValue::Integer(b)) => {
                (a.to_f64().unwrap_or(0.0), *b as f64)
            }
            (FieldValue::Integer(a), FieldValue::Decimal(b)) => {
                (*a as f64, b.to_f64().unwrap_or(0.0))
            }
            (FieldValue::Decimal(a), FieldValue::Float(b)) => (a.to_f64().unwrap_or(0.0), *b),
            (FieldValue::Float(a), FieldValue::Decimal(b)) => (*a, b.to_f64().unwrap_or(0.0)),
            (FieldValue::Decimal(a), FieldValue::ScaledInteger(b, scale)) => {
                (a.to_f64().unwrap_or(0.0), Self::scaled_to_f64(*b, *scale))
            }
            (FieldValue::ScaledInteger(a, scale), FieldValue::Decimal(b)) => {
                (Self::scaled_to_f64(*a, *scale), b.to_f64().unwrap_or(0.0))
            }
            (FieldValue::Decimal(a), FieldValue::Decimal(b)) => {
                (a.to_f64().unwrap_or(0.0), b.to_f64().unwrap_or(0.0))
            }

            _ => {
                return Err(SqlError::TypeError {
                    expected: "numeric".to_string(),
                    actual: "non-numeric".to_string(),
                    value: None,
                });
            }
        };

        Ok(op(left_num, right_num))
    }

    /// Convert ScaledInteger to f64 for comparison operations
    /// Convert a ScaledInteger to f64 by dividing by the scale factor
    pub fn scaled_to_f64(value: i64, scale: u8) -> f64 {
        let divisor = 10_i64.pow(scale as u32) as f64;
        value as f64 / divisor
    }

    /// Compare two ScaledInteger values for equality with scale normalization
    fn scaled_integers_equal(a: i64, scale_a: u8, b: i64, scale_b: u8) -> bool {
        if scale_a == scale_b {
            // Same scale - direct comparison for exact precision
            a == b
        } else {
            // Different scales - normalize and compare as f64
            let normalized_a = Self::scaled_to_f64(a, scale_a);
            let normalized_b = Self::scaled_to_f64(b, scale_b);
            (normalized_a - normalized_b).abs() < f64::EPSILON
        }
    }

    /// Compare two ScaledInteger values using exact integer arithmetic for optimal precision
    fn compare_scaled_integers<F>(a: i64, scale_a: u8, b: i64, scale_b: u8, op: &F) -> bool
    where
        F: Fn(f64, f64) -> bool,
    {
        // Always normalize to common scale for exact comparison
        let max_scale = scale_a.max(scale_b);
        let normalized_a = if scale_a < max_scale {
            a * 10_i64.pow((max_scale - scale_a) as u32)
        } else {
            a
        };
        let normalized_b = if scale_b < max_scale {
            b * 10_i64.pow((max_scale - scale_b) as u32)
        } else {
            b
        };

        // Use f64 for the actual comparison since we can't implement the comparison
        // directly on i64 without knowing the specific operator
        let f_a = normalized_a as f64;
        let f_b = normalized_b as f64;
        op(f_a, f_b)
    }
}
