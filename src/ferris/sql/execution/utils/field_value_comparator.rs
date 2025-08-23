use crate::ferris::sql::SqlError;
/// Field Value Comparison Utilities
///
/// Handles comparison operations between FieldValue instances.
/// These are pure comparison functions with no engine state dependency.
use crate::ferris::sql::ast::BinaryOperator;
use crate::ferris::sql::execution::FieldValue;

/// Utility class for comparing FieldValue instances
pub struct FieldValueComparator;

impl FieldValueComparator {
    /// Compare two FieldValues for equality with exact type matching
    pub fn values_equal(left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
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
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,

            // Numeric coercion: Integer and Float should be comparable
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (FieldValue::Float(a), FieldValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,

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
    pub fn compare_values_for_boolean(
        left: &FieldValue,
        right: &FieldValue,
        op: &BinaryOperator,
    ) -> Result<bool, SqlError> {
        match op {
            BinaryOperator::Equal => Ok(Self::values_equal(left, right)),
            BinaryOperator::NotEqual => Ok(!Self::values_equal(left, right)),
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
    fn compare_numeric_values<F>(
        left: &FieldValue,
        right: &FieldValue,
        op: F,
    ) -> Result<bool, SqlError>
    where
        F: Fn(f64, f64) -> bool,
    {
        let (left_num, right_num) = match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => (*a as f64, *b as f64),
            (FieldValue::Float(a), FieldValue::Float(b)) => (*a, *b),
            (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64, *b),
            (FieldValue::Float(a), FieldValue::Integer(b)) => (*a, *b as f64),
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
}
