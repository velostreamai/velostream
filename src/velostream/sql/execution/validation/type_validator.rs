//! Type Validation for SQL Expressions
//!
//! Implements type compatibility checking for ORDER BY, WHERE, and other clauses.
//! This is Phase 6 of the correctness work - ensures expressions contain compatible types.

use crate::velostream::sql::execution::types::FieldValue;
use std::fmt;

/// Type categories for compatibility checking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    /// Integer types
    Integer,
    /// Float types
    Float,
    /// Decimal types (arbitrary precision)
    Decimal,
    /// ScaledInteger (financial precision)
    ScaledInteger,
    /// String types
    String,
    /// Boolean types
    Boolean,
    /// Date types
    Date,
    /// Timestamp types
    Timestamp,
    /// Interval types
    Interval,
    /// Numeric (any numeric type)
    Numeric,
    /// Temporal (Date or Timestamp)
    Temporal,
    /// Any type
    Any,
}

impl fmt::Display for TypeCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeCategory::Integer => write!(f, "INTEGER"),
            TypeCategory::Float => write!(f, "FLOAT"),
            TypeCategory::Decimal => write!(f, "DECIMAL"),
            TypeCategory::ScaledInteger => write!(f, "SCALED_INTEGER"),
            TypeCategory::String => write!(f, "STRING"),
            TypeCategory::Boolean => write!(f, "BOOLEAN"),
            TypeCategory::Date => write!(f, "DATE"),
            TypeCategory::Timestamp => write!(f, "TIMESTAMP"),
            TypeCategory::Interval => write!(f, "INTERVAL"),
            TypeCategory::Numeric => write!(f, "NUMERIC"),
            TypeCategory::Temporal => write!(f, "TEMPORAL"),
            TypeCategory::Any => write!(f, "ANY"),
        }
    }
}

/// Type compatibility error
#[derive(Debug, Clone)]
pub struct TypeCompatibilityError {
    pub left_type: TypeCategory,
    pub right_type: TypeCategory,
    pub operation: String,
}

impl fmt::Display for TypeCompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Type mismatch in {}: cannot apply to {} and {}",
            self.operation, self.left_type, self.right_type
        )
    }
}

/// Type validator for SQL expressions
pub struct TypeValidator;

impl TypeValidator {
    /// Get the type category of a FieldValue
    pub fn get_type_category(value: &FieldValue) -> TypeCategory {
        match value {
            FieldValue::Integer(_) => TypeCategory::Integer,
            FieldValue::Float(_) => TypeCategory::Float,
            FieldValue::Decimal(_) => TypeCategory::Decimal,
            FieldValue::ScaledInteger(_, _) => TypeCategory::ScaledInteger,
            FieldValue::String(_) => TypeCategory::String,
            FieldValue::Boolean(_) => TypeCategory::Boolean,
            FieldValue::Date(_) => TypeCategory::Date,
            FieldValue::Timestamp(_) => TypeCategory::Timestamp,
            FieldValue::Interval { .. } => TypeCategory::Interval,
            FieldValue::Null => TypeCategory::Any, // NULL is compatible with any type
            FieldValue::Array(_) => TypeCategory::Any,
            FieldValue::Map(_) => TypeCategory::Any,
            FieldValue::Struct(_) => TypeCategory::Any,
        }
    }

    /// Check if two types are compatible for comparison (ORDER BY, WHERE)
    ///
    /// # Rules
    ///
    /// - Numeric types (Integer, Float, ScaledInteger, Decimal) are mutually compatible
    /// - String is only compatible with String
    /// - Boolean is only compatible with Boolean
    /// - Temporal types (Date, Timestamp) are compatible with each other
    /// - Interval is only compatible with Interval
    /// - NULL is compatible with any type
    pub fn are_comparable(left: TypeCategory, right: TypeCategory) -> bool {
        // NULL is always compatible
        if matches!(left, TypeCategory::Any) || matches!(right, TypeCategory::Any) {
            return true;
        }

        match (left, right) {
            // Exact type match
            (a, b) if a == b => true,

            // Numeric type compatibility (Integer, Float, Decimal, ScaledInteger all compatible)
            (TypeCategory::Integer, TypeCategory::Float) => true,
            (TypeCategory::Float, TypeCategory::Integer) => true,
            (TypeCategory::Integer, TypeCategory::Decimal) => true,
            (TypeCategory::Decimal, TypeCategory::Integer) => true,
            (TypeCategory::Float, TypeCategory::Decimal) => true,
            (TypeCategory::Decimal, TypeCategory::Float) => true,
            (TypeCategory::Integer, TypeCategory::ScaledInteger) => true,
            (TypeCategory::ScaledInteger, TypeCategory::Integer) => true,
            (TypeCategory::Float, TypeCategory::ScaledInteger) => true,
            (TypeCategory::ScaledInteger, TypeCategory::Float) => true,
            (TypeCategory::Decimal, TypeCategory::ScaledInteger) => true,
            (TypeCategory::ScaledInteger, TypeCategory::Decimal) => true,

            // Temporal type compatibility (Date and Timestamp can compare)
            (TypeCategory::Date, TypeCategory::Timestamp) => true,
            (TypeCategory::Timestamp, TypeCategory::Date) => true,

            // Everything else is incompatible
            _ => false,
        }
    }

    /// Check if a type is sortable (can be used in ORDER BY)
    pub fn is_sortable(category: TypeCategory) -> bool {
        matches!(
            category,
            TypeCategory::Integer
                | TypeCategory::Float
                | TypeCategory::Decimal
                | TypeCategory::ScaledInteger
                | TypeCategory::String
                | TypeCategory::Boolean
                | TypeCategory::Date
                | TypeCategory::Timestamp
                | TypeCategory::Interval
        )
    }

    /// Check if a type can be used in arithmetic operations
    pub fn is_numeric(category: TypeCategory) -> bool {
        matches!(
            category,
            TypeCategory::Integer
                | TypeCategory::Float
                | TypeCategory::Decimal
                | TypeCategory::ScaledInteger
        )
    }

    /// Check if two types are compatible for arithmetic operations
    pub fn are_arithmetic_compatible(left: TypeCategory, right: TypeCategory) -> bool {
        (Self::is_numeric(left) || matches!(left, TypeCategory::Any))
            && (Self::is_numeric(right) || matches!(right, TypeCategory::Any))
    }

    /// Check if a type can be compared with NULL
    pub fn is_null_comparable(category: TypeCategory) -> bool {
        !matches!(category, TypeCategory::Interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_compatibility() {
        assert!(TypeValidator::are_comparable(
            TypeCategory::Integer,
            TypeCategory::Float
        ));
        assert!(TypeValidator::are_comparable(
            TypeCategory::Float,
            TypeCategory::Integer
        ));
        assert!(TypeValidator::are_comparable(
            TypeCategory::ScaledInteger,
            TypeCategory::Decimal
        ));
    }

    #[test]
    fn test_string_incompatibility() {
        assert!(!TypeValidator::are_comparable(
            TypeCategory::String,
            TypeCategory::Integer
        ));
        assert!(!TypeValidator::are_comparable(
            TypeCategory::String,
            TypeCategory::Float
        ));
    }

    #[test]
    fn test_temporal_compatibility() {
        assert!(TypeValidator::are_comparable(
            TypeCategory::Date,
            TypeCategory::Timestamp
        ));
        assert!(TypeValidator::are_comparable(
            TypeCategory::Timestamp,
            TypeCategory::Date
        ));
    }

    #[test]
    fn test_null_compatibility() {
        assert!(TypeValidator::are_comparable(
            TypeCategory::Any,
            TypeCategory::Integer
        ));
        assert!(TypeValidator::are_comparable(
            TypeCategory::Any,
            TypeCategory::String
        ));
    }

    #[test]
    fn test_sortable_types() {
        assert!(TypeValidator::is_sortable(TypeCategory::Integer));
        assert!(TypeValidator::is_sortable(TypeCategory::String));
        assert!(TypeValidator::is_sortable(TypeCategory::Timestamp));
        assert!(!TypeValidator::is_sortable(TypeCategory::Any));
    }

    #[test]
    fn test_numeric_detection() {
        assert!(TypeValidator::is_numeric(TypeCategory::Integer));
        assert!(TypeValidator::is_numeric(TypeCategory::Float));
        assert!(!TypeValidator::is_numeric(TypeCategory::String));
    }

    #[test]
    fn test_arithmetic_compatibility() {
        assert!(TypeValidator::are_arithmetic_compatible(
            TypeCategory::Integer,
            TypeCategory::Float
        ));
        assert!(!TypeValidator::are_arithmetic_compatible(
            TypeCategory::String,
            TypeCategory::Integer
        ));
    }
}
