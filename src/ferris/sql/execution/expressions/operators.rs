use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::FieldValue;

/// Add two numeric values
pub fn add_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
    match (left, right) {
        (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a + b)),
        (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a + b)),
        (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 + b)),
        (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a + *b as f64)),
        _ => Err(SqlError::TypeError {
            expected: "numeric".to_string(),
            actual: "non-numeric".to_string(),
            value: None,
        }),
    }
}

/// Subtract two numeric values
pub fn subtract_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
    match (left, right) {
        (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a - b)),
        (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a - b)),
        (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 - b)),
        (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a - *b as f64)),
        _ => Err(SqlError::TypeError {
            expected: "numeric".to_string(),
            actual: "non-numeric".to_string(),
            value: None,
        }),
    }
}

/// Multiply two numeric values
pub fn multiply_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
    match (left, right) {
        (FieldValue::Integer(a), FieldValue::Integer(b)) => Ok(FieldValue::Integer(a * b)),
        (FieldValue::Float(a), FieldValue::Float(b)) => Ok(FieldValue::Float(a * b)),
        (FieldValue::Integer(a), FieldValue::Float(b)) => Ok(FieldValue::Float(*a as f64 * b)),
        (FieldValue::Float(a), FieldValue::Integer(b)) => Ok(FieldValue::Float(a * *b as f64)),
        _ => Err(SqlError::TypeError {
            expected: "numeric".to_string(),
            actual: "non-numeric".to_string(),
            value: None,
        }),
    }
}

/// Divide two numeric values
pub fn divide_values(left: &FieldValue, right: &FieldValue) -> Result<FieldValue, SqlError> {
    match (left, right) {
        (FieldValue::Integer(a), FieldValue::Integer(b)) => {
            if *b == 0 {
                Err(SqlError::ExecutionError {
                    message: "Division by zero".to_string(),
                    query: None,
                })
            } else {
                Ok(FieldValue::Float(*a as f64 / *b as f64))
            }
        }
        (FieldValue::Float(a), FieldValue::Float(b)) => {
            if *b == 0.0 {
                Err(SqlError::ExecutionError {
                    message: "Division by zero".to_string(),
                    query: None,
                })
            } else {
                Ok(FieldValue::Float(a / b))
            }
        }
        (FieldValue::Integer(a), FieldValue::Float(b)) => {
            if *b == 0.0 {
                Err(SqlError::ExecutionError {
                    message: "Division by zero".to_string(),
                    query: None,
                })
            } else {
                Ok(FieldValue::Float(*a as f64 / b))
            }
        }
        (FieldValue::Float(a), FieldValue::Integer(b)) => {
            if *b == 0 {
                Err(SqlError::ExecutionError {
                    message: "Division by zero".to_string(),
                    query: None,
                })
            } else {
                Ok(FieldValue::Float(a / *b as f64))
            }
        }
        _ => Err(SqlError::TypeError {
            expected: "numeric".to_string(),
            actual: "non-numeric".to_string(),
            value: None,
        }),
    }
}
