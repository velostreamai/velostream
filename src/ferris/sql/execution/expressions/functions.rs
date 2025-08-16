use super::evaluator::ExpressionEvaluator;
use crate::ferris::sql::ast::Expr;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::{FieldValue, HeaderMutation, StreamRecord};

/// Built-in SQL function evaluation
pub struct FunctionEvaluator;

impl FunctionEvaluator {
    /// Evaluate a function call
    pub fn evaluate_function(
        name: &str,
        args: &[Expr],
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        let mut header_mutations = Vec::new();
        Self::evaluate_function_with_mutations(name, args, record, &mut header_mutations)
    }

    /// Evaluate a function call with header mutations support
    pub fn evaluate_function_with_mutations(
        name: &str,
        args: &[Expr],
        record: &StreamRecord,
        header_mutations: &mut Vec<HeaderMutation>,
    ) -> Result<FieldValue, SqlError> {
        match name.to_uppercase().as_str() {
            // Aggregate functions (simplified for streaming)
            "COUNT" => Self::evaluate_count(args),
            "SUM" => Self::evaluate_sum(args, record),
            "AVG" => Self::evaluate_avg(args, record),
            "MIN" => Self::evaluate_min(args, record),
            "MAX" => Self::evaluate_max(args, record),

            // String functions
            "CONCAT" => Self::evaluate_concat(args, record),
            "LENGTH" => Self::evaluate_length(args, record),
            "UPPER" => Self::evaluate_upper(args, record),
            "LOWER" => Self::evaluate_lower(args, record),
            "TRIM" => Self::evaluate_trim(args, record),

            // Math functions
            "ABS" => Self::evaluate_abs(args, record),
            "ROUND" => Self::evaluate_round(args, record),
            "CEIL" => Self::evaluate_ceil(args, record),
            "FLOOR" => Self::evaluate_floor(args, record),

            // Date/time functions
            "NOW" => Self::evaluate_now(),
            "CURRENT_TIMESTAMP" => Self::evaluate_current_timestamp(),

            // Header functions
            "HEADER" => Self::evaluate_header(args, record),
            "HAS_HEADER" => Self::evaluate_has_header(args, record),
            "HEADER_KEYS" => Self::evaluate_header_keys(record),

            // Utility functions
            "COALESCE" => Self::evaluate_coalesce(args, record),
            "NULLIF" => Self::evaluate_nullif(args, record),

            _ => Err(SqlError::ExecutionError {
                message: format!("Unknown function {}", name),
                query: None,
            }),
        }
    }

    // Aggregate functions
    fn evaluate_count(_args: &[Expr]) -> Result<FieldValue, SqlError> {
        Ok(FieldValue::Integer(1)) // Simplified for streaming
    }

    fn evaluate_sum(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "SUM requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn evaluate_avg(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "AVG requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn evaluate_min(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "MIN requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn evaluate_max(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "MAX requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    // String functions
    fn evaluate_concat(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.is_empty() {
            return Ok(FieldValue::String(String::new()));
        }

        let mut result = String::new();
        for arg in args {
            let value = ExpressionEvaluator::evaluate_expression_value(arg, record)?;
            match value {
                FieldValue::String(s) => result.push_str(&s),
                FieldValue::Integer(i) => result.push_str(&i.to_string()),
                FieldValue::Float(f) => result.push_str(&f.to_string()),
                FieldValue::Boolean(b) => result.push_str(&b.to_string()),
                FieldValue::Null => {} // NULL values are ignored in CONCAT
                _ => result.push_str(&value.to_display_string()),
            }
        }
        Ok(FieldValue::String(result))
    }

    fn evaluate_length(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "LENGTH requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::Integer(s.len() as i64)),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "LENGTH function requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_upper(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "UPPER requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::String(s.to_uppercase())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "UPPER function requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_lower(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "LOWER requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::String(s.to_lowercase())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "LOWER function requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_trim(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "TRIM requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::String(s.trim().to_string())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "TRIM function requires string argument".to_string(),
                query: None,
            }),
        }
    }

    // Math functions
    fn evaluate_abs(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "ABS requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::Integer(i) => Ok(FieldValue::Integer(i.abs())),
            FieldValue::Float(f) => Ok(FieldValue::Float(f.abs())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "ABS function requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_round(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 && args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "ROUND requires one or two arguments".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let precision = if args.len() == 2 {
            match ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                FieldValue::Integer(p) => p as u32,
                _ => 0,
            }
        } else {
            0
        };

        match value {
            FieldValue::Float(f) => {
                let multiplier = 10.0_f64.powi(precision as i32);
                Ok(FieldValue::Float((f * multiplier).round() / multiplier))
            }
            FieldValue::Integer(i) => Ok(FieldValue::Integer(i)), // Integers don't need rounding
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "ROUND function requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_ceil(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "CEIL requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::Float(f) => Ok(FieldValue::Integer(f.ceil() as i64)),
            FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "CEIL function requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn evaluate_floor(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "FLOOR requires exactly one argument".to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::Float(f) => Ok(FieldValue::Integer(f.floor() as i64)),
            FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "FLOOR function requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    // Date/time functions
    fn evaluate_now() -> Result<FieldValue, SqlError> {
        use chrono::Utc;
        Ok(FieldValue::Integer(Utc::now().timestamp_millis()))
    }

    fn evaluate_current_timestamp() -> Result<FieldValue, SqlError> {
        use chrono::Utc;
        Ok(FieldValue::Timestamp(Utc::now().naive_utc()))
    }

    // Header functions
    fn evaluate_header(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "HEADER requires exactly one argument (header key)".to_string(),
                query: None,
            });
        }

        let key_value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let header_key = match key_value {
            FieldValue::String(key) => key,
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "HEADER key must be a string".to_string(),
                    query: None,
                });
            }
        };

        Ok(record
            .headers
            .get(&header_key)
            .map(|v| FieldValue::String(v.clone()))
            .unwrap_or(FieldValue::Null))
    }

    fn evaluate_has_header(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "HAS_HEADER requires exactly one argument".to_string(),
                query: None,
            });
        }

        let key_value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let header_key = match key_value {
            FieldValue::String(key) => key,
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "HAS_HEADER key must be a string".to_string(),
                    query: None,
                });
            }
        };

        Ok(FieldValue::Boolean(
            record.headers.contains_key(&header_key),
        ))
    }

    fn evaluate_header_keys(record: &StreamRecord) -> Result<FieldValue, SqlError> {
        let keys: Vec<FieldValue> = record
            .headers
            .keys()
            .map(|k| FieldValue::String(k.clone()))
            .collect();
        Ok(FieldValue::Array(keys))
    }

    // Utility functions
    fn evaluate_coalesce(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "COALESCE requires at least one argument".to_string(),
                query: None,
            });
        }

        for arg in args {
            let value = ExpressionEvaluator::evaluate_expression_value(arg, record)?;
            if !matches!(value, FieldValue::Null) {
                return Ok(value);
            }
        }
        Ok(FieldValue::Null)
    }

    fn evaluate_nullif(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "NULLIF requires exactly two arguments".to_string(),
                query: None,
            });
        }

        let value1 = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let value2 = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        // If the values are equal, return NULL, otherwise return the first value
        if Self::values_equal(&value1, &value2) {
            Ok(FieldValue::Null)
        } else {
            Ok(value1)
        }
    }

    fn values_equal(a: &FieldValue, b: &FieldValue) -> bool {
        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a == b,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Null, FieldValue::Null) => true,
            _ => false,
        }
    }
}
