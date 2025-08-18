//! Built-in SQL function implementations.
//!
//! This module implements all supported SQL functions including:
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - String functions (UPPER, LOWER, SUBSTRING, REPLACE)
//! - Math functions (ABS, ROUND, CEIL, FLOOR)
//! - Header functions (HEADER, HEADER_KEYS, HAS_HEADER)
//! - JSON functions (JSON_EXTRACT, JSON_VALUE)
//! - Conversion functions (CAST)

use crate::ferris::sql::ast::{Expr, LiteralValue};
use crate::ferris::sql::error::SqlError;
use super::super::types::{FieldValue, StreamRecord};
use super::evaluator::ExpressionEvaluator;

/// Provides built-in SQL function implementations
pub struct BuiltinFunctions;

impl BuiltinFunctions {
    /// Evaluates a function call against a record
    ///
    /// Takes a function call AST node and evaluates it, returning the result.
    /// This is the main entry point for all built-in function evaluation.
    pub fn evaluate_function(func: &Expr, record: &StreamRecord) -> Result<FieldValue, SqlError> {
        match func {
            Expr::Function { name, args } => {
                Self::evaluate_function_by_name(name, args, record)
            }
            _ => Err(SqlError::ExecutionError {
                message: "Expected function expression".to_string(),
                query: None,
            }),
        }
    }

    /// Evaluates a function by name with arguments
    pub fn evaluate_function_by_name(
        name: &str,
        args: &[Expr],
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        match name.to_uppercase().as_str() {
            // Aggregate functions
            "COUNT" => Self::count_function(args, record),
            "SUM" => Self::sum_function(args, record),
            "AVG" => Self::avg_function(args, record),
            "MIN" => Self::min_function(args, record),
            "MAX" => Self::max_function(args, record),
            "APPROX_COUNT_DISTINCT" => Self::approx_count_distinct_function(args, record),
            "FIRST_VALUE" => Self::first_value_function(args, record),
            "LAST_VALUE" => Self::last_value_function(args, record),
            "LISTAGG" => Self::listagg_function(args, record),

            // Header functions
            "HEADER" => Self::header_function(args, record),
            "HEADER_KEYS" => Self::header_keys_function(args, record),
            "HAS_HEADER" => Self::has_header_function(args, record),

            // Math functions
            "ABS" => Self::abs_function(args, record),
            "ROUND" => Self::round_function(args, record),
            "CEIL" | "CEILING" => Self::ceil_function(args, record),
            "FLOOR" => Self::floor_function(args, record),
            "SQRT" => Self::sqrt_function(args, record),
            "POWER" | "POW" => Self::power_function(args, record),
            "MOD" => Self::mod_function(args, record),

            // String functions
            "UPPER" => Self::upper_function(args, record),
            "LOWER" => Self::lower_function(args, record),
            "SUBSTRING" => Self::substring_function(args, record),
            "REPLACE" => Self::replace_function(args, record),
            "TRIM" => Self::trim_function(args, record),
            "LTRIM" => Self::ltrim_function(args, record),
            "RTRIM" => Self::rtrim_function(args, record),
            "LENGTH" | "LEN" => Self::length_function(args, record),
            "SPLIT" => Self::split_function(args, record),
            "JOIN" => Self::join_function(args, record),

            // JSON functions
            "JSON_EXTRACT" => Self::json_extract_function(args, record),
            "JSON_VALUE" => Self::json_value_function(args, record),

            // Conversion functions
            "CAST" => Self::cast_function(args, record),

            // System functions
            "TIMESTAMP" => Self::timestamp_function(args, record),

            _ => Err(SqlError::ExecutionError {
                message: format!("Unknown function: {}", name),
                query: None,
            }),
        }
    }

    // Aggregate Functions

    fn count_function(args: &[Expr], _record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() > 1 {
            return Err(SqlError::ExecutionError {
                message: "COUNT takes 0 or 1 arguments".to_string(),
                query: None,
            });
        }
        // Simplified for streaming - returns 1 for each record
        Ok(FieldValue::Integer(1))
    }

    fn sum_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "SUM requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn avg_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "AVG requires exactly one argument".to_string(),
                query: None,
            });
        }
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn min_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "MIN requires exactly one argument".to_string(),
                query: None,
            });
        }
        // For streaming, return the current value (full aggregation would require state)
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn max_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "MAX requires exactly one argument".to_string(),
                query: None,
            });
        }
        // For streaming, return the current value (full aggregation would require state)
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn approx_count_distinct_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "APPROX_COUNT_DISTINCT requires exactly one argument".to_string(),
                query: None,
            });
        }
        // For streaming, simplified implementation - returns 1 if value is not null
        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::Null => Ok(FieldValue::Integer(0)),
            _ => Ok(FieldValue::Integer(1)),
        }
    }

    fn first_value_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "FIRST_VALUE requires exactly one argument".to_string(),
                query: None,
            });
        }
        // For streaming, return the current value (full windowing would require state)
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn last_value_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "LAST_VALUE requires exactly one argument".to_string(),
                query: None,
            });
        }
        // For streaming, return the current value (full windowing would require state)
        ExpressionEvaluator::evaluate_expression_value(&args[0], record)
    }

    fn listagg_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        // LISTAGG(expression, delimiter) or LISTAGG(DISTINCT expression, delimiter)
        if args.len() < 2 || args.len() > 3 {
            return Err(SqlError::ExecutionError {
                message: "LISTAGG requires 2 arguments: LISTAGG(expression, delimiter) or LISTAGG(DISTINCT expression, delimiter)".to_string(),
                query: None,
            });
        }

        // For streaming, we can only aggregate the current record
        // In a real implementation, this would need windowing or group by support
        let value_expr = if args.len() == 3 {
            // Handle LISTAGG(DISTINCT expression, delimiter) case
            // args[0] would be the DISTINCT keyword (but we simplify this for now)
            &args[1]
        } else {
            // Handle LISTAGG(expression, delimiter) case
            &args[0]
        };

        let delimiter_expr = &args[args.len() - 1]; // Last argument is always delimiter

        let value = ExpressionEvaluator::evaluate_expression_value(value_expr, record)?;
        let delimiter = ExpressionEvaluator::evaluate_expression_value(delimiter_expr, record)?;

        match (value, delimiter) {
            (FieldValue::String(val), FieldValue::String(_delim)) => {
                // For single record, just return the value
                // In real streaming aggregation, this would collect values with delimiter
                Ok(FieldValue::String(val))
            }
            (FieldValue::Array(arr), FieldValue::String(delim)) => {
                // If the input is an array, concatenate all string values
                let string_vals: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| match v {
                        FieldValue::String(s) => Ok(s.clone()),
                        FieldValue::Integer(i) => Ok(i.to_string()),
                        FieldValue::Float(f) => Ok(f.to_string()),
                        FieldValue::Boolean(b) => Ok(b.to_string()),
                        FieldValue::Null => Ok("".to_string()),
                        _ => Err(SqlError::ExecutionError {
                            message: "LISTAGG array elements must be convertible to string"
                                .to_string(),
                            query: None,
                        }),
                    })
                    .collect();

                match string_vals {
                    Ok(vals) => Ok(FieldValue::String(vals.join(&delim))),
                    Err(e) => Err(e),
                }
            }
            (FieldValue::Null, _) => Ok(FieldValue::String("".to_string())),
            _ => Err(SqlError::ExecutionError {
                message:
                    "LISTAGG requires string or array first argument and string delimiter"
                        .to_string(),
                query: None,
            }),
        }
    }

    // Header Functions

    fn header_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "HEADER requires exactly one argument (header key)".to_string(),
                query: None,
            });
        }

        // Evaluate the argument to get the header key
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

        // Look up the header value
        match record.headers.get(&header_key) {
            Some(value) => Ok(FieldValue::String(value.clone())),
            None => Ok(FieldValue::Null),
        }
    }

    fn header_keys_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "HEADER_KEYS takes no arguments".to_string(),
                query: None,
            });
        }

        // Return comma-separated list of header keys
        let keys: Vec<String> = record.headers.keys().cloned().collect();
        Ok(FieldValue::String(keys.join(",")))
    }

    fn has_header_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "HAS_HEADER requires exactly one argument (header key)"
                    .to_string(),
                query: None,
            });
        }

        // Evaluate the argument to get the header key
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

        // Check if header exists
        Ok(FieldValue::Boolean(
            record.headers.contains_key(&header_key),
        ))
    }

    // Math Functions

    fn abs_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "ABS can only be applied to numeric values".to_string(),
                query: None,
            }),
        }
    }

    fn round_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() < 1 || args.len() > 2 {
            return Err(SqlError::ExecutionError {
                message: "ROUND requires 1 or 2 arguments: ROUND(number[, precision])"
                    .to_string(),
                query: None,
            });
        }
        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let precision = if args.len() == 2 {
            match ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                FieldValue::Integer(p) => p as i32,
                FieldValue::Null => return Ok(FieldValue::Null), // If precision is NULL, result is NULL
                _ => {
                    return Err(SqlError::ExecutionError {
                        message: "ROUND precision must be an integer".to_string(),
                        query: None,
                    });
                }
            }
        } else {
            0
        };

        match value {
            FieldValue::Float(f) => {
                let multiplier = 10_f64.powi(precision);
                Ok(FieldValue::Float((f * multiplier).round() / multiplier))
            }
            FieldValue::Integer(i) => Ok(FieldValue::Integer(i)), // Integers don't need rounding
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "ROUND requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn ceil_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "CEIL requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn floor_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "FLOOR requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn sqrt_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "SQRT requires exactly one argument".to_string(),
                query: None,
            });
        }
        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::Float(f) => {
                if f < 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "SQRT cannot be applied to negative numbers".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(f.sqrt()))
                }
            }
            FieldValue::Integer(i) => {
                if i < 0 {
                    Err(SqlError::ExecutionError {
                        message: "SQRT cannot be applied to negative numbers".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float((i as f64).sqrt()))
                }
            }
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "SQRT requires numeric argument".to_string(),
                query: None,
            }),
        }
    }

    fn power_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "POWER requires exactly two arguments".to_string(),
                query: None,
            });
        }
        let base = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let exponent = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        match (base, exponent) {
            (FieldValue::Float(b), FieldValue::Float(e)) => Ok(FieldValue::Float(b.powf(e))),
            (FieldValue::Integer(b), FieldValue::Integer(e)) => {
                Ok(FieldValue::Float((b as f64).powf(e as f64)))
            }
            (FieldValue::Integer(b), FieldValue::Float(e)) => {
                Ok(FieldValue::Float((b as f64).powf(e)))
            }
            (FieldValue::Float(b), FieldValue::Integer(e)) => {
                Ok(FieldValue::Float(b.powf(e as f64)))
            }
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "POWER requires numeric arguments".to_string(),
                query: None,
            }),
        }
    }

    fn mod_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "MOD requires exactly two arguments".to_string(),
                query: None,
            });
        }
        let dividend = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let divisor = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        match (dividend, divisor) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => {
                if b == 0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero in MOD".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Integer(a % b))
                }
            }
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                if b == 0.0 {
                    Err(SqlError::ExecutionError {
                        message: "Division by zero in MOD".to_string(),
                        query: None,
                    })
                } else {
                    Ok(FieldValue::Float(a % b))
                }
            }
            (FieldValue::Null, _) | (_, FieldValue::Null) => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "MOD requires numeric arguments".to_string(),
                query: None,
            }),
        }
    }

    // String Functions

    fn upper_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "UPPER requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn lower_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "LOWER requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn substring_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() < 2 || args.len() > 3 {
            return Err(SqlError::ExecutionError {
                message: "SUBSTRING requires 2 or 3 arguments: SUBSTRING(string, start[, length])".to_string(),
                query: None
            });
        }

        let string_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let start_val = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        let (string, start) = match (string_val, start_val) {
            (FieldValue::String(s), FieldValue::Integer(start)) => (s, start as usize),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "SUBSTRING requires string and integer arguments".to_string(),
                    query: None,
                });
            }
        };

        // Handle optional length argument
        let result = if args.len() == 3 {
            let length_val = ExpressionEvaluator::evaluate_expression_value(&args[2], record)?;
            match length_val {
                FieldValue::Integer(length) => string
                    .chars()
                    .skip(start.saturating_sub(1))
                    .take(length as usize)
                    .collect(),
                _ => {
                    return Err(SqlError::ExecutionError {
                        message: "SUBSTRING length must be an integer".to_string(),
                        query: None,
                    });
                }
            }
        } else {
            // No length specified, take from start to end
            string.chars().skip(start.saturating_sub(1)).collect()
        };

        Ok(FieldValue::String(result))
    }

    fn replace_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 3 {
            return Err(SqlError::ExecutionError {
                message: "REPLACE requires exactly 3 arguments: REPLACE(string, search, replacement)".to_string(),
                query: None,
            });
        }

        let string_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let search_val = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;
        let replacement_val = ExpressionEvaluator::evaluate_expression_value(&args[2], record)?;

        match (string_val, search_val, replacement_val) {
            (FieldValue::String(s), FieldValue::String(search), FieldValue::String(replacement)) => {
                Ok(FieldValue::String(s.replace(&search, &replacement)))
            }
            (FieldValue::Null, _, _) | (_, FieldValue::Null, _) | (_, _, FieldValue::Null) => {
                Ok(FieldValue::Null)
            }
            _ => Err(SqlError::ExecutionError {
                message: "REPLACE requires string arguments".to_string(),
                query: None,
            }),
        }
    }

    fn trim_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "TRIM requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn ltrim_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "LTRIM requires exactly one argument".to_string(),
                query: None,
            });
        }
        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::String(s.trim_start().to_string())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "LTRIM requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn rtrim_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: "RTRIM requires exactly one argument".to_string(),
                query: None,
            });
        }
        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        match value {
            FieldValue::String(s) => Ok(FieldValue::String(s.trim_end().to_string())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: "RTRIM requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn length_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
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
                message: "LENGTH requires string argument".to_string(),
                query: None,
            }),
        }
    }

    fn split_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "SPLIT requires exactly two arguments: SPLIT(string, delimiter)"
                    .to_string(),
                query: None,
            });
        }

        let string_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let delimiter_val = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        let (string, delimiter) = match (string_val, delimiter_val) {
            (FieldValue::String(s), FieldValue::String(d)) => (s, d),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "SPLIT requires string arguments".to_string(),
                    query: None,
                });
            }
        };

        // Return first part for simplicity (full array support would need array type)
        let parts: Vec<&str> = string.split(&delimiter).collect();
        Ok(FieldValue::String(parts.get(0).unwrap_or(&"").to_string()))
    }

    fn join_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() < 2 {
            return Err(SqlError::ExecutionError {
                message: "JOIN requires at least two arguments".to_string(),
                query: None,
            });
        }

        // Evaluate all arguments and join them with the first argument as delimiter
        let delimiter_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let delimiter = match delimiter_val {
            FieldValue::String(d) => d,
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "JOIN delimiter must be a string".to_string(),
                    query: None,
                });
            }
        };

        let mut parts = Vec::new();
        for arg in &args[1..] {
            let val = ExpressionEvaluator::evaluate_expression_value(arg, record)?;
            let str_val = match val {
                FieldValue::String(s) => s,
                FieldValue::Integer(i) => i.to_string(),
                FieldValue::Float(f) => f.to_string(),
                FieldValue::Boolean(b) => b.to_string(),
                FieldValue::Null => "NULL".to_string(),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Decimal(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_) => val.to_display_string(),
            };
            parts.push(str_val);
        }

        Ok(FieldValue::String(parts.join(&delimiter)))
    }

    // JSON Functions

    fn json_extract_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "JSON_EXTRACT requires exactly two arguments: JSON_EXTRACT(json_string, path)".to_string(),
                query: None
            });
        }

        let json_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let path_val = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        let (json_string, path) = match (json_val, path_val) {
            (FieldValue::String(j), FieldValue::String(p)) => (j, p),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "JSON_EXTRACT requires string arguments".to_string(),
                    query: None,
                });
            }
        };

        Self::extract_json_value(&json_string, &path)
    }

    fn json_value_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "JSON_VALUE requires exactly two arguments: JSON_VALUE(json_string, path)".to_string(),
                query: None
            });
        }

        let json_val = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let path_val = ExpressionEvaluator::evaluate_expression_value(&args[1], record)?;

        let (json_string, path) = match (json_val, path_val) {
            (FieldValue::String(j), FieldValue::String(p)) => (j, p),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "JSON_VALUE requires string arguments".to_string(),
                    query: None,
                });
            }
        };

        Self::extract_json_value(&json_string, &path)
    }

    // Helper function for JSON extraction
    fn extract_json_value(json_string: &str, path: &str) -> Result<FieldValue, SqlError> {
        // Simplified JSON extraction - in real implementation would use proper JSON parsing
        if path == "$" {
            Ok(FieldValue::String(json_string.to_string()))
        } else {
            // For now, return null for any complex path
            Ok(FieldValue::Null)
        }
    }

    // Conversion Functions

    fn cast_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: "CAST requires exactly two arguments: CAST(value, type)"
                    .to_string(),
                query: None,
            });
        }

        let value = ExpressionEvaluator::evaluate_expression_value(&args[0], record)?;
        let target_type = match &args[1] {
            Expr::Literal(LiteralValue::String(type_str)) => type_str.to_uppercase(),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "CAST target type must be a string literal".to_string(),
                    query: None,
                });
            }
        };

        Self::cast_value(value, &target_type)
    }

    // Helper function for type casting
    fn cast_value(value: FieldValue, target_type: &str) -> Result<FieldValue, SqlError> {
        match target_type {
            "INTEGER" | "INT" | "BIGINT" => match value {
                FieldValue::String(s) => {
                    s.parse::<i64>()
                        .map(FieldValue::Integer)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to INTEGER", s),
                            query: None,
                        })
                }
                FieldValue::Float(f) => Ok(FieldValue::Integer(f as i64)),
                FieldValue::Boolean(b) => Ok(FieldValue::Integer(if b { 1 } else { 0 })),
                FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {:?} to INTEGER", value),
                    query: None,
                }),
            },
            "FLOAT" | "DOUBLE" | "REAL" => match value {
                FieldValue::String(s) => {
                    s.parse::<f64>()
                        .map(FieldValue::Float)
                        .map_err(|_| SqlError::ExecutionError {
                            message: format!("Cannot cast '{}' to FLOAT", s),
                            query: None,
                        })
                }
                FieldValue::Integer(i) => Ok(FieldValue::Float(i as f64)),
                FieldValue::Boolean(b) => Ok(FieldValue::Float(if b { 1.0 } else { 0.0 })),
                FieldValue::Float(f) => Ok(FieldValue::Float(f)),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {:?} to FLOAT", value),
                    query: None,
                }),
            },
            "STRING" | "VARCHAR" | "TEXT" => match value {
                FieldValue::String(s) => Ok(FieldValue::String(s)),
                FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
                FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
                FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Ok(FieldValue::String(value.to_display_string())),
            },
            "BOOLEAN" | "BOOL" => match value {
                FieldValue::String(s) => match s.to_lowercase().as_str() {
                    "true" | "1" | "yes" | "on" => Ok(FieldValue::Boolean(true)),
                    "false" | "0" | "no" | "off" => Ok(FieldValue::Boolean(false)),
                    _ => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to BOOLEAN", s),
                        query: None,
                    }),
                },
                FieldValue::Integer(i) => Ok(FieldValue::Boolean(i != 0)),
                FieldValue::Float(f) => Ok(FieldValue::Boolean(f != 0.0)),
                FieldValue::Boolean(b) => Ok(FieldValue::Boolean(b)),
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {:?} to BOOLEAN", value),
                    query: None,
                }),
            },
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported cast target type: {}", target_type),
                query: None,
            }),
        }
    }

    // System Functions

    fn timestamp_function(args: &[Expr], record: &StreamRecord) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "TIMESTAMP() takes no arguments".to_string(),
                query: None,
            });
        }
        // Return current record timestamp
        Ok(FieldValue::Integer(record.timestamp))
    }
}