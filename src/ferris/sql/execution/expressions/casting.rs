use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::FieldValue;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::str::FromStr;

/// Cast a FieldValue to the specified target type
pub fn cast_value(value: FieldValue, target_type: &str) -> Result<FieldValue, SqlError> {
    match target_type {
        "INTEGER" | "INT" => {
            match value {
                FieldValue::Integer(i) => Ok(FieldValue::Integer(i)),
                FieldValue::Float(f) => Ok(FieldValue::Integer(f as i64)),
                FieldValue::String(s) => s.parse::<i64>().map(FieldValue::Integer).map_err(|_| {
                    SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to INTEGER", s),
                        query: None,
                    }
                }),
                FieldValue::Boolean(b) => Ok(FieldValue::Integer(if b { 1 } else { 0 })),
                FieldValue::Decimal(d) => {
                    // Convert decimal to integer, truncating fractional part
                    let int_part = d.trunc();
                    match int_part.to_string().parse::<i64>() {
                        Ok(i) => Ok(FieldValue::Integer(i)),
                        Err(_) => Err(SqlError::ExecutionError {
                            message: format!("Cannot cast DECIMAL {} to INTEGER", d),
                            query: None,
                        }),
                    }
                }
                FieldValue::Null => Ok(FieldValue::Null),
                FieldValue::Date(_)
                | FieldValue::Timestamp(_)
                | FieldValue::Array(_)
                | FieldValue::Map(_)
                | FieldValue::Struct(_) => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to INTEGER", value.type_name()),
                    query: None,
                }),
            }
        }
        "FLOAT" | "DOUBLE" => match value {
            FieldValue::Integer(i) => Ok(FieldValue::Float(i as f64)),
            FieldValue::Float(f) => Ok(FieldValue::Float(f)),
            FieldValue::String(s) => {
                s.parse::<f64>()
                    .map(FieldValue::Float)
                    .map_err(|_| SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to FLOAT", s),
                        query: None,
                    })
            }
            FieldValue::Boolean(b) => Ok(FieldValue::Float(if b { 1.0 } else { 0.0 })),
            FieldValue::Decimal(d) => {
                // Convert decimal to float
                match d.to_string().parse::<f64>() {
                    Ok(f) => Ok(FieldValue::Float(f)),
                    Err(_) => Err(SqlError::ExecutionError {
                        message: format!("Cannot cast DECIMAL {} to FLOAT", d),
                        query: None,
                    }),
                }
            }
            FieldValue::Null => Ok(FieldValue::Null),
            FieldValue::Date(_)
            | FieldValue::Timestamp(_)
            | FieldValue::Array(_)
            | FieldValue::Map(_)
            | FieldValue::Struct(_) => Err(SqlError::ExecutionError {
                message: format!("Cannot cast {} to FLOAT", value.type_name()),
                query: None,
            }),
        },
        "STRING" | "VARCHAR" | "TEXT" => match value {
            FieldValue::Integer(i) => Ok(FieldValue::String(i.to_string())),
            FieldValue::Float(f) => Ok(FieldValue::String(f.to_string())),
            FieldValue::String(s) => Ok(FieldValue::String(s)),
            FieldValue::Boolean(b) => Ok(FieldValue::String(b.to_string())),
            FieldValue::Null => Ok(FieldValue::String("NULL".to_string())),
            FieldValue::Date(_)
            | FieldValue::Timestamp(_)
            | FieldValue::Decimal(_)
            | FieldValue::Array(_)
            | FieldValue::Map(_)
            | FieldValue::Struct(_) => Ok(FieldValue::String(value.to_display_string())),
        },
        "BOOLEAN" | "BOOL" => match value {
            FieldValue::Integer(i) => Ok(FieldValue::Boolean(i != 0)),
            FieldValue::Float(f) => Ok(FieldValue::Boolean(f != 0.0)),
            FieldValue::String(s) => match s.to_uppercase().as_str() {
                "TRUE" | "T" | "1" => Ok(FieldValue::Boolean(true)),
                "FALSE" | "F" | "0" => Ok(FieldValue::Boolean(false)),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast '{}' to BOOLEAN", s),
                    query: None,
                }),
            },
            FieldValue::Boolean(b) => Ok(FieldValue::Boolean(b)),
            FieldValue::Null => Ok(FieldValue::Null),
            FieldValue::Date(_)
            | FieldValue::Timestamp(_)
            | FieldValue::Decimal(_)
            | FieldValue::Array(_)
            | FieldValue::Map(_)
            | FieldValue::Struct(_) => Err(SqlError::ExecutionError {
                message: format!("Cannot cast {} to BOOLEAN", value.type_name()),
                query: None,
            }),
        },
        "DATE" => match value {
            FieldValue::Date(d) => Ok(FieldValue::Date(d)),
            FieldValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .or_else(|_| NaiveDate::parse_from_str(&s, "%Y/%m/%d"))
                .or_else(|_| NaiveDate::parse_from_str(&s, "%m/%d/%Y"))
                .or_else(|_| NaiveDate::parse_from_str(&s, "%d-%m-%Y"))
                .map(FieldValue::Date)
                .map_err(|_| SqlError::ExecutionError {
                    message: format!("Cannot cast '{}' to DATE. Expected format: YYYY-MM-DD", s),
                    query: None,
                }),
            FieldValue::Timestamp(ts) => Ok(FieldValue::Date(ts.date())),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: format!("Cannot cast {} to DATE", value.type_name()),
                query: None,
            }),
        },
        "TIMESTAMP" | "DATETIME" => {
            match value {
                FieldValue::Timestamp(ts) => Ok(FieldValue::Timestamp(ts)),
                FieldValue::Date(d) => Ok(FieldValue::Timestamp(d.and_hms_opt(0, 0, 0).unwrap())),
                FieldValue::String(s) => {
                    // Try various timestamp formats
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.3f"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3f"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y/%m/%d %H:%M:%S"))
                    .or_else(|_| {
                        // Try parsing as date only and add time
                        NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                    })
                    .map(FieldValue::Timestamp)
                    .map_err(|_| SqlError::ExecutionError {
                        message: format!("Cannot cast '{}' to TIMESTAMP. Expected format: YYYY-MM-DD HH:MM:SS", s),
                        query: None,
                    })
                }
                FieldValue::Integer(i) => {
                    // Treat as Unix timestamp (seconds)
                    let dt =
                        DateTime::from_timestamp(i, 0).ok_or_else(|| SqlError::ExecutionError {
                            message: format!("Invalid Unix timestamp: {}", i),
                            query: None,
                        })?;
                    Ok(FieldValue::Timestamp(dt.naive_utc()))
                }
                FieldValue::Null => Ok(FieldValue::Null),
                _ => Err(SqlError::ExecutionError {
                    message: format!("Cannot cast {} to TIMESTAMP", value.type_name()),
                    query: None,
                }),
            }
        }
        "DECIMAL" | "NUMERIC" => match value {
            FieldValue::Decimal(d) => Ok(FieldValue::Decimal(d)),
            FieldValue::Integer(i) => Ok(FieldValue::Decimal(Decimal::from(i))),
            FieldValue::Float(f) => Decimal::from_str(&f.to_string())
                .map(FieldValue::Decimal)
                .map_err(|_| SqlError::ExecutionError {
                    message: format!("Cannot cast float {} to DECIMAL", f),
                    query: None,
                }),
            FieldValue::String(s) => Decimal::from_str(&s).map(FieldValue::Decimal).map_err(|_| {
                SqlError::ExecutionError {
                    message: format!("Cannot cast '{}' to DECIMAL", s),
                    query: None,
                }
            }),
            FieldValue::Boolean(b) => Ok(FieldValue::Decimal(if b {
                Decimal::ONE
            } else {
                Decimal::ZERO
            })),
            FieldValue::Null => Ok(FieldValue::Null),
            _ => Err(SqlError::ExecutionError {
                message: format!("Cannot cast {} to DECIMAL", value.type_name()),
                query: None,
            }),
        },
        _ => Err(SqlError::ExecutionError {
            message: format!("Unsupported cast target type: {}", target_type),
            query: None,
        }),
    }
}
