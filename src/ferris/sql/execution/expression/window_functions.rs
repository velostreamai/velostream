/*!
# Window Functions Module

Implements SQL window functions for streaming execution context.

Window functions operate over a set of rows related to the current row, but unlike aggregate functions,
they don't cause rows to become grouped into a single output row.

## Supported Window Functions

- **LAG(expr [, offset] [, default])** - Access previous row values
- **LEAD(expr [, offset] [, default])** - Access next row values (limited in streaming)
- **ROW_NUMBER()** - Sequential row numbering
- **RANK()** - Ranking with gaps
- **DENSE_RANK()** - Ranking without gaps
- **FIRST_VALUE(expr)** - First value in window frame
- **LAST_VALUE(expr)** - Last value in window frame
- **NTH_VALUE(expr, n)** - Nth value in window frame
- **PERCENT_RANK()** - Relative rank as percentage
- **CUME_DIST()** - Cumulative distribution
- **NTILE(n)** - Distribute rows into n tiles

## Streaming Context Notes

In streaming data processing, window functions have some limitations:
- LEAD functions cannot look ahead in most cases
- Window frames are typically bounded by available data
- Some functions may return different results than in batch processing
*/

use crate::ferris::sql::ast::{Expr, OverClause};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};

/// Window function evaluator for streaming SQL execution
pub struct WindowFunctions;

impl WindowFunctions {
    /// Evaluate window functions in streaming context
    ///
    /// Note: This is a simplified implementation for streaming. Real window functions
    /// would require proper windowing and buffering, which should be handled at the
    /// processor level for full functionality.
    pub fn evaluate_window_function(
        function_name: &str,
        args: &[Expr],
        _over_clause: &OverClause,
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        // Create a minimal window buffer - for proper implementation, this should come from processor context
        let window_buffer: Vec<StreamRecord> = vec![];

        match function_name.to_uppercase().as_str() {
            "LAG" => Self::evaluate_lag_function(args, record, &window_buffer),
            "LEAD" => Self::evaluate_lead_function(args, record, &window_buffer),
            "ROW_NUMBER" => Self::evaluate_row_number_function(args, &window_buffer),
            "RANK" | "DENSE_RANK" => {
                Self::evaluate_rank_function(function_name, args, &window_buffer)
            }
            "FIRST_VALUE" => Self::evaluate_first_value_function(args, record, &window_buffer),
            "LAST_VALUE" => Self::evaluate_last_value_function(args, record),
            "NTH_VALUE" => Self::evaluate_nth_value_function(args, record, &window_buffer),
            "PERCENT_RANK" => Self::evaluate_percent_rank_function(args, &window_buffer),
            "CUME_DIST" => Self::evaluate_cume_dist_function(args, &window_buffer),
            "NTILE" => Self::evaluate_ntile_function(args, record, &window_buffer),
            other => Err(SqlError::ExecutionError {
                message: format!(
                    "Unsupported window function: '{}'. Supported window functions are: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE",
                    other
                ),
                query: Some(format!("{}(...) OVER (...)", other)),
            }),
        }
    }

    /// Evaluate LAG window function
    fn evaluate_lag_function(
        args: &[Expr],
        record: &StreamRecord,
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        // Validate argument count
        if args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "LAG function requires at least 1 argument (expression)".to_string(),
                query: Some("LAG(expression, [offset], [default_value])".to_string()),
            });
        }
        if args.len() > 3 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "LAG function accepts at most 3 arguments (expression, offset, default_value), but {} were provided",
                    args.len()
                ),
                query: Some("LAG(expression, [offset], [default_value])".to_string()),
            });
        }

        // Parse offset (default is 1)
        let offset = if args.len() >= 2 {
            match crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                FieldValue::Integer(n) => {
                    if n < 0 {
                        return Err(SqlError::ExecutionError {
                            message: format!("LAG offset must be non-negative, got {}", n),
                            query: Some(format!("LAG(expression, {})", n)),
                        });
                    }
                    n as usize
                }
                FieldValue::Null => {
                    return Err(SqlError::ExecutionError {
                        message: "LAG offset cannot be NULL".to_string(),
                        query: Some("LAG(expression, NULL)".to_string()),
                    });
                }
                other => {
                    return Err(SqlError::ExecutionError {
                        message: format!("LAG offset must be an integer, got {}", other.type_name()),
                        query: Some(format!("LAG(expression, {})", other.type_name().to_lowercase())),
                    });
                }
            }
        } else {
            1
        };

        // Parse default value (if provided)
        let default_value = if args.len() == 3 {
            Some(crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[2], record)?)
        } else {
            None
        };

        // Look back in the window buffer
        if offset == 0 {
            // Offset 0 means current record
            crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
        } else if window_buffer.len() >= offset {
            let lag_record = &window_buffer[window_buffer.len() - offset];
            crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], lag_record)
        } else {
            // Not enough records in buffer, return default or NULL
            Ok(default_value.unwrap_or(FieldValue::Null))
        }
    }

    /// Evaluate LEAD window function
    fn evaluate_lead_function(
        args: &[Expr],
        record: &StreamRecord,
        _window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        // Validate argument count
        if args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "LEAD function requires at least 1 argument (expression)".to_string(),
                query: Some("LEAD(expression, [offset], [default_value])".to_string()),
            });
        }
        if args.len() > 3 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "LEAD function accepts at most 3 arguments (expression, offset, default_value), but {} were provided",
                    args.len()
                ),
                query: Some("LEAD(expression, [offset], [default_value])".to_string()),
            });
        }

        // Parse offset (default is 1)
        let offset = if args.len() >= 2 {
            match crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
                FieldValue::Integer(n) => {
                    if n < 0 {
                        return Err(SqlError::ExecutionError {
                            message: format!("LEAD offset must be non-negative, got {}", n),
                            query: Some(format!("LEAD(expression, {})", n)),
                        });
                    }
                    n as usize
                }
                FieldValue::Null => {
                    return Err(SqlError::ExecutionError {
                        message: "LEAD offset cannot be NULL".to_string(),
                        query: Some("LEAD(expression, NULL)".to_string()),
                    });
                }
                other => {
                    return Err(SqlError::ExecutionError {
                        message: format!("LEAD offset must be an integer, got {}", other.type_name()),
                        query: Some(format!("LEAD(expression, {})", other.type_name().to_lowercase())),
                    });
                }
            }
        } else {
            1
        };

        // Parse default value (if provided)
        let default_value = if args.len() == 3 {
            Some(crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[2], record)?)
        } else {
            None
        };

        // For streaming LEAD, we cannot look forward in most cases
        if offset > 0 {
            // Cannot look forward in streaming data
            Ok(default_value.unwrap_or(FieldValue::Null))
        } else {
            // Offset 0 means current record
            crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
        }
    }

    /// Evaluate ROW_NUMBER window function
    fn evaluate_row_number_function(
        args: &[Expr],
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "ROW_NUMBER function takes no arguments, but {} were provided",
                    args.len()
                ),
                query: Some(format!("ROW_NUMBER({} arguments)", args.len())),
            });
        }
        // ROW_NUMBER() OVER (...) - in streaming context, starts at 1
        // For proper implementation, this should come from processor context
        let row_number = window_buffer.len() + 1;
        Ok(FieldValue::Integer(row_number as i64))
    }

    /// Evaluate RANK and DENSE_RANK window functions
    fn evaluate_rank_function(
        function_name: &str,
        args: &[Expr],
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "{} function takes no arguments, but {} were provided",
                    function_name.to_uppercase(),
                    args.len()
                ),
                query: Some(format!(
                    "{}({} arguments)",
                    function_name.to_uppercase(),
                    args.len()
                )),
            });
        }
        // For streaming without proper partitioning, RANK and DENSE_RANK behave like ROW_NUMBER
        let rank = window_buffer.len() + 1;
        Ok(FieldValue::Integer(rank as i64))
    }

    /// Evaluate FIRST_VALUE window function
    fn evaluate_first_value_function(
        args: &[Expr],
        record: &StreamRecord,
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "FIRST_VALUE function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("FIRST_VALUE(expression)".to_string()),
            });
        }
        // Return the value from the first record in the window buffer
        if !window_buffer.is_empty() {
            crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_buffer[0])
        } else {
            // If buffer is empty, evaluate against current record
            crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
        }
    }

    /// Evaluate LAST_VALUE window function
    fn evaluate_last_value_function(
        args: &[Expr],
        record: &StreamRecord,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "LAST_VALUE function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("LAST_VALUE(expression)".to_string()),
            });
        }
        // Return the value from the last record (current record in streaming)
        crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(
            &args[0], record,
        )
    }

    /// Evaluate NTH_VALUE window function
    fn evaluate_nth_value_function(
        args: &[Expr],
        record: &StreamRecord,
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 2 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "NTH_VALUE function requires exactly 2 arguments (expression, n), but {} were provided",
                    args.len()
                ),
                query: Some("NTH_VALUE(expression, n)".to_string()),
            });
        }

        // Parse the nth position
        let nth = match crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
            FieldValue::Integer(n) => {
                if n <= 0 {
                    return Err(SqlError::ExecutionError {
                        message: format!("NTH_VALUE position must be positive, got {}", n),
                        query: Some(format!("NTH_VALUE(expression, {})", n)),
                    });
                }
                n as usize
            }
            FieldValue::Null => {
                return Err(SqlError::ExecutionError {
                    message: "NTH_VALUE position cannot be NULL".to_string(),
                    query: Some("NTH_VALUE(expression, NULL)".to_string()),
                });
            }
            other => {
                return Err(SqlError::ExecutionError {
                    message: format!("NTH_VALUE position must be an integer, got {}", other.type_name()),
                    query: Some(format!("NTH_VALUE(expression, {})", other.type_name().to_lowercase())),
                });
            }
        };

        // Get the nth record from the window buffer (1-indexed)
        let total_records = window_buffer.len() + 1; // +1 for current record
        if nth <= total_records {
            if nth <= window_buffer.len() {
                // nth record is in the buffer
                crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_buffer[nth - 1])
            } else {
                // nth record is the current record
                crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
            }
        } else {
            // nth record doesn't exist
            Ok(FieldValue::Null)
        }
    }

    /// Evaluate PERCENT_RANK window function
    fn evaluate_percent_rank_function(
        args: &[Expr],
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "PERCENT_RANK function takes no arguments, but {} were provided",
                    args.len()
                ),
                query: Some(format!("PERCENT_RANK({} arguments)", args.len())),
            });
        }
        // PERCENT_RANK() = (rank - 1) / (total_rows - 1)
        let current_rank = window_buffer.len() + 1;
        let total_rows = current_rank; // In streaming, we only know current position
        if total_rows <= 1 {
            Ok(FieldValue::Float(0.0))
        } else {
            let percent_rank = (current_rank - 1) as f64 / (total_rows - 1) as f64;
            Ok(FieldValue::Float(percent_rank))
        }
    }

    /// Evaluate CUME_DIST window function
    fn evaluate_cume_dist_function(
        args: &[Expr],
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "CUME_DIST function takes no arguments, but {} were provided",
                    args.len()
                ),
                query: Some(format!("CUME_DIST({} arguments)", args.len())),
            });
        }
        // CUME_DIST() = current_position / total_known_rows
        let current_position = window_buffer.len() + 1;
        let total_known_rows = current_position;
        let cume_dist = current_position as f64 / total_known_rows as f64;
        Ok(FieldValue::Float(cume_dist))
    }

    /// Evaluate NTILE window function
    fn evaluate_ntile_function(
        args: &[Expr],
        record: &StreamRecord,
        window_buffer: &[StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "NTILE function requires exactly 1 argument (n), but {} were provided",
                    args.len()
                ),
                query: Some("NTILE(n)".to_string()),
            });
        }

        // Parse the number of tiles
        let tiles = match crate::ferris::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)? {
            FieldValue::Integer(n) => {
                if n <= 0 {
                    return Err(SqlError::ExecutionError {
                        message: format!("NTILE tiles count must be positive, got {}", n),
                        query: Some(format!("NTILE({})", n)),
                    });
                }
                n
            }
            FieldValue::Null => {
                return Err(SqlError::ExecutionError {
                    message: "NTILE tiles count cannot be NULL".to_string(),
                    query: Some("NTILE(NULL)".to_string()),
                });
            }
            other => {
                return Err(SqlError::ExecutionError {
                    message: format!("NTILE tiles count must be an integer, got {}", other.type_name()),
                    query: Some(format!("NTILE({})", other.type_name().to_lowercase())),
                });
            }
        };

        // Calculate which tile the current row belongs to
        let current_row = window_buffer.len() + 1;
        let total_rows = current_row; // In streaming, we only know current position
        // Calculate tile number (1-indexed)
        let rows_per_tile = (total_rows as f64 / tiles as f64).ceil() as i64;
        let tile_number = ((current_row - 1) as i64 / rows_per_tile) + 1;
        let tile_number = tile_number.min(tiles); // Ensure we don't exceed max tiles
        Ok(FieldValue::Integer(tile_number))
    }
}
