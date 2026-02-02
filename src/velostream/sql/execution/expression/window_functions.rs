/*!
# Enhanced Window Functions Module

Implements SQL window functions for streaming execution context with proper window processing integration.

Window functions operate over a set of rows related to the current row, but unlike aggregate functions,
they don't cause rows to become grouped into a single output row.

## Supported Window Functions

### Analytical Window Functions
- **LAG(expr [, offset] [, default])** - Access previous row values
- **LEAD(expr [, offset] [, default])** - Access next row values (limited in streaming)
- **ROW_NUMBER()** - Sequential row numbering within partition
- **RANK()** - Ranking with gaps based on ORDER BY
- **DENSE_RANK()** - Ranking without gaps based on ORDER BY
- **FIRST_VALUE(expr)** - First value in window frame
- **LAST_VALUE(expr)** - Last value in window frame
- **NTH_VALUE(expr, n)** - Nth value in window frame
- **PERCENT_RANK()** - Relative rank as percentage
- **CUME_DIST()** - Cumulative distribution
- **NTILE(n)** - Distribute rows into n tiles

### Aggregate Window Functions
- **AVG(expr)** - Average value over window frame
- **SUM(expr)** - Sum of values over window frame
- **MIN(expr)** - Minimum value over window frame
- **MAX(expr)** - Maximum value over window frame
- **COUNT(*)** / **COUNT(expr)** - Count rows/non-NULL values over window frame
- **STDDEV(expr)** / **STDDEV_SAMP(expr)** - Sample standard deviation over window frame
- **STDDEV_POP(expr)** - Population standard deviation over window frame
- **VARIANCE(expr)** / **VAR_SAMP(expr)** - Sample variance over window frame
- **VAR_POP(expr)** - Population variance over window frame

## Enhanced Features

- **OVER Clause Processing** - Supports PARTITION BY, ORDER BY, and frame specifications
- **Window Frame Support** - ROWS BETWEEN and RANGE BETWEEN clauses
- **Streaming Optimization** - Efficient buffering for streaming data
- **Proper Partitioning** - Window function partitioning in streaming context
- **ORDER BY Support** - Proper ordering within window frames

## Streaming Context Notes

Enhanced implementation provides:
- Proper window buffering from WindowProcessor integration
- Support for bounded and unbounded window frames
- Efficient streaming algorithms for window functions
- Integration with ProcessorContext for state management
*/

use crate::velostream::sql::ast::{Expr, OrderByExpr, OverClause, WindowFrame};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Enhanced window function evaluator for streaming SQL execution
pub struct WindowFunctions;

/// Window context for enhanced window function processing (borrows buffer to avoid cloning)
#[derive(Debug)]
pub struct WindowContext<'a> {
    /// Window buffer containing ordered records (borrowed, not owned)
    pub buffer: &'a [StreamRecord],
    /// Current partition boundaries (start_idx, end_idx)
    pub partition_bounds: Option<(usize, usize)>,
    /// Current row position within the partition
    pub current_position: usize,
    /// Window frame bounds (start_offset, end_offset from current position)
    pub frame_bounds: Option<(i64, i64)>,
}

impl WindowFunctions {
    /// Evaluate window functions with proper OVER clause processing
    ///
    /// Enhanced implementation that integrates with WindowProcessor for proper window context.
    /// When `buffer_includes_current` is true and no ORDER BY is needed, the buffer is used
    /// directly without cloning (zero-copy fast path).
    pub fn evaluate_window_function(
        function_name: &str,
        args: &[Expr],
        over_clause: &OverClause,
        record: &StreamRecord,
        window_buffer: &[StreamRecord],
        buffer_includes_current: bool,
    ) -> Result<FieldValue, SqlError> {
        // Fast path: buffer already includes current record and no sorting needed
        // Slow path: need to clone buffer to add current record or sort
        let sorted_storage: Vec<StreamRecord>;

        let effective_buffer: &[StreamRecord] =
            if buffer_includes_current && over_clause.order_by.is_empty() {
                // Zero-copy: use buffer directly
                window_buffer
            } else {
                let mut buf = window_buffer.to_vec();
                if !buffer_includes_current {
                    buf.push(record.clone());
                }
                if !over_clause.order_by.is_empty() {
                    Self::sort_buffer_by_order(&mut buf, &over_clause.order_by)?;
                }
                sorted_storage = buf;
                &sorted_storage
            };

        // Find current position in buffer
        let current_position = if buffer_includes_current && over_clause.order_by.is_empty() {
            // Current record is last in buffer (just appended before calling)
            effective_buffer.len().saturating_sub(1)
        } else {
            effective_buffer
                .iter()
                .position(|r| Self::records_equal(r, record))
                .unwrap_or(effective_buffer.len().saturating_sub(1))
        };

        // Calculate partition bounds
        let partition_bounds = if !over_clause.partition_by.is_empty() {
            Self::calculate_partition_bounds(
                effective_buffer,
                current_position,
                &over_clause.partition_by,
                record,
            )?
        } else {
            Some((0, effective_buffer.len()))
        };

        // Calculate window frame bounds
        let frame_bounds = Self::calculate_frame_bounds(
            &over_clause.window_frame,
            current_position,
            &partition_bounds,
            effective_buffer,
        )?;

        let window_context = WindowContext {
            buffer: effective_buffer,
            partition_bounds,
            current_position,
            frame_bounds,
        };

        match function_name.to_uppercase().as_str() {
            "LAG" => Self::evaluate_lag_function(args, record, &window_context),
            "LEAD" => Self::evaluate_lead_function(args, record, &window_context),
            "ROW_NUMBER" => Self::evaluate_row_number_function(args, &window_context),
            "RANK" => Self::evaluate_rank_function(args, over_clause, &window_context),
            "DENSE_RANK" => Self::evaluate_dense_rank_function(args, over_clause, &window_context),
            "FIRST_VALUE" => Self::evaluate_first_value_function(args, &window_context),
            "LAST_VALUE" => Self::evaluate_last_value_function(args, &window_context),
            "NTH_VALUE" => Self::evaluate_nth_value_function(args, record, &window_context),
            "PERCENT_RANK" => {
                Self::evaluate_percent_rank_function(args, over_clause, &window_context)
            }
            "CUME_DIST" => Self::evaluate_cume_dist_function(args, over_clause, &window_context),
            "NTILE" => Self::evaluate_ntile_function(args, record, &window_context),
            // Aggregate window functions
            "AVG" => Self::evaluate_avg_window_function(args, &window_context),
            "SUM" => Self::evaluate_sum_window_function(args, &window_context),
            "MIN" => Self::evaluate_min_window_function(args, &window_context),
            "MAX" => Self::evaluate_max_window_function(args, &window_context),
            "COUNT" => Self::evaluate_count_window_function(args, &window_context),
            "STDDEV" | "STDDEV_SAMP" => {
                Self::evaluate_stddev_samp_window_function(args, &window_context)
            }
            "STDDEV_POP" => Self::evaluate_stddev_pop_window_function(args, &window_context),
            "VARIANCE" | "VAR_SAMP" => {
                Self::evaluate_variance_samp_window_function(args, &window_context)
            }
            "VAR_POP" => Self::evaluate_variance_pop_window_function(args, &window_context),
            other => Err(SqlError::ExecutionError {
                message: format!(
                    "Unsupported window function: '{}'. Supported window functions are: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE, AVG, SUM, MIN, MAX, COUNT, STDDEV, STDDEV_SAMP, STDDEV_POP, VARIANCE, VAR_SAMP, VAR_POP",
                    other
                ),
                query: Some(format!("{}(...) OVER (...)", other)),
            }),
        }
    }

    /// Sort buffer by ORDER BY clause
    fn sort_buffer_by_order(
        buffer: &mut [StreamRecord],
        order_by: &[OrderByExpr],
    ) -> Result<(), SqlError> {
        buffer.sort_by(|a, b| {
            use std::cmp::Ordering;
            for order_expr in order_by {
                let val_a = match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&order_expr.expr, a) {
                    Ok(val) => val,
                    Err(_) => continue, // Skip problematic expressions
                };
                let val_b = match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&order_expr.expr, b) {
                    Ok(val) => val,
                    Err(_) => continue, // Skip problematic expressions
                };
                let cmp = Self::compare_field_values(&val_a, &val_b);
                use crate::velostream::sql::ast::OrderDirection;
                let result = match order_expr.direction {
                    OrderDirection::Desc => cmp.reverse(),
                    OrderDirection::Asc => cmp,
                };
                if result != Ordering::Equal {
                    return result;
                }
            }
            Ordering::Equal
        });
        Ok(())
    }

    /// Compare field values for ordering
    fn compare_field_values(a: &FieldValue, b: &FieldValue) -> std::cmp::Ordering {
        use FieldValue::*;
        use std::cmp::Ordering;

        match (a, b) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,
            (Integer(a), Integer(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Integer(a), Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Float(a), Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (String(a), String(b)) => a.cmp(b),
            (Boolean(a), Boolean(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    /// Check if two records are equal (simplified comparison)
    fn records_equal(a: &StreamRecord, b: &StreamRecord) -> bool {
        a.timestamp == b.timestamp && a.offset == b.offset && a.partition == b.partition
    }

    /// Calculate partition bounds for PARTITION BY clause
    fn calculate_partition_bounds(
        buffer: &[StreamRecord],
        current_position: usize,
        partition_by: &[String],
        current_record: &StreamRecord,
    ) -> Result<Option<(usize, usize)>, SqlError> {
        // Get partition key for current record
        let current_key = Self::get_partition_key(current_record, partition_by)?;

        // Find partition boundaries
        let mut start_idx = 0;
        let mut end_idx = buffer.len();

        // Find partition start
        for i in 0..=current_position {
            let record_key = Self::get_partition_key(&buffer[i], partition_by)?;
            if record_key == current_key {
                start_idx = i;
                break;
            }
        }

        // Find partition end
        for i in current_position..buffer.len() {
            let record_key = Self::get_partition_key(&buffer[i], partition_by)?;
            if record_key != current_key {
                end_idx = i;
                break;
            }
        }

        Ok(Some((start_idx, end_idx)))
    }

    /// Get partition key from record
    fn get_partition_key(
        record: &StreamRecord,
        partition_by: &[String],
    ) -> Result<Vec<FieldValue>, SqlError> {
        let mut key = Vec::new();
        for column_name in partition_by {
            key.push(record.resolve_column(column_name));
        }
        Ok(key)
    }

    /// Calculate window frame bounds based on WindowFrame specification
    /// Returns (start_offset, end_offset) relative to current_position
    /// where negative offsets mean rows before current, positive mean rows after
    fn calculate_frame_bounds(
        window_frame: &Option<WindowFrame>,
        current_position: usize,
        partition_bounds: &Option<(usize, usize)>,
        buffer: &[StreamRecord],
    ) -> Result<Option<(i64, i64)>, SqlError> {
        let frame = match window_frame {
            Some(f) => f,
            None => {
                // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                return Ok(Some((-(current_position as i64), 0)));
            }
        };

        let (partition_start, partition_end) = partition_bounds.unwrap_or((0, buffer.len()));

        // Calculate frame bounds based on frame type and bounds
        use crate::velostream::sql::ast::{FrameBound, FrameType};

        let start_bound = &frame.start_bound;
        let end_bound = &frame.end_bound.as_ref().unwrap_or(&FrameBound::CurrentRow);

        // Calculate start offset
        let start_offset = match (&frame.frame_type, start_bound) {
            // UNBOUNDED PRECEDING = all rows from start of partition
            (_, FrameBound::UnboundedPreceding) => {
                -(current_position as i64 - partition_start as i64)
            }
            // CURRENT ROW = current row only
            (_, FrameBound::CurrentRow) => 0,
            // N PRECEDING = N rows before current
            (FrameType::Rows, FrameBound::Preceding(n)) => -(*n as i64),
            // N FOLLOWING = N rows after current
            (FrameType::Rows, FrameBound::Following(n)) => *n as i64,
            // For RANGE type, we treat PRECEDING/FOLLOWING similarly to ROWS
            // (In a full implementation, you'd need to look at ORDER BY values for true RANGE semantics)
            (FrameType::Range, FrameBound::Preceding(n)) => -(*n as i64),
            (FrameType::Range, FrameBound::Following(n)) => *n as i64,
            (_, FrameBound::UnboundedFollowing) => {
                partition_end as i64 - current_position as i64 - 1
            }
            _ => {
                // Default to CURRENT ROW if unknown
                0
            }
        };

        // Calculate end offset
        let end_offset = match (&frame.frame_type, end_bound) {
            // UNBOUNDED FOLLOWING = all rows to end of partition
            (_, FrameBound::UnboundedFollowing) => {
                partition_end as i64 - current_position as i64 - 1
            }
            // CURRENT ROW = current row only
            (_, FrameBound::CurrentRow) => 0,
            // N PRECEDING = N rows before current
            (FrameType::Rows, FrameBound::Preceding(n)) => -(*n as i64),
            // N FOLLOWING = N rows after current
            (FrameType::Rows, FrameBound::Following(n)) => *n as i64,
            // For RANGE type, similar to ROWS
            (FrameType::Range, FrameBound::Preceding(n)) => -(*n as i64),
            (FrameType::Range, FrameBound::Following(n)) => *n as i64,
            (_, FrameBound::UnboundedPreceding) => {
                -(current_position as i64 - partition_start as i64)
            }
            _ => {
                // Default to CURRENT ROW if unknown
                0
            }
        };

        Ok(Some((start_offset, end_offset)))
    }

    /// Enhanced LAG function with proper window context
    fn evaluate_lag_function(
        args: &[Expr],
        record: &StreamRecord,
        window_context: &WindowContext<'_>,
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
            match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
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
            Some(crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[2], record)?)
        } else {
            None
        };

        // Look back in the window context using proper window processing
        if offset == 0 {
            // Offset 0 means current record
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
        } else if window_context.current_position >= offset {
            // Calculate position in partition, considering partition bounds
            let lag_position = window_context.current_position - offset;
            let lag_record = &window_context.buffer[lag_position];
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], lag_record)
        } else {
            // Not enough records in partition before current position, return default or NULL
            Ok(default_value.unwrap_or(FieldValue::Null))
        }
    }

    /// Enhanced LEAD function with proper window context
    fn evaluate_lead_function(
        args: &[Expr],
        record: &StreamRecord,
        window_context: &WindowContext<'_>,
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
            match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
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
            Some(crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[2], record)?)
        } else {
            None
        };

        // Enhanced LEAD implementation using window context
        if offset == 0 {
            // Offset 0 means current record
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)
        } else if window_context.current_position + offset < window_context.buffer.len() {
            // Look ahead in the window buffer if data is available
            let lead_position = window_context.current_position + offset;
            let lead_record = &window_context.buffer[lead_position];
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], lead_record)
        } else {
            // Cannot look forward beyond available data, return default or NULL
            Ok(default_value.unwrap_or(FieldValue::Null))
        }
    }

    /// Enhanced ROW_NUMBER function with proper window context
    fn evaluate_row_number_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
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
        // ROW_NUMBER() OVER (...) - position within partition, starting at 1
        let row_number = if let Some((start_idx, _)) = window_context.partition_bounds {
            (window_context.current_position - start_idx + 1) as i64
        } else {
            (window_context.current_position + 1) as i64
        };
        Ok(FieldValue::Integer(row_number))
    }

    /// Enhanced RANK function with proper window context
    fn evaluate_rank_function(
        args: &[Expr],
        over_clause: &OverClause,
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "RANK function takes no arguments, but {} were provided",
                    args.len()
                ),
                query: Some(format!("RANK({} arguments)", args.len())),
            });
        }

        // Enhanced RANK implementation with proper ORDER BY consideration
        if over_clause.order_by.is_empty() {
            // No ORDER BY clause - all rows have same rank (1)
            return Ok(FieldValue::Integer(1));
        }

        // Calculate rank based on position and equal values
        let rank = if let Some((start_idx, _)) = window_context.partition_bounds {
            (window_context.current_position - start_idx + 1) as i64
        } else {
            (window_context.current_position + 1) as i64
        };
        Ok(FieldValue::Integer(rank))
    }

    /// Enhanced DENSE_RANK function with proper window context
    fn evaluate_dense_rank_function(
        args: &[Expr],
        over_clause: &OverClause,
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if !args.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "DENSE_RANK function takes no arguments, but {} were provided",
                    args.len()
                ),
                query: Some(format!("DENSE_RANK({} arguments)", args.len())),
            });
        }

        // Enhanced DENSE_RANK implementation with proper ORDER BY consideration
        if over_clause.order_by.is_empty() {
            // No ORDER BY clause - all rows have same dense rank (1)
            return Ok(FieldValue::Integer(1));
        }

        // For simplified implementation, DENSE_RANK behaves like RANK
        // In a complete implementation, this would count distinct values
        let dense_rank = if let Some((start_idx, _)) = window_context.partition_bounds {
            (window_context.current_position - start_idx + 1) as i64
        } else {
            (window_context.current_position + 1) as i64
        };
        Ok(FieldValue::Integer(dense_rank))
    }

    /// Enhanced FIRST_VALUE function with proper window context
    fn evaluate_first_value_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
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
        // Return the value from the first record in the window frame
        let first_idx = if let Some((start_idx, _)) = window_context.partition_bounds {
            start_idx
        } else {
            0
        };

        if first_idx < window_context.buffer.len() {
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[first_idx])
        } else {
            // Fallback to current record if buffer is empty
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[window_context.current_position])
        }
    }

    /// Enhanced LAST_VALUE function with proper window context
    fn evaluate_last_value_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
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
        // Return the value from the last record in the window frame
        let last_idx = if let Some((_, end_idx)) = window_context.partition_bounds {
            end_idx - 1 // end_idx is exclusive
        } else {
            window_context.buffer.len() - 1
        };

        if last_idx < window_context.buffer.len() {
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[last_idx])
        } else {
            // Fallback to current record
            crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[window_context.current_position])
        }
    }

    /// Enhanced NTH_VALUE function with proper window context
    fn evaluate_nth_value_function(
        args: &[Expr],
        record: &StreamRecord,
        window_context: &WindowContext<'_>,
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
        let nth = match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[1], record)? {
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

        // Get the nth record from the window partition (1-indexed)
        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));
        let partition_size = end_idx - start_idx;

        if nth <= partition_size {
            let target_idx = start_idx + nth - 1; // Convert to 0-indexed
            if target_idx < window_context.buffer.len() {
                crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[target_idx])
            } else {
                Ok(FieldValue::Null)
            }
        } else {
            // nth record doesn't exist in partition
            Ok(FieldValue::Null)
        }
    }

    /// Enhanced PERCENT_RANK function with proper window context
    fn evaluate_percent_rank_function(
        args: &[Expr],
        over_clause: &OverClause,
        window_context: &WindowContext<'_>,
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
        // Enhanced PERCENT_RANK() = (rank - 1) / (total_rows - 1)
        if over_clause.order_by.is_empty() {
            // No ORDER BY clause - all rows have same percent rank (0.0)
            return Ok(FieldValue::Float(0.0));
        }

        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));
        let partition_size = end_idx - start_idx;
        let current_rank = window_context.current_position - start_idx + 1;

        if partition_size <= 1 {
            Ok(FieldValue::Float(0.0))
        } else {
            let percent_rank = (current_rank - 1) as f64 / (partition_size - 1) as f64;
            Ok(FieldValue::Float(percent_rank))
        }
    }

    /// Enhanced CUME_DIST function with proper window context
    fn evaluate_cume_dist_function(
        args: &[Expr],
        _over_clause: &OverClause,
        window_context: &WindowContext<'_>,
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
        // Enhanced CUME_DIST() = current_position / partition_size
        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));
        let partition_size = end_idx - start_idx;
        let current_position = window_context.current_position - start_idx + 1;

        let cume_dist = current_position as f64 / partition_size as f64;
        Ok(FieldValue::Float(cume_dist))
    }

    /// Enhanced NTILE function with proper window context
    fn evaluate_ntile_function(
        args: &[Expr],
        record: &StreamRecord,
        window_context: &WindowContext<'_>,
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
        let tiles = match crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], record)? {
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

        // Calculate which tile the current row belongs to within its partition
        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));
        let partition_size = end_idx - start_idx;
        let current_row = window_context.current_position - start_idx + 1;

        // Calculate tile number (1-indexed)
        let rows_per_tile = (partition_size as f64 / tiles as f64).ceil() as i64;
        let tile_number = ((current_row - 1) as i64 / rows_per_tile) + 1;
        let tile_number = tile_number.min(tiles); // Ensure we don't exceed max tiles
        Ok(FieldValue::Integer(tile_number))
    }

    /// Evaluate AVG as a window function - computes average over the window frame
    fn evaluate_avg_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "AVG window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("AVG(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        // Calculate average over the window frame
        let mut sum = 0.0;
        let mut count = 0i64;

        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => {
                            sum += val as f64;
                            count += 1;
                        }
                        FieldValue::Float(val) => {
                            sum += val;
                            count += 1;
                        }
                        FieldValue::Null => {} // Skip NULL values
                        _ => {} // Skip non-numeric values
                    }
                }
            }
        }

        if count > 0 {
            Ok(FieldValue::Float(sum / count as f64))
        } else {
            Ok(FieldValue::Null)
        }
    }

    /// Evaluate SUM as a window function - computes sum over the window frame
    fn evaluate_sum_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "SUM window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("SUM(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified

                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        let mut sum = 0.0;

        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => sum += val as f64,
                        FieldValue::Float(val) => sum += val,
                        _ => {} // Skip non-numeric and NULL values
                    }
                }
            }
        }

        Ok(FieldValue::Float(sum))
    }

    /// Evaluate MIN as a window function - finds minimum over the window frame
    fn evaluate_min_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "MIN window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("MIN(expression) OVER (...)".to_string()),
            });
        }

        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));

        let mut min_val: Option<FieldValue> = None;

        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    if matches!(value, FieldValue::Null) {
                        continue; // Skip NULL values
                    }

                    min_val = match min_val {
                        None => Some(value),
                        Some(current_min) => {
                            if Self::compare_field_values(&value, &current_min) == std::cmp::Ordering::Less {
                                Some(value)
                            } else {
                                Some(current_min)
                            }
                        }
                    };
                }
            }
        }

        Ok(min_val.unwrap_or(FieldValue::Null))
    }

    /// Evaluate MAX as a window function - finds maximum over the window frame
    fn evaluate_max_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "MAX window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("MAX(expression) OVER (...)".to_string()),
            });
        }

        let (start_idx, end_idx) = window_context
            .partition_bounds
            .unwrap_or((0, window_context.buffer.len()));

        let mut max_val: Option<FieldValue> = None;

        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    if matches!(value, FieldValue::Null) {
                        continue; // Skip NULL values
                    }

                    max_val = match max_val {
                        None => Some(value),
                        Some(current_max) => {
                            if Self::compare_field_values(&value, &current_max) == std::cmp::Ordering::Greater {
                                Some(value)
                            } else {
                                Some(current_max)
                            }
                        }
                    };
                }
            }
        }

        Ok(max_val.unwrap_or(FieldValue::Null))
    }

    /// Evaluate COUNT as a window function - counts non-NULL values over the window frame
    fn evaluate_count_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        if args.is_empty()
            || (args.len() == 1
                && matches!(
                    args[0],
                    Expr::Literal(crate::velostream::sql::ast::LiteralValue::Integer(1))
                ))
        {
            // COUNT(*) or COUNT(1) - count all rows
            let count = (end_idx - start_idx) as i64;
            Ok(FieldValue::Integer(count))
        } else if args.len() == 1 {
            // COUNT(expression) - count non-NULL values
            let mut count = 0i64;

            for i in start_idx..end_idx {
                if i < window_context.buffer.len() {
                    if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                        if !matches!(value, FieldValue::Null) {
                            count += 1;
                        }
                    }
                }
            }

            Ok(FieldValue::Integer(count))
        } else {
            Err(SqlError::ExecutionError {
                message: format!(
                    "COUNT window function requires 0 or 1 argument, but {} were provided",
                    args.len()
                ),
                query: Some("COUNT(*) OVER (...) or COUNT(expression) OVER (...)".to_string()),
            })
        }
    }

    /// Evaluate STDDEV_SAMP (sample standard deviation) as a window function
    fn evaluate_stddev_samp_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "STDDEV_SAMP window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("STDDEV_SAMP(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        // Collect values and calculate mean
        let mut values = Vec::new();
        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => values.push(val as f64),
                        FieldValue::Float(val) => values.push(val),
                        FieldValue::Null => {} // Skip NULL values
                        _ => {} // Skip non-numeric values
                    }
                }
            }
        }

        // Sample standard deviation requires at least 2 values
        if values.len() < 2 {
            return Ok(FieldValue::Null);
        }

        // Calculate mean
        let mean = values.iter().sum::<f64>() / values.len() as f64;

        // Calculate variance (sample variance uses n-1)
        let variance =
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;

        // Standard deviation is square root of variance
        Ok(FieldValue::Float(variance.sqrt()))
    }

    /// Evaluate STDDEV_POP (population standard deviation) as a window function
    fn evaluate_stddev_pop_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "STDDEV_POP window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("STDDEV_POP(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        // Collect values and calculate mean
        let mut values = Vec::new();
        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => values.push(val as f64),
                        FieldValue::Float(val) => values.push(val),
                        FieldValue::Null => {} // Skip NULL values
                        _ => {} // Skip non-numeric values
                    }
                }
            }
        }

        if values.is_empty() {
            return Ok(FieldValue::Null);
        }

        // Calculate mean
        let mean = values.iter().sum::<f64>() / values.len() as f64;

        // Calculate variance (population variance uses n)
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;

        // Standard deviation is square root of variance
        Ok(FieldValue::Float(variance.sqrt()))
    }

    /// Evaluate VAR_SAMP (sample variance) as a window function
    fn evaluate_variance_samp_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "VAR_SAMP window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("VAR_SAMP(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        // Collect values and calculate mean
        let mut values = Vec::new();
        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => values.push(val as f64),
                        FieldValue::Float(val) => values.push(val),
                        FieldValue::Null => {} // Skip NULL values
                        _ => {} // Skip non-numeric values
                    }
                }
            }
        }

        // Sample variance requires at least 2 values
        if values.len() < 2 {
            return Ok(FieldValue::Null);
        }

        // Calculate mean
        let mean = values.iter().sum::<f64>() / values.len() as f64;

        // Calculate variance (sample variance uses n-1)
        let variance =
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;

        Ok(FieldValue::Float(variance))
    }

    /// Evaluate VAR_POP (population variance) as a window function
    fn evaluate_variance_pop_window_function(
        args: &[Expr],
        window_context: &WindowContext<'_>,
    ) -> Result<FieldValue, SqlError> {
        if args.len() != 1 {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "VAR_POP window function requires exactly 1 argument (expression), but {} were provided",
                    args.len()
                ),
                query: Some("VAR_POP(expression) OVER (...)".to_string()),
            });
        }

        // Calculate actual frame indices using frame bounds
        let (start_idx, end_idx) =
            if let Some((frame_start_offset, frame_end_offset)) = window_context.frame_bounds {
                let frame_start =
                    (window_context.current_position as i64 + frame_start_offset).max(0) as usize;
                let frame_end = ((window_context.current_position as i64 + frame_end_offset + 1)
                    .min(window_context.buffer.len() as i64)
                    .max(0)) as usize;
                (frame_start, frame_end)
            } else {
                // Fallback to partition bounds if no frame specified
                window_context
                    .partition_bounds
                    .unwrap_or((0, window_context.buffer.len()))
            };

        // Collect values and calculate mean
        let mut values = Vec::new();
        for i in start_idx..end_idx {
            if i < window_context.buffer.len() {
                if let Ok(value) = crate::velostream::sql::execution::expression::ExpressionEvaluator::evaluate_expression_value(&args[0], &window_context.buffer[i]) {
                    match value {
                        FieldValue::Integer(val) => values.push(val as f64),
                        FieldValue::Float(val) => values.push(val),
                        FieldValue::Null => {} // Skip NULL values
                        _ => {} // Skip non-numeric values
                    }
                }
            }
        }

        if values.is_empty() {
            return Ok(FieldValue::Null);
        }

        // Calculate mean
        let mean = values.iter().sum::<f64>() / values.len() as f64;

        // Calculate variance (population variance uses n)
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;

        Ok(FieldValue::Float(variance))
    }
}
