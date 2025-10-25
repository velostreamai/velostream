//! Window Frame Validation
//!
//! Validates window frame specifications in OVER clauses:
//! - ORDER BY requirements for different frame types
//! - RANGE vs ROWS compatibility
//! - Frame boundary specifications (UNBOUNDED, CURRENT ROW, INTERVAL)
//! - Temporal window validation for INTERVAL clauses
//! This is Phase 8 of the correctness work.

use crate::velostream::sql::ast::{FrameBound, FrameType, OverClause, TimeUnit, WindowFrame};
use crate::velostream::sql::SqlError;

/// Validator for SQL window frame specifications
pub struct WindowFrameValidator;

impl WindowFrameValidator {
    /// Validate a complete OVER clause
    pub fn validate_over_clause(over_clause: &OverClause) -> Result<(), SqlError> {
        // If there's a window frame, validate it
        if let Some(frame) = &over_clause.window_frame {
            // Check if ORDER BY is present (required for RANGE frames)
            let has_order_by = !over_clause.order_by.is_empty();

            // Validate frame based on type
            match frame.frame_type {
                FrameType::Rows => {
                    Self::validate_rows_frame(&frame.start_bound, frame.end_bound.as_ref())?;
                }
                FrameType::Range => {
                    // RANGE frames always require ORDER BY
                    if !has_order_by {
                        return Err(SqlError::ExecutionError {
                            message: "RANGE frame requires ORDER BY clause in OVER".to_string(),
                            query: None,
                        });
                    }
                    Self::validate_range_frame(&frame.start_bound, frame.end_bound.as_ref())?;
                }
            }
        }

        Ok(())
    }

    /// Validate ROWS frame bounds
    fn validate_rows_frame(start: &FrameBound, end: Option<&FrameBound>) -> Result<(), SqlError> {
        // ROWS can use UNBOUNDED, CURRENT ROW, or numeric expressions
        Self::validate_rows_bound(start)?;
        if let Some(end_bound) = end {
            Self::validate_rows_bound(end_bound)?;
            // Validate bound ordering: start should come before end
            Self::validate_bound_order(start, end_bound)?;
        } else {
            // If no end bound specified, default is CURRENT ROW
        }

        Ok(())
    }

    /// Validate RANGE frame bounds
    fn validate_range_frame(start: &FrameBound, end: Option<&FrameBound>) -> Result<(), SqlError> {
        // RANGE requires value expressions or INTERVAL syntax
        match start {
            FrameBound::UnboundedPreceding => {
                // UNBOUNDED is valid for RANGE
            }
            FrameBound::UnboundedFollowing => {
                return Err(SqlError::ExecutionError {
                    message: "UNBOUNDED FOLLOWING not allowed as frame start".to_string(),
                    query: None,
                });
            }
            FrameBound::CurrentRow => {
                // Valid, but unusual
            }
            FrameBound::Preceding(_) => {
                // Numeric or expression preceding is valid for RANGE
            }
            FrameBound::Following(_) => {
                return Err(SqlError::ExecutionError {
                    message: "FOLLOWING not allowed as frame start".to_string(),
                    query: None,
                });
            }
            FrameBound::IntervalPreceding { .. } => {
                // Valid for temporal RANGE
            }
            FrameBound::IntervalFollowing { .. } => {
                return Err(SqlError::ExecutionError {
                    message: "INTERVAL FOLLOWING not allowed as frame start".to_string(),
                    query: None,
                });
            }
        }

        // Validate end bound if present
        if let Some(end_bound) = end {
            match end_bound {
                FrameBound::UnboundedFollowing => {
                    // UNBOUNDED FOLLOWING is valid for frame end
                }
                FrameBound::UnboundedPreceding => {
                    return Err(SqlError::ExecutionError {
                        message: "UNBOUNDED PRECEDING not allowed as frame end".to_string(),
                        query: None,
                    });
                }
                FrameBound::CurrentRow => {
                    // Valid
                }
                FrameBound::Preceding(_) => {
                    return Err(SqlError::ExecutionError {
                        message: "PRECEDING not allowed as frame end".to_string(),
                        query: None,
                    });
                }
                FrameBound::Following(_) => {
                    // Numeric or expression following is valid for frame end
                }
                FrameBound::IntervalPreceding { .. } => {
                    return Err(SqlError::ExecutionError {
                        message: "INTERVAL PRECEDING not allowed as frame end".to_string(),
                        query: None,
                    });
                }
                FrameBound::IntervalFollowing { .. } => {
                    // Valid for temporal RANGE
                }
            }

            Self::validate_bound_order(start, end_bound)?;
        }

        Ok(())
    }

    /// Validate individual ROWS frame bounds
    fn validate_rows_bound(bound: &FrameBound) -> Result<(), SqlError> {
        match bound {
            FrameBound::UnboundedPreceding
            | FrameBound::UnboundedFollowing
            | FrameBound::CurrentRow => Ok(()),
            FrameBound::Preceding(_) | FrameBound::Following(_) => {
                // Numeric expressions are valid
                Ok(())
            }
            FrameBound::IntervalPreceding { .. } | FrameBound::IntervalFollowing { .. } => {
                Err(SqlError::ExecutionError {
                    message: "INTERVAL syntax only allowed in RANGE frames, not ROWS".to_string(),
                    query: None,
                })
            }
        }
    }

    /// Validate ordering of frame bounds
    fn validate_bound_order(start: &FrameBound, end: &FrameBound) -> Result<(), SqlError> {
        // Start should not be FOLLOWING (it should precede the end)
        if matches!(
            start,
            FrameBound::Following(_)
                | FrameBound::IntervalFollowing { .. }
                | FrameBound::UnboundedFollowing
        ) {
            return Err(SqlError::ExecutionError {
                message: "Frame start cannot be FOLLOWING or UNBOUNDED FOLLOWING".to_string(),
                query: None,
            });
        }

        // End should not be PRECEDING (it should follow the start)
        if matches!(
            end,
            FrameBound::Preceding(_)
                | FrameBound::IntervalPreceding { .. }
                | FrameBound::UnboundedPreceding
        ) {
            return Err(SqlError::ExecutionError {
                message: "Frame end cannot be PRECEDING or UNBOUNDED PRECEDING".to_string(),
                query: None,
            });
        }

        Ok(())
    }

    /// Validate INTERVAL syntax for temporal windows
    pub fn validate_interval_temporal(
        unit: TimeUnit,
        order_by_is_temporal: bool,
    ) -> Result<(), SqlError> {
        // INTERVAL syntax requires a temporal ORDER BY field
        if !order_by_is_temporal {
            return Err(SqlError::ExecutionError {
                message:
                    "INTERVAL syntax in window frame requires temporal ORDER BY (TIMESTAMP or DATE)"
                        .to_string(),
                query: None,
            });
        }

        // All TimeUnit values are valid for windows
        match unit {
            TimeUnit::Nanosecond
            | TimeUnit::Microsecond
            | TimeUnit::Millisecond
            | TimeUnit::Second
            | TimeUnit::Minute
            | TimeUnit::Hour
            | TimeUnit::Day
            | TimeUnit::Week
            | TimeUnit::Month
            | TimeUnit::Year => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_rows_frame() {
        let start = FrameBound::Preceding(10);
        let end = FrameBound::CurrentRow;
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(result.is_ok(), "Valid ROWS frame should pass");
    }

    #[test]
    fn test_validate_unbounded_preceding() {
        let start = FrameBound::UnboundedPreceding;
        let end = FrameBound::CurrentRow;
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(result.is_ok(), "UNBOUNDED PRECEDING should be valid");
    }

    #[test]
    fn test_validate_unbounded_following() {
        let start = FrameBound::CurrentRow;
        let end = FrameBound::UnboundedFollowing;
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(result.is_ok(), "UNBOUNDED FOLLOWING as end should be valid");
    }

    #[test]
    fn test_interval_not_allowed_in_rows() {
        let start = FrameBound::IntervalPreceding {
            value: 1,
            unit: TimeUnit::Day,
        };
        let end = FrameBound::CurrentRow;
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(
            result.is_err(),
            "INTERVAL should not be allowed in ROWS frames"
        );
    }

    #[test]
    fn test_invalid_bound_ordering() {
        let start = FrameBound::Following(10);
        let end = FrameBound::CurrentRow;
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(result.is_err(), "FOLLOWING as start should fail");
    }

    #[test]
    fn test_invalid_end_ordering() {
        let start = FrameBound::CurrentRow;
        let end = FrameBound::Preceding(10);
        let result = WindowFrameValidator::validate_rows_frame(&start, Some(&end));
        assert!(result.is_err(), "PRECEDING as end should fail");
    }

    #[test]
    fn test_validate_interval_temporal_success() {
        let result = WindowFrameValidator::validate_interval_temporal(TimeUnit::Day, true);
        assert!(
            result.is_ok(),
            "INTERVAL with temporal ORDER BY should pass"
        );
    }

    #[test]
    fn test_validate_interval_non_temporal_fails() {
        let result = WindowFrameValidator::validate_interval_temporal(TimeUnit::Day, false);
        assert!(
            result.is_err(),
            "INTERVAL without temporal ORDER BY should fail"
        );
    }

    #[test]
    fn test_range_requires_order_by() {
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![], // No ORDER BY
            window_frame: Some(WindowFrame {
                frame_type: FrameType::Range,
                start_bound: FrameBound::Preceding(1),
                end_bound: Some(FrameBound::CurrentRow),
            }),
        };
        let result = WindowFrameValidator::validate_over_clause(&over_clause);
        assert!(result.is_err(), "RANGE without ORDER BY should fail");
    }

    #[test]
    fn test_rows_without_order_by_ok() {
        let over_clause = OverClause {
            partition_by: vec![],
            order_by: vec![], // No ORDER BY
            window_frame: Some(WindowFrame {
                frame_type: FrameType::Rows,
                start_bound: FrameBound::Preceding(1),
                end_bound: Some(FrameBound::CurrentRow),
            }),
        };
        let result = WindowFrameValidator::validate_over_clause(&over_clause);
        assert!(result.is_ok(), "ROWS without ORDER BY should be ok");
    }
}
