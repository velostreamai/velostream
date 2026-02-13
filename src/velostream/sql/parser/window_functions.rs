/*!
# Window Function Parsing

This module handles parsing of window functions with ROWS WINDOW BUFFER syntax, including
OVER clauses with partitioning, ordering, frame bounds, and expiration modes.

## Window Function vs Time-Based Windows

Velostream has **TWO different window mechanisms** that serve different purposes:

### ROWS WINDOW (This Module)
- **Location**: Inside function OVER clause
- **Syntax**: `AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol)`
- **Purpose**: Row-count based window functions (sliding buffers, running aggregates)
- **Watermarks**: ❌ NO - operates on row counts, not time
- **Frame Bounds**: Supports ROWS BETWEEN for precise frame control

### Time-Based Windows (Not This Module)
- **Location**: Top-level SELECT clause
- **Syntax**: `GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE)`
- **Purpose**: Time-bucketed aggregations with event-time semantics
- **Watermarks**: ✅ YES - supports late data handling
- **Types**: TUMBLING, SLIDING, SESSION

## Supported OVER Clause Syntax

All window functions in Velostream **MUST** use the ROWS WINDOW BUFFER syntax:

```sql
AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS          -- Required: buffer size
    [PARTITION BY symbol, exchange]      -- Optional: partitioning
    [ORDER BY timestamp ASC]             -- Optional: ordering
    [ROWS BETWEEN 10 PRECEDING AND 5 FOLLOWING]  -- Optional: frame bounds
    [EMIT EVERY RECORD | ON BUFFER FULL] -- Optional: emission mode
    [EXPIRE AFTER INTERVAL '5' MINUTES INACTIVITY | NEVER]  -- Optional: expiration
)
```

### Required Components
- `ROWS WINDOW BUFFER N ROWS` - Defines the maximum buffer size (always required)

### Optional Components
- `PARTITION BY` - Separate window instances per partition key(s)
- `ORDER BY` - Sort rows within each window partition
- `ROWS BETWEEN` - Fine-grained frame bounds (UNBOUNDED, numeric, INTERVAL)
- `EMIT` - Control when results are emitted (every record vs buffer full)
- `EXPIRE AFTER` - Buffer expiration policy (inactivity timeout or never)

## Frame Bound Syntax

### ROWS BETWEEN Variants
```sql
-- Unbounded bounds
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- Numeric offsets
ROWS BETWEEN 10 PRECEDING AND 5 FOLLOWING
ROWS BETWEEN 100 PRECEDING AND CURRENT ROW

-- Time-based intervals (for RANGE frames)
RANGE BETWEEN INTERVAL '5' MINUTES PRECEDING AND CURRENT ROW
RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND INTERVAL '30' MINUTES FOLLOWING
```

### Supported Bound Types
- `UNBOUNDED PRECEDING` - Start of partition
- `UNBOUNDED FOLLOWING` - End of partition
- `N PRECEDING` - N rows before current
- `N FOLLOWING` - N rows after current
- `CURRENT ROW` - Current row position
- `INTERVAL 'N' UNIT PRECEDING/FOLLOWING` - Time-based offset

## Emit Modes

Controls when window results are emitted:

- `EMIT EVERY RECORD` (default) - Emit updated result on every input row
- `EMIT ON BUFFER FULL` - Only emit when buffer reaches capacity

## Expiration Modes

Controls when window buffers are expired and removed:

- `EXPIRE AFTER INTERVAL 'N' MINUTES INACTIVITY` - Remove after N minutes of no new data
- `EXPIRE AFTER NEVER` - Keep buffers indefinitely
- Default: System-defined retention policy

## Examples

```sql
-- Simple 100-row moving average
AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)

-- Partitioned by symbol, ordered by time
SUM(volume) OVER (
    ROWS WINDOW BUFFER 1000 ROWS
    PARTITION BY symbol
    ORDER BY event_time DESC
)

-- Custom frame with expiration
MAX(price) OVER (
    ROWS WINDOW BUFFER 500 ROWS
    PARTITION BY exchange, symbol
    ORDER BY timestamp
    ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
    EMIT ON BUFFER FULL
    EXPIRE AFTER INTERVAL '10' MINUTES INACTIVITY
)
```

## Error Handling

All parsing methods return `Result<T, SqlError>` with detailed error information:
- Position of syntax errors
- Expected vs. actual token types
- Clear error messages for invalid configurations
*/

use super::common::{TokenParser, normalize_column_name};
use super::*;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use std::time::Duration;

impl<'a> TokenParser<'a> {
    /// Parse OVER clause for window functions (Phase 8 - ROWS WINDOW BUFFER only)
    ///
    /// Syntax: `OVER (ROWS WINDOW BUFFER 100 ROWS [PARTITION BY ...] [ORDER BY ...] [ROWS BETWEEN ...] [EMIT ...])`
    ///
    /// NOTE: All window functions must use ROWS WINDOW syntax with explicit BUFFER size.
    /// This is the ONLY supported OVER clause format in Velostream.
    ///
    /// # Returns
    /// - `Ok(OverClause)` - Parsed OVER clause with window specification
    /// - `Err(SqlError)` - Parse error with position information
    ///
    /// # Errors
    /// - Missing ROWS WINDOW BUFFER keywords
    /// - Invalid buffer size (non-numeric or out of range)
    /// - Malformed PARTITION BY or ORDER BY clauses
    /// - Invalid frame bounds or expiration syntax
    pub(super) fn parse_over_clause(&mut self) -> Result<OverClause, SqlError> {
        self.expect(TokenType::LeftParen)?;

        // Phase 8: ROWS WINDOW BUFFER is the ONLY supported OVER clause syntax
        self.expect_keyword("ROWS")?;
        self.expect_keyword("WINDOW")?;
        self.expect_keyword("BUFFER")?;

        // Parse buffer size
        let buffer_size_token = self.expect(TokenType::Number)?;
        let buffer_size: u32 =
            buffer_size_token
                .value
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid buffer size: {}", buffer_size_token.value),
                    position: None,
                })?;

        // Expect ROWS keyword after buffer size
        self.expect_keyword("ROWS")?;

        // Parse optional PARTITION BY
        let mut partition_by = None;
        if self.current_token().value.to_uppercase() == "PARTITION" {
            self.advance(); // consume PARTITION
            self.expect_keyword("BY")?;

            let mut exprs = Vec::new();
            loop {
                let column_name = self.expect(TokenType::Identifier)?.value;
                let column = if self.current_token().token_type == TokenType::Dot {
                    self.advance(); // consume dot
                    let field_name = self.expect(TokenType::Identifier)?.value;
                    format!("{}.{}", column_name, field_name)
                } else {
                    column_name
                };
                exprs.push(Expr::Column(normalize_column_name(column)));

                if self.current_token().token_type == TokenType::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            partition_by = Some(exprs);
        }

        // Parse optional ORDER BY
        let mut order_by = None;
        if self.current_token().token_type == TokenType::OrderBy {
            self.advance(); // consume ORDER
            self.expect_keyword("BY")?;

            let mut order_specs = Vec::new();
            loop {
                let column_name = self.expect(TokenType::Identifier)?.value;
                let full_column_name = if self.current_token().token_type == TokenType::Dot {
                    self.advance(); // consume dot
                    let field_name = self.expect(TokenType::Identifier)?.value;
                    format!("{}.{}", column_name, field_name)
                } else {
                    column_name
                };
                let expr = Expr::Column(normalize_column_name(full_column_name));
                let direction = if self.current_token().token_type == TokenType::Asc {
                    self.advance();
                    OrderDirection::Asc
                } else if self.current_token().token_type == TokenType::Desc {
                    self.advance();
                    OrderDirection::Desc
                } else {
                    OrderDirection::Asc // Default
                };

                order_specs.push(OrderByExpr { expr, direction });

                if self.current_token().token_type == TokenType::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            order_by = Some(order_specs);
        }

        // Parse optional window frame (ROWS BETWEEN)
        let window_frame = if self.current_token().token_type == TokenType::Rows
            || self.current_token().token_type == TokenType::Range
        {
            Some(self.parse_window_frame()?)
        } else {
            None
        };

        // Parse optional EMIT mode
        let emit_mode = if self.current_token().value.to_uppercase() == "EMIT" {
            self.advance(); // consume EMIT
            if self.current_token().value.to_uppercase() == "EVERY" {
                self.advance(); // consume EVERY
                self.expect_keyword("RECORD")?;
                RowsEmitMode::EveryRecord
            } else if self.current_token().value.to_uppercase() == "ON" {
                self.advance(); // consume ON
                self.expect_keyword("BUFFER")?;
                self.expect_keyword("FULL")?;
                RowsEmitMode::BufferFull
            } else {
                RowsEmitMode::EveryRecord
            }
        } else {
            RowsEmitMode::EveryRecord // Default
        };

        // Parse optional EXPIRE AFTER clause
        let expire_after = if self.current_token().value.to_uppercase() == "EXPIRE" {
            self.advance(); // consume EXPIRE
            self.expect_keyword("AFTER")?;

            if self.current_token().value.to_uppercase() == "NEVER" {
                self.advance(); // consume NEVER
                RowExpirationMode::Never
            } else if self.current_token().value.to_uppercase() == "INTERVAL" {
                self.advance(); // consume INTERVAL
                let interval_str = self.expect(TokenType::String)?.value;
                // Remove quotes from the string
                let interval_value: u64 =
                    interval_str
                        .trim_matches('\'')
                        .parse()
                        .map_err(|_| SqlError::ParseError {
                            message: format!("Invalid INTERVAL value: {}", interval_str),
                            position: Some(self.current_token().position),
                        })?;

                // Parse the time unit (MINUTE, SECOND, HOUR)
                let time_unit = self.current_token().value.to_uppercase();
                let duration = match time_unit.as_str() {
                    "SECOND" | "SECONDS" => {
                        self.advance();
                        Duration::from_secs(interval_value)
                    }
                    "MINUTE" | "MINUTES" => {
                        self.advance();
                        Duration::from_secs(interval_value * 60)
                    }
                    "HOUR" | "HOURS" => {
                        self.advance();
                        Duration::from_secs(interval_value * 3600)
                    }
                    _ => {
                        return Err(SqlError::ParseError {
                            message: format!(
                                "Expected SECOND, MINUTE, or HOUR in EXPIRE AFTER INTERVAL, got: {}",
                                time_unit
                            ),
                            position: Some(self.current_token().position),
                        });
                    }
                };

                // Expect INACTIVITY keyword
                self.expect_keyword("INACTIVITY")?;
                RowExpirationMode::InactivityGap(duration)
            } else {
                return Err(SqlError::ParseError {
                    message: "Expected INTERVAL or NEVER in EXPIRE AFTER clause".to_string(),
                    position: Some(self.current_token().position),
                });
            }
        } else {
            RowExpirationMode::Default
        };

        self.expect(TokenType::RightParen)?;

        // Create WindowSpec::Rows and return OverClause with it
        let window_spec = WindowSpec::Rows {
            buffer_size,
            partition_by: partition_by.unwrap_or_default(),
            order_by: order_by.unwrap_or_default(),
            time_gap: None,
            window_frame,
            emit_mode,
            expire_after,
        };

        Ok(OverClause {
            window_spec: Some(Box::new(window_spec)),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: None,
        })
    }

    /// Parse window frame specification: ROWS/RANGE BETWEEN ... AND ...
    ///
    /// Supports both ROWS (row-count based) and RANGE (value-based) frame types.
    ///
    /// # Syntax
    /// ```sql
    /// ROWS BETWEEN <start_bound> AND <end_bound>
    /// RANGE BETWEEN <start_bound> AND <end_bound>
    /// ```
    ///
    /// # Returns
    /// - `Ok(WindowFrame)` - Parsed frame specification
    /// - `Err(SqlError)` - Invalid frame syntax
    ///
    /// # Errors
    /// - Missing ROWS or RANGE keyword
    /// - Missing BETWEEN or AND keywords
    /// - Invalid frame bounds
    fn parse_window_frame(&mut self) -> Result<WindowFrame, SqlError> {
        // Parse frame type (ROWS or RANGE)
        let frame_type = match self.current_token().token_type {
            TokenType::Rows => {
                self.advance();
                FrameType::Rows
            }
            TokenType::Range => {
                self.advance();
                FrameType::Range
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: "Expected ROWS or RANGE in window frame".to_string(),
                    position: Some(self.current_token().position),
                });
            }
        };

        self.expect(TokenType::Between)?;

        // Parse start bound
        let start_bound = self.parse_frame_bound()?;

        self.expect(TokenType::And)?;

        // Parse end bound
        let end_bound = Some(self.parse_frame_bound()?);

        Ok(WindowFrame {
            frame_type,
            start_bound,
            end_bound,
        })
    }

    /// Parse frame bound: UNBOUNDED PRECEDING/FOLLOWING, n PRECEDING/FOLLOWING, CURRENT ROW
    ///
    /// Handles all supported frame bound types including:
    /// - Unbounded bounds (start/end of partition)
    /// - Numeric offsets (N rows before/after)
    /// - Current row position
    /// - Time-based intervals (for RANGE frames)
    ///
    /// # Returns
    /// - `Ok(FrameBound)` - Parsed frame bound
    /// - `Err(SqlError)` - Invalid bound syntax
    ///
    /// # Errors
    /// - Invalid numeric offset
    /// - Unsupported time unit in INTERVAL
    /// - Missing PRECEDING/FOLLOWING keyword
    fn parse_frame_bound(&mut self) -> Result<FrameBound, SqlError> {
        match self.current_token().token_type {
            TokenType::Unbounded => {
                self.advance(); // consume UNBOUNDED
                match self.current_token().token_type {
                    TokenType::Preceding => {
                        self.advance();
                        Ok(FrameBound::UnboundedPreceding)
                    }
                    TokenType::Following => {
                        self.advance();
                        Ok(FrameBound::UnboundedFollowing)
                    }
                    _ => Err(SqlError::ParseError {
                        message: "Expected PRECEDING or FOLLOWING after UNBOUNDED".to_string(),
                        position: Some(self.current_token().position),
                    }),
                }
            }
            TokenType::Current => {
                self.advance(); // consume CURRENT
                self.expect(TokenType::Row)?;
                Ok(FrameBound::CurrentRow)
            }
            TokenType::Number => {
                let offset_str = self.current_token().value.clone();
                let offset = offset_str
                    .parse::<u64>()
                    .map_err(|_| SqlError::ParseError {
                        message: format!("Invalid numeric offset: {}", offset_str),
                        position: Some(self.current_token().position),
                    })?;
                self.advance(); // consume number

                match self.current_token().token_type {
                    TokenType::Preceding => {
                        self.advance();
                        Ok(FrameBound::Preceding(offset))
                    }
                    TokenType::Following => {
                        self.advance();
                        Ok(FrameBound::Following(offset))
                    }
                    _ => Err(SqlError::ParseError {
                        message: "Expected PRECEDING or FOLLOWING after numeric offset".to_string(),
                        position: Some(self.current_token().position),
                    }),
                }
            }
            TokenType::Interval => {
                self.advance(); // consume INTERVAL

                // Parse the interval value (as string literal)
                let value_str = self.expect(TokenType::String)?.value;
                let value = value_str.parse::<i64>().map_err(|_| SqlError::ParseError {
                    message: format!("Invalid interval value: {}", value_str),
                    position: Some(self.current_token().position),
                })?;

                // Parse the time unit (DAY, HOUR, MINUTE, etc.)
                let unit_str = self.expect(TokenType::Identifier)?.value.to_uppercase();
                let unit = match unit_str.as_str() {
                    "MILLISECOND" | "MILLISECONDS" => TimeUnit::Millisecond,
                    "SECOND" | "SECONDS" => TimeUnit::Second,
                    "MINUTE" | "MINUTES" => TimeUnit::Minute,
                    "HOUR" | "HOURS" => TimeUnit::Hour,
                    "DAY" | "DAYS" => TimeUnit::Day,
                    _ => {
                        return Err(SqlError::ParseError {
                            message: format!(
                                "Unsupported time unit '{}'. Supported units: MILLISECOND, SECOND, MINUTE, HOUR, DAY",
                                unit_str
                            ),
                            position: Some(self.current_token().position),
                        });
                    }
                };

                // Parse PRECEDING or FOLLOWING
                match self.current_token().token_type {
                    TokenType::Preceding => {
                        self.advance();
                        Ok(FrameBound::IntervalPreceding { value, unit })
                    }
                    TokenType::Following => {
                        self.advance();
                        Ok(FrameBound::IntervalFollowing { value, unit })
                    }
                    _ => Err(SqlError::ParseError {
                        message: "Expected PRECEDING or FOLLOWING after INTERVAL".to_string(),
                        position: Some(self.current_token().position),
                    }),
                }
            }
            _ => Err(SqlError::ParseError {
                message: "Expected UNBOUNDED, CURRENT, numeric offset, or INTERVAL in frame bound"
                    .to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }
}
