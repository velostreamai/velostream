/*!
# SQL Clause Parsing Module

This module contains parsing functions for SQL clauses including:
- Window specifications (TUMBLING, SLIDING, SESSION)
- Duration parsing (time intervals)
- WITH properties and configuration
- INTO clause (sink destinations)
- GROUP BY and ORDER BY lists
- EMIT mode clauses
- Column definitions and data types

All parsers are implemented as methods on `TokenParser<'a>` and use the parent
parser's token stream and error handling mechanisms.
*/

use super::common::TokenParser;
use super::*;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;
use std::time::Duration;

impl<'a> TokenParser<'a> {
    /// Parse a window specification (TUMBLING, SLIDING, or SESSION).
    ///
    /// Supports both simple and complex syntax:
    /// - Simple: `TUMBLING(duration)`, `SLIDING(size, advance)`, `SESSION(gap)`
    /// - Complex: `TUMBLING(time_column, INTERVAL duration)`, etc.
    pub(super) fn parse_window_spec(&mut self) -> Result<WindowSpec, SqlError> {
        let window_type = match self.current_token().value.to_uppercase().as_str() {
            "TUMBLING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;

                // Check if this is complex TUMBLING syntax with time column
                // Complex: TUMBLING (time_column, INTERVAL duration)
                // Simple: TUMBLING(duration)
                let time_column;
                let size;

                // Peek ahead to see if we have a comma after the first parameter
                let first_param = self.parse_session_parameter()?;

                if self.current_token().token_type == TokenType::Comma {
                    // Complex syntax: TUMBLING (time_column, INTERVAL duration)
                    time_column = Some(first_param);
                    self.advance(); // consume comma

                    // Parse duration (could be INTERVAL syntax or simple duration)
                    let size_str = if self.current_token().value.to_uppercase() == "INTERVAL" {
                        self.advance(); // consume INTERVAL
                        let value_str = self.expect(TokenType::String)?.value;
                        let unit_str = self.expect(TokenType::Identifier)?.value.to_uppercase();
                        // Convert full unit names to short forms
                        let short_unit = match unit_str.as_str() {
                            "MILLISECOND" | "MILLISECONDS" => "ms",
                            "SECOND" | "SECONDS" => "s",
                            "MINUTE" | "MINUTES" => "m",
                            "HOUR" | "HOURS" => "h",
                            "DAY" | "DAYS" => "d",
                            _ => "s",
                        };
                        format!("{}{}", value_str, short_unit)
                    } else {
                        self.parse_duration_token()?
                    };
                    size = self.parse_duration(&size_str)?;
                } else {
                    // Simple syntax: TUMBLING(duration)
                    time_column = None;
                    size = self.parse_duration(&first_param)?;
                }

                self.expect(TokenType::RightParen)?;

                WindowSpec::Tumbling { size, time_column }
            }
            "SLIDING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;

                // Check if this is complex SLIDING syntax with time column
                // Complex: SLIDING (time_column, size, advance)
                // Simple: SLIDING(size, advance)
                let time_column;
                let size;
                let advance;

                // Peek ahead to see if we have a comma after the first parameter
                let first_param = self.parse_session_parameter()?;

                if self.current_token().token_type == TokenType::Comma {
                    // Could be either:
                    // 1. Complex: SLIDING (time_column, size, advance)
                    // 2. Simple: SLIDING (size, advance) where first_param is size

                    // Look ahead to determine which one
                    // If there are exactly 2 commas, it's complex syntax
                    // Save current position to tentatively assume complex
                    let tentative_time_column = Some(first_param.clone());
                    self.advance(); // consume comma

                    let second_param = self.parse_session_parameter()?;

                    if self.current_token().token_type == TokenType::Comma {
                        // Complex syntax confirmed: SLIDING (time_column, size, advance)
                        time_column = tentative_time_column;
                        let size_str = second_param;
                        self.advance(); // consume comma

                        let advance_str = self.parse_duration_token()?;

                        size = self.parse_duration(&size_str)?;
                        advance = self.parse_duration(&advance_str)?;
                    } else {
                        // Simple syntax: SLIDING (size, advance)
                        time_column = None;
                        let size_str = first_param;
                        let advance_str = second_param;

                        size = self.parse_duration(&size_str)?;
                        advance = self.parse_duration(&advance_str)?;
                    }
                } else {
                    // This shouldn't happen in valid SLIDING syntax, but handle it
                    return Err(SqlError::ParseError {
                        message: "SLIDING window requires at least two parameters (size, advance) or three parameters (time_column, size, advance)".to_string(),
                        position: None,
                    });
                }

                self.expect(TokenType::RightParen)?;

                WindowSpec::Sliding {
                    size,
                    advance,
                    time_column,
                }
            }
            "SESSION" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;

                // Check if this is complex SESSION syntax with time column and partition key
                // Complex: SESSION (time_column, gap, partition_key)
                // Simple: SESSION(gap)
                let time_column;
                let gap;
                let mut partition_by = Vec::new();

                // Peek ahead to see if we have a comma after the first parameter
                let first_param = self.parse_session_parameter()?;

                if self.current_token().token_type == TokenType::Comma {
                    // Complex syntax: SESSION (time_column, gap, partition_key)
                    time_column = Some(first_param);
                    self.advance(); // consume comma

                    // Parse gap (could be INTERVAL syntax or simple duration)
                    let gap_str = if self.current_token().value.to_uppercase() == "INTERVAL" {
                        self.advance(); // consume INTERVAL
                        let value_str = self.expect(TokenType::String)?.value;
                        let unit_str = self.expect(TokenType::Identifier)?.value.to_uppercase();
                        // Convert full unit names to short forms (HOUR -> h, MINUTE -> m, etc.)
                        let short_unit = match unit_str.as_str() {
                            "MILLISECOND" | "MILLISECONDS" => "ms",
                            "SECOND" | "SECONDS" => "s",
                            "MINUTE" | "MINUTES" => "m",
                            "HOUR" | "HOURS" => "h",
                            "DAY" | "DAYS" => "d",
                            _ => "s", // default to seconds
                        };
                        format!("{}{}", value_str, short_unit)
                    } else {
                        self.parse_duration_token()?
                    };
                    gap = self.parse_duration(&gap_str)?;

                    // Check for partition key
                    if self.current_token().token_type == TokenType::Comma {
                        self.advance(); // consume comma
                        let partition_key = self.parse_session_parameter()?;
                        partition_by.push(partition_key);
                    }
                } else {
                    // Simple syntax: SESSION(gap)
                    time_column = None;
                    gap = self.parse_duration(&first_param)?;
                }

                self.expect(TokenType::RightParen)?;

                WindowSpec::Session {
                    gap,
                    time_column,
                    partition_by,
                }
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: "Expected window type (TUMBLING, SLIDING, SESSION, or HOPPING)"
                        .to_string(),
                    position: None,
                });
            }
        };

        Ok(window_type)
    }

    /// Parse a session parameter which can be:
    /// - A simple identifier: `event_time`
    /// - A table-qualified column: `p.event_time`
    /// - A duration: `4h`
    /// - INTERVAL duration: `INTERVAL '4' HOUR`
    fn parse_session_parameter(&mut self) -> Result<String, SqlError> {
        let token = self.current_token().clone();
        match token.token_type {
            TokenType::Identifier => {
                // Check for INTERVAL syntax first
                if token.value.to_uppercase() == "INTERVAL" {
                    self.advance(); // consume INTERVAL
                    let value_str = self.expect(TokenType::String)?.value;
                    let unit_str = self.expect(TokenType::Identifier)?.value.to_uppercase();
                    // Convert full unit names to short forms
                    let short_unit = match unit_str.as_str() {
                        "MILLISECOND" | "MILLISECONDS" => "ms",
                        "SECOND" | "SECONDS" => "s",
                        "MINUTE" | "MINUTES" => "m",
                        "HOUR" | "HOURS" => "h",
                        "DAY" | "DAYS" => "d",
                        _ => "s",
                    };
                    Ok(format!("{}{}", value_str, short_unit))
                } else {
                    // Regular identifier, check for table.column syntax
                    let first_part = token.value;
                    self.advance();

                    if self.current_token().token_type == TokenType::Dot {
                        self.advance(); // consume dot
                        let field_name = self.expect(TokenType::Identifier)?.value;
                        Ok(format!("{}.{}", first_part, field_name))
                    } else {
                        Ok(first_part)
                    }
                }
            }
            TokenType::Number => {
                // Handle numeric duration like in simple SESSION(4h)
                self.advance();
                if matches!(self.current_token().token_type, TokenType::Identifier) {
                    let unit = self.current_token().value.clone();
                    self.advance();
                    Ok(format!("{}{}", token.value, unit))
                } else {
                    Ok(format!("{}s", token.value))
                }
            }
            _ => Err(SqlError::ParseError {
                message: format!(
                    "Expected identifier, INTERVAL, or duration in SESSION parameter, found {}",
                    token.value
                ),
                position: Some(token.position),
            }),
        }
    }

    /// Parse a duration token which can be:
    /// - An identifier: `5m`
    /// - A number + unit: `5` `m`
    /// - A string: `'5'`
    /// - INTERVAL syntax: `INTERVAL 5 MINUTES`
    pub(super) fn parse_duration_token(&mut self) -> Result<String, SqlError> {
        let token = self.current_token().clone();
        match token.token_type {
            TokenType::Identifier => {
                self.advance();
                Ok(token.value)
            }
            TokenType::Number => {
                self.advance();
                // Check if next token is an identifier (like 's', 'm', 'h')
                if matches!(self.current_token().token_type, TokenType::Identifier) {
                    let unit = self.current_token().value.clone();
                    self.advance();
                    Ok(format!("{}{}", token.value, unit))
                } else {
                    // Just a number, assume seconds
                    Ok(format!("{}s", token.value))
                }
            }
            TokenType::String => {
                // Handle string literals like '5' in INTERVAL '5' MINUTES
                self.advance();
                Ok(token.value)
            }
            TokenType::Interval => {
                // Handle INTERVAL syntax: INTERVAL 5 MINUTES
                self.advance(); // consume INTERVAL

                // Parse the value (number or string)
                let value_token = self.current_token().clone();
                let value_str = match value_token.token_type {
                    TokenType::Number => {
                        self.advance();
                        value_token.value
                    }
                    TokenType::String => {
                        self.advance();
                        value_token.value
                    }
                    _ => {
                        return Err(SqlError::ParseError {
                            message: "Expected number or string after INTERVAL".to_string(),
                            position: Some(value_token.position),
                        });
                    }
                };

                // Parse the time unit
                let unit_token = self.current_token().clone();
                if unit_token.token_type != TokenType::Identifier {
                    return Err(SqlError::ParseError {
                        message: "Expected time unit after INTERVAL value".to_string(),
                        position: Some(unit_token.position),
                    });
                }

                let unit = match unit_token.value.to_uppercase().as_str() {
                    "MINUTES" | "MINUTE" => "m",
                    "SECONDS" | "SECOND" => "s",
                    "HOURS" | "HOUR" => "h",
                    "MILLISECONDS" | "MILLISECOND" => "ms",
                    _ => {
                        return Err(SqlError::ParseError {
                            message: format!("Unsupported time unit: {}", unit_token.value),
                            position: Some(unit_token.position),
                        });
                    }
                };

                self.advance(); // consume time unit
                Ok(format!("{}{}", value_str, unit))
            }
            _ => Err(SqlError::ParseError {
                message: format!("Expected duration, found {:?}", token.token_type),
                position: Some(token.position),
            }),
        }
    }

    /// Parse a duration string into a `Duration` object.
    ///
    /// Supported units: ns, us/μs, ms, s, m, h, d, w
    /// Default unit (if no suffix): seconds
    pub(super) fn parse_duration(&self, duration_str: &str) -> Result<Duration, SqlError> {
        if duration_str.ends_with("ns") {
            let nanos: u64 = duration_str[..duration_str.len() - 2]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'ns' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_nanos(nanos))
        } else if duration_str.ends_with("us") || duration_str.ends_with("μs") {
            let micros: u64 = if duration_str.ends_with("μs") {
                // Unicode μ is 2 bytes, so we need to handle it differently
                let end_pos = duration_str.len() - "μs".len();
                duration_str[..end_pos].parse()
            } else {
                duration_str[..duration_str.len() - 2].parse()
            }
            .map_err(|_| SqlError::ParseError {
                message: format!("Invalid 'us/μs' duration: {}", duration_str),
                position: None,
            })?;
            Ok(Duration::from_micros(micros))
        } else if duration_str.ends_with("ms") {
            let millis: u64 = duration_str[..duration_str.len() - 2]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'ms' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_millis(millis))
        } else if duration_str.ends_with('s') {
            let seconds: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 's' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(seconds))
        } else if duration_str.ends_with('m') {
            let minutes: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'm' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(minutes * 60))
        } else if duration_str.ends_with('h') {
            let hours: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'h' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(hours * 3600))
        } else if duration_str.ends_with('d') {
            let days: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'd' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(days * 3600 * 24))
        } else if duration_str.ends_with('w') {
            let week: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid 'w' duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(week * 3600 * 24 * 7))
        } else {
            // Default to seconds if no unit specified
            let seconds: u64 = duration_str.parse().map_err(|_| SqlError::ParseError {
                message: format!("Invalid/Unknown duration: {}", duration_str),
                position: None,
            })?;
            Ok(Duration::from_secs(seconds))
        }
    }

    /// Parse column definitions: `(column_name data_type [NOT NULL], ...)`
    pub(super) fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDef>, SqlError> {
        self.expect(TokenType::LeftParen)?;
        let mut columns = Vec::new();

        loop {
            let name = self.expect(TokenType::Identifier)?.value;
            let data_type = self.parse_data_type()?;
            let nullable = !self.consume_if_matches("NOT");

            columns.push(ColumnDef {
                name,
                data_type,
                nullable,
                properties: HashMap::new(),
            });

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(TokenType::RightParen)?;
        Ok(columns)
    }

    /// Parse a data type specification.
    ///
    /// Supports: INT, INTEGER, FLOAT, DOUBLE, REAL, STRING, VARCHAR, TEXT,
    /// BOOLEAN, BOOL, TIMESTAMP, DECIMAL, NUMERIC, ARRAY(...), MAP(..., ...)
    pub(super) fn parse_data_type(&mut self) -> Result<DataType, SqlError> {
        let type_name = self.expect(TokenType::Identifier)?.value.to_uppercase();

        match type_name.as_str() {
            "INT" | "INTEGER" => Ok(DataType::Integer),
            "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
            "STRING" | "VARCHAR" | "TEXT" => Ok(DataType::String),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "TIMESTAMP" => Ok(DataType::Timestamp),
            "DECIMAL" | "NUMERIC" => Ok(DataType::Decimal),
            "ARRAY" => {
                self.expect(TokenType::LeftParen)?;
                let inner_type = self.parse_data_type()?;
                self.expect(TokenType::RightParen)?;
                Ok(DataType::Array(Box::new(inner_type)))
            }
            "MAP" => {
                self.expect(TokenType::LeftParen)?;
                let key_type = self.parse_data_type()?;
                self.expect(TokenType::Comma)?;
                let value_type = self.parse_data_type()?;
                self.expect(TokenType::RightParen)?;
                Ok(DataType::Map(Box::new(key_type), Box::new(value_type)))
            }
            _ => Err(SqlError::ParseError {
                message: format!("Unknown data type: {}", type_name),
                position: None,
            }),
        }
    }

    /// Parse WITH properties: `WITH ("key" = "value", ...)`
    pub(super) fn parse_with_properties(&mut self) -> Result<HashMap<String, String>, SqlError> {
        self.expect(TokenType::With)?;
        self.expect(TokenType::LeftParen)?;

        let mut properties = HashMap::new();

        loop {
            let key = self.expect(TokenType::String)?.value;
            // Expect = sign (we don't have it as a token, so look for identifier)
            let equals_token = self.current_token().clone();
            if equals_token.value != "=" {
                return Err(SqlError::ParseError {
                    message: "Expected '=' in property definition".to_string(),
                    position: Some(equals_token.position),
                });
            }
            self.advance();

            let value = self.expect(TokenType::String)?.value;
            properties.insert(key, value);

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(TokenType::RightParen)?;
        Ok(properties)
    }

    /// Parse INTO clause: `INTO sink_name` or `INTO 'kafka://broker/topic'`
    pub(super) fn parse_into_clause(&mut self) -> Result<IntoClause, SqlError> {
        self.expect(TokenType::Into)?;

        // Support both identifiers and URI strings (FR-047) for INTO clause
        let sink_name = match self.current_token().token_type {
            TokenType::Identifier => {
                let name = self.current_token().value.clone();
                self.advance();
                name
            }
            TokenType::String => {
                // URI string like 'file://path' or 'kafka://broker/topic'
                let uri = self.current_token().value.clone();
                self.advance();
                uri
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: "Expected sink name or data sink URI after INTO".to_string(),
                    position: Some(self.current_token().position),
                });
            }
        };

        Ok(IntoClause {
            sink_name,
            sink_properties: HashMap::new(), // Future use for sink-specific properties
        })
    }

    /// Parse enhanced WITH properties with categorized configuration.
    ///
    /// Supports: source_config, sink_config, monitoring_config, security_config
    /// and inline properties for backward compatibility.
    pub(super) fn parse_enhanced_with_properties(&mut self) -> Result<ConfigProperties, SqlError> {
        self.expect(TokenType::With)?;
        self.expect(TokenType::LeftParen)?;

        let mut config = ConfigProperties::default();

        loop {
            let key = self.expect(TokenType::String)?.value;

            // Expect = sign
            let equals_token = self.current_token().clone();
            if equals_token.value != "=" {
                return Err(SqlError::ParseError {
                    message: "Expected '=' in property definition".to_string(),
                    position: Some(equals_token.position),
                });
            }
            self.advance();

            let raw_value = self.expect(TokenType::String)?.value;
            let value = self.resolve_environment_variables(&raw_value)?;

            // Categorize configuration properties
            match key.as_str() {
                "source_config" => config.source_config = Some(value),
                "sink_config" => config.sink_config = Some(value),
                "monitoring_config" => config.monitoring_config = Some(value),
                "security_config" => config.security_config = Some(value),
                // Removed: base_source_config and base_sink_config no longer supported
                _ => {
                    // All other properties go into inline_properties for backward compatibility
                    config.inline_properties.insert(key, value);
                }
            }

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(TokenType::RightParen)?;
        Ok(config)
    }

    /// Resolve environment variables in configuration values.
    ///
    /// Supports patterns:
    /// - `${VAR}`: Simple substitution (error if not set)
    /// - `${VAR:-default}`: With default value
    /// - `${VAR:?error_msg}`: Required with custom error message
    fn resolve_environment_variables(&self, value: &str) -> Result<String, SqlError> {
        use std::env;

        // Simple regex-like replacement for ${...} patterns
        let mut result = value.to_string();

        // Find all ${...} patterns
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_expr = &result[start + 2..start + end];
                let replacement = if let Some(default_pos) = var_expr.find(":-") {
                    // ${VAR:-default} pattern
                    let var_name = &var_expr[..default_pos];
                    let default_value = &var_expr[default_pos + 2..];
                    env::var(var_name).unwrap_or_else(|_| default_value.to_string())
                } else if let Some(error_pos) = var_expr.find(":?") {
                    // ${VAR:?error} pattern
                    let var_name = &var_expr[..error_pos];
                    let error_msg = &var_expr[error_pos + 2..];
                    env::var(var_name).map_err(|_| {
                        SqlError::parse_error(
                            format!(
                                "Required environment variable '{}' not set: {}",
                                var_name, error_msg
                            ),
                            None,
                        )
                    })?
                } else {
                    // Simple ${VAR} pattern
                    env::var(var_expr).map_err(|_| {
                        SqlError::parse_error(
                            format!("Environment variable '{}' not found", var_expr),
                            None,
                        )
                    })?
                };

                // Replace the ${...} with the resolved value
                result.replace_range(start..start + end + 1, &replacement);
            } else {
                return Err(SqlError::parse_error(
                    format!("Malformed environment variable reference in: {}", value),
                    None,
                ));
            }
        }

        Ok(result)
    }

    /// Parse optional EMIT clause: `EMIT CHANGES` or `EMIT FINAL`
    pub(super) fn parse_emit_clause(
        &mut self,
    ) -> Result<Option<crate::velostream::sql::ast::EmitMode>, SqlError> {
        if self.current_token().token_type == TokenType::Emit {
            self.advance();

            let emit_token = self.current_token().clone();

            match emit_token.token_type {
                TokenType::Changes => {
                    self.advance();
                    Ok(Some(crate::velostream::sql::ast::EmitMode::Changes))
                }
                TokenType::Final => {
                    self.advance();
                    Ok(Some(crate::velostream::sql::ast::EmitMode::Final))
                }
                _ => Err(SqlError::ParseError {
                    message: "Expected CHANGES or FINAL after EMIT".to_string(),
                    position: Some(emit_token.position),
                }),
            }
        } else {
            Ok(None)
        }
    }

    /// Parse GROUP BY list: `GROUP BY expr1, expr2, ...`
    pub(super) fn parse_group_by_list(&mut self) -> Result<Vec<Expr>, SqlError> {
        let mut expressions = Vec::new();

        loop {
            let expr = self.parse_expression()?;
            expressions.push(expr);

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse ORDER BY list: `ORDER BY expr1 [ASC|DESC], expr2 [ASC|DESC], ...`
    pub(super) fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, SqlError> {
        let mut expressions = Vec::new();

        loop {
            let expr = self.parse_expression()?;
            let direction = if self.current_token().token_type == TokenType::Asc {
                self.advance();
                OrderDirection::Asc
            } else if self.current_token().token_type == TokenType::Desc {
                self.advance();
                OrderDirection::Desc
            } else {
                OrderDirection::Asc // Default to ASC
            };

            expressions.push(OrderByExpr { expr, direction });

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(expressions)
    }
}
