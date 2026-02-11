/*!
# SELECT Statement Parsing

This module handles parsing of SELECT statements and related SELECT field operations.
It provides methods for parsing complete SELECT queries with all optional clauses,
as well as specialized variants for different contexts (CREATE TABLE, job commands).

## Purpose

SELECT parsing is the core of the SQL query processor, handling:
- **Field Selection**: Columns, expressions, wildcards, and aliases
- **Data Sources**: FROM clauses with streams, URIs, and WITH options
- **Joins**: Multiple JOIN clauses with various types (INNER, LEFT, RIGHT, FULL OUTER)
- **Filtering**: WHERE and HAVING clauses
- **Grouping**: GROUP BY with optional aggregation
- **Windowing**: Time-based WINDOW specifications
- **Ordering**: ORDER BY with ASC/DESC
- **Limits**: LIMIT clause for result truncation
- **Emit Modes**: CHANGES vs FINAL for streaming aggregations
- **Primary Keys**: PRIMARY KEY annotations for Kafka message keys

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   parse_select  │────▶│ parse_select_   │────▶│ StreamingQuery  │
│  (full WITH)    │     │   fields()      │     │    ::Select     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│ parse_select_   │────▶│ parse_join_     │
│   no_with()     │     │   clauses()     │
└─────────────────┘     └─────────────────┘
```

## Key Methods

### parse_select()
Full SELECT parsing with WITH clause support. Handles:
- SELECT ... INTO syntax (transforms to InsertInto)
- UNION operations
- FROM clause WITH options
- Statement-level WITH properties

### parse_select_no_with()
SELECT parsing without consuming WITH clause. Used in job contexts
where WITH properties belong to the parent command (JOB, CREATE TABLE).

### parse_select_for_create_table()
Variant for CREATE TABLE AS SELECT that doesn't consume semicolons,
allowing the parent parser to handle WITH clauses.

### parse_select_fields()
Parses the field list with optional PRIMARY KEY annotations.
Supports:
- Wildcard: `SELECT *`
- Expressions: `SELECT price * quantity`
- Aliases: `SELECT price AS unit_price`
- Primary Keys: `SELECT symbol PRIMARY KEY, price`

### parse_join_clauses() / parse_join_clause()
Handles multiple JOIN operations with:
- JOIN types: INNER, LEFT, RIGHT, FULL OUTER
- ON conditions
- Optional WITHIN windows for time-bounded joins
- Stream aliases

## Clause Ordering

**Standard SQL ordering (enforced in parse_select):**
```sql
SELECT ... FROM ... [JOIN ...] WHERE ... GROUP BY ... WINDOW ... HAVING ... ORDER BY ... LIMIT ... EMIT ...
```

**CRITICAL**: GROUP BY must appear before WINDOW clause. This is validated
with explicit error messages to catch common mistakes.

## Examples

### Basic SELECT
```sql
SELECT symbol, price FROM trades WHERE price > 100
```

### Windowed Aggregation
```sql
SELECT symbol, AVG(price) AS avg_price
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
```

### JOIN with Time Window
```sql
SELECT o.symbol, o.quantity, p.price
FROM orders o
INNER JOIN prices p ON o.symbol = p.symbol
WITHIN INTERVAL '1' MINUTE
```

### SELECT INTO (transforms to InsertInto)
```sql
SELECT symbol, AVG(price) AS avg_price
FROM trades
GROUP BY symbol
INTO aggregated_trades
WITH (format = 'avro')
```

### Primary Keys for Kafka Partitioning
```sql
SELECT symbol PRIMARY KEY, price, volume
FROM trades
```

## Implementation Notes

- **Three Parsing Variants**: Each serves a different context with precise WITH/semicolon handling
- **Primary Key Tracking**: key_fields extracted from PRIMARY KEY annotations, used for Kafka message keys
- **URI Support**: FROM clause accepts both stream names and data source URIs (FR-047)
- **Alias Handling**: Table aliases supported in FROM and JOIN clauses
- **Error Messages**: Context-aware errors with position information for better diagnostics
*/

use super::common::TokenParser;
use super::*;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use std::time::Duration;

impl<'a> TokenParser<'a> {
    /// Parse a complete SELECT statement with WITH clause support.
    ///
    /// This is the primary SELECT parsing entry point, handling:
    /// - Full SELECT syntax with all clauses
    /// - SELECT ... INTO transformation to InsertInto
    /// - UNION operations (UNION and UNION ALL)
    /// - FROM clause WITH options and SELECT-level WITH properties
    ///
    /// Grammar:
    /// ```text
    /// SELECT [DISTINCT] fields FROM source [alias] [WITH (...)]
    ///   [JOIN ...] [WHERE ...] [GROUP BY ...] [WINDOW ...]
    ///   [HAVING ...] [ORDER BY ...] [LIMIT n] [EMIT mode]
    ///   [INTO target [WITH (...)]] [WITH (...)] [UNION [ALL] SELECT ...]
    /// ```
    pub(super) fn parse_select(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;

        let distinct = self.parse_distinct();

        let (fields, key_fields) = self.parse_select_fields()?;

        // FROM clause is optional (for scalar subqueries like SELECT 1)
        let mut from_alias = None;
        let mut _from_with_options: Option<std::collections::HashMap<String, String>> = None;
        let from_stream = if self.current_token().token_type == TokenType::From {
            self.advance(); // consume FROM

            // Support identifiers, keywords as identifiers, and URI strings (FR-047)
            let stream_name = match self.current_token().token_type {
                TokenType::String => {
                    // URI string like 'file://path' or 'kafka://broker/topic'
                    let uri = self.current_token().value.clone();
                    self.advance();
                    uri
                }
                _ => {
                    // Try to parse as identifier or keyword that can be used as identifier
                    self.expect_identifier_or_keyword()
                        .map_err(|_| SqlError::ParseError {
                            message: "Expected stream name or data source URI after FROM"
                                .to_string(),
                            position: Some(self.current_token().position),
                        })?
                }
            };

            // Parse optional alias for FROM clause (e.g., "FROM events s" or "FROM 'file://data.csv' f")
            from_alias = if self.current_token().token_type == TokenType::Identifier {
                let alias = self.current_token().value.clone();
                self.advance();
                Some(alias)
            } else {
                None
            };

            // Parse WITH clause for FROM source (e.g., "FROM kafka_source WITH (...)")
            _from_with_options = if self.current_token().token_type == TokenType::With {
                self.advance(); // consume WITH
                self.expect(TokenType::LeftParen)?;

                let mut options = std::collections::HashMap::new();

                // Parse key-value pairs
                loop {
                    if self.current_token().token_type == TokenType::RightParen {
                        break;
                    }

                    // Parse key
                    let key = match self.current_token().token_type {
                        TokenType::String => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        TokenType::Identifier => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message: "Expected string or identifier for WITH option key"
                                    .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    self.expect(TokenType::Equal)?;

                    // Parse value
                    let value = match self.current_token().token_type {
                        TokenType::String => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Identifier => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Number => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message:
                                    "Expected string, identifier, or number for WITH option value"
                                        .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    options.insert(key, value);

                    // Check for comma or end
                    if self.current_token().token_type == TokenType::Comma {
                        self.advance();
                    } else if self.current_token().token_type == TokenType::RightParen {
                        break;
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Expected comma or closing parenthesis in WITH clause"
                                .to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                }

                self.expect(TokenType::RightParen)?;
                Some(options)
            } else {
                None
            };

            stream_name
        } else {
            // No FROM clause - use a dummy stream name for scalar queries
            "".to_string()
        };

        // Parse JOIN clauses
        let joins = self.parse_join_clauses()?;

        let mut where_clause = None;
        if self.current_token().token_type == TokenType::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        // Parse GROUP BY first (correct ordering: GROUP BY comes first, then WINDOW)
        let mut group_by = None;
        if self.current_token().token_type == TokenType::GroupBy {
            self.advance();
            self.expect_keyword("BY")?;
            group_by = Some(self.parse_group_by_list()?);
        }

        // Parse WINDOW clause after GROUP BY
        let mut window = None;
        if self.current_token().token_type == TokenType::Window {
            self.advance();
            window = Some(self.parse_window_spec()?);
        }

        // STRICT ORDERING VALIDATION: If WINDOW was parsed, ensure GROUP BY doesn't appear after it
        if window.is_some() && self.current_token().token_type == TokenType::GroupBy {
            return Err(SqlError::ParseError {
                message: "GROUP BY clause must come before WINDOW clause. Correct syntax: SELECT ... GROUP BY ... WINDOW ... HAVING ...".to_string(),
                position: Some(self.current_token().position),
            });
        }

        let mut having = None;
        if self.current_token().token_type == TokenType::Having {
            self.advance();
            having = Some(self.parse_expression()?);
        }

        let mut order_by = None;
        if self.current_token().token_type == TokenType::OrderBy {
            self.advance();
            self.expect_keyword("BY")?;
            order_by = Some(self.parse_order_by_list()?);
        }

        let mut limit = None;
        if self.current_token().token_type == TokenType::Limit {
            self.advance();
            let limit_token = self.expect(TokenType::Number)?;
            limit = Some(
                limit_token
                    .value
                    .parse::<u64>()
                    .map_err(|_| self.create_parse_error("Invalid LIMIT value"))?,
            );
        }

        // Aggregation mode is now fully replaced by EMIT clauses

        // Parse optional EMIT clause
        let mut emit_mode = None;
        if self.current_token().token_type == TokenType::Emit {
            self.advance();

            let emit_token = self.current_token().clone();

            match emit_token.token_type {
                TokenType::Changes => {
                    self.advance();
                    emit_mode = Some(crate::velostream::sql::ast::EmitMode::Changes);
                }
                TokenType::Final => {
                    self.advance();
                    emit_mode = Some(crate::velostream::sql::ast::EmitMode::Final);
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected CHANGES or FINAL after EMIT".to_string(),
                        position: Some(emit_token.position),
                    });
                }
            }
        }

        // Check for INTO clause (to support SELECT ... INTO syntax)
        if self.current_token().token_type == TokenType::Into {
            // This is a SELECT ... INTO statement, parse it as InsertInto
            self.advance(); // consume INTO

            // Parse the target table/sink name
            let table_name = match self.current_token().token_type {
                TokenType::Identifier => {
                    let name = self.current_token().value.clone();
                    self.advance();
                    name
                }
                TokenType::String => {
                    let uri = self.current_token().value.clone();
                    self.advance();
                    uri
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected table name or sink URI after INTO".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            };

            // Parse optional WITH clause for INSERT INTO statements
            let _properties = if self.current_token().token_type == TokenType::With {
                Some(self.parse_with_properties()?)
            } else {
                None
            };

            // Combine FROM WITH properties with INSERT INTO WITH properties
            let combined_properties = match (_from_with_options, _properties) {
                (Some(mut from_props), Some(into_props)) => {
                    from_props.extend(into_props);
                    Some(from_props)
                }
                (Some(from_props), None) => Some(from_props),
                (None, Some(into_props)) => Some(into_props),
                (None, None) => None,
            };

            // Determine if from_stream is a URI or named stream
            let from_source = if from_stream.contains("://") {
                StreamSource::Uri(from_stream)
            } else {
                StreamSource::Stream(from_stream) // Both scalar queries and named streams
            };

            // Create the nested SELECT query
            let select_query = StreamingQuery::Select {
                fields,
                distinct,
                key_fields: key_fields.clone(),
                from: from_source,
                from_alias,
                joins,
                where_clause,
                group_by,
                having,
                window,
                order_by,
                limit,
                emit_mode,
                properties: combined_properties,
                job_mode: None,
                batch_size: None,
                num_partitions: None,
                partitioning_strategy: None,
            };

            // Consume optional semicolon
            self.consume_semicolon();

            // Return as InsertInto with SELECT source
            return Ok(StreamingQuery::InsertInto {
                table_name,
                columns: None, // SELECT ... INTO doesn't specify column names
                source: InsertSource::Select {
                    query: Box::new(select_query),
                },
            });
        }

        // Parse optional WITH clause for SELECT statements
        let with_properties = if self.current_token().token_type == TokenType::With {
            Some(self.parse_with_properties()?)
        } else {
            None
        };

        // Combine FROM WITH properties with SELECT WITH properties
        let combined_properties = match (_from_with_options, with_properties) {
            (Some(mut from_props), Some(select_props)) => {
                from_props.extend(select_props);
                Some(from_props)
            }
            (Some(from_props), None) => Some(from_props),
            (None, Some(select_props)) => Some(select_props),
            (None, None) => None,
        };

        // Determine if from_stream is a URI or named stream
        let from_source = if from_stream.contains("://") {
            StreamSource::Uri(from_stream)
        } else {
            StreamSource::Stream(from_stream) // Both scalar queries and named streams
        };

        // Consume optional semicolon
        self.consume_semicolon();

        let select_query = StreamingQuery::Select {
            fields,
            distinct,
            key_fields: key_fields.clone(),
            from: from_source,
            from_alias,
            joins,
            where_clause,
            group_by,
            having,
            window,
            order_by,
            limit,
            emit_mode,
            properties: combined_properties,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        // Check for UNION after SELECT
        if self.current_token().token_type == TokenType::Union {
            self.advance(); // consume UNION

            // Check for ALL keyword
            let all = if self.current_token().token_type == TokenType::All {
                self.advance(); // consume ALL
                true
            } else {
                false
            };

            // Parse the right side of the UNION
            let right_query = self.parse_select()?;

            Ok(StreamingQuery::Union {
                left: Box::new(select_query),
                right: Box::new(right_query),
                all,
            })
        } else {
            Ok(select_query)
        }
    }

    /// Parse SELECT for CREATE TABLE AS SELECT context.
    ///
    /// This variant differs from parse_select in two ways:
    /// 1. Does NOT consume trailing semicolon (parent parser handles it)
    /// 2. Allows flexible clause ordering for legacy compatibility
    ///
    /// Used by: CREATE TABLE parser to allow WITH clause on parent command
    pub(super) fn parse_select_for_create_table(&mut self) -> Result<StreamingQuery, SqlError> {
        // Similar to parse_select_no_with but without consuming semicolon
        // to allow CREATE TABLE parser to continue with WITH clause
        self.expect(TokenType::Select)?;

        let distinct = self.parse_distinct();

        let (fields, key_fields) = self.parse_select_fields()?;

        // FROM clause is optional (for scalar subqueries like SELECT 1)
        let mut from_alias = None;
        let mut _from_with_options: Option<std::collections::HashMap<String, String>> = None;
        let from_stream = if self.current_token().token_type == TokenType::From {
            self.advance(); // consume FROM

            // Support both identifiers and URI strings (FR-047)
            let stream_name = match self.current_token().token_type {
                TokenType::Identifier => {
                    let name = self.current_token().value.clone();
                    self.advance();

                    // Check for alias (AS keyword or direct identifier)
                    if self.current_token().token_type == TokenType::As {
                        self.advance(); // consume AS
                        from_alias = Some(self.expect(TokenType::Identifier)?.value);
                    } else if self.current_token().token_type == TokenType::Identifier {
                        // Direct alias without AS keyword
                        from_alias = Some(self.current_token().value.clone());
                        self.advance();
                    }

                    name
                }
                TokenType::String => {
                    let uri = self.current_token().value.clone();
                    self.advance();
                    uri
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected stream name or data source URI after FROM".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            };

            // Parse WITH clause for FROM source (e.g., "FROM kafka_source WITH (...)")
            _from_with_options = if self.current_token().token_type == TokenType::With {
                self.advance(); // consume WITH
                self.expect(TokenType::LeftParen)?;

                let mut options = std::collections::HashMap::new();

                // Parse key-value pairs
                loop {
                    if self.current_token().token_type == TokenType::RightParen {
                        break;
                    }

                    // Parse key
                    let key = match self.current_token().token_type {
                        TokenType::String => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        TokenType::Identifier => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message: "Expected string or identifier for WITH option key"
                                    .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    self.expect(TokenType::Equal)?;

                    // Parse value
                    let value = match self.current_token().token_type {
                        TokenType::String => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Identifier => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Number => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message:
                                    "Expected string, identifier, or number for WITH option value"
                                        .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    options.insert(key, value);

                    if self.current_token().token_type == TokenType::Comma {
                        self.advance();
                    } else if self.current_token().token_type == TokenType::RightParen {
                        // End of options
                        break;
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Expected ',' or ')' in WITH options".to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                }

                self.expect(TokenType::RightParen)?;
                Some(options)
            } else {
                None
            };

            stream_name
        } else {
            // No FROM clause - use a dummy stream name for scalar queries
            "".to_string()
        };

        // Parse JOIN clauses
        let joins = self.parse_join_clauses()?;

        let mut where_clause = None;
        if self.current_token().token_type == TokenType::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        // Parse WINDOW, GROUP BY, HAVING, ORDER BY, LIMIT in any order
        // This allows flexible clause ordering (though some orderings are non-standard)
        let mut window = None;
        let mut group_by = None;
        let mut having = None;
        let mut order_by = None;
        let mut limit = None;
        let mut emit_mode = None;

        // Loop to parse clauses in any order - continues until no more recognized clauses
        loop {
            match self.current_token().token_type {
                TokenType::Window if window.is_none() => {
                    self.advance();
                    window = Some(self.parse_window_spec()?);
                }
                TokenType::GroupBy if group_by.is_none() => {
                    self.advance();
                    self.expect_keyword("BY")?;
                    group_by = Some(self.parse_group_by_list()?);
                }
                TokenType::Having if having.is_none() => {
                    self.advance();
                    having = Some(self.parse_expression()?);
                }
                TokenType::OrderBy if order_by.is_none() => {
                    self.advance();
                    self.expect_keyword("BY")?;
                    order_by = Some(self.parse_order_by_list()?);
                }
                TokenType::Limit if limit.is_none() => {
                    self.advance();
                    limit = Some(self.expect(TokenType::Number)?.value.parse().unwrap_or(100));
                }
                // EMIT CHANGES/FINAL is part of SELECT syntax
                TokenType::Emit if emit_mode.is_none() => {
                    emit_mode = self.parse_emit_clause()?;
                }
                _ => break, // No more recognized clauses, exit loop
            }
        }

        // Determine if from_stream is a URI or named stream
        let from_source = if from_stream.contains("://") {
            StreamSource::Uri(from_stream)
        } else {
            StreamSource::Stream(from_stream) // Both scalar queries and named streams
        };

        // DO NOT consume semicolon here - let CREATE TABLE parser handle WITH clause

        Ok(StreamingQuery::Select {
            fields,
            distinct,
            key_fields,
            from: from_source,
            from_alias,
            joins,
            where_clause,
            group_by,
            having,
            window,
            order_by,
            limit,
            emit_mode,
            properties: _from_with_options, // WITH clause options from FROM source
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        })
    }

    /// Parse SELECT without consuming WITH clause.
    ///
    /// This variant is used in job contexts where the WITH clause belongs
    /// to the parent command (JOB, CREATE TABLE) rather than the SELECT.
    ///
    /// Differences from parse_select:
    /// - Does NOT parse trailing WITH clause (parent handles it)
    /// - Supports UNION operations
    /// - Standard clause ordering (GROUP BY before WINDOW)
    ///
    /// Used by: JOB command parser
    pub(super) fn parse_select_no_with(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;

        let distinct = self.parse_distinct();

        let (fields, key_fields) = self.parse_select_fields()?;

        // FROM clause is optional (for scalar subqueries like SELECT 1)
        let mut from_alias = None;
        let mut _from_with_options: Option<std::collections::HashMap<String, String>> = None;
        let from_stream = if self.current_token().token_type == TokenType::From {
            self.advance(); // consume FROM

            // Support identifiers, keywords as identifiers, and URI strings (FR-047)
            let stream_name = match self.current_token().token_type {
                TokenType::String => {
                    // URI string like 'file://path' or 'kafka://broker/topic'
                    let uri = self.current_token().value.clone();
                    self.advance();
                    uri
                }
                _ => {
                    // Try to parse as identifier or keyword that can be used as identifier
                    self.expect_identifier_or_keyword()
                        .map_err(|_| SqlError::ParseError {
                            message: "Expected stream name or data source URI after FROM"
                                .to_string(),
                            position: Some(self.current_token().position),
                        })?
                }
            };

            // Parse optional alias for FROM clause (e.g., "FROM events s" or "FROM 'file://data.csv' f")
            from_alias = if self.current_token().token_type == TokenType::Identifier {
                let alias = self.current_token().value.clone();
                self.advance();
                Some(alias)
            } else {
                None
            };

            // Parse WITH clause for FROM source (e.g., "FROM kafka_source WITH (...)")
            _from_with_options = if self.current_token().token_type == TokenType::With {
                self.advance(); // consume WITH
                self.expect(TokenType::LeftParen)?;

                let mut options = std::collections::HashMap::new();

                // Parse key-value pairs
                loop {
                    if self.current_token().token_type == TokenType::RightParen {
                        break;
                    }

                    // Parse key
                    let key = match self.current_token().token_type {
                        TokenType::String => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        TokenType::Identifier => {
                            let k = self.current_token().value.clone();
                            self.advance();
                            k
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message: "Expected string or identifier for WITH option key"
                                    .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    self.expect(TokenType::Equal)?;

                    // Parse value
                    let value = match self.current_token().token_type {
                        TokenType::String => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Identifier => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        TokenType::Number => {
                            let v = self.current_token().value.clone();
                            self.advance();
                            v
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message:
                                    "Expected string, identifier, or number for WITH option value"
                                        .to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };

                    options.insert(key, value);

                    // Check for comma or end
                    if self.current_token().token_type == TokenType::Comma {
                        self.advance();
                    } else if self.current_token().token_type == TokenType::RightParen {
                        break;
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Expected comma or closing parenthesis in WITH clause"
                                .to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                }

                self.expect(TokenType::RightParen)?;
                Some(options)
            } else {
                None
            };

            stream_name
        } else {
            // No FROM clause - use a dummy stream name for scalar queries
            "".to_string()
        };

        // Parse JOIN clauses
        let joins = self.parse_join_clauses()?;

        let mut where_clause = None;
        if self.current_token().token_type == TokenType::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }

        // Parse GROUP BY first (standard SQL ordering)
        let mut group_by = None;
        if self.current_token().token_type == TokenType::GroupBy {
            self.advance();
            self.expect_keyword("BY")?;
            group_by = Some(self.parse_group_by_list()?);
        }

        // Parse WINDOW clause after GROUP BY (standard SQL: GROUP BY comes before WINDOW)
        let mut window = None;
        if self.current_token().token_type == TokenType::Window {
            self.advance();
            window = Some(self.parse_window_spec()?);
        }

        let mut having = None;
        if self.current_token().token_type == TokenType::Having {
            self.advance();
            having = Some(self.parse_expression()?);
        }

        let mut order_by = None;
        if self.current_token().token_type == TokenType::OrderBy {
            self.advance();
            self.expect_keyword("BY")?;
            order_by = Some(self.parse_order_by_list()?);
        }

        let mut limit = None;
        if self.current_token().token_type == TokenType::Limit {
            self.advance();
            let limit_token = self.expect(TokenType::Number)?;
            limit = Some(
                limit_token
                    .value
                    .parse::<u64>()
                    .map_err(|_| self.create_parse_error("Invalid LIMIT value"))?,
            );
        }

        // Parse optional EMIT clause
        let mut emit_mode = None;
        if self.current_token().token_type == TokenType::Emit {
            self.advance();

            let emit_token = self.current_token().clone();

            match emit_token.token_type {
                TokenType::Changes => {
                    self.advance();
                    emit_mode = Some(crate::velostream::sql::ast::EmitMode::Changes);
                }
                TokenType::Final => {
                    self.advance();
                    emit_mode = Some(crate::velostream::sql::ast::EmitMode::Final);
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected CHANGES or FINAL after EMIT".to_string(),
                        position: Some(emit_token.position),
                    });
                }
            }
        }

        // Skip INTO and WITH clauses - they belong to the parent job command
        // This method is specifically for job contexts where WITH belongs to the job

        // Determine if from_stream is a URI or named stream
        let from_source = if from_stream.contains("://") {
            StreamSource::Uri(from_stream)
        } else {
            StreamSource::Stream(from_stream) // Both scalar queries and named streams
        };

        // Consume optional semicolon
        self.consume_semicolon();

        let select_query = StreamingQuery::Select {
            fields,
            distinct,
            key_fields,
            from: from_source,
            from_alias,
            joins,
            where_clause,
            group_by,
            having,
            window,
            order_by,
            limit,
            emit_mode,
            properties: _from_with_options, // WITH clause options from FROM source
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        // Check for UNION after SELECT
        if self.current_token().token_type == TokenType::Union {
            self.advance(); // consume UNION

            // Check for ALL keyword
            let all = if self.current_token().token_type == TokenType::All {
                self.advance(); // consume ALL
                true
            } else {
                false
            };

            // Parse the right side of the UNION (also without WITH)
            let right_query = self.parse_select_no_with()?;

            Ok(StreamingQuery::Union {
                left: Box::new(select_query),
                right: Box::new(right_query),
                all,
            })
        } else {
            Ok(select_query)
        }
    }

    /// Parse SELECT fields with optional PRIMARY KEY annotation (SQL standard).
    ///
    /// Syntax: SELECT field1 PRIMARY KEY, field2, field3 PRIMARY KEY FROM ...
    ///
    /// Returns (fields, key_fields) where key_fields contains the names of
    /// fields marked with PRIMARY KEY for Kafka message key designation.
    fn parse_select_fields(&mut self) -> Result<(Vec<SelectField>, Option<Vec<String>>), SqlError> {
        let mut fields = Vec::new();
        let mut key_fields = Vec::new();

        loop {
            if self.current_token().token_type == TokenType::Asterisk {
                self.advance();
                fields.push(SelectField::Wildcard);
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.current_token().token_type == TokenType::As {
                    self.advance();
                    let alias_name = self.expect(TokenType::Identifier)?.value;
                    Some(normalize_column_name(alias_name))
                } else {
                    None
                };

                // Check for PRIMARY KEY annotation after expression/alias (SQL standard)
                let is_key = if self.current_token().token_type == TokenType::Primary {
                    self.advance();
                    if self.current_token().token_type == TokenType::Key {
                        self.advance();
                        true
                    } else {
                        return Err(SqlError::parse_error(
                            "Expected KEY after PRIMARY",
                            Some(self.current_token().position),
                        ));
                    }
                } else {
                    false
                };

                // If marked as PRIMARY KEY, extract the field name for key_fields
                if is_key {
                    let field_name = alias.clone().or_else(|| {
                        if let Expr::Column(name) = &expr {
                            // Strip table alias prefix (e.g., "a.symbol" → "symbol")
                            // so PRIMARY KEY field names match output field names
                            let base_name = if let Some(pos) = name.rfind('.') {
                                name[pos + 1..].to_string()
                            } else {
                                name.clone()
                            };
                            Some(base_name)
                        } else {
                            None
                        }
                    });
                    if let Some(name) = field_name {
                        key_fields.push(name);
                    }
                }

                fields.push(SelectField::Expression { expr, alias });
            }

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        let key_fields_opt = if key_fields.is_empty() {
            None
        } else {
            Some(key_fields)
        };

        Ok((fields, key_fields_opt))
    }

    /// Parse multiple JOIN clauses.
    ///
    /// Supports various JOIN types:
    /// - [INNER] JOIN
    /// - LEFT [OUTER] JOIN
    /// - RIGHT [OUTER] JOIN
    /// - FULL OUTER JOIN
    ///
    /// Returns None if no JOIN clauses found, Some(vec) otherwise.
    fn parse_join_clauses(&mut self) -> Result<Option<Vec<JoinClause>>, SqlError> {
        let mut joins = Vec::new();

        while matches!(
            self.current_token().token_type,
            TokenType::Join
                | TokenType::Inner
                | TokenType::Left
                | TokenType::Right
                | TokenType::Full
        ) {
            let join_clause = self.parse_join_clause()?;
            joins.push(join_clause);
        }

        if joins.is_empty() {
            Ok(None)
        } else {
            Ok(Some(joins))
        }
    }

    /// Parse a single JOIN clause.
    ///
    /// Grammar:
    /// ```text
    /// [INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL OUTER] JOIN stream [alias] ON condition [WITHIN duration]
    /// ```
    ///
    /// Examples:
    /// - `JOIN prices p ON o.symbol = p.symbol`
    /// - `LEFT JOIN inventory i ON p.sku = i.sku WITHIN INTERVAL '5' MINUTE`
    /// - `FULL OUTER JOIN suppliers s ON p.supplier_id = s.id`
    fn parse_join_clause(&mut self) -> Result<JoinClause, SqlError> {
        // Parse JOIN type
        let join_type = match self.current_token().token_type {
            TokenType::Join => {
                self.advance();
                JoinType::Inner
            }
            TokenType::Inner => {
                self.advance();
                self.expect(TokenType::Join)?;
                JoinType::Inner
            }
            TokenType::Left => {
                self.advance();
                // Optional OUTER keyword
                if self.current_token().token_type == TokenType::Outer {
                    self.advance();
                }
                self.expect(TokenType::Join)?;
                JoinType::Left
            }
            TokenType::Right => {
                self.advance();
                // Optional OUTER keyword
                if self.current_token().token_type == TokenType::Outer {
                    self.advance();
                }
                self.expect(TokenType::Join)?;
                JoinType::Right
            }
            TokenType::Full => {
                self.advance();
                // OUTER keyword is required for FULL joins
                self.expect(TokenType::Outer)?;
                self.expect(TokenType::Join)?;
                JoinType::FullOuter
            }
            _ => {
                return Err(self.create_parse_error("Expected JOIN keyword"));
            }
        };

        // Parse the right side stream/table
        let right_source = self.expect(TokenType::Identifier)?.value;

        // Optional alias for the right source
        let right_alias = if self.current_token().token_type == TokenType::Identifier {
            Some(self.current_token().value.clone())
        } else if self.current_token().token_type == TokenType::As {
            self.advance();
            Some(self.expect(TokenType::Identifier)?.value)
        } else {
            None
        };

        if right_alias.is_some() {
            self.advance();
        }

        // Parse ON condition
        self.expect(TokenType::On)?;
        let condition = self.parse_expression()?;

        // Parse optional WITHIN clause for windowed joins
        let mut window = None;
        if self.current_token().token_type == TokenType::Within {
            self.advance();
            let duration_str = self.parse_duration_token()?;
            let time_window = self.parse_duration(&duration_str)?;

            window = Some(JoinWindow {
                time_window,
                grace_period: Some(Duration::from_secs(0)), // Default grace period
            });
        }

        Ok(JoinClause {
            join_type,
            right_source: StreamSource::Stream(right_source),
            right_alias,
            condition,
            window,
        })
    }
}
