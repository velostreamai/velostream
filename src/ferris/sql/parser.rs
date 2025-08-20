/*!
# Streaming SQL Parser

This module implements a recursive descent parser for streaming SQL queries, designed specifically
for continuous data processing over Kafka streams. The parser converts SQL text into an Abstract
Syntax Tree (AST) that can be executed by the streaming engine.

## Key Features

- **Complete SQL Grammar**: Supports SELECT, CREATE STREAM, CREATE TABLE with all standard clauses
- **Streaming Extensions**: Native support for windowing operations (TUMBLING, SLIDING, SESSION)
- **Expression Parsing**: Full arithmetic, logical, and comparison expressions with proper precedence
- **Function Support**: Built-in aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- **Error Recovery**: Detailed error messages with position information for debugging
- **Case Insensitive**: SQL keywords are case-insensitive as per SQL standard
- **Robust Tokenization**: Handles strings, numbers, identifiers, operators, and special characters

## Grammar Overview

The parser supports the following SQL grammar (simplified):

```sql
-- SELECT statements
SELECT field_list FROM stream_name
[WHERE condition]
[GROUP BY expression_list]
[HAVING condition]
[WINDOW window_spec]
[ORDER BY order_list]
[LIMIT number]

-- Stream creation
CREATE STREAM stream_name [(column_definitions)] AS select_statement [WITH (properties)]
CREATE TABLE table_name [(column_definitions)] AS select_statement [WITH (properties)]

-- Window specifications
WINDOW TUMBLING(duration)
WINDOW SLIDING(size, advance)
WINDOW SESSION(gap)
```

## Examples

```rust,no_run
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

let parser = StreamingSqlParser::new();

// Simple SELECT query
let query = parser.parse("SELECT * FROM orders WHERE amount > 100").unwrap();

// Windowed aggregation
let query = parser.parse(
    "SELECT customer_id, COUNT(*), AVG(amount)
     FROM orders
     GROUP BY customer_id
     WINDOW TUMBLING(5m)"
).unwrap();

// Stream creation
let query = parser.parse(
    "CREATE STREAM high_value_orders AS
     SELECT * FROM orders WHERE amount > 1000"
).unwrap();
```

## Architecture

The parser is implemented as a two-phase process:

1. **Tokenization**: Converts SQL text into a stream of tokens with position information
2. **Parsing**: Uses recursive descent to build the AST from tokens

### Token Types
- Keywords (SELECT, FROM, WHERE, etc.)
- Identifiers (table names, column names)
- Literals (strings, numbers)
- Operators (+, -, *, /, =, !=, <, >, etc.)
- Punctuation (parentheses, commas, dots)

### Expression Precedence
The parser respects SQL operator precedence:
1. Parentheses (highest)
2. Multiplication, Division
3. Addition, Subtraction
4. Comparison operators
5. Logical operators (lowest)

## Error Handling

The parser provides detailed error messages with position information:
- Syntax errors with expected vs. actual tokens
- Invalid number formats
- Unclosed string literals
- Unknown keywords or operators
- Missing required clauses

All errors implement the `SqlError` type for consistent error handling throughout the system.
*/

use crate::ferris::sql::ast::*;
use crate::ferris::sql::error::SqlError;
use std::collections::HashMap;
use std::time::Duration;

/// Main parser for streaming SQL queries.
///
/// The `StreamingSqlParser` handles the complete parsing pipeline from SQL text to AST.
/// It maintains a keyword lookup table for efficient token classification and provides
/// a simple interface for parsing various types of SQL statements.
///
/// # Examples
///
/// ```rust,no_run
/// use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let parser = StreamingSqlParser::new();
///
///     // Parse a simple SELECT query
///     let query = parser.parse("SELECT customer_id, amount FROM orders")?;
///
///     // Parse a complex windowed aggregation
///     let query = parser.parse(
///         "SELECT customer_id, COUNT(*), SUM(amount)
///          FROM orders
///          WHERE amount > 100
///          GROUP BY customer_id
///          WINDOW TUMBLING(INTERVAL 5 MINUTES)
///          ORDER BY customer_id
///          LIMIT 100"
///     )?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct StreamingSqlParser {
    /// Lookup table mapping SQL keywords to token types for fast classification
    keywords: HashMap<String, TokenType>,
}

/// Token types recognized by the SQL lexer.
///
/// Each token type represents a different category of SQL syntax element,
/// from keywords and operators to literals and punctuation.
#[derive(Debug, Clone, PartialEq)]
enum TokenType {
    // SQL Keywords
    Select,     // SELECT
    From,       // FROM
    Where,      // WHERE
    GroupBy,    // GROUP (parsed as GROUP BY)
    Having,     // HAVING
    OrderBy,    // ORDER (parsed as ORDER BY)
    Asc,        // ASC
    Desc,       // DESC
    Window,     // WINDOW
    Limit,      // LIMIT
    Stream,     // STREAM
    Table,      // TABLE
    Create,     // CREATE
    Into,       // INTO
    As,         // AS
    With,       // WITH
    Show,       // SHOW
    List,       // LIST (alias for SHOW)
    Streams,    // STREAMS
    Tables,     // TABLES
    Topics,     // TOPICS
    Functions,  // FUNCTIONS
    Schema,     // SCHEMA
    Properties, // PROPERTIES
    Jobs,       // JOBS (renamed from Queries)
    Partitions, // PARTITIONS
    Start,      // START
    Stop,       // STOP
    Job,        // JOB (renamed from Query)
    Force,      // FORCE
    Pause,      // PAUSE
    Resume,     // RESUME
    Deploy,     // DEPLOY
    Rollback,   // ROLLBACK
    Version,    // VERSION
    Strategy,   // STRATEGY
    BlueGreen,  // BLUE_GREEN
    Canary,     // CANARY
    Rolling,    // ROLLING
    Replace,    // REPLACE
    Status,     // STATUS
    Versions,   // VERSIONS
    Metrics,    // METRICS
    Describe,   // DESCRIBE
    
    // Aggregation Mode Keywords
    AggregationMode, // AGGREGATION_MODE
    Windowed,   // WINDOWED
    Continuous, // CONTINUOUS

    // Literals and Identifiers
    Identifier, // Column names, table names, function names
    String,     // String literals ('hello', "world")
    Null,       // NULL literal
    Number,     // Numeric literals (42, 3.14)

    // Punctuation
    LeftParen,  // (
    RightParen, // )
    Comma,      // ,
    Asterisk,   // * (wildcard or multiplication)
    Dot,        // . (qualified names)

    // Arithmetic Operators
    Plus,  // +
    Minus, // -
    #[allow(dead_code)]
    Multiply, // * (when used as operator)
    Divide, // /

    // Comparison Operators
    Equal,              // =
    NotEqual,           // !=
    LessThan,           // <
    GreaterThan,        // >
    LessThanOrEqual,    // <=
    GreaterThanOrEqual, // >=

    // JOIN Keywords
    Join,   // JOIN
    Inner,  // INNER
    Left,   // LEFT
    Right,  // RIGHT
    Full,   // FULL
    Outer,  // OUTER
    On,     // ON
    Within, // WITHIN

    // Time Keywords
    Interval, // INTERVAL

    // Conditional Keywords
    Case,   // CASE
    When,   // WHEN
    Then,   // THEN
    Else,   // ELSE
    End,    // END
    Is,     // IS (for IS NULL, IS NOT NULL)
    In,     // IN (for IN operator)
    Not,    // NOT (for NOT IN, IS NOT NULL, etc.)
    Exists, // EXISTS (for EXISTS subqueries)
    Any,    // ANY (for ANY subqueries)
    All,    // ALL (for ALL subqueries)

    // Window Frame Keywords
    Rows,      // ROWS
    Range,     // RANGE
    Between,   // BETWEEN
    And,       // AND
    Preceding, // PRECEDING
    Following, // FOLLOWING
    Current,   // CURRENT
    Row,       // ROW
    Unbounded, // UNBOUNDED
    Over,      // OVER

    // Special
    Eof, // End of input
}

/// A token with its type, value, and position information.
///
/// Tokens are the atomic units of SQL syntax, produced by the lexer
/// and consumed by the parser. Position information enables detailed
/// error reporting.
#[derive(Debug, Clone)]
struct Token {
    /// The type of this token (keyword, operator, literal, etc.)
    token_type: TokenType,
    /// The original text value of the token
    value: String,
    /// Character position in the original SQL string (for error reporting)
    position: usize,
}

impl StreamingSqlParser {
    /// Creates a new SQL parser with all supported keywords initialized.
    ///
    /// The parser is ready to use immediately after construction and can
    /// parse any supported SQL statement type.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
    ///
    /// let parser = StreamingSqlParser::new();
    /// let result = parser.parse("SELECT * FROM orders");
    /// ```
    pub fn new() -> Self {
        let mut keywords = HashMap::new();
        keywords.insert("SELECT".to_string(), TokenType::Select);
        keywords.insert("FROM".to_string(), TokenType::From);
        keywords.insert("WHERE".to_string(), TokenType::Where);
        keywords.insert("GROUP".to_string(), TokenType::GroupBy);
        keywords.insert("HAVING".to_string(), TokenType::Having);
        keywords.insert("ORDER".to_string(), TokenType::OrderBy);
        keywords.insert("ASC".to_string(), TokenType::Asc);
        keywords.insert("DESC".to_string(), TokenType::Desc);
        keywords.insert("WINDOW".to_string(), TokenType::Window);
        keywords.insert("LIMIT".to_string(), TokenType::Limit);
        keywords.insert("STREAM".to_string(), TokenType::Stream);
        keywords.insert("TABLE".to_string(), TokenType::Table);
        keywords.insert("CREATE".to_string(), TokenType::Create);
        keywords.insert("INTO".to_string(), TokenType::Into);
        keywords.insert("AS".to_string(), TokenType::As);
        keywords.insert("WITH".to_string(), TokenType::With);
        keywords.insert("SHOW".to_string(), TokenType::Show);
        keywords.insert("LIST".to_string(), TokenType::List);
        keywords.insert("STREAMS".to_string(), TokenType::Streams);
        keywords.insert("TABLES".to_string(), TokenType::Tables);
        keywords.insert("TOPICS".to_string(), TokenType::Topics);
        keywords.insert("FUNCTIONS".to_string(), TokenType::Functions);
        keywords.insert("SCHEMA".to_string(), TokenType::Schema);
        keywords.insert("PROPERTIES".to_string(), TokenType::Properties);
        keywords.insert("JOBS".to_string(), TokenType::Jobs);
        keywords.insert("PARTITIONS".to_string(), TokenType::Partitions);
        keywords.insert("START".to_string(), TokenType::Start);
        keywords.insert("STOP".to_string(), TokenType::Stop);
        keywords.insert("JOB".to_string(), TokenType::Job);
        keywords.insert("FORCE".to_string(), TokenType::Force);
        keywords.insert("PAUSE".to_string(), TokenType::Pause);
        keywords.insert("RESUME".to_string(), TokenType::Resume);
        keywords.insert("DEPLOY".to_string(), TokenType::Deploy);
        keywords.insert("ROLLBACK".to_string(), TokenType::Rollback);
        keywords.insert("VERSION".to_string(), TokenType::Version);
        keywords.insert("STRATEGY".to_string(), TokenType::Strategy);
        keywords.insert("BLUE_GREEN".to_string(), TokenType::BlueGreen);
        keywords.insert("CANARY".to_string(), TokenType::Canary);
        keywords.insert("ROLLING".to_string(), TokenType::Rolling);
        keywords.insert("REPLACE".to_string(), TokenType::Replace);
        keywords.insert("STATUS".to_string(), TokenType::Status);
        keywords.insert("VERSIONS".to_string(), TokenType::Versions);
        keywords.insert("METRICS".to_string(), TokenType::Metrics);
        keywords.insert("DESCRIBE".to_string(), TokenType::Describe);
        
        // Aggregation Mode Keywords
        keywords.insert("AGGREGATION_MODE".to_string(), TokenType::AggregationMode);
        keywords.insert("WINDOWED".to_string(), TokenType::Windowed);
        keywords.insert("CONTINUOUS".to_string(), TokenType::Continuous);
        
        keywords.insert("JOIN".to_string(), TokenType::Join);
        keywords.insert("INNER".to_string(), TokenType::Inner);
        keywords.insert("LEFT".to_string(), TokenType::Left);
        keywords.insert("RIGHT".to_string(), TokenType::Right);
        keywords.insert("FULL".to_string(), TokenType::Full);
        keywords.insert("OUTER".to_string(), TokenType::Outer);
        keywords.insert("ON".to_string(), TokenType::On);
        keywords.insert("WITHIN".to_string(), TokenType::Within);
        keywords.insert("INTERVAL".to_string(), TokenType::Interval);
        keywords.insert("CASE".to_string(), TokenType::Case);
        keywords.insert("WHEN".to_string(), TokenType::When);
        keywords.insert("THEN".to_string(), TokenType::Then);
        keywords.insert("ELSE".to_string(), TokenType::Else);
        keywords.insert("END".to_string(), TokenType::End);
        keywords.insert("IS".to_string(), TokenType::Is);
        keywords.insert("IN".to_string(), TokenType::In);
        keywords.insert("NOT".to_string(), TokenType::Not);
        keywords.insert("EXISTS".to_string(), TokenType::Exists);
        keywords.insert("ANY".to_string(), TokenType::Any);
        keywords.insert("ALL".to_string(), TokenType::All);
        keywords.insert("ROWS".to_string(), TokenType::Rows);
        keywords.insert("RANGE".to_string(), TokenType::Range);
        keywords.insert("BETWEEN".to_string(), TokenType::Between);
        keywords.insert("AND".to_string(), TokenType::And);
        keywords.insert("PRECEDING".to_string(), TokenType::Preceding);
        keywords.insert("FOLLOWING".to_string(), TokenType::Following);
        keywords.insert("CURRENT".to_string(), TokenType::Current);
        keywords.insert("ROW".to_string(), TokenType::Row);
        keywords.insert("UNBOUNDED".to_string(), TokenType::Unbounded);
        keywords.insert("OVER".to_string(), TokenType::Over);
        keywords.insert("NULL".to_string(), TokenType::Null);

        Self { keywords }
    }

    /// Parses a SQL string into a StreamingQuery AST.
    ///
    /// This is the main entry point for the parser. It handles the complete
    /// parsing pipeline from tokenization to AST construction.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to parse
    ///
    /// # Returns
    /// * `Ok(StreamingQuery)` - Successfully parsed query
    /// * `Err(SqlError)` - Parse error with position and message
    ///
    /// # Supported Statements
    /// - SELECT queries with all standard clauses
    /// - CREATE STREAM AS SELECT
    /// - CREATE TABLE AS SELECT  
    /// - Window specifications (TUMBLING, SLIDING, SESSION)
    /// - Aggregation functions and expressions
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let parser = StreamingSqlParser::new();
    ///
    ///     // Simple query
    ///     let query = parser.parse("SELECT * FROM orders")?;
    ///
    ///     // Complex windowed aggregation
    ///     let query = parser.parse(
    ///         "SELECT customer_id, COUNT(*), AVG(amount)
    ///          FROM orders
    ///          WHERE status = 'active'
    ///          GROUP BY customer_id
    ///          HAVING COUNT(*) > 5
    ///          WINDOW TUMBLING(5m)
    ///          ORDER BY customer_id DESC
    ///          LIMIT 100"
    ///     )?;
    ///     
    ///     // Stream creation
    ///     let query = parser.parse(
    ///         "CREATE STREAM high_value_orders AS
    ///          SELECT * FROM orders WHERE amount > 1000"
    ///     )?;
    ///     Ok(())
    /// }
    /// ```
    pub fn parse(&self, sql: &str) -> Result<StreamingQuery, SqlError> {
        let tokens = self.tokenize(sql)?;
        self.parse_tokens(tokens)
    }

    fn tokenize(&self, sql: &str) -> Result<Vec<Token>, SqlError> {
        let mut tokens = Vec::new();
        let mut chars = sql.chars().peekable();
        let mut position = 0;

        while let Some(&ch) = chars.peek() {
            match ch {
                ' ' | '\t' | '\n' | '\r' => {
                    chars.next();
                    position += 1;
                }
                '(' => {
                    tokens.push(Token {
                        token_type: TokenType::LeftParen,
                        value: "(".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                ')' => {
                    tokens.push(Token {
                        token_type: TokenType::RightParen,
                        value: ")".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                ',' => {
                    tokens.push(Token {
                        token_type: TokenType::Comma,
                        value: ",".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '*' => {
                    // We need to determine context - for now, always treat as asterisk
                    // The parser will handle multiplication vs wildcard contexts
                    tokens.push(Token {
                        token_type: TokenType::Asterisk,
                        value: "*".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '.' => {
                    tokens.push(Token {
                        token_type: TokenType::Dot,
                        value: ".".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '+' => {
                    tokens.push(Token {
                        token_type: TokenType::Plus,
                        value: "+".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '-' => {
                    tokens.push(Token {
                        token_type: TokenType::Minus,
                        value: "-".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '/' => {
                    tokens.push(Token {
                        token_type: TokenType::Divide,
                        value: "/".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '=' => {
                    tokens.push(Token {
                        token_type: TokenType::Equal,
                        value: "=".to_string(),
                        position,
                    });
                    chars.next();
                    position += 1;
                }
                '<' => {
                    chars.next();
                    position += 1;
                    if let Some(&'=') = chars.peek() {
                        tokens.push(Token {
                            token_type: TokenType::LessThanOrEqual,
                            value: "<=".to_string(),
                            position: position - 1,
                        });
                        chars.next();
                        position += 1;
                    } else if let Some(&'>') = chars.peek() {
                        tokens.push(Token {
                            token_type: TokenType::NotEqual,
                            value: "<>".to_string(),
                            position: position - 1,
                        });
                        chars.next();
                        position += 1;
                    } else {
                        tokens.push(Token {
                            token_type: TokenType::LessThan,
                            value: "<".to_string(),
                            position: position - 1,
                        });
                    }
                }
                '>' => {
                    chars.next();
                    position += 1;
                    if let Some(&'=') = chars.peek() {
                        tokens.push(Token {
                            token_type: TokenType::GreaterThanOrEqual,
                            value: ">=".to_string(),
                            position: position - 1,
                        });
                        chars.next();
                        position += 1;
                    } else {
                        tokens.push(Token {
                            token_type: TokenType::GreaterThan,
                            value: ">".to_string(),
                            position: position - 1,
                        });
                    }
                }
                '!' => {
                    chars.next();
                    position += 1;
                    if let Some(&'=') = chars.peek() {
                        tokens.push(Token {
                            token_type: TokenType::NotEqual,
                            value: "!=".to_string(),
                            position: position - 1,
                        });
                        chars.next();
                        position += 1;
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Unexpected character '!' - did you mean '!='?".to_string(),
                            position: Some(position - 1),
                        });
                    }
                }
                '\'' | '"' => {
                    let quote = ch;
                    chars.next();
                    position += 1;
                    let mut value = String::new();

                    while let Some(&next_ch) = chars.peek() {
                        if next_ch == quote {
                            chars.next();
                            position += 1;
                            break;
                        }
                        value.push(next_ch);
                        chars.next();
                        position += 1;
                    }

                    tokens.push(Token {
                        token_type: TokenType::String,
                        value,
                        position,
                    });
                }
                '0'..='9' => {
                    let mut value = String::new();
                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_ascii_digit() || next_ch == '.' {
                            value.push(next_ch);
                            chars.next();
                            position += 1;
                        } else {
                            break;
                        }
                    }

                    tokens.push(Token {
                        token_type: TokenType::Number,
                        value,
                        position,
                    });
                }
                _ if ch.is_alphabetic() || ch == '_' => {
                    let mut value = String::new();
                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_alphanumeric() || next_ch == '_' {
                            value.push(next_ch);
                            chars.next();
                            position += 1;
                        } else {
                            break;
                        }
                    }

                    let token_type = self
                        .keywords
                        .get(&value.to_uppercase())
                        .cloned()
                        .unwrap_or(TokenType::Identifier);

                    tokens.push(Token {
                        token_type,
                        value,
                        position,
                    });
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: format!("Unexpected character '{}' at position {}", ch, position),
                        position: Some(position),
                    });
                }
            }
        }

        tokens.push(Token {
            token_type: TokenType::Eof,
            value: String::new(),
            position,
        });

        Ok(tokens)
    }

    fn parse_tokens(&self, tokens: Vec<Token>) -> Result<StreamingQuery, SqlError> {
        let mut parser = TokenParser::new(tokens);

        match parser.current_token().token_type {
            TokenType::Select => parser.parse_select(),
            TokenType::Create => parser.parse_create(),
            TokenType::Show | TokenType::List => parser.parse_show(),
            TokenType::Start => parser.parse_start_job(),
            TokenType::Stop => parser.parse_stop_job(),
            TokenType::Pause => parser.parse_pause_job(),
            TokenType::Resume => parser.parse_resume_job(),
            TokenType::Deploy => parser.parse_deploy_job(),
            TokenType::Rollback => parser.parse_rollback_job(),
            TokenType::Describe => parser.parse_describe(),
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT, CREATE, SHOW, LIST, START, STOP, PAUSE, RESUME, DEPLOY, ROLLBACK, or DESCRIBE statement".to_string(),
                position: Some(parser.current_token().position)
            })
        }
    }
}

struct TokenParser {
    tokens: Vec<Token>,
    current: usize,
}

impl TokenParser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, current: 0 }
    }

    fn current_token(&self) -> &Token {
        if self.current < self.tokens.len() {
            &self.tokens[self.current]
        } else {
            static EOF_TOKEN: Token = Token {
                token_type: TokenType::Eof,
                value: String::new(),
                position: 0,
            };
            &EOF_TOKEN
        }
    }

    fn peek_token(&self, offset: usize) -> Option<&Token> {
        let peek_index = self.current + offset;
        if peek_index < self.tokens.len() {
            Some(&self.tokens[peek_index])
        } else {
            None
        }
    }

    fn advance(&mut self) {
        if self.current < self.tokens.len() - 1 {
            self.current += 1;
        }
    }

    fn expect(&mut self, expected: TokenType) -> Result<Token, SqlError> {
        let token = self.current_token().clone();
        if token.token_type == expected {
            self.advance();
            Ok(token)
        } else {
            Err(SqlError::ParseError {
                message: format!(
                    "Expected {:?}, found {:?} at position {}",
                    expected, token.token_type, token.position
                ),
                position: Some(token.position),
            })
        }
    }

    fn parse_select(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;

        let fields = self.parse_select_fields()?;

        // FROM clause is optional (for scalar subqueries like SELECT 1)
        let from_stream = if self.current_token().token_type == TokenType::From {
            self.advance(); // consume FROM
            let stream_name = self.expect(TokenType::Identifier)?.value;

            // Parse optional alias for FROM clause (e.g., "FROM events s")
            let _from_alias = if self.current_token().token_type == TokenType::Identifier {
                let alias = self.current_token().value.clone();
                self.advance();
                Some(alias)
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

        let mut group_by = None;
        if self.current_token().token_type == TokenType::GroupBy {
            self.advance();
            self.expect_keyword("BY")?;
            group_by = Some(self.parse_group_by_list()?);
        }

        let mut having = None;
        if self.current_token().token_type == TokenType::Having {
            self.advance();
            having = Some(self.parse_expression()?);
        }

        let mut window = None;
        if self.current_token().token_type == TokenType::Window {
            self.advance();
            window = Some(self.parse_window_spec()?);
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
                    .map_err(|_| SqlError::ParseError {
                        message: "Invalid LIMIT value".to_string(),
                        position: Some(limit_token.position),
                    })?,
            );
        }

        // Parse optional WITH AGGREGATION_MODE clause
        let mut aggregation_mode = None;
        if self.current_token().token_type == TokenType::With {
            self.advance();
            if self.current_token().token_type == TokenType::AggregationMode {
                self.advance();
                self.expect(TokenType::Equal)?;
                
                // Parse the aggregation mode value (can be string or identifier)
                let mode_token = self.current_token().clone();
                let mode_str = match mode_token.token_type {
                    TokenType::String => {
                        self.advance();
                        // Remove quotes from string literal
                        mode_token.value.trim_matches(|c| c == '\'' || c == '"').to_uppercase()
                    },
                    TokenType::Windowed => {
                        self.advance();
                        "WINDOWED".to_string()
                    },
                    TokenType::Continuous => {
                        self.advance(); 
                        "CONTINUOUS".to_string()
                    },
                    TokenType::Identifier => {
                        self.advance();
                        mode_token.value.to_uppercase()
                    },
                    _ => return Err(SqlError::ParseError {
                        message: "Expected aggregation mode value (WINDOWED, CONTINUOUS, or string literal)".to_string(),
                        position: Some(mode_token.position),
                    }),
                };
                
                // Convert string to AggregationMode enum
                use crate::ferris::sql::ast::AggregationMode;
                aggregation_mode = Some(match mode_str.as_str() {
                    "WINDOWED" => AggregationMode::Windowed,
                    "CONTINUOUS" => AggregationMode::Continuous,
                    _ => return Err(SqlError::ParseError {
                        message: format!("Invalid aggregation mode '{}'. Expected 'WINDOWED' or 'CONTINUOUS'", mode_str),
                        position: Some(mode_token.position),
                    }),
                });
            } else {
                return Err(SqlError::ParseError {
                    message: "Expected AGGREGATION_MODE after WITH".to_string(),
                    position: Some(self.current_token().position),
                });
            }
        }

        Ok(StreamingQuery::Select {
            fields,
            from: StreamSource::Stream(from_stream),
            joins,
            where_clause,
            group_by,
            having,
            window,
            order_by,
            limit,
            aggregation_mode,
        })
    }

    fn parse_select_fields(&mut self) -> Result<Vec<SelectField>, SqlError> {
        let mut fields = Vec::new();

        loop {
            if self.current_token().token_type == TokenType::Asterisk {
                self.advance();
                fields.push(SelectField::Wildcard);
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.current_token().token_type == TokenType::As {
                    self.advance();
                    Some(self.expect(TokenType::Identifier)?.value)
                } else {
                    None
                };
                fields.push(SelectField::Expression { expr, alias });
            }

            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(fields)
    }

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
                return Err(SqlError::ParseError {
                    message: "Expected JOIN keyword".to_string(),
                    position: Some(self.current_token().position),
                });
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
            self.expect_keyword("INTERVAL")?;
            let duration_str = self.parse_duration_token()?;
            let time_unit = match self.current_token().token_type {
                TokenType::Identifier => {
                    let unit = self.current_token().value.to_uppercase();
                    self.advance();
                    match unit.as_str() {
                        "MINUTES" | "MINUTE" => TimeUnit::Minute,
                        "SECONDS" | "SECOND" => TimeUnit::Second,
                        "HOURS" | "HOUR" => TimeUnit::Hour,
                        _ => {
                            return Err(SqlError::ParseError {
                                message: format!("Unsupported time unit: {}", unit),
                                position: Some(self.current_token().position),
                            });
                        }
                    }
                }
                _ => TimeUnit::Second, // Default
            };

            // Parse the duration value and convert to Duration
            let duration_value = duration_str
                .chars()
                .take_while(|c| c.is_numeric())
                .collect::<String>()
                .parse::<u64>()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid duration value: {}", duration_str),
                    position: None,
                })?;

            // Convert to Duration based on time unit
            let time_window = match time_unit {
                TimeUnit::Second => Duration::from_secs(duration_value),
                TimeUnit::Minute => Duration::from_secs(duration_value * 60),
                TimeUnit::Hour => Duration::from_secs(duration_value * 3600),
                _ => Duration::from_secs(duration_value), // Default to seconds
            };

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

    fn parse_expression(&mut self) -> Result<Expr, SqlError> {
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_additive()?;

        while matches!(
            self.current_token().token_type,
            TokenType::Equal
                | TokenType::NotEqual
                | TokenType::LessThan
                | TokenType::GreaterThan
                | TokenType::LessThanOrEqual
                | TokenType::GreaterThanOrEqual
                | TokenType::Is
                | TokenType::In
                | TokenType::Not
        ) {
            let op_token = self.current_token().clone();
            self.advance();

            if op_token.token_type == TokenType::Is {
                // Handle IS NULL or IS NOT NULL
                if self.current_token().value.to_uppercase() == "NOT" {
                    self.advance(); // consume NOT
                    if self.current_token().value.to_uppercase() == "NULL" {
                        self.advance(); // consume NULL
                        left = Expr::UnaryOp {
                            op: UnaryOperator::IsNotNull,
                            expr: Box::new(left),
                        };
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Expected NULL after IS NOT".to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                } else if self.current_token().value.to_uppercase() == "NULL" {
                    self.advance(); // consume NULL
                    left = Expr::UnaryOp {
                        op: UnaryOperator::IsNull,
                        expr: Box::new(left),
                    };
                } else {
                    return Err(SqlError::ParseError {
                        message: "Expected NULL after IS".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            } else if op_token.token_type == TokenType::In {
                // Handle IN operator: expr IN (val1, val2, val3) or expr IN (SELECT ...)
                if self.current_token().token_type != TokenType::LeftParen {
                    return Err(SqlError::ParseError {
                        message: "Expected '(' after IN".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
                self.advance(); // consume '('

                // Check if this is a subquery
                if self.current_token().token_type == TokenType::Select {
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RightParen)?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::In,
                        right: Box::new(Expr::Subquery {
                            query: Box::new(subquery),
                            subquery_type: crate::ferris::sql::ast::SubqueryType::In,
                        }),
                    };
                } else {
                    // Regular IN list
                    let mut list_items = Vec::new();
                    loop {
                        list_items.push(self.parse_additive()?);

                        if self.current_token().token_type == TokenType::Comma {
                            self.advance(); // consume ','
                        } else if self.current_token().token_type == TokenType::RightParen {
                            self.advance(); // consume ')'
                            break;
                        } else {
                            return Err(SqlError::ParseError {
                                message: "Expected ',' or ')' in IN list".to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    }

                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::In,
                        right: Box::new(Expr::List(list_items)),
                    };
                }
            } else if op_token.token_type == TokenType::Not {
                // Handle NOT IN operator: expr NOT IN (val1, val2, val3) or expr NOT IN (SELECT ...)
                if self.current_token().token_type != TokenType::In {
                    return Err(SqlError::ParseError {
                        message: "Expected 'IN' after 'NOT'".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
                self.advance(); // consume 'IN'

                if self.current_token().token_type != TokenType::LeftParen {
                    return Err(SqlError::ParseError {
                        message: "Expected '(' after NOT IN".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
                self.advance(); // consume '('

                // Check if this is a subquery
                if self.current_token().token_type == TokenType::Select {
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RightParen)?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::NotIn,
                        right: Box::new(Expr::Subquery {
                            query: Box::new(subquery),
                            subquery_type: crate::ferris::sql::ast::SubqueryType::NotIn,
                        }),
                    };
                } else {
                    // Regular NOT IN list
                    let mut list_items = Vec::new();
                    loop {
                        list_items.push(self.parse_additive()?);

                        if self.current_token().token_type == TokenType::Comma {
                            self.advance(); // consume ','
                        } else if self.current_token().token_type == TokenType::RightParen {
                            self.advance(); // consume ')'
                            break;
                        } else {
                            return Err(SqlError::ParseError {
                                message: "Expected ',' or ')' in NOT IN list".to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    }

                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::NotIn,
                        right: Box::new(Expr::List(list_items)),
                    };
                }
            } else {
                let right = self.parse_additive()?;

                let op = match op_token.token_type {
                    TokenType::Equal => BinaryOperator::Equal,
                    TokenType::NotEqual => BinaryOperator::NotEqual,
                    TokenType::LessThan => BinaryOperator::LessThan,
                    TokenType::GreaterThan => BinaryOperator::GreaterThan,
                    TokenType::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
                    TokenType::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
                    _ => unreachable!(),
                };

                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            }
        }

        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_multiplicative()?;

        while matches!(
            self.current_token().token_type,
            TokenType::Plus | TokenType::Minus
        ) {
            let op_token = self.current_token().clone();
            self.advance();
            let right = self.parse_multiplicative()?;

            let op = match op_token.token_type {
                TokenType::Plus => BinaryOperator::Add,
                TokenType::Minus => BinaryOperator::Subtract,
                _ => unreachable!(),
            };

            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_primary()?;

        while matches!(
            self.current_token().token_type,
            TokenType::Asterisk | TokenType::Divide
        ) {
            let op_token = self.current_token().clone();
            self.advance();
            let right = self.parse_primary()?;

            let op = match op_token.token_type {
                TokenType::Asterisk => BinaryOperator::Multiply,
                TokenType::Divide => BinaryOperator::Divide,
                _ => unreachable!(),
            };

            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expr, SqlError> {
        let token = self.current_token().clone();
        match token.token_type {
            TokenType::Identifier => {
                // Special handling for boolean literals and null
                match token.value.to_uppercase().as_str() {
                    "TRUE" => {
                        self.advance();
                        return Ok(Expr::Literal(LiteralValue::Boolean(true)));
                    }
                    "FALSE" => {
                        self.advance();
                        return Ok(Expr::Literal(LiteralValue::Boolean(false)));
                    }
                    "NULL" => {
                        self.advance();
                        return Ok(Expr::Literal(LiteralValue::Null));
                    }
                    _ => {}
                }

                // Special handling for CURRENT_TIMESTAMP (no parentheses required)
                if token.value.to_uppercase() == "CURRENT_TIMESTAMP" {
                    self.advance();
                    return Ok(Expr::Function {
                        name: "CURRENT_TIMESTAMP".to_string(),
                        args: Vec::new(),
                    });
                }

                self.advance();
                if self.current_token().token_type == TokenType::LeftParen {
                    // Function call
                    self.advance(); // consume '('
                    let mut args = Vec::new();

                    if self.current_token().token_type != TokenType::RightParen {
                        loop {
                            if self.current_token().token_type == TokenType::Asterisk {
                                // Handle COUNT(*) special case
                                self.advance();
                                args.push(Expr::Literal(LiteralValue::Integer(1)));
                            } else {
                                args.push(self.parse_expression()?);
                            }

                            if self.current_token().token_type == TokenType::Comma {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }

                    self.expect(TokenType::RightParen)?;

                    // Check for OVER clause (window functions)
                    if self.current_token().token_type == TokenType::Over {
                        self.advance(); // consume OVER
                        let over_clause = self.parse_over_clause()?;
                        Ok(Expr::WindowFunction {
                            function_name: token.value,
                            args,
                            over_clause,
                        })
                    } else {
                        Ok(Expr::Function {
                            name: token.value,
                            args,
                        })
                    }
                } else if self.current_token().token_type == TokenType::Dot {
                    self.advance();
                    // Allow keywords to be used as field names in qualified expressions
                    let field = match self.current_token().token_type {
                        TokenType::Identifier => {
                            let field_name = self.current_token().value.clone();
                            self.advance();
                            field_name
                        }
                        TokenType::Status
                        | TokenType::Join
                        | TokenType::Left
                        | TokenType::Right
                        | TokenType::Inner
                        | TokenType::Full
                        | TokenType::Outer
                        | TokenType::On
                        | TokenType::Within
                        | TokenType::Versions
                        | TokenType::Metrics => {
                            let field_name = self.current_token().value.clone();
                            self.advance();
                            field_name
                        }
                        _ => {
                            return Err(SqlError::ParseError {
                                message: "Expected field name after dot".to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                    };
                    Ok(Expr::Column(format!("{}.{}", token.value, field)))
                } else {
                    Ok(Expr::Column(token.value))
                }
            }
            // Allow keywords to be used as column names or function names
            TokenType::Join
            | TokenType::Left
            | TokenType::Right
            | TokenType::Inner
            | TokenType::Full
            | TokenType::Outer
            | TokenType::On
            | TokenType::Within
            | TokenType::Replace
            | TokenType::Status => {
                if self.peek_token(1).map(|t| &t.token_type) == Some(&TokenType::LeftParen) {
                    // This keyword is being used as a function name
                    let function_name = token.value;
                    self.advance(); // consume keyword
                    self.advance(); // consume '('
                    let mut args = Vec::new();

                    if self.current_token().token_type != TokenType::RightParen {
                        loop {
                            if self.current_token().token_type == TokenType::Asterisk {
                                // Handle COUNT(*) special case
                                self.advance();
                                args.push(Expr::Literal(LiteralValue::Integer(1)));
                            } else {
                                args.push(self.parse_expression()?);
                            }

                            if self.current_token().token_type == TokenType::Comma {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }

                    self.expect(TokenType::RightParen)?;

                    // Check for OVER clause (window functions)
                    if self.current_token().token_type == TokenType::Over {
                        self.advance(); // consume OVER
                        let over_clause = self.parse_over_clause()?;
                        Ok(Expr::WindowFunction {
                            function_name,
                            args,
                            over_clause,
                        })
                    } else {
                        Ok(Expr::Function {
                            name: function_name,
                            args,
                        })
                    }
                } else {
                    // This keyword is being used as a column name
                    let column_name = token.value;
                    self.advance();
                    Ok(Expr::Column(column_name))
                }
            }
            TokenType::String => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::String(token.value)))
            }
            TokenType::Number => {
                self.advance();
                if let Ok(i) = token.value.parse::<i64>() {
                    Ok(Expr::Literal(LiteralValue::Integer(i)))
                } else if let Ok(f) = token.value.parse::<f64>() {
                    Ok(Expr::Literal(LiteralValue::Float(f)))
                } else {
                    Err(SqlError::ParseError {
                        message: format!("Invalid number: {}", token.value),
                        position: Some(token.position),
                    })
                }
            }
            TokenType::Null => {
                self.advance();
                Ok(Expr::Literal(LiteralValue::Null))
            }
            TokenType::Interval => {
                self.advance(); // consume INTERVAL

                // Parse the value (could be a string literal or number)
                let value_token = self.current_token().clone();
                let value_str = match value_token.token_type {
                    TokenType::String | TokenType::Number => {
                        self.advance();
                        value_token.value
                    }
                    _ => {
                        return Err(SqlError::ParseError {
                            message: "Expected string or number after INTERVAL".to_string(),
                            position: Some(value_token.position),
                        });
                    }
                };

                // Parse the value as integer
                let value = value_str.parse::<i64>().map_err(|_| SqlError::ParseError {
                    message: format!("Invalid interval value: {}", value_str),
                    position: Some(value_token.position),
                })?;

                // Parse the time unit
                let unit_token = self.current_token().clone();
                if unit_token.token_type != TokenType::Identifier {
                    return Err(SqlError::ParseError {
                        message: "Expected time unit after INTERVAL value".to_string(),
                        position: Some(unit_token.position),
                    });
                }

                let unit = match unit_token.value.to_uppercase().as_str() {
                    "MILLISECOND" | "MILLISECONDS" => TimeUnit::Millisecond,
                    "SECOND" | "SECONDS" => TimeUnit::Second,
                    "MINUTE" | "MINUTES" => TimeUnit::Minute,
                    "HOUR" | "HOURS" => TimeUnit::Hour,
                    "DAY" | "DAYS" => TimeUnit::Day,
                    _ => {
                        return Err(SqlError::ParseError {
                            message: format!("Invalid time unit: {}", unit_token.value),
                            position: Some(unit_token.position),
                        });
                    }
                };

                self.advance(); // consume time unit
                Ok(Expr::Literal(LiteralValue::Interval { value, unit }))
            }
            TokenType::Case => {
                self.advance(); // consume CASE
                self.parse_case_expression()
            }
            TokenType::Exists => {
                self.advance(); // consume EXISTS
                self.expect(TokenType::LeftParen)?;
                let subquery = self.parse_select()?;
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Subquery {
                    query: Box::new(subquery),
                    subquery_type: crate::ferris::sql::ast::SubqueryType::Exists,
                })
            }
            TokenType::Not => {
                self.advance(); // consume NOT
                if self.current_token().token_type == TokenType::Exists {
                    self.advance(); // consume EXISTS
                    self.expect(TokenType::LeftParen)?;
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RightParen)?;
                    Ok(Expr::Subquery {
                        query: Box::new(subquery),
                        subquery_type: crate::ferris::sql::ast::SubqueryType::NotExists,
                    })
                } else {
                    // Other NOT expressions (like NOT column_name)
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp {
                        op: crate::ferris::sql::ast::UnaryOperator::Not,
                        expr: Box::new(expr),
                    })
                }
            }
            TokenType::LeftParen => {
                self.advance();

                // Check if this is a subquery (SELECT statement in parentheses)
                if self.current_token().token_type == TokenType::Select {
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RightParen)?;
                    // This is a scalar subquery
                    Ok(Expr::Subquery {
                        query: Box::new(subquery),
                        subquery_type: crate::ferris::sql::ast::SubqueryType::Scalar,
                    })
                } else {
                    // Regular parenthesized expression
                    let expr = self.parse_expression()?;
                    self.expect(TokenType::RightParen)?;
                    Ok(expr)
                }
            }
            TokenType::Minus => {
                self.advance(); // consume '-'
                let expr = self.parse_primary()?;
                match expr {
                    Expr::Literal(LiteralValue::Integer(n)) => {
                        Ok(Expr::Literal(LiteralValue::Integer(-n)))
                    }
                    Expr::Literal(LiteralValue::Float(f)) => {
                        Ok(Expr::Literal(LiteralValue::Float(-f)))
                    }
                    _ => {
                        // For non-literal expressions, we could create a unary minus expression
                        // For now, just handle the common case of negative literals
                        Err(SqlError::ParseError {
                            message: "Minus operator only supported for numeric literals"
                                .to_string(),
                            position: Some(token.position),
                        })
                    }
                }
            }
            _ => Err(SqlError::ParseError {
                message: format!("Unexpected token in expression: {:?}", token.token_type),
                position: Some(token.position),
            }),
        }
    }

    fn parse_case_expression(&mut self) -> Result<Expr, SqlError> {
        let mut when_clauses = Vec::new();

        // Parse WHEN clauses
        while self.current_token().token_type == TokenType::When {
            self.advance(); // consume WHEN
            let condition = self.parse_expression()?;
            self.expect(TokenType::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push((condition, result));
        }

        if when_clauses.is_empty() {
            return Err(SqlError::ParseError {
                message: "CASE expression must have at least one WHEN clause".to_string(),
                position: Some(self.current_token().position),
            });
        }

        // Parse optional ELSE clause
        let else_clause = if self.current_token().token_type == TokenType::Else {
            self.advance(); // consume ELSE
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Expect END
        self.expect(TokenType::End)?;

        Ok(Expr::Case {
            when_clauses,
            else_clause,
        })
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, SqlError> {
        let window_type = match self.current_token().value.to_uppercase().as_str() {
            "TUMBLING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let duration_str = self.parse_duration_token()?;
                self.expect(TokenType::RightParen)?;

                let size = self.parse_duration(&duration_str)?;
                WindowSpec::Tumbling {
                    size,
                    time_column: None,
                }
            }
            "SLIDING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let duration_str = self.parse_duration_token()?;
                self.expect(TokenType::Comma)?;
                let advance_str = self.parse_duration_token()?;
                self.expect(TokenType::RightParen)?;

                let size = self.parse_duration(&duration_str)?;
                let advance = self.parse_duration(&advance_str)?;
                WindowSpec::Sliding {
                    size,
                    advance,
                    time_column: None,
                }
            }
            "SESSION" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let gap_str = self.parse_duration_token()?;
                self.expect(TokenType::RightParen)?;

                let gap = self.parse_duration(&gap_str)?;
                WindowSpec::Session {
                    gap,
                    partition_by: Vec::new(),
                }
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: "Expected window type (TUMBLING, SLIDING, or SESSION)".to_string(),
                    position: None,
                });
            }
        };

        Ok(window_type)
    }

    fn parse_duration_token(&mut self) -> Result<String, SqlError> {
        // Handle cases where duration might be tokenized as number + identifier
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

    fn parse_duration(&self, duration_str: &str) -> Result<Duration, SqlError> {
        if duration_str.ends_with('s') {
            let seconds: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(seconds))
        } else if duration_str.ends_with('m') {
            let minutes: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(minutes * 60))
        } else if duration_str.ends_with('h') {
            let hours: u64 = duration_str[..duration_str.len() - 1]
                .parse()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid duration: {}", duration_str),
                    position: None,
                })?;
            Ok(Duration::from_secs(hours * 3600))
        } else {
            // Default to seconds if no unit specified
            let seconds: u64 = duration_str.parse().map_err(|_| SqlError::ParseError {
                message: format!("Invalid duration: {}", duration_str),
                position: None,
            })?;
            Ok(Duration::from_secs(seconds))
        }
    }

    fn parse_create(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Create)?;

        match self.current_token().token_type {
            TokenType::Stream => self.parse_create_stream(),
            TokenType::Table => self.parse_create_table(),
            _ => Err(SqlError::ParseError {
                message: "Expected STREAM or TABLE after CREATE".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }

    fn parse_create_stream(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Stream)?;
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional column definitions
        let columns = if self.current_token().token_type == TokenType::LeftParen {
            Some(self.parse_column_definitions()?)
        } else {
            None
        };

        self.expect(TokenType::As)?;
        let as_select = Box::new(self.parse_select()?);

        // Optional WITH properties
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        Ok(StreamingQuery::CreateStream {
            name,
            columns,
            as_select,
            properties,
        })
    }

    fn parse_create_table(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Table)?;
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional column definitions
        let columns = if self.current_token().token_type == TokenType::LeftParen {
            Some(self.parse_column_definitions()?)
        } else {
            None
        };

        self.expect(TokenType::As)?;
        let as_select = Box::new(self.parse_select()?);

        // Optional WITH properties
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        Ok(StreamingQuery::CreateTable {
            name,
            columns,
            as_select,
            properties,
        })
    }

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDef>, SqlError> {
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

    fn parse_data_type(&mut self) -> Result<DataType, SqlError> {
        let type_name = self.expect(TokenType::Identifier)?.value.to_uppercase();

        match type_name.as_str() {
            "INT" | "INTEGER" => Ok(DataType::Integer),
            "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
            "STRING" | "VARCHAR" | "TEXT" => Ok(DataType::String),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "TIMESTAMP" => Ok(DataType::Timestamp),
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

    fn parse_with_properties(&mut self) -> Result<HashMap<String, String>, SqlError> {
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

    fn consume_if_matches(&mut self, expected: &str) -> bool {
        if self.current_token().value.to_uppercase() == expected.to_uppercase() {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), SqlError> {
        let token = self.current_token();
        if token.value.to_uppercase() == keyword.to_uppercase() {
            self.advance();
            Ok(())
        } else {
            Err(SqlError::ParseError {
                message: format!("Expected keyword '{}', found '{}'", keyword, token.value),
                position: Some(token.position),
            })
        }
    }

    fn parse_group_by_list(&mut self) -> Result<Vec<Expr>, SqlError> {
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

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, SqlError> {
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

    fn parse_show(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume SHOW or LIST token
        self.advance();

        let resource_type = match self.current_token().token_type {
            TokenType::Streams => {
                self.advance();
                ShowResourceType::Streams
            }
            TokenType::Tables => {
                self.advance();
                ShowResourceType::Tables
            }
            TokenType::Topics => {
                self.advance();
                ShowResourceType::Topics
            }
            TokenType::Functions => {
                self.advance();
                ShowResourceType::Functions
            }
            TokenType::Jobs => {
                self.advance();
                ShowResourceType::Jobs
            }
            TokenType::Status => {
                self.advance();
                // Optional specific query name
                let name = if self.current_token().token_type == TokenType::Identifier {
                    Some(self.expect(TokenType::Identifier)?.value)
                } else {
                    None
                };
                ShowResourceType::JobStatus { name }
            }
            TokenType::Versions => {
                self.advance();
                // Expect job name after VERSIONS
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::JobVersions { name }
            }
            TokenType::Metrics => {
                self.advance();
                // Optional specific job name
                let name = if self.current_token().token_type == TokenType::Identifier {
                    Some(self.expect(TokenType::Identifier)?.value)
                } else {
                    None
                };
                ShowResourceType::JobMetrics { name }
            }
            TokenType::Schema => {
                self.advance();
                // Expect resource name after SCHEMA
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Schema { name }
            }
            TokenType::Properties => {
                self.advance();
                // Expect resource type and name: SHOW PROPERTIES STREAM stream_name
                // Handle both identifier and keyword tokens for resource type
                let resource_type_token = match self.current_token().token_type {
                    TokenType::Identifier => self.expect(TokenType::Identifier)?.value,
                    TokenType::Stream => {
                        let token = self.current_token().clone();
                        self.advance();
                        token.value
                    }
                    TokenType::Table => {
                        let token = self.current_token().clone();
                        self.advance();
                        token.value
                    }
                    _ => {
                        return Err(SqlError::ParseError {
                            message:
                                "Expected resource type (STREAM, TABLE, etc.) after PROPERTIES"
                                    .to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                };
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Properties {
                    resource_type: resource_type_token,
                    name,
                }
            }
            TokenType::Partitions => {
                self.advance();
                // Expect resource name after PARTITIONS
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Partitions { name }
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: "Expected STREAMS, TABLES, TOPICS, FUNCTIONS, SCHEMA, PROPERTIES, JOBS, STATUS, VERSIONS, METRICS, or PARTITIONS after SHOW/LIST".to_string(),
                    position: Some(self.current_token().position)
                });
            }
        };

        // Optional LIKE pattern for filtering
        let pattern = if self.current_token().value.to_uppercase() == "LIKE" {
            self.advance(); // consume LIKE
            Some(self.expect(TokenType::String)?.value)
        } else {
            None
        };

        Ok(StreamingQuery::Show {
            resource_type,
            pattern,
        })
    }

    fn parse_start_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume START token
        self.advance();

        // Expect JOB or QUERY keyword (both are supported)
        let current_token = self.current_token().value.to_uppercase();
        if current_token == "JOB" || current_token == "QUERY" {
            self.advance();
        } else {
            return Err(SqlError::ParseError {
                message: format!(
                    "Expected JOB or QUERY after START, found '{}'",
                    current_token
                ),
                position: Some(self.current_token().position),
            });
        }

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Expect AS keyword
        self.expect(TokenType::As)?;

        // Parse the underlying query
        let query = Box::new(self.parse_tokens_inner()?);

        // Optional WITH properties
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        Ok(StreamingQuery::StartJob {
            name,
            query,
            properties,
        })
    }

    fn parse_stop_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume STOP token
        self.advance();

        // Expect JOB or QUERY keyword (both are supported)
        let current_token = self.current_token().value.to_uppercase();
        if current_token == "JOB" || current_token == "QUERY" {
            self.advance();
        } else {
            return Err(SqlError::ParseError {
                message: format!(
                    "Expected JOB or QUERY after STOP, found '{}'",
                    current_token
                ),
                position: Some(self.current_token().position),
            });
        }

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional FORCE keyword
        let force = if self.current_token().token_type == TokenType::Force {
            self.advance();
            true
        } else {
            false
        };

        Ok(StreamingQuery::StopJob { name, force })
    }

    fn parse_pause_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume PAUSE token
        self.advance();

        // Expect JOB or QUERY keyword (both are supported)
        let current_token = self.current_token().value.to_uppercase();
        if current_token == "JOB" || current_token == "QUERY" {
            self.advance();
        } else {
            return Err(SqlError::ParseError {
                message: format!(
                    "Expected JOB or QUERY after PAUSE, found '{}'",
                    current_token
                ),
                position: Some(self.current_token().position),
            });
        }

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::PauseJob { name })
    }

    fn parse_resume_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume RESUME token
        self.advance();

        // Expect JOB or QUERY keyword (both are supported)
        let current_token = self.current_token().value.to_uppercase();
        if current_token == "JOB" || current_token == "QUERY" {
            self.advance();
        } else {
            return Err(SqlError::ParseError {
                message: format!(
                    "Expected JOB or QUERY after RESUME, found '{}'",
                    current_token
                ),
                position: Some(self.current_token().position),
            });
        }

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::ResumeJob { name })
    }

    fn parse_deploy_job(&mut self) -> Result<StreamingQuery, SqlError> {
        use crate::ferris::sql::ast::DeploymentStrategy;

        // Consume DEPLOY token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get query name
        let name = self.expect(TokenType::Identifier)?.value;

        // Expect VERSION keyword
        self.expect(TokenType::Version)?;

        // Get version string
        let version = self.expect(TokenType::String)?.value;

        // Expect AS keyword
        self.expect(TokenType::As)?;

        // Parse the underlying query
        let query = Box::new(self.parse_tokens_inner()?);

        // Optional WITH properties clause
        let mut properties = HashMap::new();
        if self.current_token().token_type == TokenType::With {
            properties = self.parse_with_properties()?;
        }

        // Optional STRATEGY clause
        let strategy = if self.current_token().token_type == TokenType::Strategy {
            self.advance(); // consume STRATEGY

            match self.current_token().token_type {
                TokenType::BlueGreen => {
                    self.advance();
                    DeploymentStrategy::BlueGreen
                }
                TokenType::Canary => {
                    self.advance();
                    // Expect percentage in parentheses
                    self.expect(TokenType::LeftParen)?;
                    let percentage_token = self.expect(TokenType::Number)?;
                    let percentage: u8 =
                        percentage_token
                            .value
                            .parse()
                            .map_err(|_| SqlError::ParseError {
                                message: "Invalid percentage for canary deployment".to_string(),
                                position: Some(percentage_token.position),
                            })?;
                    self.expect(TokenType::RightParen)?;
                    DeploymentStrategy::Canary { percentage }
                }
                TokenType::Rolling => {
                    self.advance();
                    DeploymentStrategy::Rolling
                }
                TokenType::Replace => {
                    self.advance();
                    DeploymentStrategy::Replace
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected BLUE_GREEN, CANARY, ROLLING, or REPLACE strategy"
                            .to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            }
        } else {
            DeploymentStrategy::BlueGreen // Default strategy
        };

        Ok(StreamingQuery::DeployJob {
            name,
            version,
            query,
            properties,
            strategy,
        })
    }

    fn parse_rollback_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume ROLLBACK token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional target version
        let target_version = if self.current_token().token_type == TokenType::Version {
            self.advance(); // consume VERSION
            Some(self.expect(TokenType::String)?.value)
        } else {
            None
        };

        Ok(StreamingQuery::RollbackJob {
            name,
            target_version,
        })
    }

    fn parse_describe(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume DESCRIBE token
        self.advance();

        // Get resource name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::Show {
            resource_type: ShowResourceType::Describe { name },
            pattern: None,
        })
    }

    fn parse_tokens_inner(&mut self) -> Result<StreamingQuery, SqlError> {
        // This is similar to the main parse_tokens but works on the current parser state
        match self.current_token().token_type {
            TokenType::Select => self.parse_select(),
            TokenType::Create => self.parse_create(),
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT or CREATE statement in START QUERY".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }

    /// Parse OVER clause for window functions
    /// Syntax: OVER (PARTITION BY col1, col2 ORDER BY col3 ROWS BETWEEN ... AND ...)
    fn parse_over_clause(&mut self) -> Result<OverClause, SqlError> {
        self.expect(TokenType::LeftParen)?;

        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut window_frame = None;

        // Parse PARTITION BY clause (optional)
        if self.current_token().value.to_uppercase() == "PARTITION" {
            self.advance(); // consume PARTITION
            self.expect_keyword("BY")?;

            loop {
                let column = self.expect(TokenType::Identifier)?.value;
                partition_by.push(column);

                if self.current_token().token_type == TokenType::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // Parse ORDER BY clause (optional)
        if self.current_token().token_type == TokenType::OrderBy {
            self.advance(); // consume ORDER
            self.expect_keyword("BY")?;

            loop {
                let column_name = self.expect(TokenType::Identifier)?.value;
                let expr = Expr::Column(column_name);
                let direction = if self.current_token().token_type == TokenType::Asc {
                    self.advance();
                    OrderDirection::Asc
                } else if self.current_token().token_type == TokenType::Desc {
                    self.advance();
                    OrderDirection::Desc
                } else {
                    OrderDirection::Asc // Default
                };

                order_by.push(OrderByExpr { expr, direction });

                if self.current_token().token_type == TokenType::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // Parse window frame clause (optional)
        if self.current_token().token_type == TokenType::Rows
            || self.current_token().token_type == TokenType::Range
        {
            window_frame = Some(self.parse_window_frame()?);
        }

        self.expect(TokenType::RightParen)?;

        Ok(OverClause {
            partition_by,
            order_by,
            window_frame,
        })
    }

    /// Parse window frame specification: ROWS/RANGE BETWEEN ... AND ...
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
            _ => Err(SqlError::ParseError {
                message: "Expected UNBOUNDED, CURRENT, or numeric offset in frame bound"
                    .to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }
}

impl Default for StreamingSqlParser {
    fn default() -> Self {
        Self::new()
    }
}
