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
use velostream::velostream::sql::parser::StreamingSqlParser;

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

use crate::velostream::sql::annotation_parser::SqlAnnotationParser;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;
use std::time::Duration;

pub mod annotations;
pub mod validator;

// AggregateValidator is used for validation of aggregate expressions
#[allow(unused_imports)]
use self::validator::AggregateValidator;

/// Main parser for streaming SQL queries.
///
/// The `StreamingSqlParser` handles the complete parsing pipeline from SQL text to AST.
/// It maintains a keyword lookup table for efficient token classification and provides
/// a simple interface for parsing various types of SQL statements.
///
/// # Examples
///
/// ```rust,no_run
/// use velostream::velostream::sql::parser::StreamingSqlParser;
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
///
/// Public to allow external code to identify comment tokens.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
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

    // Emit Mode Keywords
    Emit,    // EMIT
    Changes, // CHANGES
    Final,   // FINAL

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
    Concat, // || (string concatenation)

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
    Case,    // CASE
    When,    // WHEN
    Then,    // THEN
    Else,    // ELSE
    End,     // END
    Is,      // IS (for IS NULL, IS NOT NULL)
    In,      // IN (for IN operator)
    Not,     // NOT (for NOT IN, IS NOT NULL, etc.)
    Between, // BETWEEN (for range queries)
    Like,    // LIKE (for pattern matching)
    Exists,  // EXISTS (for EXISTS subqueries)
    Any,     // ANY (for ANY subqueries)
    All,     // ALL (for ALL subqueries)
    Union,   // UNION (for combining result sets)

    // Window Frame Keywords
    Rows,      // ROWS
    Range,     // RANGE
    And,       // AND
    Or,        // OR
    Preceding, // PRECEDING
    Following, // FOLLOWING
    Current,   // CURRENT
    Row,       // ROW
    Unbounded, // UNBOUNDED
    Over,      // OVER

    // Comments preserved for annotation parsing
    SingleLineComment, // -- comment text
    MultiLineComment,  // /* comment text */

    // Primary Key annotation (SQL standard)
    Primary, // PRIMARY (for PRIMARY KEY)
    Key,     // KEY (for PRIMARY KEY designation)

    // Special
    Eof,       // End of input
    Semicolon, // ; (statement terminator)
}

/// A token with its type, value, and position information.
///
/// Tokens are the atomic units of SQL syntax, produced by the lexer
/// and consumed by the parser. Position information enables detailed
/// error reporting.
///
/// Public to allow external code to access comment tokens for parsing metric annotations.
#[derive(Debug, Clone)]
pub struct Token {
    /// The type of this token (keyword, operator, literal, etc.)
    pub token_type: TokenType,
    /// The original text value of the token
    pub value: String,
    /// Character position in the original SQL string (for error reporting)
    pub position: usize,
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
    /// use velostream::velostream::sql::parser::StreamingSqlParser;
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
        keywords.insert("VERSIONS".to_string(), TokenType::Versions);
        keywords.insert("DESCRIBE".to_string(), TokenType::Describe);

        // Emit Mode Keywords
        keywords.insert("EMIT".to_string(), TokenType::Emit);
        keywords.insert("CHANGES".to_string(), TokenType::Changes);
        keywords.insert("FINAL".to_string(), TokenType::Final);

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
        keywords.insert("BETWEEN".to_string(), TokenType::Between);
        keywords.insert("LIKE".to_string(), TokenType::Like);
        keywords.insert("EXISTS".to_string(), TokenType::Exists);
        keywords.insert("UNION".to_string(), TokenType::Union);
        keywords.insert("ANY".to_string(), TokenType::Any);
        keywords.insert("ALL".to_string(), TokenType::All);
        keywords.insert("ROWS".to_string(), TokenType::Rows);
        keywords.insert("RANGE".to_string(), TokenType::Range);
        keywords.insert("BETWEEN".to_string(), TokenType::Between);
        keywords.insert("AND".to_string(), TokenType::And);
        keywords.insert("OR".to_string(), TokenType::Or);
        keywords.insert("PRECEDING".to_string(), TokenType::Preceding);
        keywords.insert("FOLLOWING".to_string(), TokenType::Following);
        keywords.insert("CURRENT".to_string(), TokenType::Current);
        keywords.insert("ROW".to_string(), TokenType::Row);
        keywords.insert("UNBOUNDED".to_string(), TokenType::Unbounded);
        keywords.insert("OVER".to_string(), TokenType::Over);
        keywords.insert("NULL".to_string(), TokenType::Null);

        // Primary Key annotation (SQL standard)
        keywords.insert("PRIMARY".to_string(), TokenType::Primary);
        keywords.insert("KEY".to_string(), TokenType::Key);

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
    /// use velostream::velostream::sql::parser::StreamingSqlParser;
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
        // Use tokenize_with_comments to extract annotations
        let (tokens, comments) = self.tokenize_with_comments(sql)?;
        let mut query = self.parse_tokens_with_context(tokens, sql, comments)?;

        // Extract job-related annotations from SQL comments and apply them to the query
        let (job_mode, batch_size, num_partitions, partitioning_strategy) =
            SqlAnnotationParser::parse_job_annotations(sql);

        // Apply annotations to SELECT queries
        match &mut query {
            StreamingQuery::Select {
                job_mode: qry_job_mode,
                batch_size: qry_batch_size,
                num_partitions: qry_num_partitions,
                partitioning_strategy: qry_partitioning_strategy,
                ..
            } => {
                if job_mode.is_some() {
                    *qry_job_mode = job_mode;
                }
                if batch_size.is_some() {
                    *qry_batch_size = batch_size;
                }
                if num_partitions.is_some() {
                    *qry_num_partitions = num_partitions;
                }
                if partitioning_strategy.is_some() {
                    *qry_partitioning_strategy = partitioning_strategy;
                }
            }
            _ => {
                // Other query types (StartJob, StopJob, etc.) don't support annotations
            }
        }

        // IMPORTANT: Aggregate function validation is NOT done here at parse time.
        //
        // RATIONALE:
        // The parser should only validate syntax. Semantic validation of aggregates
        // is handled in dedicated layers that understand context:
        //
        // 1. SqlValidator.validate_aggregation_rules()
        //    - Issues WARNINGS for aggregates without GROUP BY (global aggregation)
        //    - Called during SQL validation phase (not parsing)
        //    - Allows valid patterns: SELECT COUNT(*) FROM orders
        //
        // 2. Phase 7 AggregationValidator (execution layer)
        //    - Validates GROUP BY completeness (all non-agg columns in GROUP BY)
        //    - Validates aggregate placement (no aggregates in WHERE/ORDER BY)
        //    - Validates HAVING clause field references
        //    - Runtime validation for proper execution semantics
        //
        // VALID PATTERNS (all must parse successfully):
        // - SELECT COUNT(*) FROM orders
        //   (Global aggregation - warning only, valid SQL)
        // - SELECT status, COUNT(*) FROM orders GROUP BY status
        //   (With GROUP BY - valid)
        // - SELECT symbol, AVG(price) FROM orders WINDOW TUMBLING(1m)
        //   (With WINDOW - valid)
        //
        // The commented-out call was rejecting valid queries at parse time:
        // AggregateValidator::validate(&query)?;

        Ok(query)
    }

    // Keep the old method for backward compatibility
    fn parse_tokens(&self, tokens: Vec<Token>) -> Result<StreamingQuery, SqlError> {
        self.parse_tokens_with_context(tokens, "", Vec::new())
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
                ';' => {
                    tokens.push(Token {
                        token_type: TokenType::Semicolon,
                        value: ";".to_string(),
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
                    // Check for single-line comment "--"
                    let comment_start_pos = position;
                    chars.next();
                    position += 1;
                    if let Some(&'-') = chars.peek() {
                        // Single-line comment, preserve the text
                        chars.next(); // consume second '-'
                        position += 1;

                        let mut comment_text = String::new();
                        while let Some(&ch) = chars.peek() {
                            if ch == '\n' || ch == '\r' {
                                break;
                            }
                            comment_text.push(ch);
                            chars.next();
                            position += 1;
                        }

                        // Create comment token with the comment text
                        tokens.push(Token {
                            token_type: TokenType::SingleLineComment,
                            value: comment_text.trim().to_string(),
                            position: comment_start_pos,
                        });
                    } else {
                        // Regular minus token
                        tokens.push(Token {
                            token_type: TokenType::Minus,
                            value: "-".to_string(),
                            position: position - 1, // Adjust position since we already advanced
                        });
                    }
                }
                '/' => {
                    // Check for multi-line comment "/*"
                    let comment_start_pos = position;
                    chars.next();
                    position += 1;
                    if let Some(&'*') = chars.peek() {
                        // Multi-line comment, preserve the text
                        chars.next(); // consume '*'
                        position += 1;

                        let mut comment_text = String::new();
                        let mut found_end = false;
                        while let Some(&ch) = chars.peek() {
                            chars.next();
                            position += 1;
                            if ch == '*' {
                                if let Some(&'/') = chars.peek() {
                                    chars.next(); // consume '/'
                                    position += 1;
                                    found_end = true;
                                    break;
                                }
                                // Not the end, keep the '*'
                                comment_text.push(ch);
                            } else {
                                comment_text.push(ch);
                            }
                        }
                        if !found_end {
                            return Err(SqlError::ParseError {
                                message: "Unterminated multi-line comment".to_string(),
                                position: Some(position),
                            });
                        }

                        // Create comment token with the comment text
                        tokens.push(Token {
                            token_type: TokenType::MultiLineComment,
                            value: comment_text.trim().to_string(),
                            position: comment_start_pos,
                        });
                    } else {
                        // Regular divide token
                        tokens.push(Token {
                            token_type: TokenType::Divide,
                            value: "/".to_string(),
                            position: position - 1, // Adjust position since we already advanced
                        });
                    }
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
                '|' => {
                    chars.next();
                    position += 1;
                    if let Some(&'|') = chars.peek() {
                        tokens.push(Token {
                            token_type: TokenType::Concat,
                            value: "||".to_string(),
                            position: position - 1,
                        });
                        chars.next();
                        position += 1;
                    } else {
                        return Err(SqlError::ParseError {
                            message:
                                "Unexpected character '|' - did you mean '||' for concatenation?"
                                    .to_string(),
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
                    let mut has_decimal = false;
                    let mut has_exponent = false;

                    while let Some(&next_ch) = chars.peek() {
                        if next_ch.is_ascii_digit() {
                            value.push(next_ch);
                            chars.next();
                            position += 1;
                        } else if next_ch == '.' && !has_decimal && !has_exponent {
                            // Allow one decimal point, but not in exponent
                            has_decimal = true;
                            value.push(next_ch);
                            chars.next();
                            position += 1;
                        } else if (next_ch == 'e' || next_ch == 'E') && !has_exponent {
                            // Scientific notation
                            has_exponent = true;
                            value.push(next_ch);
                            chars.next();
                            position += 1;

                            // Check for optional +/- after 'e'/'E'
                            if let Some(&sign_ch) = chars.peek() {
                                if sign_ch == '+' || sign_ch == '-' {
                                    value.push(sign_ch);
                                    chars.next();
                                    position += 1;
                                }
                            }
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

    /// Tokenize SQL and separate comments from other tokens
    ///
    /// Returns a tuple of (non-comment tokens, comment tokens).
    /// Comments are extracted with their position information for annotation parsing.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to tokenize
    ///
    /// # Returns
    /// * `Ok((Vec<Token>, Vec<Token>))` - Non-comment tokens and comment tokens separately
    /// * `Err(SqlError)` - Tokenization error
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let parser = StreamingSqlParser::new();
    /// let (tokens, comments) = parser.tokenize_with_comments(
    ///     "-- @metric: my_metric_total\n\
    ///      -- @metric_type: counter\n\
    ///      CREATE STREAM my_stream AS SELECT * FROM source"
    /// )?;
    /// ```
    pub fn tokenize_with_comments(&self, sql: &str) -> Result<(Vec<Token>, Vec<Token>), SqlError> {
        let all_tokens = self.tokenize(sql)?;

        let mut tokens = Vec::new();
        let mut comments = Vec::new();

        for token in all_tokens {
            match token.token_type {
                TokenType::SingleLineComment | TokenType::MultiLineComment => {
                    comments.push(token);
                }
                _ => {
                    tokens.push(token);
                }
            }
        }

        Ok((tokens, comments))
    }

    /// Extract comments that appear before a CREATE statement
    ///
    /// This method extracts consecutive comments that appear immediately before
    /// a CREATE STREAM or CREATE TABLE statement, which can contain metric annotations.
    ///
    /// # Arguments
    /// * `comments` - All comment tokens from the SQL statement
    /// * `create_position` - Position of the CREATE keyword in the SQL text
    ///
    /// # Returns
    /// * `Vec<String>` - Comment text lines that precede the CREATE statement
    pub fn extract_preceding_comments(comments: &[Token], create_position: usize) -> Vec<String> {
        comments
            .iter()
            .filter(|token| token.position < create_position)
            .map(|token| token.value.clone())
            .collect()
    }

    fn parse_tokens_with_context(
        &self,
        tokens: Vec<Token>,
        sql_text: &str,
        comments: Vec<Token>,
    ) -> Result<StreamingQuery, SqlError> {
        let mut parser = TokenParser::new(tokens, sql_text, comments);

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
            _ => Err(parser.create_parse_error("Expected SELECT, CREATE, SHOW, LIST, START, STOP, PAUSE, RESUME, DEPLOY, ROLLBACK, or DESCRIBE statement"))
        }
    }
}

struct TokenParser<'a> {
    tokens: Vec<Token>,
    current: usize,
    sql_text: &'a str,
    // Store comments for annotation parsing
    comments: Vec<Token>,
}

impl<'a> TokenParser<'a> {
    fn new(tokens: Vec<Token>, sql_text: &'a str, comments: Vec<Token>) -> Self {
        Self {
            tokens,
            current: 0,
            sql_text,
            comments,
        }
    }

    /// Create an enhanced parse error with context
    fn create_parse_error(&self, message: impl Into<String>) -> SqlError {
        let position = if self.current < self.tokens.len() {
            Some(self.tokens[self.current].position)
        } else if !self.tokens.is_empty() {
            Some(self.tokens[self.tokens.len() - 1].position)
        } else {
            None
        };

        SqlError::parse_error_with_context(message, position, Some(self.sql_text))
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
            Err(self.create_parse_error(format!(
                "Expected {:?}, found {:?}",
                expected, token.token_type
            )))
        }
    }

    /// Accept an identifier or a keyword that can be used as an identifier (e.g., table name)
    /// This allows reserved keywords like TABLE, STREAM, etc. to be used as table/column names
    fn expect_identifier_or_keyword(&mut self) -> Result<String, SqlError> {
        let token = self.current_token();
        match token.token_type {
            TokenType::Identifier => {
                let value = token.value.clone();
                self.advance();
                Ok(value)
            }
            // Allow many keywords to be used as identifiers in appropriate contexts
            TokenType::Table
            | TokenType::Stream
            | TokenType::Create
            | TokenType::Into
            | TokenType::Show
            | TokenType::List
            | TokenType::Streams
            | TokenType::Tables
            | TokenType::Topics
            | TokenType::Functions
            | TokenType::Schema
            | TokenType::Jobs
            | TokenType::Job
            | TokenType::Partitions
            | TokenType::Start
            | TokenType::Stop
            | TokenType::Force
            | TokenType::Pause
            | TokenType::Resume
            | TokenType::Deploy
            | TokenType::Rollback
            | TokenType::Version => {
                let value = token.value.clone();
                self.advance();
                Ok(value)
            }
            _ => Err(SqlError::ParseError {
                message: format!(
                    "Expected identifier or usable keyword, found {:?}",
                    token.token_type
                ),
                position: Some(token.position),
            }),
        }
    }

    fn parse_select(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;

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

    fn parse_select_for_create_table(&mut self) -> Result<StreamingQuery, SqlError> {
        // Similar to parse_select_no_with but without consuming semicolon
        // to allow CREATE TABLE parser to continue with WITH clause
        self.expect(TokenType::Select)?;

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

    fn parse_select_no_with(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;

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
                    Some(self.expect(TokenType::Identifier)?.value)
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
                            Some(name.clone())
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

    fn parse_expression(&mut self) -> Result<Expr, SqlError> {
        self.parse_logical_or()
    }

    fn parse_logical_or(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_logical_and()?;

        while self.current_token().token_type == TokenType::Or {
            self.advance(); // consume OR
            let right = self.parse_logical_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_logical_and(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_comparison()?;

        while self.current_token().token_type == TokenType::And {
            self.advance(); // consume AND
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_concatenative()?;

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
                | TokenType::Between
                | TokenType::Like
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
                            subquery_type: crate::velostream::sql::ast::SubqueryType::In,
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
            } else if op_token.token_type == TokenType::Like {
                // Handle LIKE operator: expr LIKE 'pattern'
                let pattern = self.parse_additive()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Like,
                    right: Box::new(pattern),
                };
            } else if op_token.token_type == TokenType::Not {
                // Handle NOT IN, NOT BETWEEN, or NOT LIKE operators
                if self.current_token().token_type == TokenType::Like {
                    // Handle NOT LIKE operator: expr NOT LIKE 'pattern'
                    self.advance(); // consume 'LIKE'
                    let pattern = self.parse_additive()?;
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: BinaryOperator::NotLike,
                        right: Box::new(pattern),
                    };
                } else if self.current_token().token_type == TokenType::In {
                    // Handle NOT IN operator: expr NOT IN (val1, val2, val3) or expr NOT IN (SELECT ...)
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
                                subquery_type: crate::velostream::sql::ast::SubqueryType::NotIn,
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
                } else if self.current_token().token_type == TokenType::Between {
                    // Handle NOT BETWEEN operator: expr NOT BETWEEN low AND high
                    self.advance(); // consume 'BETWEEN'

                    let low = self.parse_additive()?;

                    if self.current_token().token_type != TokenType::And {
                        return Err(SqlError::ParseError {
                            message: "Expected 'AND' after NOT BETWEEN expression".to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                    self.advance(); // consume 'AND'

                    let high = self.parse_additive()?;

                    left = Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: true,
                    };
                } else {
                    return Err(SqlError::ParseError {
                        message: "Expected 'IN', 'BETWEEN', or 'LIKE' after 'NOT'".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            } else if op_token.token_type == TokenType::Between {
                // Handle BETWEEN operator: expr BETWEEN low AND high
                let low = self.parse_additive()?;

                if self.current_token().token_type != TokenType::And {
                    return Err(SqlError::ParseError {
                        message: "Expected 'AND' after BETWEEN expression".to_string(),
                        position: Some(self.current_token().position),
                    });
                }
                self.advance(); // consume 'AND'

                let high = self.parse_additive()?;

                left = Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                };
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

    fn parse_concatenative(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_additive()?;

        while self.current_token().token_type == TokenType::Concat {
            self.advance(); // consume ||
            let right = self.parse_additive()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Concat,
                right: Box::new(right),
            };
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

                // Special handling for EXTRACT function with multiple syntax support
                if token.value.to_uppercase() == "EXTRACT" {
                    self.advance(); // consume EXTRACT
                    self.expect(TokenType::LeftParen)?; // consume '('

                    // Support both syntaxes:
                    // 1. EXTRACT('EPOCH', timestamp) - function call style
                    // 2. EXTRACT(EPOCH FROM timestamp) - SQL standard style

                    if self.current_token().token_type == TokenType::String {
                        // Old syntax: EXTRACT('EPOCH', timestamp)
                        let part_str = self.expect(TokenType::String)?.value;
                        self.expect(TokenType::Comma)?; // consume ','
                        let expr = self.parse_expression()?;
                        self.expect(TokenType::RightParen)?; // consume ')'

                        return Ok(Expr::Function {
                            name: "EXTRACT".to_string(),
                            args: vec![Expr::Literal(LiteralValue::String(part_str)), expr],
                        });
                    } else {
                        // New syntax: EXTRACT(EPOCH FROM timestamp)
                        let part = self.expect(TokenType::Identifier)?.value;

                        // Expect FROM keyword
                        if self.current_token().token_type != TokenType::From {
                            return Err(SqlError::ParseError {
                                message: "Expected FROM keyword in EXTRACT function".to_string(),
                                position: Some(self.current_token().position),
                            });
                        }
                        self.advance(); // consume FROM

                        // Parse the expression
                        let expr = self.parse_expression()?;
                        self.expect(TokenType::RightParen)?; // consume ')'

                        return Ok(Expr::Function {
                            name: "EXTRACT".to_string(),
                            args: vec![Expr::Literal(LiteralValue::String(part)), expr],
                        });
                    }
                }

                // Special handling for CAST function with SQL standard syntax support
                if token.value.to_uppercase() == "CAST" {
                    self.advance(); // consume CAST
                    self.expect(TokenType::LeftParen)?; // consume '('

                    // SQL Standard syntax only: CAST(expr AS type)

                    let expr = self.parse_expression()?;

                    // Expect AS keyword (SQL Standard syntax)
                    if self.current_token().token_type == TokenType::As {
                        // SQL standard syntax: CAST(expr AS type)
                        self.advance(); // consume AS

                        // Parse the type name as an identifier (not a string)
                        let type_name = match self.current_token().token_type {
                            TokenType::Identifier => {
                                let name = self.current_token().value.clone();
                                self.advance();
                                name
                            }
                            _ => {
                                return Err(SqlError::ParseError {
                                    message: "Expected type name after AS in CAST".to_string(),
                                    position: Some(self.current_token().position),
                                });
                            }
                        };

                        self.expect(TokenType::RightParen)?; // consume ')'

                        return Ok(Expr::Function {
                            name: "CAST".to_string(),
                            args: vec![expr, Expr::Literal(LiteralValue::String(type_name))],
                        });
                    } else {
                        return Err(SqlError::ParseError {
                            message: "Expected AS keyword in CAST(expr AS type) - SQL Standard syntax required".to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
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
                        TokenType::Join
                        | TokenType::Left
                        | TokenType::Right
                        | TokenType::Inner
                        | TokenType::Full
                        | TokenType::Outer
                        | TokenType::On
                        | TokenType::Within
                        | TokenType::Versions => {
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
            | TokenType::Replace => {
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
                // Check for decimal literal (contains decimal point)
                if token.value.contains('.') && !token.value.to_uppercase().contains('E') {
                    // Parse as Decimal for exact precision (financial-first approach)
                    // Store as string to preserve exact representation
                    Ok(Expr::Literal(LiteralValue::Decimal(token.value)))
                } else if token.value.to_uppercase().contains('E') {
                    // Scientific notation - parse as Float
                    if let Ok(f) = token.value.parse::<f64>() {
                        Ok(Expr::Literal(LiteralValue::Float(f)))
                    } else {
                        Err(SqlError::ParseError {
                            message: format!("Invalid scientific notation: {}", token.value),
                            position: Some(self.current),
                        })
                    }
                } else if let Ok(i) = token.value.parse::<i64>() {
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
                    subquery_type: crate::velostream::sql::ast::SubqueryType::Exists,
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
                        subquery_type: crate::velostream::sql::ast::SubqueryType::NotExists,
                    })
                } else {
                    // Other NOT expressions (like NOT column_name)
                    let expr = self.parse_primary()?;
                    Ok(Expr::UnaryOp {
                        op: crate::velostream::sql::ast::UnaryOperator::Not,
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
                        subquery_type: crate::velostream::sql::ast::SubqueryType::Scalar,
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
                    // Note: Decimal literals are no longer parsed directly as LiteralValue::Decimal
                    // They are parsed as Float for backward compatibility
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

    fn parse_session_parameter(&mut self) -> Result<String, SqlError> {
        // Parse a parameter for SESSION window which could be:
        // - A simple identifier: event_time
        // - A table-qualified column: p.event_time
        // - A duration: 4h
        // - INTERVAL duration: INTERVAL '4' HOUR
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
        // Use parse_select_for_create_table to avoid consuming the WITH clause that belongs to CREATE STREAM
        let as_select = Box::new(self.parse_select_for_create_table()?);

        log::debug!(
            "CREATE STREAM parser: after SELECT, current_token={:?} (value='{}')",
            self.current_token().token_type,
            self.current_token().value
        );

        // Parse EMIT clause if present at wrapper level - must come before WITH per SQL standard
        // If not found at wrapper level, extract from inner SELECT (EMIT is part of SELECT syntax)
        let mut emit_mode = if self.current_token().token_type == TokenType::Emit {
            log::debug!("CREATE STREAM parser: EMIT token detected at wrapper level");
            self.parse_emit_clause()?
        } else {
            None
        };

        // Extract emit_mode from inner SELECT if not set at wrapper level
        if emit_mode.is_none() {
            if let StreamingQuery::Select {
                emit_mode: select_emit_mode,
                ..
            } = as_select.as_ref()
            {
                if select_emit_mode.is_some() {
                    log::debug!(
                        "CREATE STREAM parser: emit_mode extracted from inner SELECT: {:?}",
                        select_emit_mode
                    );
                    emit_mode = select_emit_mode.clone();
                }
            }
        }

        log::debug!("CREATE STREAM parser: final emit_mode={:?}", emit_mode);

        // Parse WITH properties for sink configuration (comes after EMIT)
        let mut properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        // Extract properties from the SELECT statement (FROM clause WITH properties)
        if let StreamingQuery::Select {
            properties: select_props,
            ..
        } = as_select.as_ref()
        {
            if let Some(select_properties) = select_props {
                // Merge SELECT properties into stream properties
                for (key, value) in select_properties {
                    properties.insert(key.clone(), value.clone());
                }
            }
        }

        // Consume optional semicolon
        self.consume_semicolon();

        // Extract and parse metric annotations from comments
        // Get comments that appear before this CREATE STREAM statement
        let create_position = if self.current > 0 && self.current < self.tokens.len() {
            self.tokens[self.current - 1].position
        } else if !self.tokens.is_empty() {
            self.tokens[0].position
        } else {
            0
        };

        let preceding_comment_texts =
            StreamingSqlParser::extract_preceding_comments(&self.comments, create_position);

        // Parse @job_name annotation
        let job_name = annotations::parse_job_name(&preceding_comment_texts).unwrap_or_else(|e| {
            log::warn!("Failed to parse @job_name annotation: {}", e);
            None
        });

        if let Some(ref custom_name) = job_name {
            log::info!(
                "Parsed @job_name annotation for CREATE STREAM {}: '{}'",
                name,
                custom_name
            );
        }

        let metric_annotations = annotations::parse_metric_annotations(&preceding_comment_texts)
            .unwrap_or_else(|e| {
                log::warn!("Failed to parse metric annotations: {}", e);
                Vec::new()
            });

        // FR-073: Debug logging for annotation parsing
        if !metric_annotations.is_empty() {
            log::info!(
                "Parsed {} @metric annotation(s) for CREATE STREAM {}",
                metric_annotations.len(),
                name
            );
            for annotation in &metric_annotations {
                log::info!(
                    "  - {}: {} (type: {:?})",
                    annotation.name,
                    annotation.help.as_deref().unwrap_or(""),
                    annotation.metric_type
                );
            }
        }

        Ok(StreamingQuery::CreateStream {
            name,
            columns,
            as_select,
            properties,
            emit_mode,
            metric_annotations,
            job_name,
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
        // Use parse_select_no_with to avoid consuming the WITH clause that belongs to CREATE TABLE
        let as_select = Box::new(self.parse_select_for_create_table()?);

        // Parse EMIT clause if present BEFORE WITH (common pattern: ... EMIT CHANGES WITH (...))
        let mut emit_mode = if self.current_token().token_type == TokenType::Emit {
            self.parse_emit_clause()?
        } else {
            None
        };

        // Extract emit_mode from inner SELECT if not set at wrapper level
        if emit_mode.is_none() {
            if let StreamingQuery::Select {
                emit_mode: select_emit_mode,
                ..
            } = as_select.as_ref()
            {
                if select_emit_mode.is_some() {
                    emit_mode = select_emit_mode.clone();
                }
            }
        }

        // Parse WITH properties
        let mut properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        // Extract properties from the SELECT statement (FROM clause WITH properties)
        if let StreamingQuery::Select {
            properties: select_props,
            ..
        } = as_select.as_ref()
        {
            if let Some(select_properties) = select_props {
                // Merge SELECT properties into table properties
                for (key, value) in select_properties {
                    properties.insert(key.clone(), value.clone());
                }
            }
        }

        // Also check for EMIT clause AFTER WITH (alternative ordering)
        let emit_mode = if emit_mode.is_none() && self.current_token().token_type == TokenType::Emit
        {
            self.parse_emit_clause()?
        } else {
            emit_mode
        };

        // Consume optional semicolon
        self.consume_semicolon();

        Ok(StreamingQuery::CreateTable {
            name,
            columns,
            as_select,
            properties,
            emit_mode,
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

    fn parse_into_clause(&mut self) -> Result<IntoClause, SqlError> {
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

    fn parse_enhanced_with_properties(&mut self) -> Result<ConfigProperties, SqlError> {
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

    /// Resolve environment variables in configuration values
    /// Supports patterns: ${VAR}, ${VAR:-default}, ${VAR:?error_msg}
    fn resolve_environment_variables(&self, value: &str) -> Result<String, SqlError> {
        use std::env;

        // Simple regex-like replacement for ${VAR} patterns
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

    /// Parse optional EMIT clause (EMIT CHANGES or EMIT FINAL)
    fn parse_emit_clause(
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

    fn consume_if_matches(&mut self, expected: &str) -> bool {
        if self.current_token().value.to_uppercase() == expected.to_uppercase() {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Consume an optional semicolon at the end of a statement
    fn consume_semicolon(&mut self) {
        if self.current_token().token_type == TokenType::Semicolon {
            self.advance();
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
            TokenType::Identifier if self.current_token().value.to_uppercase() == "STATUS" => {
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
            TokenType::Identifier if self.current_token().value.to_uppercase() == "METRICS" => {
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
            TokenType::Identifier if self.current_token().value.to_uppercase() == "PROPERTIES" => {
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

        // Parse the underlying query (without consuming WITH clause)
        let query = Box::new(self.parse_tokens_inner_no_with()?);

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
        use crate::velostream::sql::ast::DeploymentStrategy;

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

        // Parse the underlying query (without consuming WITH clause)
        let query = Box::new(self.parse_tokens_inner_no_with()?);

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

    fn parse_tokens_inner_no_with(&mut self) -> Result<StreamingQuery, SqlError> {
        // Similar to parse_tokens_inner but doesn't consume WITH clauses (for job contexts)
        match self.current_token().token_type {
            TokenType::Select => self.parse_select_no_with(),
            TokenType::Create => self.parse_create(),
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT or CREATE statement in job definition".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }

    /// Parse OVER clause for window functions (Phase 8 - ROWS WINDOW BUFFER only)
    /// Syntax: OVER (ROWS WINDOW BUFFER 100 ROWS [PARTITION BY ...] [ORDER BY ...] [ROWS BETWEEN ...] [EMIT ...])
    /// NOTE: All window functions must use ROWS WINDOW syntax with explicit BUFFER size
    fn parse_over_clause(&mut self) -> Result<OverClause, SqlError> {
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
                exprs.push(Expr::Column(column));

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
                let expr = Expr::Column(full_column_name);
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

impl Default for StreamingSqlParser {
    fn default() -> Self {
        Self::new()
    }
}
