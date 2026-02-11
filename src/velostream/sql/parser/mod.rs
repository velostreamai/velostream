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

use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;

// Submodule declarations
pub mod annotations;
pub mod clauses;
pub mod commands;
pub mod common;
pub mod expressions;
pub mod select;
pub mod tokenizer;
pub mod validator;
pub mod window_functions;

// Re-exports for public API
pub use common::normalize_column_name;

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
    Distinct,   // DISTINCT
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
    Versions,   // VERSIONS
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
        keywords.insert("DISTINCT".to_string(), TokenType::Distinct);
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
        keywords.insert("PRIMARY".to_string(), TokenType::Primary);
        keywords.insert("KEY".to_string(), TokenType::Key);
        keywords.insert("NULL".to_string(), TokenType::Null);

        StreamingSqlParser { keywords }
    }

    /// Parses a SQL query string into an AST.
    ///
    /// This is the main entry point for the parser. It handles tokenization,
    /// annotation extraction, and AST construction in a single call.
    ///
    /// # Arguments
    /// * `sql` - The SQL query string to parse
    ///
    /// # Returns
    /// A `StreamingQuery` AST node representing the parsed query
    ///
    /// # Errors
    /// Returns `SqlError::ParseError` if the SQL is invalid
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use velostream::velostream::sql::parser::StreamingSqlParser;
    ///
    /// let parser = StreamingSqlParser::new();
    /// let query = parser.parse("SELECT * FROM orders WHERE price > 100")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn parse(&self, sql: &str) -> Result<StreamingQuery, SqlError> {
        // First tokenize with comments preserved
        let (tokens, comments) = self.tokenize_with_comments(sql)?;

        // Parse the query with comment context
        self.parse_tokens_with_context(tokens, sql, comments)
    }

    /// Internal method for backward compatibility
    #[allow(dead_code)]
    fn parse_tokens(&self, tokens: Vec<Token>) -> Result<StreamingQuery, SqlError> {
        self.parse_tokens_with_context(tokens, "", Vec::new())
    }

    /// Parses tokens with SQL text and comments for annotation extraction
    fn parse_tokens_with_context(
        &self,
        tokens: Vec<Token>,
        sql_text: &str,
        comments: Vec<Token>,
    ) -> Result<StreamingQuery, SqlError> {
        use common::TokenParser;

        let mut parser = TokenParser::new(tokens, sql_text, comments);

        // Parse the main query using delegation to submodules
        let query = parser.parse_query()?;

        // Note: Annotations are now parsed separately by SqlAnnotationParser
        // in the application context, not during SQL parsing.
        //
        // Note: We've removed the parse-time aggregate validation that was causing
        // false rejections of valid streaming queries. Validation now happens at
        // execution time where we have full context about windowing and grouping.
        //
        // Examples of valid queries that were incorrectly rejected:
        // - SELECT COUNT(*) FROM orders
        //   (Valid: implicit window over entire stream)
        // - SELECT status, COUNT(*) FROM orders GROUP BY status
        //   (With GROUP BY - valid)
        // - SELECT symbol, AVG(price) FROM orders WINDOW TUMBLING(1m)
        //   (With WINDOW - valid)
        //
        // The commented-out call was rejecting valid queries at parse time:
        // AggregateValidator::validate(&query)?;

        Ok(query)
    }
}

impl Default for StreamingSqlParser {
    fn default() -> Self {
        Self::new()
    }
}
