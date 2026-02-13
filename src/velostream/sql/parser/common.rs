/*!
# Token Parser - Core Infrastructure

This module provides the fundamental token parsing infrastructure used throughout
the SQL parser. It contains the `TokenParser` struct and core utility methods for
navigating and consuming tokens during recursive descent parsing.

## Purpose

The `TokenParser` is the workhorse of the streaming SQL parser, providing:
- **Token Navigation**: Moving through the token stream with lookahead support
- **Expectation Methods**: Enforcing grammar rules with clear error messages
- **Utility Functions**: Common operations like consuming optional tokens
- **Error Context**: Enhanced error messages with position and SQL text context

## Architecture

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Tokenizer     │────▶│  TokenParser    │────▶│   AST Nodes     │
│ (produces       │     │ (navigates      │     │ (structured     │
│  token stream)  │     │  and consumes)  │     │  query)         │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Key Components

### TokenParser Struct
- Maintains current position in token stream
- Stores original SQL text for error context
- Preserves comments for annotation parsing

### Navigation Methods
- `current_token()`: Get current token without advancing
- `peek_token(offset)`: Lookahead without consuming
- `advance()`: Move to next token

### Expectation Methods
- `expect(TokenType)`: Consume specific token type or error
- `expect_identifier_or_keyword()`: Accept identifiers and many keywords as identifiers
- `expect_keyword(str)`: Match exact keyword string

### Utility Methods
- `consume_if_matches(str)`: Optionally consume matching keyword
- `consume_semicolon()`: Skip optional statement terminator
- `create_parse_error(msg)`: Generate context-aware error

## Helper Functions

### normalize_column_name()
Converts system column names to canonical UPPERCASE form, handling both
simple names (`_timestamp` → `_TIMESTAMP`) and qualified names
(`m._event_time` → `m._EVENT_TIME`).

## Usage Example

```rust,ignore
// Example of using TokenParser methods
fn parse_custom_clause(parser: &mut TokenParser) -> Result<(), SqlError> {
    // Expect keyword
    parser.expect_keyword("CUSTOM")?;

    // Get identifier
    let name = parser.expect_identifier_or_keyword()?;

    // Optional semicolon
    parser.consume_semicolon();

    Ok(())
}
```
*/

use super::{Token, TokenType};
use crate::velostream::sql::ast::StreamingQuery;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::system_columns;

/// Normalize system column names to canonical UPPERCASE form.
///
/// Handles both simple names (`_timestamp` → `_TIMESTAMP`) and
/// qualified names (`m._event_time` → `m._EVENT_TIME`).
///
/// # Arguments
/// * `name` - Column name to normalize (may include table qualifier)
///
/// # Returns
/// Normalized column name with system columns in UPPERCASE
///
/// # Examples
/// ```
/// use velostream::velostream::sql::parser::common::normalize_column_name;
///
/// assert_eq!(normalize_column_name("_timestamp".to_string()), "_TIMESTAMP");
/// assert_eq!(normalize_column_name("m._event_time".to_string()), "m._EVENT_TIME");
/// assert_eq!(normalize_column_name("user_id".to_string()), "user_id");
/// ```
pub fn normalize_column_name(name: String) -> String {
    if let Some(dot_pos) = name.rfind('.') {
        let field = &name[dot_pos + 1..];
        if let Some(normalized) = system_columns::normalize_if_system_column(field) {
            format!("{}.{}", &name[..dot_pos], normalized)
        } else {
            name
        }
    } else if let Some(normalized) = system_columns::normalize_if_system_column(&name) {
        normalized.to_string()
    } else {
        name
    }
}

/// Core token parser infrastructure for recursive descent parsing.
///
/// The `TokenParser` maintains the current position in a token stream and provides
/// methods for navigating, consuming, and validating tokens according to SQL grammar rules.
///
/// # Lifetime
/// - `'a`: Lifetime of the original SQL text string used for error context
///
/// # Fields
/// - `tokens`: Complete vector of tokens from tokenization phase
/// - `current`: Current position in token stream (0-indexed)
/// - `sql_text`: Original SQL text for error message context
/// - `comments`: Preserved comments for annotation parsing (@metric, @job_mode, etc.)
pub struct TokenParser<'a> {
    tokens: Vec<Token>,
    current: usize,
    sql_text: &'a str,
    // Store comments for annotation parsing
    comments: Vec<Token>,
}

impl<'a> TokenParser<'a> {
    /// Create a new token parser from a token stream.
    ///
    /// # Arguments
    /// * `tokens` - Vector of tokens produced by tokenizer
    /// * `sql_text` - Original SQL text for error context
    /// * `comments` - Preserved comment tokens for annotation parsing
    ///
    /// # Returns
    /// A new `TokenParser` positioned at the start of the token stream
    pub fn new(tokens: Vec<Token>, sql_text: &'a str, comments: Vec<Token>) -> Self {
        Self {
            tokens,
            current: 0,
            sql_text,
            comments,
        }
    }

    /// Get comment tokens for annotation parsing.
    ///
    /// Returns comment strings extracted from the token stream,
    /// which can be passed to annotation parsing functions.
    ///
    /// # Returns
    /// Vector of comment strings (without leading `--` or `/* */`)
    pub fn get_comment_strings(&self) -> Vec<String> {
        self.comments.iter().map(|t| t.value.clone()).collect()
    }

    /// Create an enhanced parse error with context from current parser state.
    ///
    /// Automatically includes position information and SQL text context
    /// for debugging and user-friendly error messages.
    ///
    /// # Arguments
    /// * `message` - Error description
    ///
    /// # Returns
    /// `SqlError::ParseError` with position and context
    pub fn create_parse_error(&self, message: impl Into<String>) -> SqlError {
        let position = if self.current < self.tokens.len() {
            Some(self.tokens[self.current].position)
        } else if !self.tokens.is_empty() {
            Some(self.tokens[self.tokens.len() - 1].position)
        } else {
            None
        };

        SqlError::parse_error_with_context(message, position, Some(self.sql_text))
    }

    /// Get the current token without advancing the parser.
    ///
    /// Returns a static EOF token if the parser has reached the end of the token stream,
    /// avoiding Option<> unwrapping in parser methods.
    ///
    /// # Returns
    /// Reference to current token or EOF token if at end
    pub fn current_token(&self) -> &Token {
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

    /// Peek ahead in the token stream without consuming.
    ///
    /// # Arguments
    /// * `offset` - Number of positions to look ahead (1 = next token)
    ///
    /// # Returns
    /// `Some(&Token)` if position is valid, `None` if beyond end
    pub fn peek_token(&self, offset: usize) -> Option<&Token> {
        let peek_index = self.current + offset;
        if peek_index < self.tokens.len() {
            Some(&self.tokens[peek_index])
        } else {
            None
        }
    }

    /// Advance to the next token in the stream.
    ///
    /// Safe to call at end of stream - will not advance beyond end.
    pub fn advance(&mut self) {
        if self.current < self.tokens.len() - 1 {
            self.current += 1;
        }
    }

    /// Consume a token of the expected type or return an error.
    ///
    /// Advances the parser on success.
    ///
    /// # Arguments
    /// * `expected` - Required token type
    ///
    /// # Returns
    /// * `Ok(Token)` - Consumed token if type matches
    /// * `Err(SqlError)` - Parse error with expected vs actual types
    pub fn expect(&mut self, expected: TokenType) -> Result<Token, SqlError> {
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

    /// Accept an identifier or a keyword that can be used as an identifier.
    ///
    /// This allows reserved keywords like TABLE, STREAM, etc. to be used as
    /// table/column names in appropriate contexts, following SQL's context-sensitive
    /// keyword rules.
    ///
    /// # Returns
    /// * `Ok(String)` - Identifier or keyword value
    /// * `Err(SqlError)` - Neither identifier nor usable keyword found
    ///
    /// # Usable Keywords
    /// TABLE, STREAM, CREATE, INTO, SHOW, LIST, STREAMS, TABLES, TOPICS, FUNCTIONS,
    /// SCHEMA, JOBS, JOB, PARTITIONS, START, STOP, FORCE, PAUSE, RESUME, DEPLOY,
    /// ROLLBACK, VERSION
    pub fn expect_identifier_or_keyword(&mut self) -> Result<String, SqlError> {
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

    /// Check for and consume the DISTINCT keyword, returning true if found.
    ///
    /// This is a shared helper to avoid duplicating the DISTINCT parsing logic
    /// across SELECT and aggregate function parsing.
    ///
    /// # Returns
    /// `true` if DISTINCT was present and consumed, `false` otherwise
    pub fn parse_distinct(&mut self) -> bool {
        if self.current_token().token_type == TokenType::Distinct {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Conditionally consume a token if it matches the expected keyword.
    ///
    /// Case-insensitive comparison for SQL keywords.
    ///
    /// # Arguments
    /// * `expected` - Keyword string to match
    ///
    /// # Returns
    /// `true` if keyword matched and consumed, `false` otherwise
    pub fn consume_if_matches(&mut self, expected: &str) -> bool {
        if self.current_token().value.to_uppercase() == expected.to_uppercase() {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Consume an optional semicolon at the end of a statement.
    ///
    /// Does nothing if semicolon is not present - allows flexible statement termination.
    pub fn consume_semicolon(&mut self) {
        if self.current_token().token_type == TokenType::Semicolon {
            self.advance();
        }
    }

    /// Expect a specific keyword or return an error.
    ///
    /// Case-insensitive keyword matching for SQL standard compliance.
    ///
    /// # Arguments
    /// * `keyword` - Expected keyword string
    ///
    /// # Returns
    /// * `Ok(())` - Keyword matched and consumed
    /// * `Err(SqlError)` - Expected keyword not found
    pub fn expect_keyword(&mut self, keyword: &str) -> Result<(), SqlError> {
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

    /// Main query parsing dispatcher.
    ///
    /// Routes to appropriate parser based on the leading token type.
    ///
    /// # Returns
    /// Parsed `StreamingQuery` AST node
    ///
    /// # Errors
    /// Returns `SqlError::ParseError` if the query doesn't match any known statement type
    pub fn parse_query(&mut self) -> Result<StreamingQuery, SqlError> {
        match self.current_token().token_type {
            TokenType::Select => self.parse_select(),
            TokenType::Create => self.parse_create(),
            TokenType::Show | TokenType::List => self.parse_show(),
            TokenType::Start => self.parse_start_job(),
            TokenType::Stop => self.parse_stop_job(),
            TokenType::Pause => self.parse_pause_job(),
            TokenType::Resume => self.parse_resume_job(),
            TokenType::Deploy => self.parse_deploy_job(),
            TokenType::Rollback => self.parse_rollback_job(),
            TokenType::Describe => self.parse_describe(),
            _ => Err(self.create_parse_error("Expected SELECT, CREATE, SHOW, LIST, START, STOP, PAUSE, RESUME, DEPLOY, ROLLBACK, or DESCRIBE statement"))
        }
    }
}
