/*!
Tokenization and lexical analysis for streaming SQL.

This module handles the first phase of SQL parsing: converting SQL text into tokens.
It recognizes keywords, operators, literals, punctuation, and preserves comments for
annotation parsing.
*/

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::system_columns;
use std::collections::HashMap;

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

/// Normalize system column names to canonical UPPERCASE form.
/// Handles both simple names (`_timestamp` → `_TIMESTAMP`) and
/// qualified names (`m._event_time` → `m._EVENT_TIME`).
pub(super) fn normalize_column_name(name: String) -> String {
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

/// Build the keyword lookup table for token classification.
pub(super) fn build_keywords() -> HashMap<String, TokenType> {
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
    keywords.insert("NULL".to_string(), TokenType::Null);

    // Primary Key annotation (SQL standard)
    keywords.insert("PRIMARY".to_string(), TokenType::Primary);
    keywords.insert("KEY".to_string(), TokenType::Key);

    keywords
}

/// Tokenize SQL text into a vector of tokens.
///
/// This is the main lexer entry point. It scans the SQL text character by character,
/// recognizing keywords, operators, literals, and punctuation.
///
/// # Arguments
/// * `sql` - The SQL text to tokenize
/// * `keywords` - Keyword lookup table for token classification
///
/// # Returns
/// * `Ok(Vec<Token>)` - Successfully tokenized SQL
/// * `Err(SqlError)` - Lexical error (invalid character, unterminated string, etc.)
pub(super) fn tokenize(sql: &str, keywords: &HashMap<String, TokenType>) -> Result<Vec<Token>, SqlError> {
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

                let token_type = keywords
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

/// Tokenize SQL and separate comments from other tokens.
///
/// Returns a tuple of (non-comment tokens, comment tokens).
/// Comments are extracted with their position information for annotation parsing.
///
/// # Arguments
/// * `sql` - The SQL statement to tokenize
/// * `keywords` - Keyword lookup table for token classification
///
/// # Returns
/// * `Ok((Vec<Token>, Vec<Token>))` - Non-comment tokens and comment tokens separately
/// * `Err(SqlError)` - Tokenization error
pub(super) fn tokenize_with_comments(
    sql: &str,
    keywords: &HashMap<String, TokenType>,
) -> Result<(Vec<Token>, Vec<Token>), SqlError> {
    let all_tokens = tokenize(sql, keywords)?;

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

/// Extract comments that appear before a CREATE statement.
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
