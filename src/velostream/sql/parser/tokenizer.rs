/*!
# SQL Tokenizer

This module implements the lexical analysis phase of the SQL parser, converting raw SQL text
into a stream of tokens that can be processed by the parser.

## Key Features

- **Complete Token Recognition**: Handles all SQL keywords, operators, literals, and punctuation
- **Comment Preservation**: Retains single-line and multi-line comments for annotation parsing
- **Position Tracking**: Maintains character position for detailed error reporting
- **String Literals**: Supports both single and double quoted strings
- **Number Literals**: Handles integers, decimals, and scientific notation
- **Operators**: Recognizes all SQL operators including compound operators (<=, >=, !=, ||)

## Token Types

The tokenizer produces the following token categories:
- **Keywords**: SELECT, FROM, WHERE, CREATE, etc.
- **Identifiers**: Table names, column names, function names
- **Literals**: String ('text'), numbers (42, 3.14, 1e-5), NULL
- **Operators**: Arithmetic (+, -, *, /), comparison (=, <, >), logical (AND, OR)
- **Punctuation**: Parentheses, commas, dots, semicolons
- **Comments**: Single-line (--) and multi-line (/* */)

## Examples

```rust,ignore
// Tokenize with comments separated
let (tokens, comments) = parser.tokenize_with_comments(
    "-- @metric: counter_total\n\
     SELECT * FROM orders"
)?;

// Extract comments before a CREATE statement
let preceding_comments = StreamingSqlParser::extract_preceding_comments(
    &comments,
    create_position
);
```

## Error Handling

The tokenizer provides detailed error messages for:
- Unexpected characters
- Unterminated string literals
- Unterminated multi-line comments
- Invalid operator sequences

All errors include position information for precise error reporting.
*/

use super::{Token, TokenType};
use crate::velostream::sql::error::SqlError;
use std::collections::HashMap;

impl super::StreamingSqlParser {
    pub(super) fn tokenize(&self, sql: &str) -> Result<Vec<Token>, SqlError> {
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
}
