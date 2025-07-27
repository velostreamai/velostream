use crate::ferris::sql::ast::*;
use crate::ferris::sql::error::SqlError;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StreamingSqlParser {
    keywords: HashMap<String, TokenType>,
}

#[derive(Debug, Clone, PartialEq)]
enum TokenType {
    Select,
    From,
    Where,
    GroupBy,
    OrderBy,
    Window,
    Stream,
    Into,
    As,
    Identifier,
    String,
    Number,
    LeftParen,
    RightParen,
    Comma,
    Asterisk,
    Dot,
    Eof,
}

#[derive(Debug, Clone)]
struct Token {
    token_type: TokenType,
    value: String,
    position: usize,
}

impl StreamingSqlParser {
    pub fn new() -> Self {
        let mut keywords = HashMap::new();
        keywords.insert("SELECT".to_string(), TokenType::Select);
        keywords.insert("FROM".to_string(), TokenType::From);
        keywords.insert("WHERE".to_string(), TokenType::Where);
        keywords.insert("GROUP".to_string(), TokenType::GroupBy);
        keywords.insert("ORDER".to_string(), TokenType::OrderBy);
        keywords.insert("WINDOW".to_string(), TokenType::Window);
        keywords.insert("STREAM".to_string(), TokenType::Stream);
        keywords.insert("INTO".to_string(), TokenType::Into);
        keywords.insert("AS".to_string(), TokenType::As);

        Self { keywords }
    }

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
                    
                    let token_type = self.keywords
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
                    return Err(SqlError::ParseError(format!(
                        "Unexpected character '{}' at position {}", ch, position
                    )));
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
        parser.parse_select()
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
        self.tokens.get(self.current).unwrap_or(&Token {
            token_type: TokenType::Eof,
            value: String::new(),
            position: 0,
        })
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
            Err(SqlError::ParseError(format!(
                "Expected {:?}, found {:?} at position {}",
                expected, token.token_type, token.position
            )))
        }
    }

    fn parse_select(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Select)?;
        
        let fields = self.parse_select_fields()?;
        
        self.expect(TokenType::From)?;
        let from_stream = self.expect(TokenType::Identifier)?.value;
        
        let mut where_clause = None;
        if self.current_token().token_type == TokenType::Where {
            self.advance();
            where_clause = Some(self.parse_expression()?);
        }
        
        let mut window = None;
        if self.current_token().token_type == TokenType::Window {
            self.advance();
            window = Some(self.parse_window_spec()?);
        }

        Ok(StreamingQuery {
            select_fields: fields,
            from_stream,
            where_clause,
            window_spec: window,
            group_by: Vec::new(),
            order_by: Vec::new(),
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
                fields.push(SelectField::Named { expr, alias });
            }
            
            if self.current_token().token_type == TokenType::Comma {
                self.advance();
            } else {
                break;
            }
        }
        
        Ok(fields)
    }

    fn parse_expression(&mut self) -> Result<Expr, SqlError> {
        let token = self.current_token().clone();
        match token.token_type {
            TokenType::Identifier => {
                self.advance();
                if self.current_token().token_type == TokenType::Dot {
                    self.advance();
                    let field = self.expect(TokenType::Identifier)?.value;
                    Ok(Expr::Column { 
                        table: Some(token.value), 
                        name: field 
                    })
                } else {
                    Ok(Expr::Column { 
                        table: None, 
                        name: token.value 
                    })
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
                    Err(SqlError::ParseError(format!("Invalid number: {}", token.value)))
                }
            }
            _ => Err(SqlError::ParseError(format!(
                "Unexpected token in expression: {:?}", token.token_type
            ))),
        }
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, SqlError> {
        let window_type = match self.current_token().value.to_uppercase().as_str() {
            "TUMBLING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let duration = self.expect(TokenType::Identifier)?.value;
                self.expect(TokenType::RightParen)?;
                WindowType::Tumbling { duration }
            }
            "HOPPING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let duration = self.expect(TokenType::Identifier)?.value;
                self.expect(TokenType::Comma)?;
                let advance = self.expect(TokenType::Identifier)?.value;
                self.expect(TokenType::RightParen)?;
                WindowType::Hopping { duration, advance }
            }
            "SESSION" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let gap = self.expect(TokenType::Identifier)?.value;
                self.expect(TokenType::RightParen)?;
                WindowType::Session { gap }
            }
            _ => {
                return Err(SqlError::ParseError(
                    "Expected window type (TUMBLING, HOPPING, or SESSION)".to_string()
                ));
            }
        };

        Ok(WindowSpec {
            window_type,
            time_column: "event_time".to_string(),
        })
    }
}

impl Default for StreamingSqlParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT * FROM orders");
        assert!(result.is_ok());
        
        let query = result.unwrap();
        assert_eq!(query.from_stream, "orders");
        assert_eq!(query.select_fields.len(), 1);
        matches!(query.select_fields[0], SelectField::Wildcard);
    }

    #[test]
    fn test_select_with_columns() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT customer_id, amount FROM orders");
        assert!(result.is_ok());
        
        let query = result.unwrap();
        assert_eq!(query.select_fields.len(), 2);
    }

    #[test]
    fn test_windowed_select() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)");
        assert!(result.is_ok());
        
        let query = result.unwrap();
        assert!(query.window_spec.is_some());
    }
}