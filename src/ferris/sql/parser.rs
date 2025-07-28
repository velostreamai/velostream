use crate::ferris::sql::ast::*;
use crate::ferris::sql::error::SqlError;
use std::collections::HashMap;
use std::time::Duration;

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
    Limit,
    Stream,
    Table,
    Create,
    Into,
    As,
    With,
    Identifier,
    String,
    Number,
    LeftParen,
    RightParen,
    Comma,
    Asterisk,
    Dot,
    Plus,
    Minus,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
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
        keywords.insert("LIMIT".to_string(), TokenType::Limit);
        keywords.insert("STREAM".to_string(), TokenType::Stream);
        keywords.insert("TABLE".to_string(), TokenType::Table);
        keywords.insert("CREATE".to_string(), TokenType::Create);
        keywords.insert("INTO".to_string(), TokenType::Into);
        keywords.insert("AS".to_string(), TokenType::As);
        keywords.insert("WITH".to_string(), TokenType::With);

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
                            position: Some(position - 1)
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
                    return Err(SqlError::ParseError { 
                        message: format!("Unexpected character '{}' at position {}", ch, position),
                        position: Some(position)
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
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT or CREATE statement".to_string(),
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
                message: format!("Expected {:?}, found {:?} at position {}", expected, token.token_type, token.position),
                position: Some(token.position)
            })
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

        let mut limit = None;
        if self.current_token().token_type == TokenType::Limit {
            self.advance();
            let limit_token = self.expect(TokenType::Number)?;
            limit = Some(limit_token.value.parse::<u64>().map_err(|_| SqlError::ParseError {
                message: "Invalid LIMIT value".to_string(),
                position: Some(limit_token.position),
            })?);
        }

        Ok(StreamingQuery::Select {
            fields,
            from: StreamSource::Stream(from_stream),
            where_clause,
            window,
            limit,
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

    fn parse_expression(&mut self) -> Result<Expr, SqlError> {
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_additive()?;
        
        while matches!(self.current_token().token_type, 
            TokenType::Equal | TokenType::NotEqual | TokenType::LessThan | 
            TokenType::GreaterThan | TokenType::LessThanOrEqual | TokenType::GreaterThanOrEqual) {
            
            let op_token = self.current_token().clone();
            self.advance();
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
        
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, SqlError> {
        let mut left = self.parse_multiplicative()?;
        
        while matches!(self.current_token().token_type, TokenType::Plus | TokenType::Minus) {
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
        
        while matches!(self.current_token().token_type, TokenType::Asterisk | TokenType::Divide) {
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
                    Ok(Expr::Function {
                        name: token.value,
                        args,
                    })
                } else if self.current_token().token_type == TokenType::Dot {
                    self.advance();
                    let field = self.expect(TokenType::Identifier)?.value;
                    Ok(Expr::Column(format!("{}.{}", token.value, field)))
                } else {
                    Ok(Expr::Column(token.value))
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
                        position: Some(token.position)
                    })
                }
            }
            TokenType::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(TokenType::RightParen)?;
                Ok(expr)
            }
            _ => Err(SqlError::ParseError { 
                message: format!("Unexpected token in expression: {:?}", token.token_type),
                position: Some(token.position)
            }),
        }
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, SqlError> {
        let window_type = match self.current_token().value.to_uppercase().as_str() {
            "TUMBLING" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let duration_str = self.parse_duration_token()?;
                self.expect(TokenType::RightParen)?;
                
                let size = self.parse_duration(&duration_str)?;
                WindowSpec::Tumbling { size, time_column: None }
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
                WindowSpec::Sliding { size, advance, time_column: None }
            }
            "SESSION" => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let gap_str = self.parse_duration_token()?;
                self.expect(TokenType::RightParen)?;
                
                let gap = self.parse_duration(&gap_str)?;
                WindowSpec::Session { gap, partition_by: Vec::new() }
            }
            _ => {
                return Err(SqlError::ParseError { 
                    message: "Expected window type (TUMBLING, SLIDING, or SESSION)".to_string(),
                    position: None
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
            _ => Err(SqlError::ParseError { 
                message: format!("Expected duration, found {:?}", token.token_type),
                position: Some(token.position)
            })
        }
    }

    fn parse_duration(&self, duration_str: &str) -> Result<Duration, SqlError> {
        if duration_str.ends_with('s') {
            let seconds: u64 = duration_str[..duration_str.len()-1].parse()
                .map_err(|_| SqlError::ParseError { 
                    message: format!("Invalid duration: {}", duration_str),
                    position: None
                })?;
            Ok(Duration::from_secs(seconds))
        } else if duration_str.ends_with('m') {
            let minutes: u64 = duration_str[..duration_str.len()-1].parse()
                .map_err(|_| SqlError::ParseError { 
                    message: format!("Invalid duration: {}", duration_str),
                    position: None
                })?;
            Ok(Duration::from_secs(minutes * 60))
        } else if duration_str.ends_with('h') {
            let hours: u64 = duration_str[..duration_str.len()-1].parse()
                .map_err(|_| SqlError::ParseError { 
                    message: format!("Invalid duration: {}", duration_str),
                    position: None
                })?;
            Ok(Duration::from_secs(hours * 3600))
        } else {
            // Default to seconds if no unit specified
            let seconds: u64 = duration_str.parse()
                .map_err(|_| SqlError::ParseError { 
                    message: format!("Invalid duration: {}", duration_str),
                    position: None
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
                position: Some(self.current_token().position)
            })
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
            },
            "MAP" => {
                self.expect(TokenType::LeftParen)?;
                let key_type = self.parse_data_type()?;
                self.expect(TokenType::Comma)?;
                let value_type = self.parse_data_type()?;
                self.expect(TokenType::RightParen)?;
                Ok(DataType::Map(Box::new(key_type), Box::new(value_type)))
            },
            _ => Err(SqlError::ParseError {
                message: format!("Unknown data type: {}", type_name),
                position: None
            })
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
                    position: Some(equals_token.position)
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
}

impl Default for StreamingSqlParser {
    fn default() -> Self {
        Self::new()
    }
}