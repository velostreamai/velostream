/*!
# Expression Parsing

This module provides comprehensive expression parsing for SQL queries, implementing a
complete precedence hierarchy from logical operators down to primary expressions.

## Expression Precedence Pyramid

The parser implements standard SQL operator precedence using a recursive descent approach:

```
Level 1 (Lowest):  Logical OR                  (parse_logical_or)
Level 2:           Logical AND                 (parse_logical_and)
Level 3:           Comparisons/IS/IN/BETWEEN   (parse_comparison)
Level 4:           String Concatenation (||)   (parse_concatenative)
Level 5:           Addition/Subtraction        (parse_additive)
Level 6:           Multiplication/Division     (parse_multiplicative)
Level 7 (Highest): Primary Expressions         (parse_primary)
```

Each level calls the next higher level, ensuring proper precedence during parsing.

## Supported Expression Types

### Binary Operations
- **Arithmetic**: `+`, `-`, `*`, `/`
- **Comparison**: `=`, `!=`, `<`, `>`, `<=`, `>=`
- **Logical**: `AND`, `OR`
- **String**: `||` (concatenation)
- **Set Operations**: `IN`, `NOT IN`
- **Pattern Matching**: `LIKE`, `NOT LIKE`
- **Range**: `BETWEEN`, `NOT BETWEEN`

### Unary Operations
- **Negation**: `-expr`
- **Logical NOT**: `NOT expr`
- **Null Checks**: `IS NULL`, `IS NOT NULL`

### Primary Expressions
- **Literals**: Numbers, strings, booleans, NULL, intervals
- **Column References**: Simple (`column`) and qualified (`table.column`)
- **Function Calls**: Standard (`COUNT(*)`) and window functions (`AVG(...) OVER (...)`)
- **Subqueries**: Scalar, IN, EXISTS, NOT EXISTS
- **CASE Expressions**: Conditional logic with WHEN/THEN/ELSE
- **Parenthesized Expressions**: Grouping for precedence override

## Special Function Handling

### EXTRACT Function
Supports both function-style and SQL standard syntax:
```sql
EXTRACT('EPOCH', timestamp)           -- Function style (legacy)
EXTRACT(EPOCH FROM timestamp)         -- SQL standard
```

### CAST Function
Only SQL standard syntax is supported:
```sql
CAST(expr AS type)                    -- SQL standard (required)
```

### CURRENT_TIMESTAMP
No parentheses required:
```sql
CURRENT_TIMESTAMP                     -- Valid
CURRENT_TIMESTAMP()                   -- Also valid
```

## CASE Expression Syntax

```sql
CASE
  WHEN condition1 THEN result1
  WHEN condition2 THEN result2
  [ELSE default_result]
END
```

## Error Handling

All parsing methods return `Result<Expr, SqlError>` with detailed error messages including:
- Position information for debugging
- Expected vs. actual token type
- Context about the specific parsing operation that failed

## Examples

```rust,no_run
use velostream::velostream::sql::parser::common::TokenParser;

// Parse a simple arithmetic expression
let expr = parser.parse_expression()?; // "amount * 1.05"

// Parse a complex conditional
let expr = parser.parse_expression()?; // "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"

// Parse a window function
let expr = parser.parse_expression()?; // "AVG(price) OVER (PARTITION BY symbol ORDER BY time)"
```
*/

use super::common::{TokenParser, normalize_column_name};
use super::*;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;

impl<'a> TokenParser<'a> {
    /// Entry point for expression parsing.
    ///
    /// Delegates to `parse_logical_or()` which implements the lowest precedence level
    /// of the expression grammar (logical OR operations).
    ///
    /// # Returns
    /// - `Ok(Expr)` - Successfully parsed expression
    /// - `Err(SqlError)` - Parse error with position information
    pub(super) fn parse_expression(&mut self) -> Result<Expr, SqlError> {
        self.parse_logical_or()
    }

    /// Parse logical OR expressions (lowest precedence).
    ///
    /// Grammar: `logical_and (OR logical_and)*`
    ///
    /// # Examples
    /// ```sql
    /// status = 'active' OR amount > 1000
    /// active OR premium OR vip
    /// ```
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

    /// Parse logical AND expressions.
    ///
    /// Grammar: `comparison (AND comparison)*`
    ///
    /// # Examples
    /// ```sql
    /// status = 'active' AND amount > 1000
    /// verified AND premium AND NOT suspended
    /// ```
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

    /// Parse comparison and set operations.
    ///
    /// Handles:
    /// - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
    /// - Null checks: `IS NULL`, `IS NOT NULL`
    /// - Set membership: `IN (...)`, `NOT IN (...)`
    /// - Range checks: `BETWEEN ... AND ...`, `NOT BETWEEN ... AND ...`
    /// - Pattern matching: `LIKE`, `NOT LIKE`
    ///
    /// # Grammar
    /// ```text
    /// concatenative (
    ///     (= | != | < | > | <= | >=) concatenative |
    ///     IS [NOT] NULL |
    ///     [NOT] IN (list | subquery) |
    ///     [NOT] BETWEEN low AND high |
    ///     [NOT] LIKE pattern
    /// )*
    /// ```
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

    /// Parse string concatenation operations.
    ///
    /// Grammar: `additive (|| additive)*`
    ///
    /// # Examples
    /// ```sql
    /// first_name || ' ' || last_name
    /// 'prefix_' || id
    /// ```
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

    /// Parse addition and subtraction operations.
    ///
    /// Grammar: `multiplicative ((+ | -) multiplicative)*`
    ///
    /// # Examples
    /// ```sql
    /// price + tax
    /// total - discount
    /// amount + fee - credit
    /// ```
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

    /// Parse multiplication and division operations.
    ///
    /// Grammar: `primary ((* | /) primary)*`
    ///
    /// # Examples
    /// ```sql
    /// quantity * price
    /// total / count
    /// amount * rate / 100
    /// ```
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

    /// Parse primary expressions (highest precedence level).
    ///
    /// Handles all atomic expression types:
    /// - Literals: numbers, strings, booleans, NULL, intervals
    /// - Column references: simple and qualified
    /// - Function calls: standard and window functions
    /// - Special functions: EXTRACT, CAST, CURRENT_TIMESTAMP
    /// - Subqueries: scalar, EXISTS, NOT EXISTS
    /// - CASE expressions
    /// - Parenthesized expressions
    /// - Unary minus: `-expr`
    /// - Unary NOT: `NOT expr`
    ///
    /// # Grammar
    /// ```text
    /// primary ::=
    ///     literal |
    ///     column_ref |
    ///     function_call [OVER (...)] |
    ///     CASE ... END |
    ///     EXISTS (subquery) |
    ///     NOT EXISTS (subquery) |
    ///     (expression) |
    ///     - primary |
    ///     NOT primary
    /// ```
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
                    Ok(Expr::Column(normalize_column_name(format!(
                        "{}.{}",
                        token.value, field
                    ))))
                } else {
                    Ok(Expr::Column(normalize_column_name(token.value)))
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
                    Ok(Expr::Column(normalize_column_name(column_name)))
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
                            position: Some(self.current_token().position),
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

    /// Parse CASE expression with WHEN/THEN/ELSE clauses.
    ///
    /// # Grammar
    /// ```text
    /// CASE
    ///   WHEN condition1 THEN result1
    ///   WHEN condition2 THEN result2
    ///   ...
    ///   [ELSE default_result]
    /// END
    /// ```
    ///
    /// # Examples
    /// ```sql
    /// CASE
    ///   WHEN amount > 1000 THEN 'high'
    ///   WHEN amount > 100 THEN 'medium'
    ///   ELSE 'low'
    /// END
    /// ```
    ///
    /// # Returns
    /// - `Ok(Expr::Case)` - Successfully parsed CASE expression
    /// - `Err(SqlError)` - Missing WHEN clause, missing THEN, or missing END
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
}
