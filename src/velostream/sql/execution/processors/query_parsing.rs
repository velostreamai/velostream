/*!
# Query Parsing Utilities for Subquery Execution

This module provides utility functions for parsing SQL queries to extract table names,
WHERE clauses, and SELECT expressions for subquery execution with KTable/Table integration.
*/

use crate::velostream::sql::ast::{Expr, LiteralValue, SelectField, StreamSource};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::StreamingQuery;

/// Extract table name from a SELECT query's FROM clause
///
/// This function parses the FROM clause to identify the target table for subquery execution.
/// Supports both Stream and Table sources.
///
/// # Arguments
/// * `query` - The SELECT query to extract the table name from
///
/// # Returns
/// * `Ok(String)` - The table name from the FROM clause
/// * `Err(SqlError)` - If the query is not a SELECT or has an invalid FROM clause
///
/// # Examples
/// ```rust,no_run
/// # use velostream::velostream::sql::parser::StreamingSqlParser;
/// # use velostream::velostream::sql::execution::processors::query_parsing::extract_table_name;
/// # fn example() -> Result<(), velostream::velostream::sql::error::SqlError> {
/// # let parser = StreamingSqlParser::new();
/// # let query = parser.parse("SELECT * FROM users WHERE active = true")?;
/// let table_name = extract_table_name(&query)?;
/// assert_eq!(table_name, "users");
/// # Ok(())
/// # }
/// ```
pub fn extract_table_name(query: &StreamingQuery) -> Result<String, SqlError> {
    match query {
        StreamingQuery::Select { from, .. } => match from {
            StreamSource::Stream(name) | StreamSource::Table(name) => Ok(name.clone()),
            StreamSource::Uri(uri) => {
                // For URI sources, try to extract a meaningful name
                // This is a fallback for cases where the URI represents a table
                let parsed_name = uri
                    .split('/')
                    .last()
                    .unwrap_or("unknown")
                    .split('?')
                    .next()
                    .unwrap_or("unknown")
                    .to_string();

                if parsed_name.is_empty() || parsed_name == "unknown" {
                    Err(SqlError::ExecutionError {
                        message: format!("Cannot extract table name from URI: {}", uri),
                        query: None,
                    })
                } else {
                    Ok(parsed_name)
                }
            }
            StreamSource::Subquery(_) => Err(SqlError::ExecutionError {
                message: "Subquery sources are not yet supported for table name extraction"
                    .to_string(),
                query: None,
            }),
        },
        _ => Err(SqlError::ExecutionError {
            message: "Query must be a SELECT statement to extract table name".to_string(),
            query: None,
        }),
    }
}

/// Extract WHERE clause from a SELECT query as a string
///
/// This function converts the WHERE clause expression back to a string format
/// that can be used with SqlQueryable methods like sql_filter() and sql_exists().
///
/// # Arguments
/// * `query` - The SELECT query to extract the WHERE clause from
///
/// # Returns
/// * `Ok(String)` - The WHERE clause as a string (empty if no WHERE clause)
/// * `Err(SqlError)` - If the query is not a SELECT or WHERE clause cannot be processed
///
/// # Examples
/// ```rust,no_run
/// # use velostream::velostream::sql::parser::StreamingSqlParser;
/// # use velostream::velostream::sql::execution::processors::query_parsing::extract_where_clause;
/// # fn example() -> Result<(), velostream::velostream::sql::error::SqlError> {
/// # let parser = StreamingSqlParser::new();
/// # let query = parser.parse("SELECT * FROM users WHERE active = true AND age > 18")?;
/// let where_clause = extract_where_clause(&query)?;
/// assert_eq!(where_clause, "active = true AND age > 18");
/// # Ok(())
/// # }
/// ```
pub fn extract_where_clause(query: &StreamingQuery) -> Result<String, SqlError> {
    match query {
        StreamingQuery::Select { where_clause, .. } => {
            match where_clause {
                Some(expr) => expr_to_string(expr),
                None => Ok("true".to_string()), // No WHERE clause means all records match
            }
        }
        _ => Err(SqlError::ExecutionError {
            message: "Query must be a SELECT statement to extract WHERE clause".to_string(),
            query: None,
        }),
    }
}

/// Extract SELECT expression for scalar subqueries
///
/// This function extracts the expression from the SELECT clause for scalar subqueries.
/// For scalar subqueries, there should be exactly one selected field.
///
/// # Arguments
/// * `query` - The SELECT query to extract the expression from
///
/// # Returns
/// * `Ok(String)` - The SELECT expression as a string
/// * `Err(SqlError)` - If the query is not a SELECT or has invalid SELECT clause
///
/// # Examples
/// ```rust,no_run
/// # use velostream::velostream::sql::parser::StreamingSqlParser;
/// # use velostream::velostream::sql::execution::processors::query_parsing::extract_select_expression;
/// # fn example() -> Result<(), velostream::velostream::sql::error::SqlError> {
/// # let parser = StreamingSqlParser::new();
/// # let query = parser.parse("SELECT MAX(price) FROM products WHERE category = 'electronics'")?;
/// let select_expr = extract_select_expression(&query)?;
/// assert_eq!(select_expr, "MAX(price)");
/// # Ok(())
/// # }
/// ```
pub fn extract_select_expression(query: &StreamingQuery) -> Result<String, SqlError> {
    match query {
        StreamingQuery::Select { fields, .. } => {
            if fields.len() != 1 {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Scalar subquery must select exactly one column, found {}",
                        fields.len()
                    ),
                    query: None,
                });
            }

            match &fields[0] {
                SelectField::Expression { expr, .. } => expr_to_string(expr),
                SelectField::Column(column) => Ok(column.clone()),
                SelectField::AliasedColumn { column, .. } => Ok(column.clone()),
                SelectField::Wildcard => Err(SqlError::ExecutionError {
                    message: "Scalar subquery cannot use wildcard (*) selection".to_string(),
                    query: None,
                }),
            }
        }
        _ => Err(SqlError::ExecutionError {
            message: "Query must be a SELECT statement to extract SELECT expression".to_string(),
            query: None,
        }),
    }
}

/// Extract column name for IN subqueries
///
/// This function extracts the column name from a SELECT query that's used in IN subqueries.
/// IN subqueries should select exactly one column.
///
/// # Arguments
/// * `query` - The SELECT query to extract the column from
///
/// # Returns
/// * `Ok(String)` - The column name
/// * `Err(SqlError)` - If the query is not a SELECT or doesn't select exactly one column
///
/// # Examples
/// ```rust,no_run
/// # use velostream::velostream::sql::parser::StreamingSqlParser;
/// # use velostream::velostream::sql::execution::processors::query_parsing::extract_select_column;
/// # fn example() -> Result<(), velostream::velostream::sql::error::SqlError> {
/// # let parser = StreamingSqlParser::new();
/// # let query = parser.parse("SELECT user_id FROM active_users WHERE tier = 'premium'")?;
/// let column = extract_select_column(&query)?;
/// assert_eq!(column, "user_id");
/// # Ok(())
/// # }
/// ```
pub fn extract_select_column(query: &StreamingQuery) -> Result<String, SqlError> {
    match query {
        StreamingQuery::Select { fields, .. } => {
            if fields.len() != 1 {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "IN subquery must select exactly one column, found {}",
                        fields.len()
                    ),
                    query: None,
                });
            }

            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::Column(column_name) => Ok(column_name.clone()),
                        _ => {
                            // For complex expressions, convert to string
                            expr_to_string(expr)
                        }
                    }
                }
                SelectField::Column(column) => Ok(column.clone()),
                SelectField::AliasedColumn { column, .. } => Ok(column.clone()),
                SelectField::Wildcard => Err(SqlError::ExecutionError {
                    message: "IN subquery cannot use wildcard (*) selection".to_string(),
                    query: None,
                }),
            }
        }
        _ => Err(SqlError::ExecutionError {
            message: "Query must be a SELECT statement to extract column".to_string(),
            query: None,
        }),
    }
}

/// Convert an AST expression to string representation
///
/// This is a helper function that converts SQL AST expressions back to string format
/// for use with SqlQueryable methods.
///
/// # Arguments
/// * `expr` - The expression to convert to string
///
/// # Returns
/// * `Ok(String)` - The expression as a string
/// * `Err(SqlError)` - If the expression cannot be converted
pub fn expr_to_string(expr: &Expr) -> Result<String, SqlError> {
    match expr {
        Expr::Column(name) => Ok(name.clone()),
        Expr::Literal(lit) => literal_to_string(lit),
        Expr::BinaryOp { left, op, right } => {
            let left_str = expr_to_string(left)?;
            let right_str = expr_to_string(right)?;
            let op_str = binary_op_to_string(op);
            Ok(format!("{} {} {}", left_str, op_str, right_str))
        }
        Expr::UnaryOp { op, expr } => {
            let expr_str = expr_to_string(expr)?;
            let op_str = unary_op_to_string(op);
            Ok(format!("{} {}", op_str, expr_str))
        }
        Expr::Function { name, args } => {
            let args_str = args
                .iter()
                .map(expr_to_string)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("{}({})", name, args_str))
        }
        Expr::Case {
            when_clauses,
            else_clause,
        } => {
            let mut result = "CASE".to_string();

            for (condition, value) in when_clauses {
                let condition_str = expr_to_string(condition)?;
                let value_str = expr_to_string(value)?;
                result.push_str(&format!(" WHEN {} THEN {}", condition_str, value_str));
            }

            if let Some(else_expr) = else_clause {
                let else_str = expr_to_string(else_expr)?;
                result.push_str(&format!(" ELSE {}", else_str));
            }

            result.push_str(" END");
            Ok(result)
        }
        Expr::List(list) => {
            let items = list
                .iter()
                .map(expr_to_string)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({})", items))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let expr_str = expr_to_string(expr)?;
            let low_str = expr_to_string(low)?;
            let high_str = expr_to_string(high)?;

            if *negated {
                Ok(format!(
                    "{} NOT BETWEEN {} AND {}",
                    expr_str, low_str, high_str
                ))
            } else {
                Ok(format!("{} BETWEEN {} AND {}", expr_str, low_str, high_str))
            }
        }
        _ => {
            // For complex expressions that we don't support yet, return a placeholder
            Err(SqlError::ExecutionError {
                message: "Complex expression conversion not yet supported for subqueries"
                    .to_string(),
                query: None,
            })
        }
    }
}

/// Convert literal values to string representation
fn literal_to_string(lit: &LiteralValue) -> Result<String, SqlError> {
    match lit {
        LiteralValue::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))), // Escape single quotes
        LiteralValue::Integer(i) => Ok(i.to_string()),
        LiteralValue::Float(f) => Ok(f.to_string()),
        LiteralValue::Boolean(b) => Ok(b.to_string()),
        LiteralValue::Null => Ok("NULL".to_string()),
        LiteralValue::Decimal(d) => Ok(d.clone()),
        LiteralValue::Interval { .. } => {
            // For now, convert intervals to string representation
            Ok("INTERVAL".to_string())
        }
    }
}

/// Convert binary operators to string representation
fn binary_op_to_string(op: &crate::velostream::sql::ast::BinaryOperator) -> &'static str {
    use crate::velostream::sql::ast::BinaryOperator;
    match op {
        BinaryOperator::Equal => "=",
        BinaryOperator::NotEqual => "!=",
        BinaryOperator::LessThan => "<",
        BinaryOperator::LessThanOrEqual => "<=",
        BinaryOperator::GreaterThan => ">",
        BinaryOperator::GreaterThanOrEqual => ">=",
        BinaryOperator::And => "AND",
        BinaryOperator::Or => "OR",
        BinaryOperator::Add => "+",
        BinaryOperator::Subtract => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::Concat => "||",
        BinaryOperator::Like => "LIKE",
        BinaryOperator::NotLike => "NOT LIKE",
        BinaryOperator::In => "IN",
        BinaryOperator::NotIn => "NOT IN",
    }
}

/// Convert unary operators to string representation
fn unary_op_to_string(op: &crate::velostream::sql::ast::UnaryOperator) -> &'static str {
    use crate::velostream::sql::ast::UnaryOperator;
    match op {
        UnaryOperator::Not => "NOT",
        UnaryOperator::Minus => "-",
        UnaryOperator::Plus => "+",
        UnaryOperator::IsNull => "IS NULL",
        UnaryOperator::IsNotNull => "IS NOT NULL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::ast::{BinaryOperator, LiteralValue, SelectField, StreamSource};
    use crate::velostream::sql::StreamingQuery;
    use std::collections::HashMap;

    #[test]
    fn test_extract_table_name_from_stream() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Stream("users".to_string()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            joins: None,
            window: None,
            emit_mode: None,
            properties: None,
        };

        let result = extract_table_name(&query).unwrap();
        assert_eq!(result, "users");
    }

    #[test]
    fn test_extract_table_name_from_table() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Table("products".to_string()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            joins: None,
            window: None,
            emit_mode: None,
            properties: None,
        };

        let result = extract_table_name(&query).unwrap();
        assert_eq!(result, "products");
    }

    #[test]
    fn test_extract_where_clause_simple() {
        let where_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("active".to_string())),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        };

        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Table("users".to_string()),
            where_clause: Some(where_expr),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            joins: None,
            window: None,
            emit_mode: None,
            properties: None,
        };

        let result = extract_where_clause(&query).unwrap();
        assert_eq!(result, "active = true");
    }

    #[test]
    fn test_extract_where_clause_none() {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Wildcard],
            from: StreamSource::Table("users".to_string()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            joins: None,
            window: None,
            emit_mode: None,
            properties: None,
        };

        let result = extract_where_clause(&query).unwrap();
        assert_eq!(result, "true");
    }

    #[test]
    fn test_extract_select_column() {
        let select_field = SelectField::Expression {
            expr: Expr::Column("user_id".to_string()),
            alias: None,
        };

        let query = StreamingQuery::Select {
            fields: vec![select_field],
            from: StreamSource::Table("users".to_string()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            joins: None,
            window: None,
            emit_mode: None,
            properties: None,
        };

        let result = extract_select_column(&query).unwrap();
        assert_eq!(result, "user_id");
    }

    #[test]
    fn test_literal_to_string() {
        assert_eq!(
            literal_to_string(&LiteralValue::String("test".to_string())).unwrap(),
            "'test'"
        );
        assert_eq!(literal_to_string(&LiteralValue::Integer(42)).unwrap(), "42");
        assert_eq!(
            literal_to_string(&LiteralValue::Float(3.14)).unwrap(),
            "3.14"
        );
        assert_eq!(
            literal_to_string(&LiteralValue::Boolean(true)).unwrap(),
            "true"
        );
        assert_eq!(literal_to_string(&LiteralValue::Null).unwrap(), "NULL");
    }

    #[test]
    fn test_string_with_quotes() {
        let result = literal_to_string(&LiteralValue::String("test's value".to_string())).unwrap();
        assert_eq!(result, "'test''s value'");
    }
}
