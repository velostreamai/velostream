//! Semantic Validator
//!
//! Validates semantic aspects of SQL queries that go beyond syntax:
//! - Function existence
//! - Window function usage in OVER clauses
//! - Expression type compatibility
//!
//! This catches errors that would otherwise only be found at runtime.

use super::function_registry::FUNCTION_REGISTRY;
use super::result_types::QueryValidationResult;
use crate::velostream::sql::ast::{Expr, SelectField, StreamingQuery};

/// Handles semantic validation of parsed SQL queries
pub struct SemanticValidator {
    /// Whether to fail on unsupported functions or just warn
    strict_functions: bool,
}

impl SemanticValidator {
    /// Create a new semantic validator
    pub fn new() -> Self {
        Self {
            strict_functions: true,
        }
    }

    /// Create a lenient semantic validator (warnings instead of errors)
    pub fn new_lenient() -> Self {
        Self {
            strict_functions: false,
        }
    }

    /// Validate semantic aspects of a query
    pub fn validate(&self, query: &StreamingQuery, result: &mut QueryValidationResult) {
        // Validate all expressions in the query
        self.validate_query_expressions(query, result);
    }

    // Private validation methods

    fn validate_query_expressions(
        &self,
        query: &StreamingQuery,
        result: &mut QueryValidationResult,
    ) {
        match query {
            StreamingQuery::Select { fields, .. } => {
                for field in fields {
                    self.validate_select_field(field, result);
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                self.validate_query_expressions(as_select, result);
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                self.validate_query_expressions(as_select, result);
            }
            _ => {
                // Other query types don't have expressions to validate
            }
        }
    }

    fn validate_select_field(&self, field: &SelectField, result: &mut QueryValidationResult) {
        match field {
            SelectField::Expression { expr, .. } => {
                self.validate_expression(expr, false, result);
            }
            SelectField::Column(_) | SelectField::AliasedColumn { .. } | SelectField::Wildcard => {
                // These are always valid
            }
        }
    }

    fn validate_expression(
        &self,
        expr: &Expr,
        in_over_clause: bool,
        result: &mut QueryValidationResult,
    ) {
        match expr {
            Expr::Function { name, args } => {
                // Validate the function name
                self.validate_function_name(name, in_over_clause, result);

                // Validate arguments recursively
                for arg in args {
                    self.validate_expression(arg, in_over_clause, result);
                }
            }
            Expr::WindowFunction {
                function_name,
                args,
                over_clause,
            } => {
                // Validate window function
                self.validate_function_name(function_name, true, result);
                self.validate_window_function_usage(function_name, result);

                // Validate arguments
                for arg in args {
                    self.validate_expression(arg, true, result);
                }

                // Validate ORDER BY expressions in OVER clause
                for order_expr in &over_clause.order_by {
                    self.validate_expression(&order_expr.expr, true, result);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expression(left, in_over_clause, result);
                self.validate_expression(right, in_over_clause, result);
            }
            Expr::UnaryOp { expr, .. } => {
                self.validate_expression(expr, in_over_clause, result);
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                // Validate all condition expressions
                for (cond, then_expr) in when_clauses {
                    self.validate_expression(cond, in_over_clause, result);
                    self.validate_expression(then_expr, in_over_clause, result);
                }
                if let Some(else_val) = else_clause {
                    self.validate_expression(else_val, in_over_clause, result);
                }
            }
            Expr::List(items) => {
                for item in items {
                    self.validate_expression(item, in_over_clause, result);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expression(expr, in_over_clause, result);
                self.validate_expression(low, in_over_clause, result);
                self.validate_expression(high, in_over_clause, result);
            }
            Expr::Subquery { query, .. } => {
                // Recursively validate the subquery
                self.validate_query_expressions(query, result);
            }
            // These expression types don't need validation
            Expr::Column(_) | Expr::Literal(_) => {
                // Always valid
            }
        }
    }

    fn validate_function_name(
        &self,
        name: &str,
        in_over_clause: bool,
        result: &mut QueryValidationResult,
    ) {
        let name_upper = name.to_uppercase();

        // Check if function exists
        if !FUNCTION_REGISTRY.is_function_supported(&name_upper) {
            let similar = FUNCTION_REGISTRY.find_similar_functions(&name_upper, 3);
            let suggestion = if similar.is_empty() {
                String::new()
            } else {
                format!("\n\nDid you mean one of these?\n{}", similar.join(", "))
            };

            let error_msg = format!("Unknown function: '{}'{}", name, suggestion);

            if self.strict_functions {
                result.add_semantic_error(error_msg);
            } else {
                result.warnings.push(error_msg);
            }
        } else if in_over_clause {
            // Function exists, but is it valid in an OVER clause?
            self.validate_window_function_usage(name, result);
        }
    }

    fn validate_window_function_usage(&self, name: &str, result: &mut QueryValidationResult) {
        let name_upper = name.to_uppercase();

        // Check if this function can be used with OVER
        if !FUNCTION_REGISTRY.is_window_function(&name_upper) {
            // Check if it's an aggregate function
            let is_aggregate = matches!(
                name_upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "APPROX_COUNT_DISTINCT"
            );

            if is_aggregate {
                result.add_semantic_error(format!(
                    "Unsupported window function: '{}'. Supported window functions are: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, NTILE",
                    name
                ));
            } else if FUNCTION_REGISTRY.is_function_supported(&name_upper) {
                result.add_semantic_error(format!(
                    "Function '{}' cannot be used in OVER clauses",
                    name
                ));
            }
        }
    }
}

impl Default for SemanticValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::parser::StreamingSqlParser;

    fn parse_query(sql: &str) -> StreamingQuery {
        let parser = StreamingSqlParser::new();
        parser.parse(sql).unwrap()
    }

    #[test]
    fn test_valid_function() {
        let validator = SemanticValidator::new();
        let query = parse_query("SELECT COUNT(*) FROM orders");
        let mut result = QueryValidationResult::new(String::new());

        validator.validate(&query, &mut result);

        assert!(result.semantic_errors.is_empty());
    }

    #[test]
    fn test_unknown_function() {
        let validator = SemanticValidator::new();
        let query = parse_query("SELECT TUMBLE_START(event_time, INTERVAL '1' SECOND) FROM orders");
        let mut result = QueryValidationResult::new(String::new());

        validator.validate(&query, &mut result);

        assert!(!result.semantic_errors.is_empty());
        assert!(result.semantic_errors[0].contains("Unknown function"));
        assert!(result.semantic_errors[0].contains("TUMBLE_START"));
    }

    #[test]
    fn test_valid_window_function() {
        let validator = SemanticValidator::new();
        let query = parse_query(
            "SELECT LAG(price) OVER (PARTITION BY symbol ORDER BY event_time) FROM market_data",
        );
        let mut result = QueryValidationResult::new(String::new());

        validator.validate(&query, &mut result);

        assert!(result.semantic_errors.is_empty());
    }
}
