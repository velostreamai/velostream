//! Accumulator utilities for streaming SQL aggregations.
//!
//! This module provides utilities for managing GroupAccumulator instances,
//! including record processing and aggregate value computation.

use super::super::internal::GroupAccumulator;
use super::super::types::{FieldValue, StreamRecord};
use crate::ferris::sql::ast::Expr;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expression::ExpressionEvaluator;

/// Utilities for GroupAccumulator management
pub struct AccumulatorManager;

impl AccumulatorManager {
    /// Process a record into an accumulator based on aggregate expressions
    pub fn process_record_into_accumulator(
        accumulator: &mut GroupAccumulator,
        record: &StreamRecord,
        aggregate_expressions: &[(String, Expr)], // (field_name, expression) pairs
    ) -> Result<(), SqlError> {
        // Always increment the count for this group
        accumulator.increment_count();

        // Store the first record as a sample for non-aggregate fields
        if accumulator.sample_record.is_none() {
            accumulator.sample_record = Some(record.clone());
        }

        // Process each aggregate expression
        for (field_name, expr) in aggregate_expressions {
            Self::process_aggregate_expression(accumulator, record, field_name, expr)?;
        }

        Ok(())
    }

    /// Process a single aggregate expression for a record
    fn process_aggregate_expression(
        accumulator: &mut GroupAccumulator,
        record: &StreamRecord,
        field_name: &str,
        expr: &Expr,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        // For COUNT(column), only count non-NULL values
                        if !args.is_empty() {
                            if let Some(arg) = args.first() {
                                let value =
                                    ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                                if !matches!(value, FieldValue::Null) {
                                    accumulator.add_non_null_count(field_name);
                                }
                            }
                        }
                        // For COUNT(*), we rely on the global count which is incremented for every record
                    }
                    "SUM" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match value {
                                FieldValue::Integer(i) => accumulator.add_sum(field_name, i as f64),
                                FieldValue::Float(f) => accumulator.add_sum(field_name, f),
                                FieldValue::Null => {
                                    // NULL values are ignored in SUM
                                }
                                _ => {
                                    return Err(SqlError::ExecutionError {
                                        message: format!(
                                            "Cannot sum non-numeric value: {:?}",
                                            value
                                        ),
                                        query: None,
                                    });
                                }
                            }
                        }
                    }
                    "MIN" | "MAX" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                if name.to_uppercase() == "MIN" {
                                    accumulator.update_min(field_name, value);
                                } else {
                                    accumulator.update_max(field_name, value);
                                }
                            }
                        }
                    }
                    "AVG" | "STDDEV" | "VARIANCE" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match value {
                                FieldValue::Integer(i) => {
                                    accumulator
                                        .numeric_values
                                        .entry(field_name.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(i as f64);
                                }
                                FieldValue::Float(f) => {
                                    accumulator
                                        .numeric_values
                                        .entry(field_name.to_string())
                                        .or_insert_with(Vec::new)
                                        .push(f);
                                }
                                FieldValue::Null => {
                                    // NULL values are ignored in AVG/STDDEV/VARIANCE
                                }
                                _ => {
                                    return Err(SqlError::ExecutionError {
                                        message: format!(
                                            "Cannot compute {} on non-numeric value: {:?}",
                                            name.to_uppercase(),
                                            value
                                        ),
                                        query: None,
                                    });
                                }
                            }
                        }
                    }
                    "COUNT_DISTINCT" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                let string_value = Self::field_value_to_string(&value);
                                accumulator
                                    .distinct_values
                                    .entry(field_name.to_string())
                                    .or_insert_with(std::collections::HashSet::new)
                                    .insert(string_value);
                            }
                        }
                    }
                    "FIRST" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            // Only set if not already set (first value wins)
                            if !accumulator.first_values.contains_key(field_name) {
                                accumulator
                                    .first_values
                                    .insert(field_name.to_string(), value);
                            }
                        }
                    }
                    "LAST" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            // Always update (last value wins)
                            accumulator
                                .last_values
                                .insert(field_name.to_string(), value);
                        }
                    }
                    "STRING_AGG" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                let string_value = Self::field_value_to_string(&value);
                                accumulator
                                    .string_values
                                    .entry(field_name.to_string())
                                    .or_insert_with(Vec::new)
                                    .push(string_value);
                            }
                        }
                    }
                    _ => {
                        // For non-recognized aggregates, store as first/last
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !accumulator.first_values.contains_key(field_name) {
                                accumulator
                                    .first_values
                                    .insert(field_name.to_string(), value.clone());
                            }
                            accumulator
                                .last_values
                                .insert(field_name.to_string(), value);
                        }
                    }
                }
            }
            _ => {
                // For non-function expressions, evaluate and store as first/last
                let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                if !accumulator.first_values.contains_key(field_name) {
                    accumulator
                        .first_values
                        .insert(field_name.to_string(), value.clone());
                }
                accumulator
                    .last_values
                    .insert(field_name.to_string(), value);
            }
        }

        Ok(())
    }

    /// Convert a FieldValue to string for distinct counting and string aggregation
    fn field_value_to_string(value: &FieldValue) -> String {
        match value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Timestamp(ts) => ts.to_string(),
            FieldValue::Array(arr) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(Self::field_value_to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            FieldValue::Map(obj) | FieldValue::Struct(obj) => {
                let mut pairs: Vec<_> = obj
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, Self::field_value_to_string(v)))
                    .collect();
                pairs.sort(); // Ensure consistent ordering
                format!("{{{}}}", pairs.join(","))
            }
            FieldValue::Date(date) => date.to_string(),
            FieldValue::Decimal(decimal) => decimal.to_string(),
            FieldValue::Interval { value, unit } => format!("{}_{:?}", value, unit),
        }
    }

    /// Extract all aggregate expressions from a list of select fields
    pub fn extract_aggregate_expressions(
        select_fields: &[crate::ferris::sql::ast::SelectField],
    ) -> Vec<(String, Expr)> {
        let mut aggregate_expressions = Vec::new();

        for field in select_fields {
            match field {
                crate::ferris::sql::ast::SelectField::Expression { expr, alias } => {
                    if Self::is_aggregate_expression(expr) {
                        let field_name = alias
                            .clone()
                            .unwrap_or_else(|| Self::generate_field_name(expr));
                        aggregate_expressions.push((field_name, expr.clone()));
                    }
                }
                crate::ferris::sql::ast::SelectField::Column(_) => {
                    // Simple column reference is not an aggregate
                }
                crate::ferris::sql::ast::SelectField::AliasedColumn { .. } => {
                    // Aliased column reference is not an aggregate
                }
                crate::ferris::sql::ast::SelectField::Wildcard => {
                    // Wildcard doesn't contain aggregates
                }
            }
        }

        aggregate_expressions
    }

    /// Check if an expression is an aggregate function
    fn is_aggregate_expression(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "COUNT_DISTINCT"
                        | "FIRST"
                        | "LAST"
                        | "STRING_AGG"
                )
            }
            _ => false,
        }
    }

    /// Generate a field name for an expression
    fn generate_field_name(expr: &Expr) -> String {
        match expr {
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}(*)", name)
                } else {
                    format!("{}({})", name, Self::expr_to_string(&args[0]))
                }
            }
            _ => Self::expr_to_string(expr),
        }
    }

    /// Convert an expression to a string representation
    fn expr_to_string(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Literal(val) => format!("{:?}", val),
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}()", name)
                } else {
                    let arg_strings: Vec<_> = args.iter().map(Self::expr_to_string).collect();
                    format!("{}({})", name, arg_strings.join(", "))
                }
            }
            _ => "expr".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::ast::SelectField;
    use std::collections::HashMap;

    fn create_test_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
        let mut field_map = HashMap::new();
        for (key, value) in fields {
            field_map.insert(key.to_string(), value);
        }

        StreamRecord {
            fields: field_map,
            timestamp: 0,
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn test_field_value_to_string() {
        assert_eq!(
            AccumulatorManager::field_value_to_string(&FieldValue::String("test".to_string())),
            "test"
        );
        assert_eq!(
            AccumulatorManager::field_value_to_string(&FieldValue::Integer(42)),
            "42"
        );
        assert_eq!(
            AccumulatorManager::field_value_to_string(&FieldValue::Null),
            "NULL"
        );
    }

    #[test]
    fn test_is_aggregate_expression() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![],
        };
        assert!(AccumulatorManager::is_aggregate_expression(&count_expr));

        let sum_expr = Expr::Function {
            name: "SUM".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        assert!(AccumulatorManager::is_aggregate_expression(&sum_expr));

        let non_agg_expr = Expr::Function {
            name: "UPPER".to_string(),
            args: vec![Expr::Column("name".to_string())],
        };
        assert!(!AccumulatorManager::is_aggregate_expression(&non_agg_expr));

        let identifier_expr = Expr::Column("name".to_string());
        assert!(!AccumulatorManager::is_aggregate_expression(
            &identifier_expr
        ));
    }

    #[test]
    fn test_generate_field_name() {
        let count_expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![],
        };
        assert_eq!(
            AccumulatorManager::generate_field_name(&count_expr),
            "COUNT(*)"
        );

        let sum_expr = Expr::Function {
            name: "SUM".to_string(),
            args: vec![Expr::Column("amount".to_string())],
        };
        assert_eq!(
            AccumulatorManager::generate_field_name(&sum_expr),
            "SUM(amount)"
        );
    }

    #[test]
    fn test_extract_aggregate_expressions() {
        let select_fields = vec![
            SelectField::Expression {
                expr: Expr::Column("category".to_string()),
                alias: None,
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                },
                alias: Some("total_count".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Function {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
                alias: None,
            },
        ];

        let aggregates = AccumulatorManager::extract_aggregate_expressions(&select_fields);
        assert_eq!(aggregates.len(), 2);
        assert_eq!(aggregates[0].0, "total_count");
        assert_eq!(aggregates[1].0, "SUM(amount)");
    }

    #[test]
    fn test_process_record_basic_count() {
        let mut accumulator = GroupAccumulator::new();
        let record = create_test_record(vec![("category", FieldValue::String("test".to_string()))]);

        let aggregate_expressions = vec![];

        let result = AccumulatorManager::process_record_into_accumulator(
            &mut accumulator,
            &record,
            &aggregate_expressions,
        );

        assert!(result.is_ok());
        assert_eq!(accumulator.count, 1);
        assert!(accumulator.sample_record.is_some());
    }
}
