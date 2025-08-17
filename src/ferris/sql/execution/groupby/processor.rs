//! GROUP BY processor implementation for streaming SQL aggregation operations.

use std::collections::HashMap;

use crate::ferris::sql::ast::{Expr, SelectField};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expressions::ExpressionEvaluator;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};

use super::state::{GroupAccumulator, GroupByState};

/// Processor for GROUP BY operations
pub struct GroupByProcessor;

impl GroupByProcessor {
    /// Process a record through the GROUP BY operation
    pub fn process_record(state: &mut GroupByState, record: &StreamRecord) -> Result<(), SqlError> {
        // Compute group key from group expressions
        let group_key = Self::compute_group_key(&state.group_expressions, record)?;

        // Clone select fields to avoid borrowing issues
        let select_fields = state.select_fields.clone();

        // Get or create accumulator for this group
        let accumulator = state.get_or_create_accumulator(group_key);

        // Update the accumulator with this record
        Self::update_group_accumulator(accumulator, record, &select_fields)?;

        Ok(())
    }

    /// Compute group key from group expressions
    pub fn compute_group_key(
        group_expressions: &[Expr],
        record: &StreamRecord,
    ) -> Result<Vec<String>, SqlError> {
        let mut group_key = Vec::new();

        for expr in group_expressions {
            let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
            let key_part = Self::field_value_to_group_key(&value);
            group_key.push(key_part);
        }

        Ok(group_key)
    }

    /// Update a group accumulator with a new record
    pub fn update_group_accumulator(
        accumulator: &mut GroupAccumulator,
        record: &StreamRecord,
        select_fields: &[SelectField],
    ) -> Result<(), SqlError> {
        // Increment count and set sample record
        accumulator.increment_count();
        accumulator.set_sample_record_if_first(record);

        // Process each SELECT field to update relevant accumulators
        for field in select_fields {
            match field {
                SelectField::Expression { expr, alias } => {
                    let expr_name = Self::get_expression_name(expr);
                    let field_name = alias.as_ref().unwrap_or(&expr_name);
                    Self::update_accumulator_for_expression(accumulator, field_name, expr, record)?;
                }
                SelectField::Column(name) => {
                    if let Some(field_value) = record.fields.get(name) {
                        // Update FIRST/LAST for all columns
                        accumulator.set_first_value(name, field_value.clone());
                        accumulator.update_last_value(name, field_value.clone());
                    }
                }
                SelectField::AliasedColumn { column, alias } => {
                    if let Some(field_value) = record.fields.get(column) {
                        accumulator.set_first_value(alias, field_value.clone());
                        accumulator.update_last_value(alias, field_value.clone());
                    }
                }
                SelectField::Wildcard => {
                    // Handle wildcard by processing all fields
                    for (field_name, field_value) in &record.fields {
                        accumulator.set_first_value(field_name, field_value.clone());
                        accumulator.update_last_value(field_name, field_value.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// Update accumulator for a specific expression
    pub fn update_accumulator_for_expression(
        accumulator: &mut GroupAccumulator,
        field_name: &str,
        expr: &Expr,
        record: &StreamRecord,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        // COUNT(*) counts all records, COUNT(column) counts non-NULL values
                        if args.is_empty() {
                            // COUNT(*) - already handled by increment_count()
                        } else if let Some(arg) = args.first() {
                            // COUNT(column) - count non-NULL values
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                accumulator.update_non_null_count(field_name);
                            }
                        }
                    }
                    "SUM" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match value {
                                FieldValue::Float(f) => accumulator.update_sum(field_name, f),
                                FieldValue::Integer(i) => {
                                    accumulator.update_sum(field_name, i as f64)
                                }
                                FieldValue::Null => {} // Skip NULL values in SUM
                                _ => {}
                            }
                        }
                    }
                    "MIN" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                accumulator.update_min(field_name, value);
                            }
                        }
                    }
                    "MAX" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                accumulator.update_max(field_name, value);
                            }
                        }
                    }
                    "AVG" | "STDDEV" | "VARIANCE" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            match value {
                                FieldValue::Float(f) => {
                                    accumulator.add_numeric_value(field_name, f)
                                }
                                FieldValue::Integer(i) => {
                                    accumulator.add_numeric_value(field_name, i as f64)
                                }
                                FieldValue::Null => {} // Skip NULL values
                                _ => {}
                            }
                        }
                    }
                    "FIRST" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            accumulator.set_first_value(field_name, value);
                        }
                    }
                    "LAST" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            accumulator.update_last_value(field_name, value);
                        }
                    }
                    "STRING_AGG" | "GROUP_CONCAT" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if let FieldValue::String(s) = value {
                                accumulator.add_string_value(field_name, s);
                            }
                        }
                    }
                    "COUNT_DISTINCT" => {
                        if let Some(arg) = args.first() {
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(arg, record)?;
                            if !matches!(value, FieldValue::Null) {
                                let key = Self::field_value_to_group_key(&value);
                                accumulator.add_distinct_value(field_name, key);
                            }
                        }
                    }
                    _ => {
                        // For non-recognized aggregates, store as first/last
                        let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                        accumulator.set_first_value(field_name, value.clone());
                        accumulator.update_last_value(field_name, value);
                    }
                }
            }
            _ => {
                // Non-function expression - store first/last values
                let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                accumulator.set_first_value(field_name, value.clone());
                accumulator.update_last_value(field_name, value);
            }
        }

        Ok(())
    }

    /// Compute result fields for a group
    pub fn compute_group_result_fields(
        accumulator: &GroupAccumulator,
        select_fields: &[SelectField],
        group_exprs: &[Expr],
        sample_record: Option<&StreamRecord>,
    ) -> Result<HashMap<String, FieldValue>, SqlError> {
        let mut result_fields = HashMap::new();

        // Process each SELECT field
        for field in select_fields {
            match field {
                SelectField::Expression { expr, alias } => {
                    let expr_name = Self::get_expression_name(expr);
                    let field_name = alias.as_ref().unwrap_or(&expr_name);
                    let value = Self::compute_field_aggregate_value(field_name, expr, accumulator)?;
                    result_fields.insert(field_name.clone(), value);
                }
                SelectField::Column(name) => {
                    // For regular columns in GROUP BY, use the sample record value
                    if let Some(sample) = sample_record {
                        if let Some(value) = sample.fields.get(name) {
                            result_fields.insert(name.clone(), value.clone());
                        }
                    }
                }
                SelectField::AliasedColumn { column, alias } => {
                    if let Some(sample) = sample_record {
                        if let Some(value) = sample.fields.get(column) {
                            result_fields.insert(alias.clone(), value.clone());
                        }
                    }
                }
                SelectField::Wildcard => {
                    // Add all fields from sample record
                    if let Some(sample) = sample_record {
                        for (field_name, field_value) in &sample.fields {
                            result_fields.insert(field_name.clone(), field_value.clone());
                        }
                    }
                }
            }
        }

        // Add GROUP BY expressions to result if not already present
        for (_i, expr) in group_exprs.iter().enumerate() {
            let expr_name = Self::get_expression_name(expr);
            if !result_fields.contains_key(&expr_name) {
                if let Some(sample) = sample_record {
                    if let Ok(value) = ExpressionEvaluator::evaluate_expression_value(expr, sample)
                    {
                        result_fields.insert(expr_name, value);
                    }
                }
            }
        }

        Ok(result_fields)
    }

    /// Compute aggregate value for a field
    pub fn compute_field_aggregate_value(
        field_name: &str,
        expr: &Expr,
        accumulator: &GroupAccumulator,
    ) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        // Return count of non-NULL values if field-specific, otherwise total count
                        if let Some(count) = accumulator.non_null_counts.get(field_name) {
                            Ok(FieldValue::Integer(*count as i64))
                        } else {
                            Ok(FieldValue::Integer(accumulator.count as i64))
                        }
                    }
                    "SUM" => Ok(FieldValue::Float(
                        accumulator.sums.get(field_name).copied().unwrap_or(0.0),
                    )),
                    "MIN" => Ok(accumulator
                        .mins
                        .get(field_name)
                        .cloned()
                        .unwrap_or(FieldValue::Null)),
                    "MAX" => Ok(accumulator
                        .maxs
                        .get(field_name)
                        .cloned()
                        .unwrap_or(FieldValue::Null)),
                    "AVG" => {
                        if let Some(values) = accumulator.numeric_values.get(field_name) {
                            if values.is_empty() {
                                Ok(FieldValue::Null)
                            } else {
                                let sum: f64 = values.iter().sum();
                                Ok(FieldValue::Float(sum / values.len() as f64))
                            }
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                    "STDDEV" => {
                        if let Some(values) = accumulator.numeric_values.get(field_name) {
                            if values.len() < 2 {
                                Ok(FieldValue::Null)
                            } else {
                                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                                let variance: f64 =
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / (values.len() - 1) as f64;
                                Ok(FieldValue::Float(variance.sqrt()))
                            }
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                    "VARIANCE" => {
                        if let Some(values) = accumulator.numeric_values.get(field_name) {
                            if values.len() < 2 {
                                Ok(FieldValue::Null)
                            } else {
                                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                                let variance: f64 =
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / (values.len() - 1) as f64;
                                Ok(FieldValue::Float(variance))
                            }
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                    "FIRST" => Ok(accumulator
                        .first_values
                        .get(field_name)
                        .cloned()
                        .unwrap_or(FieldValue::Null)),
                    "LAST" => Ok(accumulator
                        .last_values
                        .get(field_name)
                        .cloned()
                        .unwrap_or(FieldValue::Null)),
                    "STRING_AGG" | "GROUP_CONCAT" => {
                        if let Some(strings) = accumulator.string_values.get(field_name) {
                            // Extract separator from second argument, default to comma
                            let separator = if args.len() > 1 {
                                // Try to extract separator from literal string argument
                                match &args[1] {
                                    Expr::Literal(
                                        crate::ferris::sql::ast::LiteralValue::String(s),
                                    ) => s.as_str(),
                                    _ => ",", // fallback to comma if not a string literal
                                }
                            } else {
                                "," // default separator
                            };
                            Ok(FieldValue::String(strings.join(separator)))
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                    "COUNT_DISTINCT" => {
                        if let Some(distinct_set) = accumulator.distinct_values.get(field_name) {
                            Ok(FieldValue::Integer(distinct_set.len() as i64))
                        } else {
                            Ok(FieldValue::Integer(0))
                        }
                    }
                    _ => {
                        // For unknown functions, try to get first/last values
                        Ok(accumulator
                            .first_values
                            .get(field_name)
                            .cloned()
                            .unwrap_or(FieldValue::Null))
                    }
                }
            }
            _ => {
                // Non-function expressions - return first value
                Ok(accumulator
                    .first_values
                    .get(field_name)
                    .cloned()
                    .unwrap_or(FieldValue::Null))
            }
        }
    }

    /// Convert a FieldValue to a group key string
    pub fn field_value_to_group_key(value: &FieldValue) -> String {
        match value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Date(d) => d.to_string(),
            FieldValue::Timestamp(t) => t.to_string(),
            FieldValue::Decimal(d) => d.to_string(),
            FieldValue::Array(_) => "ARRAY".to_string(),
            FieldValue::Map(_) => "MAP".to_string(),
            FieldValue::Struct(_) => "STRUCT".to_string(),
        }
    }

    /// Get a readable name for an expression
    pub fn get_expression_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}()", name)
                } else {
                    format!("{}(...)", name)
                }
            }
            Expr::Literal(_) => "literal".to_string(),
            Expr::BinaryOp { .. } => "binary_op".to_string(),
            Expr::UnaryOp { .. } => "unary_op".to_string(),
            Expr::Case { .. } => "case".to_string(),
            _ => "expression".to_string(),
        }
    }
}
