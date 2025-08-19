//! SELECT Query Processor
//!
//! Handles SELECT statement processing including field selection, WHERE clause evaluation,
//! HAVING clause processing, and header mutations.

use super::{
    HeaderMutation, HeaderOperation, JoinProcessor, LimitProcessor, ProcessorContext,
    ProcessorResult,
};
use crate::ferris::sql::ast::{Expr, LiteralValue, SelectField};
use crate::ferris::sql::execution::{
    FieldValue, StreamRecord, aggregation::AggregationEngine, expression::ExpressionEvaluator,
};
use crate::ferris::sql::{SqlError, StreamingQuery};
use std::collections::HashMap;

/// SELECT query processor
pub struct SelectProcessor;

impl SelectProcessor {
    /// Process a SELECT query
    pub fn process(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        if let StreamingQuery::Select {
            fields,
            where_clause,
            joins,
            having,
            limit,
            group_by,
            ..
        } = query
        {
            // Check limit first
            if let Some(limit_value) = limit {
                if let Some(result) = LimitProcessor::check_limit(*limit_value, context)? {
                    return Ok(result);
                }
            }

            // Handle JOINs first (if any)
            let mut joined_record = record.clone();
            if let Some(join_clauses) = joins {
                joined_record =
                    JoinProcessor::process_joins(&joined_record, join_clauses, context)?;
            }

            // Apply WHERE clause
            if let Some(where_expr) = where_clause {
                if !ExpressionEvaluator::evaluate_expression(where_expr, &joined_record)? {
                    return Ok(ProcessorResult {
                        record: None,
                        header_mutations: Vec::new(),
                        should_count: false,
                    });
                }
            }

            // Handle GROUP BY if present
            if let Some(group_exprs) = group_by {
                return Self::handle_group_by_record(
                    query,
                    &joined_record,
                    group_exprs,
                    fields,
                    having,
                    context,
                );
            }

            // Apply SELECT fields
            let mut result_fields = HashMap::new();
            let mut header_mutations = Vec::new();

            for field in fields {
                match field {
                    SelectField::Wildcard => {
                        result_fields.extend(joined_record.fields.clone());
                    }
                    SelectField::Column(name) => {
                        // Check for system columns first (case insensitive)
                        let field_value = match name.to_uppercase().as_str() {
                            "_TIMESTAMP" => Some(FieldValue::Integer(joined_record.timestamp)),
                            "_OFFSET" => Some(FieldValue::Integer(joined_record.offset)),
                            "_PARTITION" => {
                                Some(FieldValue::Integer(joined_record.partition as i64))
                            }
                            _ => joined_record.fields.get(name).cloned(),
                        };

                        if let Some(value) = field_value {
                            result_fields.insert(name.clone(), value);
                        }
                    }
                    SelectField::AliasedColumn { column, alias } => {
                        // Check for system columns first (case insensitive)
                        let field_value = match column.to_uppercase().as_str() {
                            "_TIMESTAMP" => Some(FieldValue::Integer(joined_record.timestamp)),
                            "_OFFSET" => Some(FieldValue::Integer(joined_record.offset)),
                            "_PARTITION" => {
                                Some(FieldValue::Integer(joined_record.partition as i64))
                            }
                            _ => joined_record.fields.get(column).cloned(),
                        };

                        if let Some(value) = field_value {
                            result_fields.insert(alias.clone(), value);
                        }
                    }
                    SelectField::Expression { expr, alias } => {
                        let value = Self::evaluate_expression_value_with_window(
                            expr,
                            &joined_record,
                            &mut Vec::new(),
                        )?;
                        let field_name = alias
                            .as_ref()
                            .unwrap_or(&Self::get_expression_name(expr))
                            .clone();
                        result_fields.insert(field_name, value);
                    }
                }
            }

            // Apply HAVING clause on the result fields
            if let Some(having_expr) = having {
                // Create a temporary record with the result fields to evaluate HAVING
                let result_record = StreamRecord {
                    fields: result_fields.clone(),
                    timestamp: joined_record.timestamp,
                    offset: joined_record.offset,
                    partition: joined_record.partition,
                    headers: joined_record.headers.clone(),
                };

                if !ExpressionEvaluator::evaluate_expression(having_expr, &result_record)? {
                    return Ok(ProcessorResult {
                        record: None,
                        header_mutations: Vec::new(),
                        should_count: false,
                    });
                }
            }

            // Collect header mutations from fields
            Self::collect_header_mutations_from_fields(
                fields,
                &joined_record,
                &mut header_mutations,
            )?;

            let final_record = StreamRecord {
                fields: result_fields,
                timestamp: joined_record.timestamp,
                offset: joined_record.offset,
                partition: joined_record.partition,
                headers: joined_record.headers,
            };

            Ok(ProcessorResult {
                record: Some(final_record),
                header_mutations,
                should_count: true,
            })
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for SelectProcessor".to_string(),
                query: None,
            })
        }
    }

    /// Handle GROUP BY processing
    fn handle_group_by_record(
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _group_exprs: &[Expr],
        _fields: &[SelectField],
        _having: &Option<Expr>,
        _context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        // Use AggregationEngine for GROUP BY processing
        let _aggregation_engine = AggregationEngine::new();

        // For now, delegate to the aggregation engine
        // This will be implemented as part of the aggregation integration
        Err(SqlError::ExecutionError {
            message: "GROUP BY processing not yet implemented in SelectProcessor".to_string(),
            query: None,
        })
    }

    /// Evaluate expression with window support (placeholder)
    fn evaluate_expression_value_with_window(
        expr: &Expr,
        record: &StreamRecord,
        _window_buffer: &mut Vec<StreamRecord>,
    ) -> Result<FieldValue, SqlError> {
        // For now, delegate to the regular expression evaluator
        // Window support will be added when WindowProcessor is integrated
        ExpressionEvaluator::evaluate_expression_value(expr, record)
    }

    /// Get expression name for result field
    fn get_expression_name(expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Function { name, .. } => name.clone(),
            Expr::Literal(lit) => format!("{:?}", lit),
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "{}_{:?}_{}",
                    Self::get_expression_name(left),
                    op,
                    Self::get_expression_name(right)
                )
            }
            Expr::UnaryOp { op, expr } => {
                format!("{:?}_{}", op, Self::get_expression_name(expr))
            }
            Expr::Case { .. } => "case_expr".to_string(),
            Expr::List(_) => "list_expr".to_string(),
            Expr::Subquery { .. } => "subquery".to_string(),
            Expr::WindowFunction { function_name, .. } => format!("window_{}", function_name),
        }
    }

    /// Collect header mutations from SELECT fields
    fn collect_header_mutations_from_fields(
        fields: &[SelectField],
        record: &StreamRecord,
        mutations: &mut Vec<HeaderMutation>,
    ) -> Result<(), SqlError> {
        for field in fields {
            if let SelectField::Expression { expr, .. } = field {
                Self::collect_header_mutations_from_expr(expr, record, mutations)?;
            }
        }
        Ok(())
    }

    /// Recursively collect header mutations from expressions
    fn collect_header_mutations_from_expr(
        expr: &Expr,
        record: &StreamRecord,
        mutations: &mut Vec<HeaderMutation>,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "SET_HEADER" => {
                        if args.len() == 2 {
                            if let (
                                Expr::Literal(LiteralValue::String(key)),
                                Expr::Literal(LiteralValue::String(value)),
                            ) = (&args[0], &args[1])
                            {
                                mutations.push(HeaderMutation {
                                    key: key.clone(),
                                    operation: HeaderOperation::Set,
                                    value: Some(value.clone()),
                                });
                            }
                        }
                    }
                    "REMOVE_HEADER" => {
                        if args.len() == 1 {
                            if let Expr::Literal(LiteralValue::String(key)) = &args[0] {
                                mutations.push(HeaderMutation {
                                    key: key.clone(),
                                    operation: HeaderOperation::Remove,
                                    value: None,
                                });
                            }
                        }
                    }
                    _ => {
                        // Recursively check function arguments
                        for arg in args {
                            Self::collect_header_mutations_from_expr(arg, record, mutations)?;
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_header_mutations_from_expr(left, record, mutations)?;
                Self::collect_header_mutations_from_expr(right, record, mutations)?;
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_header_mutations_from_expr(expr, record, mutations)?;
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (condition, result) in when_clauses {
                    Self::collect_header_mutations_from_expr(condition, record, mutations)?;
                    Self::collect_header_mutations_from_expr(result, record, mutations)?;
                }
                if let Some(else_expr) = else_clause {
                    Self::collect_header_mutations_from_expr(else_expr, record, mutations)?;
                }
            }
            Expr::List(list) => {
                for item in list {
                    Self::collect_header_mutations_from_expr(item, record, mutations)?;
                }
            }
            Expr::WindowFunction { args, .. } => {
                for arg in args {
                    Self::collect_header_mutations_from_expr(arg, record, mutations)?;
                }
            }
            // Terminal expressions don't need recursive processing
            Expr::Column(_) | Expr::Literal(_) | Expr::Subquery { .. } => {}
        }
        Ok(())
    }
}
