//! Query planner for streaming SQL execution
//!
//! This module creates optimized execution plans from StreamingQuery AST nodes,
//! determining the order of operations and creating the necessary processor states.

use crate::ferris::sql::ast::{
    Expr, JoinClause, SelectField, StreamSource, StreamingQuery, WindowSpec,
};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::groupby::GroupByState;
use crate::ferris::sql::execution::windows::WindowState;

/// Execution plan node types
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Source node (stream/table)
    Source { name: String, alias: Option<String> },
    /// Projection (SELECT fields)
    Projection {
        fields: Vec<SelectField>,
        input: Box<PlanNode>,
    },
    /// Filter (WHERE clause)
    Filter {
        condition: Expr,
        input: Box<PlanNode>,
    },
    /// GROUP BY aggregation
    GroupBy {
        group_expressions: Vec<Expr>,
        select_fields: Vec<SelectField>,
        having: Option<Expr>,
        input: Box<PlanNode>,
    },
    /// Window operation
    Window {
        window_spec: WindowSpec,
        time_column: Option<String>,
        input: Box<PlanNode>,
    },
    /// JOIN operation
    Join {
        join_clauses: Vec<JoinClause>,
        input: Box<PlanNode>,
    },
    /// LIMIT operation
    Limit { count: usize, input: Box<PlanNode> },
}

/// Execution plan with metadata
#[derive(Debug)]
pub struct ExecutionPlan {
    pub root: PlanNode,
    pub is_windowed: bool,
    pub has_aggregation: bool,
    pub has_joins: bool,
    pub estimated_memory_usage: usize,
}

/// Query planner for creating execution plans
pub struct QueryPlanner;

impl QueryPlanner {
    /// Create an execution plan from a StreamingQuery AST
    pub fn create_plan(query: &StreamingQuery) -> Result<ExecutionPlan, SqlError> {
        match query {
            StreamingQuery::Select {
                fields,
                from,
                where_clause,
                group_by,
                having,
                window,
                joins,
                limit,
                ..
            } => {
                let mut plan = Self::create_source_node_from_source(from)?;
                let mut is_windowed = false;
                let mut has_aggregation = false;
                let mut has_joins = false;

                // Add JOIN operations first
                if let Some(join_clauses) = joins {
                    if !join_clauses.is_empty() {
                        plan = PlanNode::Join {
                            join_clauses: join_clauses.clone(),
                            input: Box::new(plan),
                        };
                        has_joins = true;
                    }
                }

                // Add WHERE filter
                if let Some(condition) = where_clause {
                    plan = PlanNode::Filter {
                        condition: condition.clone(),
                        input: Box::new(plan),
                    };
                }

                // Add window operation
                if let Some(window_spec) = window {
                    plan = PlanNode::Window {
                        window_spec: window_spec.clone(),
                        time_column: Self::extract_time_column(window_spec),
                        input: Box::new(plan),
                    };
                    is_windowed = true;
                }

                // Add GROUP BY aggregation
                if let Some(group_exprs) = group_by {
                    plan = PlanNode::GroupBy {
                        group_expressions: group_exprs.clone(),
                        select_fields: fields.clone(),
                        having: having.clone(),
                        input: Box::new(plan),
                    };
                    has_aggregation = true;
                }

                // Check if SELECT fields contain aggregations (even without GROUP BY)
                if !has_aggregation && Self::has_aggregate_functions(fields) {
                    has_aggregation = true;
                    // Only add GROUP BY node if there's no windowing, since windows handle their own aggregation
                    if !is_windowed {
                        plan = PlanNode::GroupBy {
                            group_expressions: vec![],
                            select_fields: fields.clone(),
                            having: having.clone(),
                            input: Box::new(plan),
                        };
                    }
                }

                // Add projection (SELECT)
                if !has_aggregation {
                    // Only add projection if we don't have GROUP BY (which handles SELECT)
                    plan = PlanNode::Projection {
                        fields: fields.clone(),
                        input: Box::new(plan),
                    };
                }

                // Add LIMIT
                if let Some(limit_count) = limit {
                    plan = PlanNode::Limit {
                        count: *limit_count as usize,
                        input: Box::new(plan),
                    };
                }

                // Estimate memory usage
                let estimated_memory_usage = Self::estimate_memory_usage(&plan);

                Ok(ExecutionPlan {
                    root: plan,
                    is_windowed,
                    has_aggregation,
                    has_joins,
                    estimated_memory_usage,
                })
            }
            StreamingQuery::CreateStream { .. } => Err(SqlError::ExecutionError {
                message: "CREATE STREAM not supported in query planner".to_string(),
                query: None,
            }),
            StreamingQuery::CreateTable { .. } => Err(SqlError::ExecutionError {
                message: "CREATE TABLE not supported in query planner".to_string(),
                query: None,
            }),
            _ => Err(SqlError::ExecutionError {
                message: "Query type not supported in query planner".to_string(),
                query: None,
            }),
        }
    }

    /// Create a source node from a StreamSource
    fn create_source_node_from_source(from: &StreamSource) -> Result<PlanNode, SqlError> {
        let name = match from {
            StreamSource::Stream(name) => name.clone(),
            StreamSource::Table(name) => name.clone(),
            StreamSource::Subquery(_) => {
                return Err(SqlError::ExecutionError {
                    message: "Subquery sources not yet supported".to_string(),
                    query: None,
                });
            }
        };

        Ok(PlanNode::Source { name, alias: None })
    }

    /// Extract time column from window specification
    fn extract_time_column(window_spec: &WindowSpec) -> Option<String> {
        match window_spec {
            WindowSpec::Tumbling { time_column, .. } => time_column.clone(),
            WindowSpec::Sliding { time_column, .. } => time_column.clone(),
            WindowSpec::Session { .. } => None,
        }
    }

    /// Check if SELECT fields contain aggregate functions
    fn has_aggregate_functions(fields: &[SelectField]) -> bool {
        fields.iter().any(|field| match field {
            SelectField::Expression { expr, .. } => Self::expr_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    fn expr_has_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, args } => {
                let func_name = name.to_uppercase();
                match func_name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" | "FIRST"
                    | "LAST" | "STRING_AGG" | "GROUP_CONCAT" | "COUNT_DISTINCT" => true,
                    _ => args.iter().any(Self::expr_has_aggregate),
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_aggregate(expr),
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                when_clauses.iter().any(|(cond, result)| {
                    Self::expr_has_aggregate(cond) || Self::expr_has_aggregate(result)
                }) || else_clause
                    .as_ref()
                    .map_or(false, |e| Self::expr_has_aggregate(e))
            }
            _ => false,
        }
    }

    /// Estimate memory usage for the execution plan
    fn estimate_memory_usage(plan: &PlanNode) -> usize {
        match plan {
            PlanNode::Source { .. } => 1024, // Base source overhead
            PlanNode::Projection { input, .. } => {
                1024 + Self::estimate_memory_usage(input) // Projection overhead
            }
            PlanNode::Filter { input, .. } => {
                512 + Self::estimate_memory_usage(input) // Filter overhead
            }
            PlanNode::GroupBy { input, .. } => {
                10 * 1024 + Self::estimate_memory_usage(input) // GROUP BY needs hash tables
            }
            PlanNode::Window { input, .. } => {
                50 * 1024 + Self::estimate_memory_usage(input) // Window buffering
            }
            PlanNode::Join { input, .. } => {
                20 * 1024 + Self::estimate_memory_usage(input) // JOIN state
            }
            PlanNode::Limit { input, .. } => {
                512 + Self::estimate_memory_usage(input) // Minimal overhead
            }
        }
    }

    /// Optimize the execution plan (future enhancement)
    pub fn optimize_plan(plan: ExecutionPlan) -> ExecutionPlan {
        // TODO: Implement query optimization rules
        // - Predicate pushdown
        // - Join reordering
        // - Index selection
        // - Projection elimination
        plan
    }

    /// Get execution states needed for this plan
    pub fn get_required_states(plan: &ExecutionPlan) -> Result<ExecutionStates, SqlError> {
        let mut states = ExecutionStates {
            group_by_state: None,
            window_state: None,
        };

        Self::collect_states(&plan.root, &mut states)?;
        Ok(states)
    }

    /// Recursively collect required execution states from plan nodes
    fn collect_states(node: &PlanNode, states: &mut ExecutionStates) -> Result<(), SqlError> {
        match node {
            PlanNode::GroupBy {
                group_expressions,
                select_fields,
                having,
                input,
            } => {
                states.group_by_state = Some(GroupByState::new(
                    group_expressions.clone(),
                    select_fields.clone(),
                    having.clone(),
                ));
                Self::collect_states(input, states)?;
            }
            PlanNode::Window {
                window_spec,
                time_column: _,
                input,
            } => {
                states.window_state = Some(WindowState::new(window_spec.clone()));
                Self::collect_states(input, states)?;
            }
            PlanNode::Source { .. } => {
                // Leaf node, no recursion needed
            }
            PlanNode::Projection { input, .. }
            | PlanNode::Filter { input, .. }
            | PlanNode::Join { input, .. }
            | PlanNode::Limit { input, .. } => {
                Self::collect_states(input, states)?;
            }
        }
        Ok(())
    }
}

/// Container for execution states required by a plan
#[derive(Debug)]
pub struct ExecutionStates {
    pub group_by_state: Option<GroupByState>,
    pub window_state: Option<WindowState>,
}
