//! Unit tests for the QueryPlanner component
//!
//! These tests verify the query planning functionality, ensuring proper execution
//! plan creation from StreamingQuery AST nodes with correct optimization detection.

use std::time::Duration;

use ferrisstreams::ferris::sql::ast::{
    Expr, SelectField, StreamSource, StreamingQuery, WindowSpec,
};
use ferrisstreams::ferris::sql::execution::query_planner::QueryPlanner;

#[test]
fn test_simple_select_plan() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("users".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    assert!(!plan.is_windowed);
    assert!(!plan.has_aggregation);
    assert!(!plan.has_joins);
    assert!(plan.estimated_memory_usage > 0);
}

#[test]
fn test_aggregation_plan() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "COUNT".to_string(),
                args: vec![],
            },
            alias: Some("total".to_string()),
        }],
        from: StreamSource::Stream("events".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    assert!(!plan.is_windowed);
    assert!(plan.has_aggregation);
    assert!(!plan.has_joins);
    assert!(plan.estimated_memory_usage > 0);
}

#[test]
fn test_windowed_plan() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("events".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: None,
        }),
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    assert!(plan.is_windowed);
    assert!(!plan.has_aggregation);
    assert!(!plan.has_joins);
    assert!(plan.estimated_memory_usage > 0);
}

#[test]
fn test_complex_plan_with_multiple_features() {
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("user_id".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                },
                alias: Some("event_count".to_string()),
            },
        ],
        from: StreamSource::Stream("user_events".to_string()),
        where_clause: None,
        group_by: Some(vec![Expr::Column("user_id".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(300),
            time_column: Some("event_time".to_string()),
        }),
        joins: None,
        limit: Some(100),
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    assert!(plan.is_windowed);
    assert!(plan.has_aggregation);
    assert!(!plan.has_joins);

    // Complex plans should have higher memory usage estimates
    assert!(plan.estimated_memory_usage > 10000);
}

#[test]
fn test_plan_with_joins() {
    use ferrisstreams::ferris::sql::ast::{JoinClause, JoinType};

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("*".to_string())],
        from: StreamSource::Stream("orders".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: Some(vec![JoinClause {
            join_type: JoinType::Inner,
            right_source: StreamSource::Stream("customers".to_string()),
            condition: Expr::Column("customer_id".to_string()),
            right_alias: Some("c".to_string()),
            window: None,
        }]),
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    assert!(!plan.is_windowed);
    assert!(!plan.has_aggregation);
    assert!(plan.has_joins);
    assert!(plan.estimated_memory_usage > 0);
}

#[test]
fn test_execution_states_creation() {
    // Test with GROUP BY
    let query_with_groupby = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("category".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
                alias: Some("total".to_string()),
            },
        ],
        from: StreamSource::Stream("transactions".to_string()),
        where_clause: None,
        group_by: Some(vec![Expr::Column("category".to_string())]),
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query_with_groupby).unwrap();
    let states = QueryPlanner::get_required_states(&plan).unwrap();

    assert!(states.group_by_state.is_some());
    assert!(states.window_state.is_none());
}

#[test]
fn test_execution_states_with_window() {
    let query_with_window = StreamingQuery::Select {
        fields: vec![SelectField::Column("value".to_string())],
        from: StreamSource::Stream("sensor_data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Sliding {
            size: Duration::from_secs(60),
            advance: Duration::from_secs(10),
            time_column: Some("timestamp".to_string()),
        }),
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query_with_window).unwrap();
    let states = QueryPlanner::get_required_states(&plan).unwrap();

    assert!(states.group_by_state.is_none());
    assert!(states.window_state.is_some());
}

#[test]
fn test_plan_optimization() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let plan = QueryPlanner::create_plan(&query).unwrap();
    let optimized_plan = QueryPlanner::optimize_plan(plan);

    // Currently optimization is a no-op, but we test that it doesn't break anything
    assert!(!optimized_plan.is_windowed);
    assert!(!optimized_plan.has_aggregation);
    assert!(!optimized_plan.has_joins);
}

#[test]
fn test_memory_estimation_scaling() {
    // Simple query should have lower memory usage
    let simple_query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    // Complex query with multiple operations
    let complex_query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("category".to_string()),
            SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                },
                alias: Some("count".to_string()),
            },
        ],
        from: StreamSource::Stream("events".to_string()),
        where_clause: Some(Expr::Column("active".to_string())),
        group_by: Some(vec![Expr::Column("category".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(300),
            time_column: None,
        }),
        joins: None,
        limit: None,
        order_by: None,
    };

    let simple_plan = QueryPlanner::create_plan(&simple_query).unwrap();
    let complex_plan = QueryPlanner::create_plan(&complex_query).unwrap();

    // Complex plan should have higher memory usage estimate
    assert!(complex_plan.estimated_memory_usage > simple_plan.estimated_memory_usage);
}
