//! Expression evaluation module for streaming SQL execution.
//!
//! This module contains all the logic for evaluating SQL expressions including:
//! - Expression evaluation (boolean and value expressions)
//! - Built-in function implementations (math, string, date functions)
//! - Window functions (LAG, LEAD, ROW_NUMBER, RANK, etc.)
//!
//! The expression evaluation system supports:
//! - Column references and literals
//! - Binary operations (arithmetic, comparison, logical)
//! - Function calls with arguments
//! - Window function evaluation
//! - Type coercion and casting
//! - NULL handling according to SQL semantics

pub mod evaluator;
pub mod functions;
pub mod subquery_executor;
pub mod window_functions;

// Re-export the main API
pub use evaluator::ExpressionEvaluator;
pub use subquery_executor::SubqueryExecutor;
pub use window_functions::WindowFunctions;
