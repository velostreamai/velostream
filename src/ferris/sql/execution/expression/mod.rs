//! Expression evaluation module for streaming SQL execution.
//!
//! This module contains all the logic for evaluating SQL expressions including:
//! - Expression evaluation (boolean and value expressions)
//! - Built-in function implementations (math, string, date functions)
//! - Arithmetic operations (add, subtract, multiply, divide)
//!
//! The expression evaluation system supports:
//! - Column references and literals
//! - Binary operations (arithmetic, comparison, logical)
//! - Function calls with arguments
//! - Type coercion and casting
//! - NULL handling according to SQL semantics

pub mod arithmetic;
pub mod evaluator;
pub mod functions;

// Re-export the main API
pub use arithmetic::ArithmeticOperations;
pub use evaluator::ExpressionEvaluator;
pub use functions::BuiltinFunctions;
