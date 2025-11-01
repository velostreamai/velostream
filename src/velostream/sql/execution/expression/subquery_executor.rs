/*!
# Subquery Execution Trait

This module defines the SubqueryExecutor trait that allows processors to execute
subqueries independently without calling back to the engine. This maintains the
proper parent-child architecture where the engine delegates to processors.
*/

use crate::velostream::sql::StreamingQuery;
use crate::velostream::sql::ast::SubqueryType;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::processors::ProcessorContext;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};

/// Trait for executing subqueries within expression evaluation
///
/// This trait is implemented by processors to provide subquery execution
/// capabilities to the ExpressionEvaluator. The processor acts as the parent
/// and provides the context needed to execute nested queries.
pub trait SubqueryExecutor {
    /// Execute a scalar subquery that must return exactly one row with one column
    ///
    /// # Arguments
    /// * `query` - The subquery to execute
    /// * `current_record` - The current record being processed (for correlated subqueries)
    /// * `context` - The processor context containing data sources
    ///
    /// # Returns
    /// * `Ok(FieldValue)` - The single value returned by the subquery
    /// * `Err(SqlError)` - If the subquery fails, returns zero rows, or returns multiple rows/columns
    fn execute_scalar_subquery(
        &self,
        query: &StreamingQuery,
        current_record: &StreamRecord,
        context: &ProcessorContext,
    ) -> Result<FieldValue, SqlError>;

    /// Execute an EXISTS subquery to check if any rows would be returned
    ///
    /// # Arguments
    /// * `query` - The subquery to execute
    /// * `current_record` - The current record being processed (for correlated subqueries)
    /// * `context` - The processor context containing data sources
    ///
    /// # Returns
    /// * `Ok(true)` - If the subquery returns at least one row
    /// * `Ok(false)` - If the subquery returns no rows
    /// * `Err(SqlError)` - If the subquery execution fails
    fn execute_exists_subquery(
        &self,
        query: &StreamingQuery,
        current_record: &StreamRecord,
        context: &ProcessorContext,
    ) -> Result<bool, SqlError>;

    /// Execute an IN subquery to check if a value exists in the result set
    ///
    /// # Arguments
    /// * `value` - The value to search for
    /// * `query` - The subquery to execute (should return one column)
    /// * `current_record` - The current record being processed (for correlated subqueries)
    /// * `context` - The processor context containing data sources
    ///
    /// # Returns
    /// * `Ok(true)` - If the value exists in the subquery result set
    /// * `Ok(false)` - If the value does not exist in the subquery result set
    /// * `Err(SqlError)` - If the subquery execution fails or returns multiple columns
    fn execute_in_subquery(
        &self,
        value: &FieldValue,
        query: &StreamingQuery,
        current_record: &StreamRecord,
        context: &ProcessorContext,
    ) -> Result<bool, SqlError>;

    /// Execute an ANY/ALL subquery for comparison operations
    ///
    /// # Arguments
    /// * `value` - The value to compare against
    /// * `query` - The subquery to execute (should return one column)
    /// * `current_record` - The current record being processed (for correlated subqueries)
    /// * `context` - The processor context containing data sources
    /// * `is_any` - true for ANY/SOME, false for ALL
    /// * `comparison_op` - The comparison operation to perform
    ///
    /// # Returns
    /// * `Ok(true/false)` - Based on the ANY/ALL comparison result
    /// * `Err(SqlError)` - If the subquery execution fails
    fn execute_any_all_subquery(
        &self,
        value: &FieldValue,
        query: &StreamingQuery,
        current_record: &StreamRecord,
        context: &ProcessorContext,
        is_any: bool,
        comparison_op: &str, // "=", "<", ">", "<=", ">=", "!="
    ) -> Result<bool, SqlError>;
}

/// Helper function to evaluate a subquery based on its type
///
/// This is used by ExpressionEvaluator to delegate subquery execution
/// to the appropriate SubqueryExecutor method.
pub fn evaluate_subquery_with_executor<T: SubqueryExecutor>(
    executor: &T,
    query: &StreamingQuery,
    subquery_type: &SubqueryType,
    current_record: &StreamRecord,
    context: &ProcessorContext,
    comparison_value: Option<&FieldValue>,
) -> Result<FieldValue, SqlError> {
    match subquery_type {
        SubqueryType::Scalar => executor.execute_scalar_subquery(query, current_record, context),
        SubqueryType::Exists => {
            let exists = executor.execute_exists_subquery(query, current_record, context)?;
            Ok(FieldValue::Boolean(exists))
        }
        SubqueryType::NotExists => {
            let exists = executor.execute_exists_subquery(query, current_record, context)?;
            Ok(FieldValue::Boolean(!exists))
        }
        SubqueryType::In => {
            if let Some(value) = comparison_value {
                let in_result =
                    executor.execute_in_subquery(value, query, current_record, context)?;
                Ok(FieldValue::Boolean(in_result))
            } else {
                Err(SqlError::ExecutionError {
                    message: "IN subquery requires a comparison value".to_string(),
                    query: None,
                })
            }
        }
        SubqueryType::NotIn => {
            if let Some(value) = comparison_value {
                let in_result =
                    executor.execute_in_subquery(value, query, current_record, context)?;
                Ok(FieldValue::Boolean(!in_result))
            } else {
                Err(SqlError::ExecutionError {
                    message: "NOT IN subquery requires a comparison value".to_string(),
                    query: None,
                })
            }
        }
        SubqueryType::Any => {
            if let Some(value) = comparison_value {
                let result = executor.execute_any_all_subquery(
                    value,
                    query,
                    current_record,
                    context,
                    true,
                    "=",
                )?;
                Ok(FieldValue::Boolean(result))
            } else {
                Err(SqlError::ExecutionError {
                    message: "ANY subquery requires a comparison value".to_string(),
                    query: None,
                })
            }
        }
        SubqueryType::All => {
            if let Some(value) = comparison_value {
                let result = executor.execute_any_all_subquery(
                    value,
                    query,
                    current_record,
                    context,
                    false,
                    "=",
                )?;
                Ok(FieldValue::Boolean(result))
            } else {
                Err(SqlError::ExecutionError {
                    message: "ALL subquery requires a comparison value".to_string(),
                    query: None,
                })
            }
        }
    }
}
