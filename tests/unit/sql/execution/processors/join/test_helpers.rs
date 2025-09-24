/*!
Test helpers for subquery join tests

This module provides utilities to set up test execution engines with
proper test data sources for subquery execution.

NOTE: This module is now deprecated in favor of the shared common_test_utils module.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::ast::StreamingQuery;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Import shared test utilities
use crate::unit::sql::execution::common_test_utils::{
    CommonTestRecords, MockTable, StandardTestData, TestExecutor,
};

/// Custom test execution engine that properly sets up test data
/// DEPRECATED: Use TestExecutor from common_test_utils instead
#[deprecated(note = "Use TestExecutor from common_test_utils instead")]
pub struct TestExecutionEngine {
    engine: StreamExecutionEngine,
    test_data: HashMap<String, Vec<StreamRecord>>,
}

impl TestExecutionEngine {
    /// Create a new test execution engine with test data sources
    #[deprecated(note = "Use TestExecutor::execute_with_standard_data instead")]
    pub fn new(tx: mpsc::UnboundedSender<StreamRecord>) -> Self {
        let mut engine = StreamExecutionEngine::new(tx);
        let test_data = StandardTestData::comprehensive_test_data();

        // Set up context customizer to inject test data
        engine.context_customizer = Some(TestExecutor::create_standard_context_customizer());

        Self { engine, test_data }
    }

    /// Execute a query with test data available for subqueries
    #[deprecated(note = "Use TestExecutor::execute_with_standard_data instead")]
    pub async fn execute_with_test_data(
        &mut self,
        query: &StreamingQuery,
        record: StreamRecord,
    ) -> Result<(), SqlError> {
        self.engine.execute_with_record(query, record).await
    }
}

/// Helper to create a test record for subquery join tests
/// DEPRECATED: Use CommonTestRecords::subquery_join_record() instead
#[deprecated(note = "Use CommonTestRecords::subquery_join_record() instead")]
pub fn create_test_record() -> StreamRecord {
    CommonTestRecords::subquery_join_record()
}

/// Execute a subquery join test with proper test data setup
/// DEPRECATED: Use TestExecutor::execute_with_standard_data instead
#[deprecated(note = "Use TestExecutor::execute_with_standard_data instead")]
pub async fn execute_subquery_test_with_data(
    query: &str,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    TestExecutor::execute_with_standard_data(query, None).await
}
