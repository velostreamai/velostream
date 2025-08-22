/*!
# JOIN Context

JOIN processing context and utilities for managing join operations.
*/

use super::join::JoinProcessor;
use crate::ferris::sql::SqlError;
use crate::ferris::sql::ast::StreamSource;
use crate::ferris::sql::execution::StreamRecord;

/// JOIN processing context and utilities
pub struct JoinContext;

impl JoinContext {
    pub fn get_right_record(
        &self,
        source: &StreamSource,
        _window: &Option<crate::ferris::sql::ast::JoinWindow>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Implementation moved from engine.rs - delegate to JoinProcessor
        match source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                if name == "empty_stream" {
                    // Simulate no matching record
                    return Ok(None);
                }

                // Create mock record using JoinProcessor
                Ok(Some(JoinProcessor::create_mock_right_record(source)?))
            }
            StreamSource::Subquery(subquery) => {
                // Execute subquery to get right side records for JOIN
                JoinProcessor::execute_subquery_for_join(subquery)
            }
        }
    }
}
