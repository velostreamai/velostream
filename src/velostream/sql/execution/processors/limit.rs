//! LIMIT Query Processor
//!
//! Handles LIMIT clause processing for controlling result set size.

use super::{ProcessorContext, ProcessorResult};
use crate::velostream::sql::SqlError;

/// LIMIT processing utilities
pub struct LimitProcessor;

impl LimitProcessor {
    /// Check if the limit has been reached and return early termination result if so
    pub fn check_limit(
        limit_value: u64,
        context: &mut ProcessorContext,
    ) -> Result<Option<ProcessorResult>, SqlError> {
        if context.record_count >= limit_value {
            // Return early termination signal
            Ok(Some(ProcessorResult {
                record: None,
                header_mutations: Vec::new(),
                should_count: false,
            }))
        } else {
            // Allow processing to continue
            Ok(None)
        }
    }

    /// Update the record count after successful processing
    pub fn increment_count(context: &mut ProcessorContext) {
        context.record_count += 1;
    }

    /// Check if we should increment the count based on processor result
    pub fn should_increment_count(result: &ProcessorResult) -> bool {
        result.should_count && result.record.is_some()
    }

    /// Apply limit-based filtering to determine if processing should continue
    pub fn should_process_record(limit: Option<u64>, context: &ProcessorContext) -> bool {
        match limit {
            Some(limit_value) => context.record_count < limit_value,
            None => true, // No limit, always process
        }
    }

    /// Get the remaining capacity before hitting the limit
    pub fn remaining_capacity(limit: Option<u64>, context: &ProcessorContext) -> Option<u64> {
        match limit {
            Some(limit_value) => {
                if context.record_count < limit_value {
                    Some(limit_value - context.record_count)
                } else {
                    Some(0)
                }
            }
            None => None, // No limit
        }
    }
}
