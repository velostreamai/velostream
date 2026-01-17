//! Interval Join Processor
//!
//! Processor for interval-based stream-stream joins. This processor
//! wraps the JoinCoordinator to provide a processor interface that
//! integrates with the SQL execution engine.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │            IntervalJoinProcessor                    │
//! │                                                     │
//! │  ┌───────────────────────────────────────────────┐  │
//! │  │              JoinCoordinator                  │  │
//! │  │  ┌────────────┐       ┌────────────┐        │  │
//! │  │  │ Left Store │       │ Right Store│        │  │
//! │  │  └────────────┘       └────────────┘        │  │
//! │  └───────────────────────────────────────────────┘  │
//! │                                                     │
//! │  process_left(record) ──► matches in right store   │
//! │  process_right(record) ──► matches in left store   │
//! └─────────────────────────────────────────────────────┘
//! ```

use std::time::Duration;

use crate::velostream::sql::ast::{
    BinaryOperator, Expr, JoinClause, JoinType as AstJoinType, SelectField,
};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorStats, JoinSide, JoinType,
};

/// Configuration parsed from SQL JOIN clause for interval joins
#[derive(Debug, Clone)]
pub struct IntervalJoinConfig {
    /// Name of the left source
    pub left_source: String,
    /// Name of the right source
    pub right_source: String,
    /// Join key column pairs (left_col, right_col)
    pub key_columns: Vec<(String, String)>,
    /// Lower time bound (can be negative for "before" relationships)
    pub lower_bound: Duration,
    /// Upper time bound
    pub upper_bound: Duration,
    /// State retention period
    pub retention: Duration,
    /// Join type (inner, left, right, full)
    pub join_type: JoinType,
    /// Event time field name
    pub event_time_field: String,
}

impl IntervalJoinConfig {
    /// Create a default config for two sources
    pub fn new(left_source: &str, right_source: &str) -> Self {
        Self {
            left_source: left_source.to_string(),
            right_source: right_source.to_string(),
            key_columns: Vec::new(),
            lower_bound: Duration::ZERO,
            upper_bound: Duration::from_secs(3600), // 1 hour default
            retention: Duration::from_secs(7200),   // 2 hours default
            join_type: JoinType::Inner,
            event_time_field: "event_time".to_string(),
        }
    }

    /// Add a key column pair
    pub fn with_key(mut self, left_col: &str, right_col: &str) -> Self {
        self.key_columns
            .push((left_col.to_string(), right_col.to_string()));
        self
    }

    /// Set the time bounds
    pub fn with_bounds(mut self, lower: Duration, upper: Duration) -> Self {
        self.lower_bound = lower;
        self.upper_bound = upper;
        self
    }

    /// Set the retention period
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Set the join type
    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Set the event time field
    pub fn with_event_time_field(mut self, field: &str) -> Self {
        self.event_time_field = field.to_string();
        self
    }

    /// Convert to internal JoinConfig
    pub fn to_join_config(&self) -> JoinConfig {
        JoinConfig::interval(
            &self.left_source,
            &self.right_source,
            self.key_columns.clone(),
            self.lower_bound,
            self.upper_bound,
        )
        .with_join_type(self.join_type)
        .with_retention(self.retention)
        .with_event_time_field(&self.event_time_field)
    }
}

/// Processor for interval-based stream-stream joins
///
/// This processor manages a JoinCoordinator and provides the interface
/// for processing records from left and right sources.
#[derive(Debug)]
pub struct IntervalJoinProcessor {
    /// The join coordinator managing state and matching
    coordinator: JoinCoordinator,
    /// SQL projection to apply after join (optional)
    projection: Option<Vec<SelectField>>,
    /// Optional WHERE clause filter to apply after join
    filter: Option<Expr>,
}

impl IntervalJoinProcessor {
    /// Create a new interval join processor with configuration
    pub fn new(config: IntervalJoinConfig) -> Self {
        Self {
            coordinator: JoinCoordinator::new(config.to_join_config()),
            projection: None,
            filter: None,
        }
    }

    /// Create from JoinCoordinator directly
    pub fn from_coordinator(coordinator: JoinCoordinator) -> Self {
        Self {
            coordinator,
            projection: None,
            filter: None,
        }
    }

    /// Create from a JOIN AST clause
    ///
    /// Parses the join condition to extract key columns and time bounds.
    pub fn from_ast(
        join_clause: &JoinClause,
        left_source: &str,
        config: &IntervalJoinConfig,
    ) -> Result<Self, SqlError> {
        // Extract key columns from the join condition
        let key_columns = Self::extract_key_columns(&join_clause.condition)?;

        // Create the config with extracted keys
        let mut join_config = config.clone();
        if !key_columns.is_empty() {
            join_config.key_columns = key_columns;
        }

        // Convert AST join type to our join type
        join_config.join_type = match join_clause.join_type {
            AstJoinType::Inner => JoinType::Inner,
            AstJoinType::Left => JoinType::LeftOuter,
            AstJoinType::Right => JoinType::RightOuter,
            AstJoinType::FullOuter => JoinType::FullOuter,
        };

        // Update source names
        join_config.left_source = left_source.to_string();
        join_config.right_source = Self::extract_source_name(&join_clause.right_source);

        Ok(Self::new(join_config))
    }

    /// Extract source name from StreamSource
    fn extract_source_name(source: &crate::velostream::sql::ast::StreamSource) -> String {
        match source {
            crate::velostream::sql::ast::StreamSource::Stream(name) => name.clone(),
            crate::velostream::sql::ast::StreamSource::Table(name) => name.clone(),
            crate::velostream::sql::ast::StreamSource::Subquery(_) => "subquery".to_string(),
            crate::velostream::sql::ast::StreamSource::Uri(uri) => uri.clone(),
        }
    }

    /// Extract key columns from a join condition expression
    ///
    /// Looks for equality conditions like `a.key = b.key`
    fn extract_key_columns(condition: &Expr) -> Result<Vec<(String, String)>, SqlError> {
        let mut key_columns = Vec::new();
        Self::extract_keys_recursive(condition, &mut key_columns)?;
        Ok(key_columns)
    }

    /// Recursively extract key column pairs from condition
    fn extract_keys_recursive(
        expr: &Expr,
        keys: &mut Vec<(String, String)>,
    ) -> Result<(), SqlError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::Equal {
                    // Try to extract column references
                    if let (Some(left_col), Some(right_col)) = (
                        Self::extract_column_name(left),
                        Self::extract_column_name(right),
                    ) {
                        keys.push((left_col, right_col));
                    }
                } else if *op == BinaryOperator::And {
                    // Recurse into AND conditions
                    Self::extract_keys_recursive(left, keys)?;
                    Self::extract_keys_recursive(right, keys)?;
                }
            }
            _ => {
                // Other expression types don't contribute keys
            }
        }
        Ok(())
    }

    /// Extract column name from an expression if it's a column reference
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => Some(name.clone()),
            _ => None,
        }
    }

    /// Set the projection to apply after join
    pub fn with_projection(mut self, projection: Vec<SelectField>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the filter to apply after join
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Process a record from the left source
    ///
    /// Returns joined records if matches are found in the right buffer.
    pub fn process_left(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError> {
        let joined = self.coordinator.process_left(record)?;
        self.apply_post_join_processing(joined)
    }

    /// Process a record from the right source
    ///
    /// Returns joined records if matches are found in the left buffer.
    pub fn process_right(&mut self, record: StreamRecord) -> Result<Vec<StreamRecord>, SqlError> {
        let joined = self.coordinator.process_right(record)?;
        self.apply_post_join_processing(joined)
    }

    /// Process a record from either side
    pub fn process(
        &mut self,
        side: JoinSide,
        record: StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        match side {
            JoinSide::Left => self.process_left(record),
            JoinSide::Right => self.process_right(record),
        }
    }

    /// Process a batch of records from a source
    pub fn process_batch(
        &mut self,
        side: JoinSide,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let mut all_results = Vec::new();

        for record in records {
            let results = self.process(side, record)?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    /// Apply projection and filter to joined records
    fn apply_post_join_processing(
        &self,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let mut results = Vec::with_capacity(records.len());

        for record in records {
            // Apply filter if present
            if let Some(filter) = &self.filter {
                if !ExpressionEvaluator::evaluate_expression(filter, &record)? {
                    continue;
                }
            }

            // Apply projection if present (simplified - full projection would need more context)
            // For now, just pass through the record
            results.push(record);
        }

        Ok(results)
    }

    /// Advance watermark and expire old state
    pub fn advance_watermark(&mut self, side: JoinSide, watermark: i64) -> (usize, usize) {
        self.coordinator.advance_watermark(side, watermark)
    }

    /// Get statistics
    pub fn stats(&self) -> &JoinCoordinatorStats {
        self.coordinator.stats()
    }

    /// Get the underlying coordinator (for testing/monitoring)
    pub fn coordinator(&self) -> &JoinCoordinator {
        &self.coordinator
    }

    /// Get mutable reference to coordinator
    pub fn coordinator_mut(&mut self) -> &mut JoinCoordinator {
        &mut self.coordinator
    }

    /// Check if both state stores are empty
    pub fn is_empty(&self) -> bool {
        self.coordinator.is_empty()
    }

    /// Get total buffered record count
    pub fn buffered_count(&self) -> usize {
        self.coordinator.total_records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::FieldValue;
    use std::collections::HashMap;

    fn make_record(fields: Vec<(&str, FieldValue)>, timestamp: i64) -> StreamRecord {
        let field_map: HashMap<String, FieldValue> = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let mut record = StreamRecord::new(field_map);
        record.timestamp = timestamp;
        record
    }

    #[test]
    fn test_interval_join_config() {
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600))
            .with_retention(Duration::from_secs(7200))
            .with_join_type(JoinType::Inner);

        assert_eq!(config.left_source, "orders");
        assert_eq!(config.right_source, "shipments");
        assert_eq!(config.key_columns.len(), 1);
        assert_eq!(config.upper_bound, Duration::from_secs(3600));
    }

    #[test]
    fn test_processor_basic_join() {
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Process order
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        let results = processor.process_left(order).unwrap();
        assert!(results.is_empty()); // No shipments yet

        // Process matching shipment
        let shipment = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("carrier", FieldValue::String("FedEx".to_string())),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        // Verify joined record has fields from both
        let joined = &results[0];
        assert!(joined.fields.contains_key("orders.customer"));
        assert!(joined.fields.contains_key("shipments.carrier"));
    }

    #[test]
    fn test_processor_batch_processing() {
        let config = IntervalJoinConfig::new("left", "right")
            .with_key("key", "key")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Process batch of left records
        let left_batch: Vec<StreamRecord> = (0..5)
            .map(|i| {
                make_record(
                    vec![
                        ("key", FieldValue::Integer(i)),
                        ("left_val", FieldValue::String(format!("L{}", i))),
                    ],
                    1000 + i,
                )
            })
            .collect();

        let results = processor.process_batch(JoinSide::Left, left_batch).unwrap();
        assert!(results.is_empty()); // No right records yet

        // Process batch of right records
        let right_batch: Vec<StreamRecord> = (0..3)
            .map(|i| {
                make_record(
                    vec![
                        ("key", FieldValue::Integer(i)),
                        ("right_val", FieldValue::String(format!("R{}", i))),
                    ],
                    2000 + i,
                )
            })
            .collect();

        let results = processor
            .process_batch(JoinSide::Right, right_batch)
            .unwrap();
        assert_eq!(results.len(), 3); // Keys 0, 1, 2 match
    }

    #[test]
    fn test_stats_tracking() {
        let config = IntervalJoinConfig::new("left", "right")
            .with_key("id", "id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Process some records
        for i in 0..5 {
            processor
                .process_left(make_record(vec![("id", FieldValue::Integer(i))], 1000 + i))
                .unwrap();
        }

        for i in 0..3 {
            processor
                .process_right(make_record(vec![("id", FieldValue::Integer(i))], 2000 + i))
                .unwrap();
        }

        let stats = processor.stats();
        assert_eq!(stats.left_records_processed, 5);
        assert_eq!(stats.right_records_processed, 3);
        assert_eq!(stats.matches_emitted, 3);
    }

    #[test]
    fn test_watermark_expiration() {
        let config = IntervalJoinConfig::new("left", "right")
            .with_key("id", "id")
            .with_retention(Duration::from_millis(1000));

        let mut processor = IntervalJoinProcessor::new(config);

        // Add a record
        processor
            .process_left(make_record(
                vec![
                    ("id", FieldValue::Integer(1)),
                    ("event_time", FieldValue::Integer(1000)),
                ],
                1000,
            ))
            .unwrap();

        assert_eq!(processor.buffered_count(), 1);

        // Advance watermark past expiration
        processor.advance_watermark(JoinSide::Left, 2500);

        assert_eq!(processor.buffered_count(), 0);
    }

    #[test]
    fn test_empty_processor() {
        let config = IntervalJoinConfig::new("left", "right").with_key("id", "id");

        let processor = IntervalJoinProcessor::new(config);

        assert!(processor.is_empty());
        assert_eq!(processor.buffered_count(), 0);
    }
}
