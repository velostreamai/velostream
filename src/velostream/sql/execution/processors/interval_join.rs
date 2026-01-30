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
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorStats, JoinSide, JoinType,
};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};

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
    ///
    /// Applies SQL SELECT projection to joined records, supporting:
    /// - `Column(name)` - extract field with original name
    /// - `AliasedColumn { column, alias }` - extract field and rename to alias
    /// - `Wildcard` - pass all fields through
    /// - `Expression { expr, alias }` - evaluate expression and apply alias
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

            // Apply projection if present
            let projected_record = if let Some(projection) = &self.projection {
                self.apply_projection(&record, projection)?
            } else {
                record
            };
            results.push(projected_record);
        }

        Ok(results)
    }

    /// Apply SQL SELECT projection to a joined record
    ///
    /// Handles:
    /// - `SelectField::Column(name)` - extract field with original name
    /// - `SelectField::AliasedColumn { column, alias }` - extract field and rename to alias
    /// - `SelectField::Wildcard` - pass all fields through
    /// - `SelectField::Expression { expr, alias }` - evaluate expression and apply alias
    fn apply_projection(
        &self,
        record: &StreamRecord,
        projection: &[SelectField],
    ) -> Result<StreamRecord, SqlError> {
        // Check for wildcard first - if present, return all fields
        for field in projection {
            if matches!(field, SelectField::Wildcard) {
                return Ok(record.clone());
            }
        }

        let mut projected_fields = std::collections::HashMap::with_capacity(projection.len());

        for select_field in projection {
            match select_field {
                SelectField::Column(name) => {
                    // Resolve the field, inserting NULL if not found (proper SQL semantics)
                    let value = self.resolve_field(record, name).unwrap_or(FieldValue::Null);
                    projected_fields.insert(name.clone(), value);
                }
                SelectField::AliasedColumn { column, alias } => {
                    // Resolve the column, inserting NULL with alias if not found
                    let value = self
                        .resolve_field(record, column)
                        .unwrap_or(FieldValue::Null);
                    projected_fields.insert(alias.clone(), value);
                }
                SelectField::Expression { expr, alias } => {
                    // Evaluate the expression and insert with alias (or generated name)
                    let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                    let field_name = alias
                        .clone()
                        .unwrap_or_else(|| format!("expr_{}", projected_fields.len()));
                    projected_fields.insert(field_name, value);
                }
                SelectField::Wildcard => {
                    // Already handled above
                    unreachable!("Wildcard should be handled earlier");
                }
            }
        }

        let mut projected_record = StreamRecord::new(projected_fields);
        // Preserve all record metadata
        projected_record.timestamp = record.timestamp;
        projected_record.key = record.key.clone();
        projected_record.event_time = record.event_time;
        projected_record.offset = record.offset;
        projected_record.partition = record.partition;
        Ok(projected_record)
    }

    /// Resolve a field name from a record, handling qualified and unqualified names
    ///
    /// For example:
    /// - "orders.customer" looks for "orders.customer" directly
    /// - "customer" looks for "customer" or any qualified variant like "orders.customer"
    ///
    /// If an unqualified name matches multiple qualified fields (e.g., "order_id" matches
    /// both "orders.order_id" and "shipments.order_id"), a warning is logged and the
    /// first match is returned. Users should use qualified names to avoid ambiguity.
    fn resolve_field(&self, record: &StreamRecord, field_name: &str) -> Option<FieldValue> {
        // First try exact match
        if let Some(value) = record.fields.get(field_name) {
            return Some(value.clone());
        }

        // Handle qualified names (e.g., "o.customer" or "orders.customer")
        if field_name.contains('.') {
            // Extract just the column name (e.g., "o.customer" → "customer")
            let column_name = field_name.split('.').next_back().unwrap_or(field_name);

            // Search for any qualified version of this column name
            if let Some(value) = self.find_by_column_suffix(record, column_name, field_name, true) {
                return Some(value);
            }

            // Also try unqualified match as last resort
            return record.fields.get(column_name).cloned();
        }

        // Handle unqualified names - search for any qualified version
        self.find_by_column_suffix(record, field_name, field_name, false)
    }

    /// Search for a field by column name suffix (e.g., ".customer" matches "orders.customer")
    ///
    /// Returns the value if exactly one match is found, or the first match with a warning
    /// if multiple matches exist. Returns None if no matches found.
    fn find_by_column_suffix(
        &self,
        record: &StreamRecord,
        column_name: &str,
        original_ref: &str,
        is_qualified: bool,
    ) -> Option<FieldValue> {
        let suffix = format!(".{}", column_name);

        // Collect matches - we need the keys for logging if ambiguous
        let matches: Vec<(&String, &FieldValue)> = record
            .fields
            .iter()
            .filter(|(key, _)| key.ends_with(&suffix))
            .collect();

        match matches.len() {
            0 => None,
            1 => Some(matches[0].1.clone()),
            _ => {
                // Multiple matches - log warning about ambiguity
                let matching_keys: Vec<&str> = matches.iter().map(|(k, _)| k.as_str()).collect();
                if is_qualified {
                    log::warn!(
                        "Qualified field reference '{}' (column '{}') matches multiple fields: {:?}. \
                         Using first match '{}'. Ensure table aliases match stream names.",
                        original_ref,
                        column_name,
                        matching_keys,
                        matching_keys[0]
                    );
                } else {
                    log::warn!(
                        "Ambiguous field reference '{}' matches multiple qualified fields: {:?}. \
                         Using first match '{}'. Use qualified name to avoid ambiguity.",
                        original_ref,
                        matching_keys,
                        matching_keys[0]
                    );
                }
                Some(matches[0].1.clone())
            }
        }
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

    #[test]
    fn test_projection_applies_select_fields_and_aliases() {
        // Test that projection selects only specified fields and applies aliases
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection: SELECT o.customer AS customer_name, s.carrier AS shipping_carrier
        let projection = vec![
            SelectField::AliasedColumn {
                column: "orders.customer".to_string(),
                alias: "customer_name".to_string(),
            },
            SelectField::AliasedColumn {
                column: "shipments.carrier".to_string(),
                alias: "shipping_carrier".to_string(),
            },
            SelectField::Column("orders.order_id".to_string()), // No alias
        ];
        processor = processor.with_projection(projection);

        // Process order
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
                ("amount", FieldValue::Integer(500)),
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
                ("tracking", FieldValue::String("123456".to_string())),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        // Verify projection was applied
        let joined = &results[0];

        // Should have the aliased fields
        assert!(
            joined.fields.contains_key("customer_name"),
            "Expected 'customer_name' alias, got fields: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("customer_name"),
            Some(&FieldValue::String("Alice".to_string()))
        );

        assert!(
            joined.fields.contains_key("shipping_carrier"),
            "Expected 'shipping_carrier' alias, got fields: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("shipping_carrier"),
            Some(&FieldValue::String("FedEx".to_string()))
        );

        // Should have the non-aliased field with original qualified name
        assert!(
            joined.fields.contains_key("orders.order_id"),
            "Expected 'orders.order_id', got fields: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );

        // Should NOT have fields that weren't in projection
        assert!(
            !joined.fields.contains_key("orders.amount"),
            "Should not have 'orders.amount' which wasn't projected"
        );
        assert!(
            !joined.fields.contains_key("shipments.tracking"),
            "Should not have 'shipments.tracking' which wasn't projected"
        );

        // Should have exactly 3 fields (the projected ones)
        assert_eq!(
            joined.fields.len(),
            3,
            "Should have exactly 3 projected fields, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_projection_with_wildcard_passes_all_fields() {
        // Test that Wildcard projection passes all fields through
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection: SELECT *
        let projection = vec![SelectField::Wildcard];
        processor = processor.with_projection(projection);

        // Process order
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

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

        // Wildcard should pass all fields through
        let joined = &results[0];
        assert!(joined.fields.contains_key("orders.customer"));
        assert!(joined.fields.contains_key("shipments.carrier"));
    }

    #[test]
    fn test_projection_with_expression() {
        use crate::velostream::sql::ast::{Expr, LiteralValue};

        // Test that Expression projection evaluates expressions and applies aliases
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection with a computed expression
        let projection = vec![
            SelectField::Column("orders.order_id".to_string()),
            SelectField::Expression {
                expr: Expr::Literal(LiteralValue::String("MATCHED".to_string())),
                alias: Some("status".to_string()),
            },
        ];
        processor = processor.with_projection(projection);

        // Process order
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

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

        let joined = &results[0];

        // Should have the order_id
        assert!(
            joined.fields.contains_key("orders.order_id"),
            "Expected 'orders.order_id', got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );

        // Should have the expression result with alias
        assert!(
            joined.fields.contains_key("status"),
            "Expected 'status' from expression, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("status"),
            Some(&FieldValue::String("MATCHED".to_string()))
        );

        // Should have exactly 2 fields
        assert_eq!(
            joined.fields.len(),
            2,
            "Should have exactly 2 fields, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_projection_missing_field_produces_null() {
        // Test that projecting a non-existent field produces NULL (proper SQL semantics)
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection with a field that doesn't exist
        let projection = vec![
            SelectField::Column("orders.order_id".to_string()),
            SelectField::Column("orders.nonexistent_field".to_string()), // Doesn't exist
        ];
        processor = processor.with_projection(projection);

        // Process order (without nonexistent_field)
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

        // Process matching shipment
        let shipment = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        let joined = &results[0];

        // Should have the existing field with its value
        assert!(joined.fields.contains_key("orders.order_id"));
        assert_eq!(
            joined.fields.get("orders.order_id"),
            Some(&FieldValue::Integer(100))
        );

        // Missing field should be present with NULL value (proper SQL semantics)
        assert!(
            joined.fields.contains_key("orders.nonexistent_field"),
            "Missing field should be present with NULL value, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("orders.nonexistent_field"),
            Some(&FieldValue::Null),
            "Missing field should have NULL value"
        );

        // Should have exactly 2 fields (both projected fields present)
        assert_eq!(
            joined.fields.len(),
            2,
            "Should have 2 fields (existing + NULL for missing), got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_projection_ambiguous_unqualified_field_logs_warning() {
        // Test that ambiguous unqualified field names still resolve (with warning logged)
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection with unqualified field name that exists in both sources
        let projection = vec![
            SelectField::Column("order_id".to_string()), // Ambiguous - exists in both
        ];
        processor = processor.with_projection(projection);

        // Process order
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

        // Process matching shipment
        let shipment = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        let joined = &results[0];

        // Should have the field (resolved from one of the qualified versions)
        assert!(
            joined.fields.contains_key("order_id"),
            "Unqualified field should be resolved, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );

        // Value should be 100 (from whichever source was matched)
        assert_eq!(
            joined.fields.get("order_id"),
            Some(&FieldValue::Integer(100))
        );
    }

    #[test]
    fn test_projection_qualified_name_alias_mismatch_resolves() {
        // Test that qualified names with mismatched aliases still resolve
        // e.g., SQL uses "o.customer" but record has "orders.customer"
        // (join processor prefixes fields with stream name from config)
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Set projection with aliased qualified name (o.customer instead of orders.customer)
        // The resolve_field should strip "o." and find "orders.customer" via suffix match
        let projection = vec![SelectField::AliasedColumn {
            column: "o.customer".to_string(), // SQL alias doesn't match stream name
            alias: "customer_name".to_string(),
        }];
        processor = processor.with_projection(projection);

        // Process order - join processor will add "orders." prefix automatically
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("customer", FieldValue::String("Alice".to_string())),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

        // Process matching shipment - join processor will add "shipments." prefix
        let shipment = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        let joined = &results[0];

        // Should have resolved "o.customer" to "orders.customer" and aliased to "customer_name"
        assert!(
            joined.fields.contains_key("customer_name"),
            "Aliased field should be present, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("customer_name"),
            Some(&FieldValue::String("Alice".to_string())),
            "Should resolve o.customer to orders.customer value"
        );
    }

    #[test]
    fn test_resolve_field_qualified_with_multiple_matches_logs_warning() {
        // Test that qualified names matching multiple fields logs a warning
        // e.g., "x.id" could match both "orders.id" and "shipments.id"
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let mut processor = IntervalJoinProcessor::new(config);

        // Project a qualified field that will match multiple qualified versions
        // "x.order_id" should match "orders.order_id" and "shipments.order_id"
        let projection = vec![SelectField::AliasedColumn {
            column: "x.order_id".to_string(),
            alias: "matched_id".to_string(),
        }];
        processor = processor.with_projection(projection);

        // Process order and shipment (both have order_id)
        let order = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(1000)),
            ],
            1000,
        );
        processor.process_left(order).unwrap();

        let shipment = make_record(
            vec![
                ("order_id", FieldValue::Integer(100)),
                ("event_time", FieldValue::Integer(2000)),
            ],
            2000,
        );
        let results = processor.process_right(shipment).unwrap();
        assert_eq!(results.len(), 1);

        let joined = &results[0];

        // Should still resolve (with warning logged) - takes first match
        assert!(
            joined.fields.contains_key("matched_id"),
            "Should have matched_id field, got: {:?}",
            joined.fields.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            joined.fields.get("matched_id"),
            Some(&FieldValue::Integer(100)),
            "Should have value from one of the matches"
        );
    }

    #[test]
    fn test_resolve_field_qualified_fallback_to_unqualified() {
        // Test that qualified name with no suffix match falls back to unqualified lookup
        // e.g., "x.status" where record has unqualified "status" field (not "orders.status")
        let config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let processor = IntervalJoinProcessor::new(config);

        // Create a record with an unqualified field (simulating a computed field)
        let mut fields = std::collections::HashMap::new();
        fields.insert("orders.order_id".to_string(), FieldValue::Integer(100));
        fields.insert(
            "status".to_string(),
            FieldValue::String("ACTIVE".to_string()),
        ); // Unqualified
        let record = StreamRecord::new(fields);

        // Try to resolve "x.status" - should fall back to unqualified "status"
        let value = processor.resolve_field(&record, "x.status");
        assert_eq!(
            value,
            Some(FieldValue::String("ACTIVE".to_string())),
            "Should fall back to unqualified field lookup"
        );

        // "orders.status" also falls back to unqualified "status" as last resort
        // This is intentional - allows computed fields without prefixes to be accessed
        let value = processor.resolve_field(&record, "orders.status");
        assert_eq!(
            value,
            Some(FieldValue::String("ACTIVE".to_string())),
            "Should fall back to unqualified 'status' as last resort"
        );

        // But "x.nonexistent" should return None (no suffix match, no unqualified match)
        let value = processor.resolve_field(&record, "x.nonexistent");
        assert_eq!(value, None, "Should return None when no match found at all");
    }
}
