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
    pub fn resolve_field(&self, record: &StreamRecord, field_name: &str) -> Option<FieldValue> {
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

        // Collect matches sorted by key for deterministic resolution
        let mut matches: Vec<(&String, &FieldValue)> = record
            .fields
            .iter()
            .filter(|(key, _)| key.ends_with(&suffix))
            .collect();
        matches.sort_by_key(|(k, _)| k.as_str());

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
