/*!
# Unified Table Interface - Consolidated Trait System

This module replaces the complex inheritance hierarchy of overlapping traits with a single,
unified interface that provides all table operations in a cohesive, performant way.

## Problem Solved

The previous design had confusing overlapping traits:
- `SqlDataSource` - Basic data access
- Legacy SqlQueryable - SQL query operations (now unified)
- `StreamingQueryable` - Async streaming operations

This led to:
- Complex implementations requiring multiple trait impls
- Confusing API surface with unclear responsibilities
- Performance issues due to indirection and trait object overhead
- Difficult maintenance and extension

## Solution: UnifiedTable Trait

A single trait that provides:
- Direct data access (sync)
- SQL query operations (sync/async)
- Streaming operations (async)
- Performance optimizations built-in

## Design Principles

1. **Single Responsibility**: One trait, clear interface
2. **Performance First**: Zero-cost abstractions, inlined fast paths
3. **Backward Compatible**: Easy migration from existing traits
4. **Async/Sync Unified**: Both paradigms supported cleanly
5. **Extensible**: Easy to add new operations

*/

use crate::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use crate::velostream::table::streaming::{
    RecordBatch, RecordStream, SimpleStreamRecord, StreamResult,
};
use async_trait::async_trait;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

/// Result type for table operations
pub type TableResult<T> = Result<T, SqlError>;

// ============================================================================
// PERFORMANCE-OPTIMIZED PARSING
// ============================================================================

lazy_static! {
    /// Pre-compiled regex for table.column pattern matching (performance optimization)
    static ref TABLE_COLUMN_REGEX: Regex =
        Regex::new(r"^(\w+)\.(\w+)$").expect("Invalid table.column regex");

    /// Pre-compiled regex for equality expressions: "field = 'value'" or "field = value"
    static ref EQUALITY_REGEX: Regex =
        Regex::new(r"^\s*([^=\s]+)\s*=\s*(.+)\s*$").expect("Invalid equality regex");

    /// Pre-compiled regex for quoted strings
    static ref QUOTED_STRING_REGEX: Regex =
        Regex::new(r#"^["'](.+)["']$"#).expect("Invalid quoted string regex");
}

// ============================================================================
// CACHED PREDICATE SYSTEM
// ============================================================================

/// Cached predicate for optimized WHERE clause evaluation.
///
/// This enum represents pre-parsed WHERE clause conditions that can be evaluated
/// efficiently without re-parsing for each record. It's used by `OptimizedTableImpl`
/// to filter records during table scans.
///
/// # Supported Patterns
///
/// | Pattern | Variant | Example |
/// |---------|---------|---------|
/// | Equality (string) | `FieldEqualsString` | `status = 'active'` |
/// | Equality (integer) | `FieldEqualsInteger` | `id = 123` |
/// | Equality (float) | `FieldEqualsFloat` | `price = 19.99` |
/// | Equality (boolean) | `FieldEqualsBoolean` | `active = true` |
/// | Comparison | `FieldGreaterThan`, etc. | `amount > 100` |
/// | IN (strings) | `FieldInStringList` | `tier IN ('gold', 'platinum')` |
/// | IN (integers) | `FieldInIntegerList` | `status IN (1, 2, 3)` |
/// | Always true | `AlwaysTrue` | `true`, `1=1` |
/// | Always false | `AlwaysFalse` | `false`, `1=0` |
///
/// # Unsupported Patterns (Fall back to AlwaysTrue)
///
/// Complex expressions not supported here are handled by the full SQL evaluator:
/// - `NOT IN` - Use SQL evaluator
/// - `AND`/`OR` compound expressions - Use SQL evaluator
/// - `BETWEEN`, `LIKE`, `IS NULL` - Use SQL evaluator
/// - Subqueries `IN (SELECT ...)` - Use SQL evaluator
///
/// # Type Coercion
///
/// The `FieldInStringList` and `FieldInIntegerList` variants support type coercion:
/// - `FieldInStringList` matches Integer/Float fields by converting to string
/// - `FieldInIntegerList` matches String fields by parsing, Float by truncation
#[derive(Debug, Clone)]
pub enum CachedPredicate {
    /// Always true - matches all records. Used for `true`, `1=1`, or unrecognized patterns.
    AlwaysTrue,
    /// Always false - matches no records. Used for `false`, `1=0`.
    AlwaysFalse,
    /// Field equals string value: `field = 'value'`
    FieldEqualsString { field: String, value: String },
    /// Field equals integer value: `field = 123`
    FieldEqualsInteger { field: String, value: i64 },
    /// Field equals float value: `field = 19.99`
    FieldEqualsFloat { field: String, value: f64 },
    /// Field equals boolean value: `field = true`
    FieldEqualsBoolean { field: String, value: bool },
    /// Field greater than numeric value: `field > 100`
    FieldGreaterThan { field: String, value: f64 },
    /// Field less than numeric value: `field < 100`
    FieldLessThan { field: String, value: f64 },
    /// Field greater than or equal to numeric value: `field >= 100`
    FieldGreaterThanOrEqual { field: String, value: f64 },
    /// Field less than or equal to numeric value: `field <= 100`
    FieldLessThanOrEqual { field: String, value: f64 },
    /// Field value is in a list of string values: `field IN ('a', 'b', 'c')`
    ///
    /// Supports type coercion: Integer and Float field values are converted to
    /// strings for comparison (e.g., `1` matches `"1"`).
    FieldInStringList { field: String, values: Vec<String> },
    /// Field value is in a list of integer values: `field IN (1, 2, 3)`
    ///
    /// Supports type coercion: String field values are parsed as integers,
    /// Float values are truncated to integers for comparison.
    FieldInIntegerList { field: String, values: Vec<i64> },
    /// Compound AND predicate: both sub-predicates must evaluate to true
    And(Box<CachedPredicate>, Box<CachedPredicate>),
}

impl CachedPredicate {
    /// Evaluate the predicate against a record
    #[inline]
    pub fn evaluate(&self, _key: &str, record: &HashMap<String, FieldValue>) -> bool {
        match self {
            CachedPredicate::AlwaysTrue => true,
            CachedPredicate::AlwaysFalse => false,
            CachedPredicate::FieldEqualsString { field, value } => {
                matches!(record.get(field), Some(FieldValue::String(s)) if s == value)
            }
            CachedPredicate::FieldEqualsInteger { field, value } => {
                matches!(record.get(field), Some(FieldValue::Integer(i)) if i == value)
            }
            CachedPredicate::FieldEqualsFloat { field, value } => {
                matches!(record.get(field), Some(FieldValue::Float(f)) if (f - value).abs() < f64::EPSILON)
            }
            CachedPredicate::FieldEqualsBoolean { field, value } => {
                matches!(record.get(field), Some(FieldValue::Boolean(b)) if b == value)
            }
            CachedPredicate::FieldGreaterThan { field, value } => {
                Self::compare_numeric_field(record, field, |field_val| field_val > *value)
            }
            CachedPredicate::FieldLessThan { field, value } => {
                Self::compare_numeric_field(record, field, |field_val| field_val < *value)
            }
            CachedPredicate::FieldGreaterThanOrEqual { field, value } => {
                Self::compare_numeric_field(record, field, |field_val| field_val >= *value)
            }
            CachedPredicate::FieldLessThanOrEqual { field, value } => {
                Self::compare_numeric_field(record, field, |field_val| field_val <= *value)
            }
            CachedPredicate::FieldInStringList { field, values } => {
                match record.get(field) {
                    Some(FieldValue::String(s)) => values.contains(s),
                    // Support integer fields by converting to string for comparison
                    Some(FieldValue::Integer(i)) => {
                        let i_str = i.to_string();
                        values.iter().any(|v| v == &i_str)
                    }
                    // Support float fields (exact string match)
                    Some(FieldValue::Float(f)) => {
                        let f_str = f.to_string();
                        values.iter().any(|v| v == &f_str)
                    }
                    _ => false,
                }
            }
            CachedPredicate::FieldInIntegerList { field, values } => {
                match record.get(field) {
                    Some(FieldValue::Integer(i)) => values.contains(i),
                    // Support string fields that contain numeric values
                    Some(FieldValue::String(s)) => s
                        .parse::<i64>()
                        .map(|i| values.contains(&i))
                        .unwrap_or(false),
                    // Support float fields by truncating to integer
                    Some(FieldValue::Float(f)) => values.contains(&(*f as i64)),
                    _ => false,
                }
            }
            CachedPredicate::And(left, right) => {
                left.evaluate(_key, record) && right.evaluate(_key, record)
            }
        }
    }

    /// Helper method to compare numeric fields with type coercion
    #[inline]
    fn compare_numeric_field<F>(
        record: &HashMap<String, FieldValue>,
        field: &str,
        comparator: F,
    ) -> bool
    where
        F: Fn(f64) -> bool,
    {
        match record.get(field) {
            Some(FieldValue::Float(f)) => comparator(*f),
            Some(FieldValue::Integer(i)) => comparator(*i as f64),
            Some(FieldValue::ScaledInteger(value, scale)) => {
                let float_val = *value as f64 / 10_f64.powi(*scale as i32);
                comparator(float_val)
            }
            _ => false,
        }
    }
}

/// Unified table interface combining all table operations
///
/// This trait consolidates SqlDataSource, legacy SqlQueryable, and StreamingQueryable
/// into a single, cohesive interface with optimized implementations.
#[async_trait]
pub trait UnifiedTable: Send + Sync {
    // =========================================================================
    // TRAIT OBJECT UTILITIES
    // =========================================================================

    /// Enable downcasting for performance optimizations
    ///
    /// This allows specific implementations (like OptimizedTableImpl) to provide
    /// specialized high-performance methods while maintaining trait compatibility.
    fn as_any(&self) -> &dyn std::any::Any;

    // =========================================================================
    // CORE DATA ACCESS - Direct, high-performance operations
    // =========================================================================

    /// Get a record by key - O(1) for indexed tables
    ///
    /// This is the fundamental operation all others build upon.
    /// Implementations should optimize this heavily.
    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>>;

    /// Check if a key exists - O(1) optimized
    ///
    /// Often faster than get_record(222) for existence checks.
    fn contains_key(&self, key: &str) -> bool;

    /// Get record count - O(1) if possible
    fn record_count(&self) -> usize;

    /// Check if table is empty - O(1)
    fn is_empty(&self) -> bool {
        self.record_count() == 0
    }

    // =========================================================================
    // ITERATION INTERFACE - Memory-efficient data access
    // =========================================================================

    /// Iterate over all records without loading into memory
    ///
    /// Returns an iterator for memory-efficient access.
    /// Implementations should avoid cloning data.
    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_>;

    // Note: iter_filtered removed to make trait dyn-compatible
    // Use iter_records().filter() instead

    // =========================================================================
    // SQL QUERY INTERFACE - Optimized SQL operations
    // =========================================================================

    /// Execute a SQL WHERE filter
    ///
    /// Optimized implementation that:
    /// - Detects key lookups (WHERE key = 'value') for O(1) access
    /// - Uses iterators to avoid full table clones
    /// - Caches parsed queries for repeated use
    fn sql_filter(&self, where_clause: &str) -> TableResult<HashMap<String, FieldValue>> {
        // Default implementation - subclasses can override for optimization
        self.sql_filter_with_optimizer(where_clause)
    }

    /// Execute an EXISTS query with early termination
    ///
    /// Stops at the first matching record for optimal performance.
    fn sql_exists(&self, where_clause: &str) -> TableResult<bool> {
        // Default implementation uses iterator with early termination
        self.sql_exists_optimized(where_clause)
    }

    /// Extract column values matching a condition
    ///
    /// Returns values from a specific column for all matching records.
    fn sql_column_values(&self, column: &str, where_clause: &str) -> TableResult<Vec<FieldValue>>;

    /// Extract column values matching an AST expression predicate
    ///
    /// Evaluates the `Expr` AST directly against table records using
    /// `ExpressionEvaluator`, avoiding the lossy string round-trip through
    /// `CachedPredicate` which silently falls back to `AlwaysTrue` for
    /// unsupported patterns (OR, NOT, LIKE, BETWEEN, IS NULL, etc.).
    fn sql_column_values_with_expr(
        &self,
        column: &str,
        where_expr: &Expr,
    ) -> TableResult<Vec<FieldValue>> {
        // Default: iterate records and evaluate expr against each
        let mut values = Vec::new();
        for (_key, record) in self.iter_records() {
            if evaluate_expr_against_record(where_expr, &record)? {
                if let Some(value) = record.get(column) {
                    values.push(value.clone());
                }
            }
        }
        Ok(values)
    }

    /// Execute an EXISTS query using an AST expression predicate
    ///
    /// Evaluates the `Expr` AST directly, with early termination on first match.
    fn sql_exists_with_expr(&self, where_expr: &Expr) -> TableResult<bool> {
        for (_key, record) in self.iter_records() {
            if evaluate_expr_against_record(where_expr, &record)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Execute a scalar query using an AST expression predicate
    ///
    /// Handles aggregates (COUNT, SUM, AVG, MAX, MIN) and single-row selects
    /// using direct AST evaluation instead of string-based CachedPredicate parsing.
    fn sql_scalar_with_expr(
        &self,
        select_expr: &str,
        where_expr: &Expr,
    ) -> TableResult<FieldValue> {
        let upper = select_expr.to_uppercase();
        // Check named aggregates first (handles COUNT(DISTINCT), SUM, AVG, etc.)
        if let Some(column) = extract_aggregate_column(&upper, select_expr) {
            let values = self.sql_column_values_with_expr(&column, where_expr)?;
            compute_aggregate(&upper, &values)
        } else if upper.starts_with("COUNT") {
            // Plain COUNT(*) — full scan
            let mut count = 0i64;
            for (_key, record) in self.iter_records() {
                if evaluate_expr_against_record(where_expr, &record)? {
                    count += 1;
                }
            }
            Ok(FieldValue::Integer(count))
        } else {
            // Single-row select: get first matching value
            let values = self.sql_column_values_with_expr(select_expr, where_expr)?;
            Ok(values.into_iter().next().unwrap_or(FieldValue::Null))
        }
    }

    /// Execute a scalar query (single value result)
    ///
    /// Handles aggregates (COUNT, MAX, MIN, etc.) and single-row selects.
    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> TableResult<FieldValue>;

    // =========================================================================
    // ADVANCED WILDCARD QUERY INTERFACE - For complex nested data structures
    // =========================================================================

    /// Extract values using wildcard patterns in field paths
    ///
    /// Supports sophisticated wildcard queries for nested JSON/struct data:
    /// - Single-level wildcard: `portfolio.positions.*.shares`
    /// - Deep recursive wildcard: `portfolio.positions.**.shares`
    /// - Array wildcard: `orders[*].amount`
    /// - Array indexing: `orders[1].amount`
    /// - Conditional queries: `portfolio.positions.*.shares > 100`
    ///
    /// # Arguments
    /// * `wildcard_expr` - Expression with wildcards and optional conditions
    ///
    /// # Returns
    /// * `Ok(Vec<FieldValue>)` - Values matching the wildcard pattern
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use velostream::velostream::table::unified_table::UnifiedTable;
    /// # fn example(table: &dyn UnifiedTable) -> Result<(), velostream::velostream::sql::error::SqlError> {
    /// // Find all positions with shares > 100
    /// let large_positions = table.sql_wildcard_values("portfolio.positions.*.shares > 100")?;
    ///
    /// // Get all user emails regardless of user ID
    /// let all_emails = table.sql_wildcard_values("users.*.email")?;
    ///
    /// // Extract all order amounts from arrays
    /// let order_amounts = table.sql_wildcard_values("orders[*].amount")?;
    ///
    /// // Deep recursive search for any field named 'price'
    /// let all_prices = table.sql_wildcard_values("**.price")?;
    /// # Ok(())
    /// # }
    /// ```
    fn sql_wildcard_values(&self, wildcard_expr: &str) -> TableResult<Vec<FieldValue>> {
        // Default implementation with comprehensive wildcard support
        // Validate input
        if wildcard_expr.is_empty() {
            return Err(SqlError::ParseError {
                message: "Empty wildcard expression".to_string(),
                position: Some(0),
            });
        }

        // Parse the wildcard expression
        let query = parse_wildcard_expression(wildcard_expr)?;
        let mut matching_values = Vec::new();

        // Iterate through all records and collect matching values
        for (_key, record) in self.iter_records() {
            let record_value = FieldValue::Struct(record);
            collect_wildcard_matches(&record_value, &query, &mut matching_values);
        }

        Ok(matching_values)
    }

    /// Execute aggregate functions on wildcard query results
    ///
    /// Supports all standard SQL aggregate functions on wildcard-matched values:
    /// - COUNT: Count matching values
    /// - MAX/MIN: Maximum/minimum numeric values
    /// - AVG: Average of numeric values
    /// - SUM: Sum of numeric values
    ///
    /// # Arguments
    /// * `aggregate_expr` - Aggregate function with wildcard path
    ///
    /// # Returns
    /// * `Ok(FieldValue)` - Computed aggregate value
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use velostream::velostream::table::unified_table::UnifiedTable;
    /// # fn example(table: &dyn UnifiedTable) -> Result<(), velostream::velostream::sql::error::SqlError> {
    /// // Count all positions
    /// let count = table.sql_wildcard_aggregate("COUNT(portfolio.positions.*)")?;
    ///
    /// // Maximum market value across all positions
    /// let max_value = table.sql_wildcard_aggregate("MAX(portfolio.positions.*.market_value)")?;
    ///
    /// // Average balance across all users
    /// let avg_balance = table.sql_wildcard_aggregate("AVG(users.*.balance)")?;
    ///
    /// // Sum of all order amounts
    /// let total_orders = table.sql_wildcard_aggregate("SUM(orders[*].amount)")?;
    /// # Ok(())
    /// # }
    /// ```
    fn sql_wildcard_aggregate(&self, aggregate_expr: &str) -> TableResult<FieldValue> {
        // Default implementation with full aggregate support
        let expr = aggregate_expr.trim();

        // Extract function name and field path
        let (func_name, field_path) = parse_aggregate_expression(expr)?;

        // Get all matching values
        let values = self.sql_wildcard_values(&field_path)?;

        // Apply the aggregate function
        apply_aggregate_function(&func_name, &values, aggregate_expr)
    }

    // =========================================================================
    // ASYNC STREAMING INTERFACE - For large datasets
    // =========================================================================

    /// Stream all records asynchronously
    ///
    /// For processing datasets larger than memory.
    async fn stream_all(&self) -> StreamResult<RecordStream>;

    /// Stream filtered records asynchronously
    ///
    /// Applies filtering during streaming to minimize memory usage.
    async fn stream_filter(&self, where_clause: &str) -> StreamResult<RecordStream>;

    /// Query records in batches
    ///
    /// Returns configurable batch sizes for controlled memory usage.
    async fn query_batch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> StreamResult<RecordBatch>;

    /// Count records asynchronously
    ///
    /// Useful for large tables where counting might be expensive.
    async fn stream_count(&self, where_clause: Option<&str>) -> StreamResult<usize>;

    /// Perform streaming aggregation
    ///
    /// Computes aggregates (SUM, AVG, etc.) without loading all data.
    async fn stream_aggregate(
        &self,
        aggregate_expr: &str,
        where_clause: Option<&str>,
    ) -> StreamResult<FieldValue>;

    // =========================================================================
    // PERFORMANCE OPTIMIZATION HOOKS - Override for better performance
    // =========================================================================

    /// Fast path for key-based lookups
    ///
    /// Override this for O(1) key access optimization.
    /// Default falls back to general sql_filter.
    fn sql_get_by_key(&self, key: &str) -> TableResult<Option<FieldValue>> {
        match self.get_record(key)? {
            Some(record) => Ok(Some(FieldValue::Struct(record))),
            None => Ok(None),
        }
    }

    /// Optimized EXISTS for key checks
    ///
    /// Override for O(1) key existence checks.
    fn sql_exists_key(&self, key: &str) -> bool {
        self.contains_key(key)
    }

    // =========================================================================
    // INTERNAL OPTIMIZATION METHODS - Default implementations
    // =========================================================================

    /// Internal: Optimized SQL filter with query analysis
    fn sql_filter_with_optimizer(
        &self,
        where_clause: &str,
    ) -> TableResult<HashMap<String, FieldValue>> {
        // Detect simple key lookups: "key = 'value'" or "_key = 'value'"
        if let Some(key_value) = parse_key_lookup(where_clause) {
            let mut result = HashMap::new();
            if let Some(record) = self.get_record(&key_value)? {
                result.insert(key_value, FieldValue::Struct(record));
            }
            return Ok(result);
        }

        // Fallback to general filtering
        let mut result = HashMap::new();
        let predicate = parse_where_clause(where_clause)?;

        for (key, record) in self.iter_records() {
            if predicate(&key, &record) {
                result.insert(key, FieldValue::Struct(record));
            }
        }

        Ok(result)
    }

    /// Internal: Optimized EXISTS with early termination
    fn sql_exists_optimized(&self, where_clause: &str) -> TableResult<bool> {
        // Detect simple key existence: "key = 'value'"
        if let Some(key_value) = parse_key_lookup(where_clause) {
            return Ok(self.contains_key(&key_value));
        }

        // General existence check with early termination
        let predicate = parse_where_clause(where_clause)?;

        for (key, record) in self.iter_records() {
            if predicate(&key, &record) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

// ============================================================================
// HELPER FUNCTIONS - Outside trait for dyn compatibility
// ============================================================================

/// Evaluate an AST expression against a table record (HashMap<String, FieldValue>).
///
/// Wraps the record in a minimal `StreamRecord` and delegates to
/// `ExpressionEvaluator::evaluate_expression`, which already handles all SQL
/// expression types (AND, OR, NOT, IN, NOT IN, LIKE, BETWEEN, IS NULL, etc.).
pub fn evaluate_expr_against_record(
    expr: &Expr,
    record: &HashMap<String, FieldValue>,
) -> TableResult<bool> {
    let stream_record = StreamRecord::new(record.clone());
    ExpressionEvaluator::evaluate_expression(expr, &stream_record).map_err(|e| {
        let field_types: Vec<String> = record
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v.type_name()))
            .collect();
        SqlError::ExecutionError {
            message: format!(
                "Failed to evaluate expression against table record: {} | expr={:?} | record_fields=[{}]",
                e,
                expr,
                field_types.join(", ")
            ),
            query: None,
        }
    })
}

/// Recognized scalar aggregate function names.
///
/// **Ordering constraint**: longer prefixes must come before shorter ones that
/// share the same prefix (e.g., `STDDEV_SAMP` before `STDDEV_POP`, `FIRST_VALUE`
/// before hypothetical `FIRST`) because matching uses `starts_with`.
const SCALAR_AGGREGATE_NAMES: &[&str] = &[
    "STDDEV_SAMP",
    "STDDEV_POP",
    "VAR_SAMP",
    "VAR_POP",
    "FIRST_VALUE",
    "LAST_VALUE",
    "LISTAGG",
    "COLLECT",
    "COUNT",
    "SUM",
    "AVG",
    "MAX",
    "MIN",
];

/// Extract the column name from an aggregate expression like `SUM(col)`, `AVG(col)`, etc.
///
/// Returns `None` if the expression is not a recognized aggregate or has bad syntax.
/// For `DISTINCT` variants (e.g. `COUNT(DISTINCT col)`), strips the DISTINCT keyword
/// and returns just the column name; the caller uses the uppercased function string
/// to detect DISTINCT.
fn extract_aggregate_column(upper: &str, original: &str) -> Option<String> {
    // Plain COUNT(*) or COUNT without DISTINCT is handled by the fast COUNT path.
    // COUNT(DISTINCT col) routes through here so compute_aggregate can deduplicate.
    if upper.starts_with("COUNT(DISTINCT") || upper.starts_with("COUNT (DISTINCT") {
        // falls through to the generic extraction below
    } else if upper.starts_with("COUNT") {
        return None; // plain COUNT — use the dedicated fast path
    }

    for name in SCALAR_AGGREGATE_NAMES {
        if upper.starts_with(name) {
            if let Some(start) = original.find('(') {
                // Find matching closing paren (handles nested parens)
                if let Some(end) = find_matching_close_paren(original, start) {
                    let mut inner = original[start + 1..end].trim().to_string();
                    // Strip DISTINCT keyword if present
                    let inner_upper = inner.to_uppercase();
                    if inner_upper.starts_with("DISTINCT ") {
                        inner = inner[9..].trim().to_string();
                    }
                    if !inner.is_empty() && inner != "*" {
                        return Some(inner);
                    }
                }
            }
        }
    }
    None
}

/// Find the index of the closing `)` that matches the `(` at `open_pos`,
/// handling nested parentheses correctly.
fn find_matching_close_paren(s: &str, open_pos: usize) -> Option<usize> {
    let mut depth = 0u32;
    for (i, ch) in s[open_pos..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open_pos + i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Convert a `FieldValue` to `f64` for numeric aggregation.
fn field_value_to_f64(val: &FieldValue) -> Option<f64> {
    crate::velostream::sql::execution::aggregation::compute::field_value_to_f64(val)
}

/// Check whether the uppercased function string contains DISTINCT.
fn is_distinct(upper: &str) -> bool {
    upper.contains("DISTINCT")
}

/// Deduplicate values, preserving insertion order.
///
/// Uses O(n^2) contains check rather than hashing since `FieldValue` doesn't
/// implement `Hash`. Table records in subquery context are typically small,
/// so this is acceptable.
fn deduplicate_values(values: &[FieldValue]) -> Vec<FieldValue> {
    let mut result = Vec::new();
    for val in values {
        if !result.contains(val) {
            result.push(val.clone());
        }
    }
    result
}

/// Validate that all non-null values are numeric. Returns an error naming the
/// aggregate function and the first offending value's type if a non-numeric,
/// non-null value is found.
fn require_numeric_values(func_name: &str, values: &[FieldValue]) -> TableResult<()> {
    for v in values {
        match v {
            FieldValue::Integer(_)
            | FieldValue::Float(_)
            | FieldValue::ScaledInteger(_, _)
            | FieldValue::Null => {}
            other => {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "{} requires numeric input, got {}",
                        func_name,
                        other.type_name()
                    ),
                    query: None,
                });
            }
        }
    }
    Ok(())
}

/// Compute an aggregate over a slice of `FieldValue`s.
///
/// Supports SUM, AVG, MAX, MIN, FIRST_VALUE, LAST_VALUE, COUNT(DISTINCT),
/// SUM(DISTINCT), LISTAGG/COLLECT, STDDEV_POP/SAMP, VAR_POP/SAMP.
///
/// Type rules (SQL standard):
/// - SUM, AVG, STDDEV, VAR: numeric types only — returns error on strings
/// - MAX, MIN: any ordered type — numeric, string (lexicographic), timestamp
/// - COUNT, LISTAGG, COLLECT, FIRST_VALUE, LAST_VALUE: any type
fn compute_aggregate(upper: &str, values: &[FieldValue]) -> TableResult<FieldValue> {
    if values.is_empty() {
        return Ok(FieldValue::Null);
    }

    let distinct = is_distinct(upper);
    let effective_values;
    let vals: &[FieldValue] = if distinct {
        effective_values = deduplicate_values(values);
        &effective_values
    } else {
        values
    };

    // Filter out nulls for non-null-aware operations
    let non_null: Vec<&FieldValue> = vals
        .iter()
        .filter(|v| !matches!(v, FieldValue::Null))
        .collect();

    if upper.starts_with("COUNT") {
        // COUNT(DISTINCT col) — plain COUNT(*) never reaches here
        Ok(FieldValue::Integer(vals.len() as i64))
    } else if upper.starts_with("SUM") {
        require_numeric_values("SUM", vals)?;
        let numerics: Vec<f64> = vals.iter().filter_map(field_value_to_f64).collect();
        if numerics.is_empty() {
            Ok(FieldValue::Null)
        } else {
            let sum: f64 = numerics.iter().sum();
            let all_integer = vals
                .iter()
                .all(|v| matches!(v, FieldValue::Integer(_) | FieldValue::Null));
            if all_integer && sum.fract() == 0.0 {
                Ok(FieldValue::Integer(sum as i64))
            } else {
                Ok(FieldValue::Float(sum))
            }
        }
    } else if upper.starts_with("AVG") {
        require_numeric_values("AVG", vals)?;
        let numerics: Vec<f64> = vals.iter().filter_map(field_value_to_f64).collect();
        if numerics.is_empty() {
            Ok(FieldValue::Null)
        } else {
            let sum: f64 = numerics.iter().sum();
            Ok(FieldValue::Float(sum / numerics.len() as f64))
        }
    } else if upper.starts_with("MAX") {
        compute_max_min(&non_null, true)
    } else if upper.starts_with("MIN") {
        compute_max_min(&non_null, false)
    } else if upper.starts_with("FIRST_VALUE") {
        Ok(non_null
            .first()
            .map(|v| (*v).clone())
            .unwrap_or(FieldValue::Null))
    } else if upper.starts_with("LAST_VALUE") {
        Ok(non_null
            .last()
            .map(|v| (*v).clone())
            .unwrap_or(FieldValue::Null))
    } else if upper.starts_with("STDDEV_SAMP") {
        require_numeric_values("STDDEV_SAMP", vals)?;
        Ok(compute_stddev(vals, true))
    } else if upper.starts_with("STDDEV_POP") {
        require_numeric_values("STDDEV_POP", vals)?;
        Ok(compute_stddev(vals, false))
    } else if upper.starts_with("VAR_SAMP") {
        require_numeric_values("VAR_SAMP", vals)?;
        Ok(compute_variance(vals, true))
    } else if upper.starts_with("VAR_POP") {
        require_numeric_values("VAR_POP", vals)?;
        Ok(compute_variance(vals, false))
    } else if upper.starts_with("LISTAGG") || upper.starts_with("COLLECT") {
        let strings: Vec<String> = non_null
            .iter()
            .map(|v| match v {
                FieldValue::String(s) => s.clone(),
                other => format!("{}", other),
            })
            .collect();
        Ok(FieldValue::String(strings.join(",")))
    } else {
        Ok(FieldValue::Null)
    }
}

/// Compute MAX or MIN across any ordered `FieldValue` type.
///
/// Supports: numeric (Integer/Float/ScaledInteger), String (lexicographic),
/// Timestamp, and Boolean. Returns error for mixed incompatible types.
fn compute_max_min(non_null: &[&FieldValue], is_max: bool) -> TableResult<FieldValue> {
    if non_null.is_empty() {
        return Ok(FieldValue::Null);
    }

    // Determine the dominant type category from the first non-null value
    enum TypeCategory {
        Numeric,
        StringType,
        TimestampType,
        BooleanType,
    }

    let category = match non_null[0] {
        FieldValue::Integer(_) | FieldValue::Float(_) | FieldValue::ScaledInteger(_, _) => {
            TypeCategory::Numeric
        }
        FieldValue::String(_) => TypeCategory::StringType,
        FieldValue::Timestamp(_) => TypeCategory::TimestampType,
        FieldValue::Boolean(_) => TypeCategory::BooleanType,
        _ => {
            return Ok(FieldValue::Null);
        }
    };

    match category {
        TypeCategory::Numeric => {
            // Allow mixed numeric types (Int + Float + ScaledInteger)
            let all_integer = non_null.iter().all(|v| matches!(v, FieldValue::Integer(_)));
            let mut result: Option<f64> = None;
            for v in non_null {
                if let Some(f) = field_value_to_f64(v) {
                    result = Some(match result {
                        Some(cur) => {
                            if is_max {
                                f64::max(cur, f)
                            } else {
                                f64::min(cur, f)
                            }
                        }
                        None => f,
                    });
                } else {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "{} cannot mix numeric and non-numeric types",
                            if is_max { "MAX" } else { "MIN" }
                        ),
                        query: None,
                    });
                }
            }
            Ok(result
                .map(|v| {
                    if all_integer {
                        FieldValue::Integer(v as i64)
                    } else {
                        FieldValue::Float(v)
                    }
                })
                .unwrap_or(FieldValue::Null))
        }
        TypeCategory::StringType => {
            let mut best: Option<&str> = None;
            for v in non_null {
                match v {
                    FieldValue::String(s) => {
                        best = Some(match best {
                            Some(cur) => {
                                if is_max {
                                    if s.as_str() > cur { s.as_str() } else { cur }
                                } else if s.as_str() < cur {
                                    s.as_str()
                                } else {
                                    cur
                                }
                            }
                            None => s.as_str(),
                        });
                    }
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "{} cannot mix string and non-string types",
                                if is_max { "MAX" } else { "MIN" }
                            ),
                            query: None,
                        });
                    }
                }
            }
            Ok(best
                .map(|s| FieldValue::String(s.to_string()))
                .unwrap_or(FieldValue::Null))
        }
        TypeCategory::TimestampType => {
            let mut best: Option<&chrono::NaiveDateTime> = None;
            for v in non_null {
                match v {
                    FieldValue::Timestamp(ts) => {
                        best = Some(match best {
                            Some(cur) => {
                                if is_max {
                                    if ts > cur { ts } else { cur }
                                } else if ts < cur {
                                    ts
                                } else {
                                    cur
                                }
                            }
                            None => ts,
                        });
                    }
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "{} cannot mix timestamp and non-timestamp types",
                                if is_max { "MAX" } else { "MIN" }
                            ),
                            query: None,
                        });
                    }
                }
            }
            Ok(best
                .map(|ts| FieldValue::Timestamp(*ts))
                .unwrap_or(FieldValue::Null))
        }
        TypeCategory::BooleanType => {
            // SQL: false < true, so MAX = true if any true, MIN = false if any false
            let has_true = non_null
                .iter()
                .any(|v| matches!(v, FieldValue::Boolean(true)));
            let has_false = non_null
                .iter()
                .any(|v| matches!(v, FieldValue::Boolean(false)));
            if is_max {
                Ok(FieldValue::Boolean(has_true))
            } else {
                Ok(FieldValue::Boolean(!has_false))
            }
        }
    }
}

/// Compute variance (population or sample) over numeric `FieldValue`s.
fn compute_variance(values: &[FieldValue], sample: bool) -> FieldValue {
    use crate::velostream::sql::execution::aggregation::compute;
    let numerics: Vec<f64> = values.iter().filter_map(field_value_to_f64).collect();
    match compute::compute_variance_from_values(&numerics, sample) {
        Some(var) => FieldValue::Float(var),
        None => FieldValue::Null,
    }
}

/// Compute standard deviation (population or sample) over numeric `FieldValue`s.
fn compute_stddev(values: &[FieldValue], sample: bool) -> FieldValue {
    use crate::velostream::sql::execution::aggregation::compute;
    let numerics: Vec<f64> = values.iter().filter_map(field_value_to_f64).collect();
    match compute::compute_stddev_from_values(&numerics, sample) {
        Some(sd) => FieldValue::Float(sd),
        None => FieldValue::Null,
    }
}

/// Detect a key-lookup pattern from an AST `Expr`.
///
/// Matches `key = 'value'` or `_key = 'value'` patterns, returning the key value
/// for O(1) hash-map lookup on `OptimizedTableImpl`.
fn extract_key_lookup_from_expr(expr: &Expr) -> Option<String> {
    use crate::velostream::sql::execution::types::StreamRecord;

    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Equal,
        right,
    } = expr
    {
        if let Expr::Column(col) = left.as_ref() {
            if col == "key" || col == StreamRecord::FIELD_KEY {
                if let Expr::Literal(LiteralValue::String(val)) = right.as_ref() {
                    return Some(val.clone());
                }
            }
        }
        // Also handle reversed: 'value' = key
        if let Expr::Column(col) = right.as_ref() {
            if col == "key" || col == StreamRecord::FIELD_KEY {
                if let Expr::Literal(LiteralValue::String(val)) = left.as_ref() {
                    return Some(val.clone());
                }
            }
        }
    }
    None
}

/// Detect a simple column = 'value' filter from an AST `Expr`.
///
/// Returns `(column_name, string_value)` for O(1) column-index lookup on
/// `OptimizedTableImpl`.
fn extract_column_filter_from_expr(expr: &Expr) -> Option<(String, String)> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Equal,
        right,
    } = expr
    {
        if let Expr::Column(col) = left.as_ref() {
            if let Expr::Literal(LiteralValue::String(val)) = right.as_ref() {
                return Some((col.clone(), val.clone()));
            }
        }
        // Also handle reversed: 'value' = column
        if let Expr::Column(col) = right.as_ref() {
            if let Expr::Literal(LiteralValue::String(val)) = left.as_ref() {
                return Some((col.clone(), val.clone()));
            }
        }
    }
    None
}

/// Parse simple key lookup patterns
///
/// Returns the key value if this is a simple "key = 'value'" query.
pub fn parse_key_lookup(where_clause: &str) -> Option<String> {
    use crate::velostream::sql::execution::types::StreamRecord;

    let clause = where_clause.trim();

    // Match patterns: key = 'value', key = "value", _key = 'value'
    for key_name in &["key", StreamRecord::FIELD_KEY] {
        let patterns = [
            format!("{} = '", key_name),
            format!("{} = \"", key_name),
            format!("{}='", key_name),
            format!("{}=\"", key_name),
        ];

        for pattern in &patterns {
            if clause.starts_with(pattern) {
                let quote_char = if pattern.ends_with('\'') { '\'' } else { '"' };
                if let Some(end_pos) = clause[pattern.len()..].find(quote_char) {
                    let value = &clause[pattern.len()..pattern.len() + end_pos];
                    // Ensure this is the complete clause (no AND/OR)
                    let remaining = &clause[pattern.len() + end_pos + 1..].trim();
                    if remaining.is_empty() {
                        return Some(value.to_string());
                    }
                }
            }
        }
    }

    None
}

/// Parse comparison operator expressions like "field > 123" or "field <= 45.67"
///
/// Returns (field_name, operator, value) if pattern matches
fn parse_comparison_operator(clause: &str) -> Option<(String, String, String)> {
    let clause = clause.trim();

    // Try different comparison operators in order of specificity
    for op in &[">=", "<=", ">", "<"] {
        if let Some(pos) = clause.find(op) {
            let field_part = clause[..pos].trim();
            let value_part = clause[pos + op.len()..].trim();

            if !field_part.is_empty() && !value_part.is_empty() {
                // Handle table.column prefixes in field names
                let field = if let Some(dot_pos) = field_part.rfind('.') {
                    field_part[dot_pos + 1..].to_string()
                } else {
                    field_part.to_string()
                };

                return Some((field, op.to_string(), value_part.to_string()));
            }
        }
    }

    None
}

/// Result of parsing an IN operator - can be string or integer list
#[derive(Debug)]
enum InOperatorResult {
    StringList(String, Vec<String>),
    IntegerList(String, Vec<i64>),
}

/// Parse IN operator expressions like "field IN ('value1', 'value2')" or "field IN (1, 2, 3)"
///
/// Returns field name and values, detecting whether values are all integers or strings.
/// Uses proper quote-aware parsing to handle commas within quoted values.
///
/// # Limitations
/// - Does NOT support `NOT IN` - these are handled by the full SQL evaluator
/// - Does NOT support subqueries `IN (SELECT ...)` - only literal value lists
///
/// # Examples
/// ```ignore
/// // Supported:
/// parse_in_operator("tier IN ('gold', 'platinum')") // -> Some(StringList)
/// parse_in_operator("status IN (1, 2, 3)")          // -> Some(IntegerList)
/// parse_in_operator("t.field IN ('a', 'b')")        // -> Some(StringList), field="field"
///
/// // Not supported (returns None):
/// parse_in_operator("status NOT IN (1, 2)")         // -> None (NOT IN)
/// parse_in_operator("id IN (SELECT ...)")           // -> None (subquery)
/// ```
fn parse_in_operator(clause: &str) -> Option<InOperatorResult> {
    let clause = clause.trim();

    // Case-insensitive detection
    let upper_clause = clause.to_uppercase();

    // Skip NOT IN - not supported by CachedPredicate, handled by SQL evaluator
    if upper_clause.contains(" NOT IN ") {
        return None;
    }

    let in_pos = upper_clause.find(" IN ")?;

    let field_part = clause[..in_pos].trim();
    let values_part = clause[in_pos + 4..].trim(); // Skip " IN "

    if field_part.is_empty() || !values_part.starts_with('(') || !values_part.ends_with(')') {
        return None;
    }

    // Validate field name doesn't contain spaces (would indicate malformed expression)
    if field_part.contains(' ') {
        return None;
    }

    // Handle table.column prefixes in field names
    let field = if let Some(dot_pos) = field_part.rfind('.') {
        field_part[dot_pos + 1..].to_string()
    } else {
        field_part.to_string()
    };

    // Parse the list of values inside parentheses using quote-aware splitting
    let inner = &values_part[1..values_part.len() - 1]; // Remove ( and )
    let mut string_values = Vec::new();
    let mut integer_values = Vec::new();
    let mut all_integers = true;

    // Quote-aware comma splitting
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';

    for ch in inner.chars() {
        match ch {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current.push(ch);
            }
            c if c == quote_char && in_quotes => {
                in_quotes = false;
                current.push(c);
            }
            ',' if !in_quotes => {
                process_in_value(
                    &current,
                    &mut string_values,
                    &mut integer_values,
                    &mut all_integers,
                );
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    // Process the last value
    if !current.is_empty() {
        process_in_value(
            &current,
            &mut string_values,
            &mut integer_values,
            &mut all_integers,
        );
    }

    if string_values.is_empty() && integer_values.is_empty() {
        return None;
    }

    // Return appropriate variant based on detected types
    if all_integers && !integer_values.is_empty() {
        Some(InOperatorResult::IntegerList(field, integer_values))
    } else {
        Some(InOperatorResult::StringList(field, string_values))
    }
}

/// Helper to process a single IN value, detecting type
fn process_in_value(
    raw: &str,
    string_values: &mut Vec<String>,
    integer_values: &mut Vec<i64>,
    all_integers: &mut bool,
) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }

    // Extract value from quotes (single or double)
    let value = if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        // Quoted value - definitely a string
        *all_integers = false;
        &trimmed[1..trimmed.len() - 1]
    } else {
        // Unquoted - might be integer
        trimmed
    };

    string_values.push(value.to_string());

    // Try to parse as integer
    if *all_integers {
        if let Ok(i) = value.parse::<i64>() {
            integer_values.push(i);
        } else {
            *all_integers = false;
            integer_values.clear(); // Discard partial integer list
        }
    }
}

/// Parse WHERE clause into a cached predicate for optimized evaluation.
///
/// This function converts a WHERE clause string into a [`CachedPredicate`] enum
/// that can be evaluated efficiently against records without re-parsing.
///
/// # Performance Benefits
///
/// - **Pre-compilation**: Predicates are parsed once and reused for all records
/// - **Enum dispatch**: Uses match instead of closure boxing (no heap allocation)
/// - **Inlined evaluation**: Hot path evaluation is `#[inline]` for maximum performance
///
/// # Supported Patterns
///
/// ```text
/// status = 'active'              -> FieldEqualsString
/// id = 123                       -> FieldEqualsInteger
/// amount > 100                   -> FieldGreaterThan
/// tier IN ('gold', 'platinum')   -> FieldInStringList
/// status IN (1, 2, 3)            -> FieldInIntegerList
/// true / 1=1                     -> AlwaysTrue
/// false / 1=0                    -> AlwaysFalse
/// ```
///
/// # Fallback Behavior
///
/// Unrecognized patterns return `AlwaysTrue` with a warning log. This is safer
/// than `AlwaysFalse` as it avoids silently filtering all records. Complex
/// expressions (AND/OR, NOT IN, BETWEEN, etc.) are handled by the full SQL evaluator.
///
/// # Examples
///
/// ```ignore
/// let predicate = parse_where_clause_cached("tier IN ('gold', 'platinum')")?;
/// assert!(predicate.evaluate("key", &record)); // if record.tier == "gold"
/// ```
/// Find the position of a top-level AND keyword in a WHERE clause.
///
/// Returns the byte offset of the first top-level `AND` (case-insensitive),
/// skipping any AND that appears inside single/double quotes or parentheses.
fn find_top_level_and(clause: &str) -> Option<usize> {
    let upper = clause.to_uppercase();
    let bytes = upper.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut paren_depth: i32 = 0;

    while i < len {
        let ch = bytes[i];
        match ch {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            b'(' if !in_single_quote && !in_double_quote => paren_depth += 1,
            b')' if !in_single_quote && !in_double_quote => paren_depth -= 1,
            b'A' if !in_single_quote && !in_double_quote && paren_depth == 0 => {
                // Check for " AND " (with word boundaries)
                if i + 3 <= len
                    && bytes[i + 1] == b'N'
                    && bytes[i + 2] == b'D'
                    && (i == 0 || bytes[i - 1] == b' ')
                    && (i + 3 == len || bytes[i + 3] == b' ')
                {
                    // Skip AND that is part of BETWEEN ... AND ...
                    // Check if there's a BETWEEN keyword before this AND
                    // at the same paren level that hasn't been consumed
                    let prefix = &upper[..i];
                    let is_between_and = prefix
                        .rfind("BETWEEN")
                        .map(|bp| {
                            // Verify no other top-level AND between BETWEEN and here
                            !prefix[bp + 7..].contains(" AND ")
                        })
                        .unwrap_or(false);
                    if !is_between_and {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

pub fn parse_where_clause_cached(where_clause: &str) -> TableResult<CachedPredicate> {
    let clause = where_clause.trim();

    // Handle 'true' literal and common "always true" patterns FIRST
    // (before parse_simple_equality which would match "1=1" as field=value)
    if clause == "true" || clause == "1=1" || clause == "1 = 1" {
        return Ok(CachedPredicate::AlwaysTrue);
    }

    // Handle 'false' literal and common "always false" patterns
    if clause == "false" || clause == "1=0" || clause == "1 = 0" {
        return Ok(CachedPredicate::AlwaysFalse);
    }

    // Handle compound AND expressions BEFORE simple patterns.
    // This prevents parse_simple_equality from greedily consuming the entire
    // compound clause as a single equality (e.g., "field = 'val' AND ..." would
    // incorrectly match as field = "'val' AND ...").
    if let Some(and_pos) = find_top_level_and(clause) {
        let left_clause = clause[..and_pos].trim();
        let right_clause = clause[and_pos + 3..].trim(); // skip "AND"
        let left = parse_where_clause_cached(left_clause)?;
        let right = parse_where_clause_cached(right_clause)?;
        return Ok(CachedPredicate::And(Box::new(left), Box::new(right)));
    }

    // Handle simple equality comparisons: "field = 'value'"
    if let Some((field, value)) = parse_simple_equality(clause) {
        // Try to parse as different types for optimal evaluation
        if let Ok(int_val) = value.parse::<i64>() {
            return Ok(CachedPredicate::FieldEqualsInteger {
                field,
                value: int_val,
            });
        }
        if let Ok(float_val) = value.parse::<f64>() {
            return Ok(CachedPredicate::FieldEqualsFloat {
                field,
                value: float_val,
            });
        }
        if let Ok(bool_val) = value.parse::<bool>() {
            return Ok(CachedPredicate::FieldEqualsBoolean {
                field,
                value: bool_val,
            });
        }
        // Default to string comparison
        return Ok(CachedPredicate::FieldEqualsString { field, value });
    }

    // Handle comparison operators: "field > value", "field < value", etc.
    if let Some((field, operator, value)) = parse_comparison_operator(clause) {
        if let Ok(numeric_val) = value.parse::<f64>() {
            return Ok(match operator.as_str() {
                ">" => CachedPredicate::FieldGreaterThan {
                    field,
                    value: numeric_val,
                },
                "<" => CachedPredicate::FieldLessThan {
                    field,
                    value: numeric_val,
                },
                ">=" => CachedPredicate::FieldGreaterThanOrEqual {
                    field,
                    value: numeric_val,
                },
                "<=" => CachedPredicate::FieldLessThanOrEqual {
                    field,
                    value: numeric_val,
                },
                _ => return Ok(CachedPredicate::AlwaysFalse),
            });
        }
    }

    // Handle IN operator: "field IN ('value1', 'value2')" or "field IN (1, 2, 3)"
    if let Some(in_result) = parse_in_operator(clause) {
        return Ok(match in_result {
            InOperatorResult::StringList(field, values) => {
                CachedPredicate::FieldInStringList { field, values }
            }
            InOperatorResult::IntegerList(field, values) => {
                CachedPredicate::FieldInIntegerList { field, values }
            }
        });
    }

    // For unrecognized patterns, return AlwaysTrue to avoid silently filtering all records.
    // This is safer than AlwaysFalse which would cause data loss without warning.
    // Complex WHERE clauses (AND/OR) are handled by the full SQL evaluator, not this cache.
    log::warn!(
        "Unrecognized WHERE clause pattern for predicate caching: '{}'. \
         Defaulting to AlwaysTrue (no filtering). Complex expressions are evaluated by SQL engine.",
        clause
    );
    Ok(CachedPredicate::AlwaysTrue)
}

/// Legacy parse WHERE clause into a predicate function (backwards compatibility)
///
/// **Deprecated**: Use `parse_where_clause_cached` for better performance
#[allow(clippy::type_complexity)]
fn parse_where_clause(
    where_clause: &str,
) -> TableResult<Box<dyn Fn(&str, &HashMap<String, FieldValue>) -> bool>> {
    let predicate = parse_where_clause_cached(where_clause)?;
    Ok(Box::new(move |key, record| predicate.evaluate(key, record)))
}

/// Parse simple equality expressions like "field = 'value'" or "field = value"
///
/// **Performance Optimized**:
/// - Uses pre-compiled regex patterns
/// - Minimizes string allocations
/// - Handles table.column prefixes efficiently
fn parse_simple_equality(clause: &str) -> Option<(String, String)> {
    let captures = EQUALITY_REGEX.captures(clause)?;

    let field_raw = captures.get(1)?.as_str();
    let value_part = captures.get(2)?.as_str();

    // Extract field name efficiently (handle table.column -> column)
    let field = if let Some(table_captures) = TABLE_COLUMN_REGEX.captures(field_raw) {
        // Extract column name from table.column pattern
        table_captures.get(2)?.as_str().to_string()
    } else {
        // No table prefix, use field as-is
        field_raw.to_string()
    };

    // Extract value efficiently (remove quotes if present)
    let value = if let Some(quoted_captures) = QUOTED_STRING_REGEX.captures(value_part) {
        // Extract content from quoted string
        quoted_captures.get(1)?.as_str().to_string()
    } else {
        // Unquoted value
        value_part.to_string()
    };

    Some((field, value))
}

// Legacy adapter removed - OptimizedTableImpl is the preferred implementation

// ============================================================================
// PERFORMANCE-OPTIMIZED REFERENCE IMPLEMENTATION
// ============================================================================

/// High-performance implementation showing best practices
///
/// Features:
/// - O(1) key lookups with HashMap storage
/// - Query plan caching to avoid re-parsing
/// - Zero-copy iteration where possible
/// - Built-in performance monitoring
/// - Memory-efficient string interning
#[derive(Clone)]
pub struct OptimizedTableImpl {
    /// Core data storage - O(1) key access
    data: Arc<RwLock<HashMap<String, HashMap<String, FieldValue>>>>,

    /// Query plan cache for repeated queries
    query_cache: Arc<RwLock<HashMap<String, CachedQuery>>>,

    /// Performance statistics
    stats: Arc<RwLock<TableStats>>,

    /// String interning pool for memory efficiency
    string_pool: Arc<RwLock<HashMap<String, Arc<String>>>>,

    /// Simple column indexes for common queries
    #[allow(clippy::type_complexity)]
    column_indexes: Arc<RwLock<HashMap<String, HashMap<String, Vec<String>>>>>,
}

/// Cached query plan for performance (optimized)
#[derive(Clone)]
struct CachedQuery {
    /// Parsed query type
    query_type: QueryType,
    /// Pre-compiled predicate for WHERE clauses (performance optimized)
    predicate: Option<CachedPredicate>,
    /// Legacy filter function for backward compatibility
    #[allow(clippy::type_complexity)]
    filter_fn: Option<Arc<dyn Fn(&HashMap<String, FieldValue>) -> bool + Send + Sync>>,
    /// Target column for column queries
    target_column: Option<String>,
    /// Last access time for cache eviction
    last_used: std::time::Instant,
}

/// Types of cached queries
#[derive(Debug, Clone)]
enum QueryType {
    /// Direct key lookup: key = 'value'
    KeyLookup(String),
    /// Column filter: column = 'value'
    ColumnFilter { column: String, value: String },
    /// Column extraction: SELECT column FROM table WHERE ...
    ColumnExtraction(String),
    /// Aggregate query: COUNT, SUM, etc.
    Aggregate {
        function: String,
        column: Option<String>,
    },
    /// Full scan with filter
    FullScan,
}

/// Performance statistics
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub record_count: usize,
    pub query_cache_hits: u64,
    pub query_cache_misses: u64,
    pub total_queries: u64,
    pub average_query_time_ms: f64,
    pub memory_usage_bytes: usize,
}

impl Default for OptimizedTableImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizedTableImpl {
    /// Create a new optimized table implementation
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            query_cache: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(TableStats::default())),
            string_pool: Arc::new(RwLock::new(HashMap::new())),
            column_indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert a record (not part of UnifiedTable trait, but useful for testing)
    pub fn insert(&self, key: String, record: HashMap<String, FieldValue>) -> TableResult<()> {
        let start_time = std::time::Instant::now();

        // Update data
        {
            let mut data = self.data.write().unwrap();
            data.insert(key.clone(), record.clone());
        }

        // Update column indexes
        {
            let mut indexes = self.column_indexes.write().unwrap();
            for (column_name, field_value) in &record {
                let column_index = indexes.entry(column_name.clone()).or_default();

                // Convert field value to index key
                let index_key = match field_value {
                    FieldValue::String(s) => s.clone(),
                    FieldValue::Integer(i) => i.to_string(),
                    FieldValue::ScaledInteger(value, _scale) => value.to_string(),
                    FieldValue::Float(f) => f.to_string(),
                    FieldValue::Boolean(b) => b.to_string(),
                    _ => continue, // Skip complex types for indexing
                };

                column_index.entry(index_key).or_default().push(key.clone());
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.record_count = self.data.read().unwrap().len();

            // Update average query time (rolling average)
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.total_queries += 1;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(())
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> TableStats {
        self.stats.read().unwrap().clone()
    }

    /// Clear query cache (useful for testing)
    pub fn clear_cache(&self) {
        self.query_cache.write().unwrap().clear();
    }

    /// Parse and cache a WHERE clause for optimization
    fn get_or_create_cached_query(&self, where_clause: &str) -> CachedQuery {
        let cache_key = where_clause.to_string();

        // Check cache first
        {
            let mut cache = self.query_cache.write().unwrap();
            if let Some(cached) = cache.get_mut(&cache_key) {
                cached.last_used = std::time::Instant::now();
                self.stats.write().unwrap().query_cache_hits += 1;
                return cached.clone();
            }
        }

        // Cache miss - parse the query
        self.stats.write().unwrap().query_cache_misses += 1;
        let cached_query = self.parse_where_clause(where_clause);

        // Store in cache
        {
            let mut cache = self.query_cache.write().unwrap();

            // Simple cache eviction - remove old entries if cache is too large
            if cache.len() > 1000 {
                let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(300);
                cache.retain(|_, query| query.last_used > cutoff);
            }

            cache.insert(cache_key, cached_query.clone());
        }

        cached_query
    }

    /// Parse a WHERE clause into an optimized query plan
    fn parse_where_clause(&self, where_clause: &str) -> CachedQuery {
        let clause = where_clause.trim();

        // Parse the predicate for WHERE clause filtering
        let predicate = match parse_where_clause_cached(clause) {
            Ok(p) => Some(p),
            Err(e) => {
                log::debug!(
                    "Failed to parse predicate for WHERE clause '{}': {:?}. Using no predicate filter.",
                    clause,
                    e
                );
                None
            }
        };

        // Detect key lookups
        if let Some(key_value) = parse_key_lookup(clause) {
            return CachedQuery {
                query_type: QueryType::KeyLookup(key_value),
                predicate,
                filter_fn: None,
                target_column: None,
                last_used: std::time::Instant::now(),
            };
        }

        // Detect simple column filters: column = 'value'
        if let Some((column, value)) = self.parse_column_filter(clause) {
            return CachedQuery {
                query_type: QueryType::ColumnFilter { column, value },
                predicate,
                filter_fn: None,
                target_column: None,
                last_used: std::time::Instant::now(),
            };
        }

        // Fallback to full scan with predicate for filtering
        CachedQuery {
            query_type: QueryType::FullScan,
            predicate,
            filter_fn: None,
            target_column: None,
            last_used: std::time::Instant::now(),
        }
    }

    /// Parse simple column filter patterns: column = 'value'
    fn parse_column_filter(&self, clause: &str) -> Option<(String, String)> {
        // Simple regex-like parsing for column = 'value' or column = "value"
        for quote in &['\'', '"'] {
            if let Some(eq_pos) = clause.find('=') {
                let column_part = clause[..eq_pos].trim();
                let value_part = clause[eq_pos + 1..].trim();

                if value_part.starts_with(*quote) && value_part.ends_with(*quote) {
                    let column = column_part.to_string();
                    let value = value_part[1..value_part.len() - 1].to_string();
                    return Some((column, value));
                }
            }
        }
        None
    }

    /// Full scan fallback for column value extraction
    fn full_scan_column_values(
        &self,
        column: &str,
        cached_query: &CachedQuery,
        values: &mut Vec<FieldValue>,
    ) {
        let data = self.data.read().unwrap();
        for (key, record) in data.iter() {
            // Apply predicate filtering if available
            let passes_filter = if let Some(ref predicate) = cached_query.predicate {
                predicate.evaluate(key, record)
            } else {
                true // No predicate means all records pass
            };

            if passes_filter {
                if let Some(value) = record.get(column) {
                    values.push(value.clone());
                }
            }
        }
    }

    // ============================================================================
    // O(1) JOIN OPTIMIZATION METHODS
    // ============================================================================

    /// O(1) lookup for join operations using column indexes
    ///
    /// This method provides massive performance improvement over O(n) linear search
    /// for stream-table joins by leveraging the existing column_indexes.
    ///
    /// **Performance**: O(1) vs O(n) - up to 95%+ improvement for large tables
    pub fn lookup_by_join_keys(
        &self,
        join_keys: &HashMap<String, FieldValue>,
    ) -> TableResult<Vec<HashMap<String, FieldValue>>> {
        let start_time = std::time::Instant::now();
        let mut matching_records = Vec::new();

        // Use column indexes for O(1) lookup when possible
        if join_keys.len() == 1 {
            // Single key lookup - use column index directly
            let (field_name, field_value) = join_keys.iter().next().unwrap();
            if let Some(records) = self.lookup_by_single_field(field_name, field_value)? {
                matching_records.extend(records);
            }
        } else {
            // Multi-key lookup - intersect results from multiple indexes
            matching_records = self.lookup_by_multiple_fields(join_keys)?;
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(matching_records)
    }

    /// O(1) lookup for single field using column index
    fn lookup_by_single_field(
        &self,
        field_name: &str,
        field_value: &FieldValue,
    ) -> TableResult<Option<Vec<HashMap<String, FieldValue>>>> {
        let indexes = self.column_indexes.read().unwrap();
        let data = self.data.read().unwrap();

        // Convert field value to index key
        let index_key = match field_value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::ScaledInteger(value, _scale) => value.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            _ => {
                // For unsupported field types, fall back to linear search
                return Ok(Some(self.fallback_linear_search_single(
                    field_name,
                    field_value,
                    &data,
                )));
            }
        };

        // Use column index for O(1) lookup
        if let Some(column_index) = indexes.get(field_name) {
            if let Some(record_keys) = column_index.get(&index_key) {
                let mut results = Vec::with_capacity(record_keys.len());
                for record_key in record_keys {
                    if let Some(record) = data.get(record_key) {
                        results.push(record.clone());
                    }
                }
                return Ok(Some(results));
            }
        }

        // Index not available - return None to trigger fallback
        Ok(None)
    }

    /// O(n) fallback for single field lookup when index is not available
    fn fallback_linear_search_single(
        &self,
        field_name: &str,
        field_value: &FieldValue,
        data: &HashMap<String, HashMap<String, FieldValue>>,
    ) -> Vec<HashMap<String, FieldValue>> {
        let mut results = Vec::new();
        for (_key, record) in data.iter() {
            if let Some(record_value) = record.get(field_name) {
                if record_value == field_value {
                    results.push(record.clone());
                }
            }
        }
        results
    }

    /// Optimized lookup for multiple fields using index intersection
    fn lookup_by_multiple_fields(
        &self,
        join_keys: &HashMap<String, FieldValue>,
    ) -> TableResult<Vec<HashMap<String, FieldValue>>> {
        let indexes = self.column_indexes.read().unwrap();
        let data = self.data.read().unwrap();

        // Find the most selective field (smallest index) to start with
        let mut candidate_keys: Option<Vec<String>> = None;
        let mut most_selective_count = usize::MAX;

        for (field_name, field_value) in join_keys {
            let index_key = match field_value {
                FieldValue::String(s) => s.clone(),
                FieldValue::Integer(i) => i.to_string(),
                FieldValue::ScaledInteger(value, _scale) => value.to_string(),
                FieldValue::Float(f) => f.to_string(),
                FieldValue::Boolean(b) => b.to_string(),
                _ => continue, // Skip unsupported types for indexing
            };

            if let Some(column_index) = indexes.get(field_name) {
                if let Some(record_keys) = column_index.get(&index_key) {
                    if record_keys.len() < most_selective_count {
                        most_selective_count = record_keys.len();
                        candidate_keys = Some(record_keys.clone());
                    }
                }
            }
        }

        // If no indexed fields found, fall back to linear search
        let candidate_keys = match candidate_keys {
            Some(keys) => keys,
            None => return Ok(self.fallback_linear_search_multiple(join_keys, &data)),
        };

        // Filter candidates by remaining join keys
        let mut results = Vec::new();
        for record_key in candidate_keys {
            if let Some(record) = data.get(&record_key) {
                let mut matches = true;
                for (field_name, required_value) in join_keys {
                    if let Some(record_value) = record.get(field_name) {
                        if record_value != required_value {
                            matches = false;
                            break;
                        }
                    } else {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    results.push(record.clone());
                }
            }
        }

        Ok(results)
    }

    /// O(n) fallback for multiple field lookup when indexes are not available
    fn fallback_linear_search_multiple(
        &self,
        join_keys: &HashMap<String, FieldValue>,
        data: &HashMap<String, HashMap<String, FieldValue>>,
    ) -> Vec<HashMap<String, FieldValue>> {
        let mut results = Vec::new();
        for (_key, record) in data.iter() {
            let mut matches = true;
            for (field_name, required_value) in join_keys {
                if let Some(record_value) = record.get(field_name) {
                    if record_value != required_value {
                        matches = false;
                        break;
                    }
                } else {
                    matches = false;
                    break;
                }
            }
            if matches {
                results.push(record.clone());
            }
        }
        results
    }

    /// Bulk lookup for batch processing optimization
    ///
    /// Processes multiple join key sets in a single operation,
    /// reducing overhead and improving cache efficiency.
    pub fn bulk_lookup_by_join_keys(
        &self,
        join_keys_batch: &[HashMap<String, FieldValue>],
    ) -> TableResult<Vec<Vec<HashMap<String, FieldValue>>>> {
        let mut results = Vec::with_capacity(join_keys_batch.len());

        for join_keys in join_keys_batch {
            let matches = self.lookup_by_join_keys(join_keys)?;
            results.push(matches);
        }

        Ok(results)
    }

    // =========================================================================
    // PHASE 7: UNIFIED TABLE LOADING METHODS
    // =========================================================================

    /// Load data from any DataSource using bulk loading pattern
    ///
    /// This method implements Phase 1 of the unified loading pattern by loading
    /// all available data from the source into the table.
    ///
    /// # Arguments
    /// - `data_source`: Any implementation of the DataSource trait
    /// - `config`: Optional loading configuration
    ///
    /// # Returns
    /// - `Ok(LoadingStats)`: Statistics about the loading operation
    /// - `Err(SqlError)`: If loading fails
    pub async fn bulk_load_from_source<T>(
        &mut self,
        data_source: &T,
        config: Option<crate::velostream::table::loading_helpers::LoadingConfig>,
    ) -> TableResult<crate::velostream::table::loading_helpers::LoadingStats>
    where
        T: crate::velostream::datasource::traits::DataSource,
    {
        let load_start = std::time::Instant::now();

        // Use the unified loading helper
        let records =
            crate::velostream::table::loading_helpers::bulk_load_table(data_source, config)
                .await
                .map_err(|e| SqlError::ExecutionError {
                    message: format!("Bulk load failed: {}", e),
                    query: None,
                })?;

        // Insert all records into the table
        {
            let mut data = self.data.write().unwrap();
            for record in &records {
                let key = if let Some(key_field) =
                    record.fields.get("id").or_else(|| record.fields.get("key"))
                {
                    match key_field {
                        crate::velostream::sql::execution::types::FieldValue::String(s) => {
                            s.clone()
                        }
                        crate::velostream::sql::execution::types::FieldValue::Integer(i) => {
                            i.to_string()
                        }
                        _ => key_field.to_display_string(),
                    }
                } else {
                    // Use offset as fallback key
                    record.offset.to_string()
                };
                data.insert(key, record.fields.clone());
            }
        }

        // Update table statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.record_count = self.data.read().unwrap().len();
        }

        let loading_stats = crate::velostream::table::loading_helpers::LoadingStats {
            bulk_records_loaded: records.len() as u64,
            bulk_load_duration_ms: load_start.elapsed().as_millis() as u64,
            incremental_records_loaded: 0,
            incremental_load_duration_ms: 0,
            total_load_operations: 1,
            failed_load_operations: 0,
            last_successful_load: Some(std::time::SystemTime::now()),
        };

        log::info!(
            "Bulk loaded {} records into OptimizedTableImpl in {:?}",
            records.len(),
            load_start.elapsed()
        );

        Ok(loading_stats)
    }

    /// Load incremental data from any DataSource using incremental loading pattern
    ///
    /// This method implements Phase 2 of the unified loading pattern by loading
    /// only new/changed data since a specific offset.
    ///
    /// # Arguments
    /// - `data_source`: Any implementation of the DataSource trait
    /// - `since_offset`: The offset to start loading from
    /// - `config`: Optional loading configuration
    ///
    /// # Returns
    /// - `Ok(LoadingStats)`: Statistics about the loading operation
    /// - `Err(SqlError)`: If loading fails
    pub async fn incremental_load_from_source<T>(
        &mut self,
        data_source: &T,
        since_offset: crate::velostream::datasource::types::SourceOffset,
        config: Option<crate::velostream::table::loading_helpers::LoadingConfig>,
    ) -> TableResult<crate::velostream::table::loading_helpers::LoadingStats>
    where
        T: crate::velostream::datasource::traits::DataSource,
    {
        let load_start = std::time::Instant::now();

        // Use the unified loading helper
        let records = crate::velostream::table::loading_helpers::incremental_load_table(
            data_source,
            since_offset,
            config,
        )
        .await
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Incremental load failed: {}", e),
            query: None,
        })?;

        // Insert/update records in the table
        {
            let mut data = self.data.write().unwrap();
            for record in &records {
                let key = if let Some(key_field) =
                    record.fields.get("id").or_else(|| record.fields.get("key"))
                {
                    match key_field {
                        crate::velostream::sql::execution::types::FieldValue::String(s) => {
                            s.clone()
                        }
                        crate::velostream::sql::execution::types::FieldValue::Integer(i) => {
                            i.to_string()
                        }
                        _ => key_field.to_display_string(),
                    }
                } else {
                    // Use offset as fallback key
                    record.offset.to_string()
                };
                data.insert(key, record.fields.clone());
            }
        }

        // Update table statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.record_count = self.data.read().unwrap().len();
        }

        let loading_stats = crate::velostream::table::loading_helpers::LoadingStats {
            bulk_records_loaded: 0,
            bulk_load_duration_ms: 0,
            incremental_records_loaded: records.len() as u64,
            incremental_load_duration_ms: load_start.elapsed().as_millis() as u64,
            total_load_operations: 1,
            failed_load_operations: 0,
            last_successful_load: Some(std::time::SystemTime::now()),
        };

        log::info!(
            "Incremental loaded {} records into OptimizedTableImpl in {:?}",
            records.len(),
            load_start.elapsed()
        );

        Ok(loading_stats)
    }

    /// Load data using the unified loading pattern (bulk or incremental based on offset)
    ///
    /// This is a convenience method that automatically chooses between bulk and
    /// incremental loading based on whether a previous offset is provided.
    ///
    /// # Arguments
    /// - `data_source`: Any implementation of the DataSource trait
    /// - `previous_offset`: Optional previous offset for incremental loading
    /// - `config`: Optional loading configuration
    ///
    /// # Returns
    /// - `Ok(LoadingStats)`: Statistics about the loading operation
    /// - `Err(SqlError)`: If loading fails
    pub async fn unified_load_from_source<T>(
        &mut self,
        data_source: &T,
        previous_offset: Option<crate::velostream::datasource::types::SourceOffset>,
        config: Option<crate::velostream::table::loading_helpers::LoadingConfig>,
    ) -> TableResult<crate::velostream::table::loading_helpers::LoadingStats>
    where
        T: crate::velostream::datasource::traits::DataSource,
    {
        match previous_offset {
            None => {
                // No previous offset: perform bulk load
                self.bulk_load_from_source(data_source, config).await
            }
            Some(offset) => {
                // Previous offset exists: perform incremental load
                self.incremental_load_from_source(data_source, offset, config)
                    .await
            }
        }
    }

    /// Check what loading capabilities a data source supports
    ///
    /// # Arguments
    /// - `data_source`: The data source to check
    ///
    /// # Returns
    /// - `(bool, bool)`: (supports_bulk, supports_incremental)
    pub fn check_loading_support<T>(&self, data_source: &T) -> (bool, bool)
    where
        T: crate::velostream::datasource::traits::DataSource,
    {
        crate::velostream::table::loading_helpers::check_loading_support(data_source)
    }
}

#[async_trait]
impl UnifiedTable for OptimizedTableImpl {
    /// Enable downcasting for O(1) join optimizations
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// O(1) HashMap lookup - extremely fast key access
    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        let start_time = std::time::Instant::now();

        let result = self.data.read().unwrap().get(key).cloned();

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(result)
    }

    /// O(1) key existence check - optimal performance
    fn contains_key(&self, key: &str) -> bool {
        self.data.read().unwrap().contains_key(key)
    }

    /// O(1) count from HashMap length
    fn record_count(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Zero-copy iterator over internal storage - memory efficient
    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        let data = self.data.read().unwrap();
        let records: Vec<_> = data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Box::new(records.into_iter())
    }

    /// Optimized column extraction with indexed lookups
    fn sql_column_values(&self, column: &str, where_clause: &str) -> TableResult<Vec<FieldValue>> {
        let start_time = std::time::Instant::now();

        // Parse and cache the query
        let cached_query = self.get_or_create_cached_query(where_clause);

        let mut values = Vec::new();

        match &cached_query.query_type {
            QueryType::KeyLookup(key) => {
                // O(1) key lookup
                if let Some(record) = self.data.read().unwrap().get(key) {
                    if let Some(value) = record.get(column) {
                        values.push(value.clone());
                    }
                }
            }
            QueryType::ColumnFilter {
                column: filter_col,
                value: filter_val,
            } => {
                // Use column index if available
                let indexes = self.column_indexes.read().unwrap();
                if let Some(column_index) = indexes.get(filter_col) {
                    if let Some(matching_keys) = column_index.get(filter_val) {
                        let data = self.data.read().unwrap();
                        for key in matching_keys {
                            if let Some(record) = data.get(key) {
                                if let Some(value) = record.get(column) {
                                    values.push(value.clone());
                                }
                            }
                        }
                    }
                } else {
                    // Fallback to full scan
                    self.full_scan_column_values(column, &cached_query, &mut values);
                }
            }
            _ => {
                // Fallback to full scan
                self.full_scan_column_values(column, &cached_query, &mut values);
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(values)
    }

    /// Optimized AST-based column value extraction
    ///
    /// Evaluates the `Expr` directly against table records held in the RwLock,
    /// avoiding the string round-trip and CachedPredicate limitations.
    fn sql_column_values_with_expr(
        &self,
        column: &str,
        where_expr: &Expr,
    ) -> TableResult<Vec<FieldValue>> {
        let data = self.data.read().unwrap();
        let mut values = Vec::new();
        for (_key, record) in data.iter() {
            if evaluate_expr_against_record(where_expr, record)? {
                if let Some(value) = record.get(column) {
                    values.push(value.clone());
                }
            }
        }
        Ok(values)
    }

    /// Optimized AST-based EXISTS with early termination
    fn sql_exists_with_expr(&self, where_expr: &Expr) -> TableResult<bool> {
        let data = self.data.read().unwrap();
        for (_key, record) in data.iter() {
            if evaluate_expr_against_record(where_expr, record)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Optimized AST-based scalar queries with aggregate support
    ///
    /// Detects key-lookup and column-filter patterns from the AST `Expr` to use
    /// O(1) hash lookups / column indexes (matching the string-based `sql_scalar`
    /// fast paths) before falling back to a full table scan.
    fn sql_scalar_with_expr(
        &self,
        select_expr: &str,
        where_expr: &Expr,
    ) -> TableResult<FieldValue> {
        let start_time = std::time::Instant::now();

        let upper = select_expr.to_uppercase();
        // Check named aggregates first (handles COUNT(DISTINCT), SUM, AVG, etc.)
        let result = if let Some(column) = extract_aggregate_column(&upper, select_expr) {
            let values = self.sql_column_values_with_expr(&column, where_expr)?;
            compute_aggregate(&upper, &values)?
        } else if upper.starts_with("COUNT") {
            // Plain COUNT — try O(1) optimizations first
            if let Some(key) = extract_key_lookup_from_expr(where_expr) {
                if self.data.read().unwrap().contains_key(&key) {
                    FieldValue::Integer(1)
                } else {
                    FieldValue::Integer(0)
                }
            } else if let Some((column, value)) = extract_column_filter_from_expr(where_expr) {
                let count = self
                    .column_indexes
                    .read()
                    .unwrap()
                    .get(&column)
                    .and_then(|idx| idx.get(&value))
                    .map(|keys| keys.len())
                    .unwrap_or(0);
                FieldValue::Integer(count as i64)
            } else {
                // Full scan fallback
                let data = self.data.read().unwrap();
                let mut count = 0i64;
                for (_key, record) in data.iter() {
                    if evaluate_expr_against_record(where_expr, record)? {
                        count += 1;
                    }
                }
                FieldValue::Integer(count)
            }
        } else {
            let values = self.sql_column_values_with_expr(select_expr, where_expr)?;
            values.into_iter().next().unwrap_or(FieldValue::Null)
        };

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(result)
    }

    /// Optimized scalar queries with aggregate support
    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> TableResult<FieldValue> {
        let start_time = std::time::Instant::now();

        // Parse and cache the query
        let cached_query = self.get_or_create_cached_query(where_clause);

        let result = if select_expr.to_uppercase().starts_with("COUNT") {
            // Optimized COUNT queries
            match &cached_query.query_type {
                QueryType::KeyLookup(key) => {
                    if self.data.read().unwrap().contains_key(key) {
                        FieldValue::Integer(1)
                    } else {
                        FieldValue::Integer(0)
                    }
                }
                QueryType::ColumnFilter { column, value } => {
                    let count = self
                        .column_indexes
                        .read()
                        .unwrap()
                        .get(column)
                        .and_then(|idx| idx.get(value))
                        .map(|keys| keys.len())
                        .unwrap_or(0);
                    FieldValue::Integer(count as i64)
                }
                _ => FieldValue::Integer(self.record_count() as i64),
            }
        } else if select_expr.to_uppercase().starts_with("SUM") {
            // SUM aggregation queries
            let upper_expr = select_expr.to_uppercase();
            if let Some(start) = upper_expr.find('(') {
                if let Some(end) = upper_expr.find(')') {
                    let column = &select_expr[start + 1..end];
                    let values = self.sql_column_values(column, where_clause)?;

                    let sum = values.iter().fold(0i64, |acc, val| match val {
                        FieldValue::Integer(i) => acc + i,
                        FieldValue::ScaledInteger(value, _) => acc + value,
                        FieldValue::Float(f) => acc + (*f as i64),
                        _ => acc,
                    });

                    FieldValue::Integer(sum)
                } else {
                    FieldValue::Null
                }
            } else {
                FieldValue::Null
            }
        } else {
            // For other scalar queries, get the first matching value
            let column_values = self.sql_column_values(select_expr, where_clause)?;
            column_values.into_iter().next().unwrap_or(FieldValue::Null)
        };

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            let query_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            stats.average_query_time_ms =
                (stats.average_query_time_ms * (stats.total_queries - 1) as f64 + query_time_ms)
                    / stats.total_queries as f64;
        }

        Ok(result)
    }

    /// High-performance async streaming implementation
    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Clone data for async processing
        let data = self.data.read().unwrap().clone();

        tokio::spawn(async move {
            for (key, record) in data {
                let stream_record = SimpleStreamRecord {
                    key,
                    fields: record,
                };
                if tx.send(Ok(stream_record)).is_err() {
                    break;
                }
            }
        });

        Ok(crate::velostream::table::streaming::RecordStream { receiver: rx })
    }

    /// Filtered async streaming with query optimization
    async fn stream_filter(&self, where_clause: &str) -> StreamResult<RecordStream> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Parse and cache the query
        let cached_query = self.get_or_create_cached_query(where_clause);
        let data = self.data.clone();
        let column_indexes = self.column_indexes.clone();

        tokio::spawn(async move {
            match &cached_query.query_type {
                QueryType::KeyLookup(key) => {
                    // O(1) key lookup
                    if let Some(record) = data.read().unwrap().get(key) {
                        let stream_record = SimpleStreamRecord {
                            key: key.clone(),
                            fields: record.clone(),
                        };
                        let _ = tx.send(Ok(stream_record));
                    }
                }
                QueryType::ColumnFilter { column, value } => {
                    // Use column index if available
                    let indexes = column_indexes.read().unwrap();
                    if let Some(column_index) = indexes.get(column) {
                        if let Some(matching_keys) = column_index.get(value) {
                            let data_guard = data.read().unwrap();
                            for key in matching_keys {
                                if let Some(record) = data_guard.get(key) {
                                    let stream_record = SimpleStreamRecord {
                                        key: key.clone(),
                                        fields: record.clone(),
                                    };
                                    if tx.send(Ok(stream_record)).is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Fallback to full scan
                    let data_guard = data.read().unwrap();
                    for (key, record) in data_guard.iter() {
                        let stream_record = SimpleStreamRecord {
                            key: key.clone(),
                            fields: record.clone(),
                        };
                        if tx.send(Ok(stream_record)).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        Ok(crate::velostream::table::streaming::RecordStream { receiver: rx })
    }

    /// Efficient batch processing with controlled memory usage
    async fn query_batch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> StreamResult<crate::velostream::table::streaming::RecordBatch> {
        let offset = offset.unwrap_or(0);
        let data = self.data.read().unwrap();

        let records: Vec<SimpleStreamRecord> = data
            .iter()
            .skip(offset)
            .take(batch_size)
            .map(|(key, fields)| SimpleStreamRecord {
                key: key.clone(),
                fields: fields.clone(),
            })
            .collect();

        let has_more = offset + records.len() < data.len();

        Ok(crate::velostream::table::streaming::RecordBatch { records, has_more })
    }

    /// O(1) counting for fast analytics
    async fn stream_count(&self, where_clause: Option<&str>) -> StreamResult<usize> {
        if let Some(clause) = where_clause {
            // Use optimized filtering count
            let cached_query = self.get_or_create_cached_query(clause);

            match &cached_query.query_type {
                QueryType::KeyLookup(key) => Ok(if self.data.read().unwrap().contains_key(key) {
                    1
                } else {
                    0
                }),
                QueryType::ColumnFilter { column, value } => {
                    let count = self
                        .column_indexes
                        .read()
                        .unwrap()
                        .get(column)
                        .and_then(|idx| idx.get(value))
                        .map(|keys| keys.len())
                        .unwrap_or(0);
                    Ok(count)
                }
                _ => Ok(self.record_count()),
            }
        } else {
            Ok(self.record_count())
        }
    }

    /// High-performance streaming aggregation
    async fn stream_aggregate(
        &self,
        aggregate_expr: &str,
        where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        let upper_expr = aggregate_expr.to_uppercase();

        if upper_expr.starts_with("COUNT") {
            let count = self.stream_count(where_clause).await?;
            Ok(FieldValue::Integer(count as i64))
        } else if upper_expr.starts_with("SUM") {
            // Extract column name from SUM(column)
            if let Some(start) = upper_expr.find('(') {
                if let Some(end) = upper_expr.find(')') {
                    let column = &aggregate_expr[start + 1..end];
                    let values = if let Some(clause) = where_clause {
                        self.sql_column_values(column, clause)?
                    } else {
                        self.sql_column_values(column, "1=1")?
                    };

                    let sum = values.iter().fold(0i64, |acc, val| match val {
                        FieldValue::Integer(i) => acc + i,
                        FieldValue::ScaledInteger(value, _) => acc + value,
                        FieldValue::Float(f) => acc + (*f as i64),
                        _ => acc,
                    });

                    return Ok(FieldValue::Integer(sum));
                }
            }
            Ok(FieldValue::Null)
        } else {
            // Other aggregates not implemented yet
            Ok(FieldValue::Null)
        }
    }
}

// =========================================================================
// ADVANCED WILDCARD QUERY IMPLEMENTATION
// =========================================================================

/// Path component for wildcard queries
#[derive(Debug, Clone, PartialEq)]
enum PathPart {
    /// Regular field name: "field"
    Field(String),
    /// Single-level wildcard: "*"
    Wildcard,
    /// Deep recursive wildcard: "**"
    DeepWildcard,
    /// Array wildcard: "[*]"
    ArrayWildcard,
    /// Specific array index: "[5]"
    ArrayIndex(usize),
    /// Predicate filter: "[field > 100]"
    Predicate(String),
}

/// Parsed wildcard query with optional condition
#[derive(Debug, Clone)]
struct WildcardQuery {
    /// Parsed path components
    path_parts: Vec<PathPart>,
    /// Optional comparison operator and value
    condition: Option<WildcardCondition>,
}

/// Condition for wildcard filtering
#[derive(Debug, Clone)]
struct WildcardCondition {
    /// Comparison operator: ">", "<", ">=", "<=", "=", "!="
    operator: String,
    /// Comparison value (parsed as f64)
    value: f64,
}

/// Parse a wildcard expression into components and optional condition
fn parse_wildcard_expression(expr: &str) -> TableResult<WildcardQuery> {
    // Check for conditional operators
    let condition_operators = [" >= ", " <= ", " > ", " < ", " = ", " != "];
    let mut condition = None;
    let mut path_expr = expr;

    for op in &condition_operators {
        if let Some(pos) = expr.find(op) {
            let field_path = &expr[..pos];
            let value_str = &expr[pos + op.len()..].trim();

            let value = value_str.parse::<f64>().map_err(|_| SqlError::ParseError {
                message: format!("Invalid numeric value in condition: {}", value_str),
                position: Some(pos + op.len()),
            })?;

            condition = Some(WildcardCondition {
                operator: op.trim().to_string(),
                value,
            });
            path_expr = field_path;
            break;
        }
    }

    // Parse the path components
    let path_parts = parse_path_components(path_expr)?;

    Ok(WildcardQuery {
        path_parts,
        condition,
    })
}

/// Parse a path string into path components with array notation support
fn parse_path_components(path: &str) -> TableResult<Vec<PathPart>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if !current.is_empty() {
                    parts.push(parse_single_component(&current)?);
                    current.clear();
                }
            }
            '[' => {
                // Handle field name before bracket
                if !current.is_empty() {
                    parts.push(parse_single_component(&current)?);
                    current.clear();
                }

                // Parse bracket content
                let mut bracket_content = String::new();
                let mut bracket_depth = 1;

                for bracket_ch in chars.by_ref() {
                    if bracket_ch == '[' {
                        bracket_depth += 1;
                    } else if bracket_ch == ']' {
                        bracket_depth -= 1;
                        if bracket_depth == 0 {
                            break;
                        }
                    }
                    bracket_content.push(bracket_ch);
                }

                if bracket_depth != 0 {
                    return Err(SqlError::ParseError {
                        message: "Unmatched brackets in path".to_string(),
                        position: Some(path.len()),
                    });
                }

                // Parse bracket content
                let bracket_content = bracket_content.trim();
                if bracket_content == "*" {
                    parts.push(PathPart::ArrayWildcard);
                } else if let Ok(index) = bracket_content.parse::<usize>() {
                    parts.push(PathPart::ArrayIndex(index));
                } else {
                    parts.push(PathPart::Predicate(bracket_content.to_string()));
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    // Handle final component
    if !current.is_empty() {
        parts.push(parse_single_component(&current)?);
    }

    Ok(parts)
}

/// Parse a single path component (field name or wildcard)
fn parse_single_component(component: &str) -> TableResult<PathPart> {
    match component {
        "*" => Ok(PathPart::Wildcard),
        "**" => Ok(PathPart::DeepWildcard),
        _ => Ok(PathPart::Field(component.to_string())),
    }
}

/// Recursively collect values matching the wildcard pattern
fn collect_wildcard_matches(
    current: &FieldValue,
    query: &WildcardQuery,
    results: &mut Vec<FieldValue>,
) {
    collect_wildcard_recursive(current, &query.path_parts, 0, &query.condition, results);
}

/// Recursive helper for wildcard pattern matching
fn collect_wildcard_recursive(
    current: &FieldValue,
    parts: &[PathPart],
    index: usize,
    condition: &Option<WildcardCondition>,
    results: &mut Vec<FieldValue>,
) {
    if index >= parts.len() {
        // Reached the end of the path - check condition and collect value
        if let Some(cond) = condition {
            if evaluate_condition(current, cond) {
                results.push(current.clone());
            }
        } else {
            results.push(current.clone());
        }
        return;
    }

    match (&parts[index], current) {
        (PathPart::Field(field_name), FieldValue::Struct(fields)) => {
            if let Some(value) = fields.get(field_name) {
                collect_wildcard_recursive(value, parts, index + 1, condition, results);
            }
        }
        (PathPart::Wildcard, FieldValue::Struct(fields)) => {
            // Single-level wildcard - match all fields at this level
            for value in fields.values() {
                collect_wildcard_recursive(value, parts, index + 1, condition, results);
            }
        }
        (PathPart::DeepWildcard, FieldValue::Struct(fields)) => {
            // Deep recursive wildcard - search at any depth
            for value in fields.values() {
                // Try continuing from next part
                if index + 1 < parts.len() {
                    collect_wildcard_recursive(value, parts, index + 1, condition, results);
                }
                // Also recurse deeper with same pattern
                collect_wildcard_recursive(value, parts, index, condition, results);
            }
        }
        (PathPart::ArrayWildcard, FieldValue::Array(arr)) => {
            // Process all array elements
            for element in arr {
                collect_wildcard_recursive(element, parts, index + 1, condition, results);
            }
        }
        (PathPart::ArrayIndex(idx), FieldValue::Array(arr)) => {
            // Access specific array index
            if let Some(element) = arr.get(*idx) {
                collect_wildcard_recursive(element, parts, index + 1, condition, results);
            }
        }
        (PathPart::Predicate(_predicate), _) => {
            // TODO: Implement predicate filtering
            // For now, treat as passthrough
            collect_wildcard_recursive(current, parts, index + 1, condition, results);
        }
        _ => {
            // Type mismatch or unsupported pattern - skip
        }
    }
}

/// Evaluate a condition against a field value
fn evaluate_condition(value: &FieldValue, condition: &WildcardCondition) -> bool {
    let numeric_value = match extract_numeric_value(value) {
        Some(v) => v,
        None => return false,
    };

    match condition.operator.as_str() {
        ">" => numeric_value > condition.value,
        "<" => numeric_value < condition.value,
        ">=" => numeric_value >= condition.value,
        "<=" => numeric_value <= condition.value,
        "=" => (numeric_value - condition.value).abs() < f64::EPSILON,
        "!=" => (numeric_value - condition.value).abs() >= f64::EPSILON,
        _ => false,
    }
}

/// Extract numeric value from a FieldValue for comparisons
fn extract_numeric_value(value: &FieldValue) -> Option<f64> {
    crate::velostream::sql::execution::aggregation::compute::field_value_to_f64(value)
}

/// Parse an aggregate expression to extract function and path
fn parse_aggregate_expression(expr: &str) -> TableResult<(String, String)> {
    let expr = expr.trim();

    let start_pos = expr.find('(').ok_or_else(|| SqlError::ParseError {
        message: "Invalid aggregate expression: missing opening parenthesis".to_string(),
        position: Some(0),
    })?;

    let end_pos = expr.rfind(')').ok_or_else(|| SqlError::ParseError {
        message: "Invalid aggregate expression: missing closing parenthesis".to_string(),
        position: Some(expr.len()),
    })?;

    let func_name = expr[..start_pos].trim().to_uppercase();
    let field_path = expr[start_pos + 1..end_pos].trim().to_string();

    Ok((func_name, field_path))
}

/// Apply an aggregate function to a list of values
fn apply_aggregate_function(
    func_name: &str,
    values: &[FieldValue],
    aggregate_expr: &str,
) -> TableResult<FieldValue> {
    match func_name {
        "COUNT" => Ok(FieldValue::Integer(values.len() as i64)),
        "MAX" => {
            let mut max_val: Option<f64> = None;
            for value in values {
                if let Some(num) = extract_numeric_value(value) {
                    max_val = Some(max_val.map_or(num, |m| m.max(num)));
                }
            }
            max_val
                .map(FieldValue::Float)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: "No numeric values found for MAX".to_string(),
                    query: Some(aggregate_expr.to_string()),
                })
        }
        "MIN" => {
            let mut min_val: Option<f64> = None;
            for value in values {
                if let Some(num) = extract_numeric_value(value) {
                    min_val = Some(min_val.map_or(num, |m| m.min(num)));
                }
            }
            min_val
                .map(FieldValue::Float)
                .ok_or_else(|| SqlError::ExecutionError {
                    message: "No numeric values found for MIN".to_string(),
                    query: Some(aggregate_expr.to_string()),
                })
        }
        "AVG" => {
            let mut sum = 0.0;
            let mut count = 0;
            for value in values {
                if let Some(num) = extract_numeric_value(value) {
                    sum += num;
                    count += 1;
                }
            }
            if count > 0 {
                Ok(FieldValue::Float(sum / count as f64))
            } else {
                Err(SqlError::ExecutionError {
                    message: "No numeric values found for AVG".to_string(),
                    query: Some(aggregate_expr.to_string()),
                })
            }
        }
        "SUM" => {
            let mut sum = 0.0;
            for value in values {
                if let Some(num) = extract_numeric_value(value) {
                    sum += num;
                }
            }
            Ok(FieldValue::Float(sum))
        }
        _ => Err(SqlError::ExecutionError {
            message: format!("Unsupported aggregate function: {}", func_name),
            query: Some(aggregate_expr.to_string()),
        }),
    }
}
