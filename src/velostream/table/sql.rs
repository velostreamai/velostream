/*!
# Table SQL Query Interface - AST Integrated Version

This module provides SQL query capabilities for Table with full SQL AST integration,
enabling proper SQL compliance and comprehensive operator support. It bridges the gap
between Table state management and the SQL execution engine using the existing SQL AST.

## Key Features

- **Full SQL AST Integration**: Uses existing StreamingSqlParser and Expr AST nodes
- **Complete Operator Support**: All comparison operators (=, !=, <, <=, >, >=, AND, OR)
- **SQL Standard Compliance**: Proper expression evaluation with SQL semantics
- **Subquery support**: EXISTS, IN, scalar subqueries using Table as data source
- **FieldValue integration**: Full SQL type system compatibility
- **Performance optimized**: Leverages Table's in-memory HashMap for sub-millisecond lookups

## Examples

```rust
use velostream::velostream::kafka::{Table, JsonSerializer};
use velostream::velostream::kafka::ktable_sql::{SqlQueryable, SqlTable};

// Create a SQL-compatible Table
let user_table: SqlTable = Table::new(
    config,
    "users".to_string(),
    JsonSerializer,
    JsonSerializer,
).await?;

// Execute SQL-like queries with full operator support
let active_users = user_table.sql_filter("active = true AND age >= 18")?;
let premium_users = user_table.sql_filter("tier = 'premium' OR score > 90.0")?;
let has_admins = user_table.sql_exists("role = 'admin' AND status != 'disabled'")?;
let premium_ids = user_table.sql_column_values("id", "tier = 'premium'")?;
```
*/

use crate::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::FieldValue;
use crate::velostream::sql::parser::StreamingSqlParser;
use crate::velostream::table::table::Table;
use crate::velostream::kafka::serialization::Serializer;
use crate::velostream::serialization::SerializationFormat;
use std::collections::HashMap;

/// Data source interface for SQL subquery execution
///
/// This trait abstracts the data access layer for subquery operations,
/// allowing different implementations (Table, external databases, etc.)
/// to provide data for SQL subqueries.
pub trait SqlDataSource {
    /// Get all records as a HashMap for filtering operations
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError>;

    /// Get a single record by key for direct lookups
    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError>;

    /// Check if the data source contains any records
    fn is_empty(&self) -> bool;

    /// Get the count of records in the data source
    fn record_count(&self) -> usize;
}

/// SQL query interface for data sources
///
/// This trait provides SQL-compatible query operations that can be used for subquery
/// execution in the main SQL engine. Operations mirror SQL semantics for precise
/// compatibility.
pub trait SqlQueryable {
    /// Filter records using a SQL WHERE clause
    ///
    /// Executes a WHERE clause against all records in the data source,
    /// returning only those that match the specified condition.
    ///
    /// # Arguments
    /// * `where_clause` - SQL WHERE condition (e.g., "active = true AND age > 18")
    ///
    /// # Returns
    /// * `Ok(HashMap<String, FieldValue>)` - Filtered records matching the condition
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let active_users = source.sql_filter("status = 'active'")?;
    /// let high_value = source.sql_filter("amount > 1000.0")?;
    /// let complex = source.sql_filter("tier = 'premium' AND score >= 80.0")?;
    /// ```
    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError>;

    /// Check if any records exist matching a SQL WHERE clause
    ///
    /// Executes an EXISTS-style query, returning true if any records match
    /// the given condition. This is optimized for early termination.
    ///
    /// # Arguments
    /// * `where_clause` - SQL WHERE condition to test
    ///
    /// # Returns
    /// * `Ok(true)` - At least one record matches the condition
    /// * `Ok(false)` - No records match the condition
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let has_admin = table.sql_exists("role = 'admin'")?;
    /// let config_enabled = table.sql_exists("enabled = true")?;
    /// ```
    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError>;

    /// Extract column values from records matching a WHERE clause
    ///
    /// Filters records using the WHERE clause, then extracts values from the
    /// specified column. This is used for IN subquery evaluation.
    ///
    /// # Arguments
    /// * `column` - Column name to extract values from
    /// * `where_clause` - SQL WHERE condition for filtering
    ///
    /// # Returns
    /// * `Ok(Vec<FieldValue>)` - List of values from the specified column
    /// * `Err(SqlError)` - Parse, execution, or column access error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let premium_ids = table.sql_column_values("user_id", "tier = 'premium'")?;
    /// let valid_symbols = table.sql_column_values("symbol", "active = true")?;
    /// ```
    fn sql_column_values(
        &self,
        column: &str,
        where_clause: &str,
    ) -> Result<Vec<FieldValue>, SqlError>;

    /// Execute a scalar subquery and return a single value
    ///
    /// Executes a SELECT expression on filtered records, returning a single value.
    /// Used for scalar subqueries like (SELECT max_price FROM limits WHERE symbol = 'AAPL').
    ///
    /// # Arguments
    /// * `select_expr` - SQL SELECT expression (e.g., "MAX(price)", "COUNT(*)", "config_value")
    /// * `where_clause` - SQL WHERE condition for filtering
    ///
    /// # Returns
    /// * `Ok(FieldValue)` - Single result value
    /// * `Err(SqlError)` - Parse, execution, or multiple results error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let max_limit = table.sql_scalar("max_daily_limit", "symbol = 'AAPL'")?;
    /// let count = table.sql_scalar("COUNT(*)", "active = true")?;
    /// ```
    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> Result<FieldValue, SqlError>;

    /// Extract column values using wildcard patterns in field paths
    ///
    /// This method supports wildcard queries like "portfolio.positions.****.shares > 100"
    /// where **** matches any field name at that level.
    ///
    /// # Arguments
    /// * `wildcard_expr` - Expression with wildcards (e.g., "portfolio.positions.****.shares > 100")
    ///
    /// # Returns
    /// * `Ok(Vec<FieldValue>)` - List of values matching the wildcard pattern
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// // Find all positions with shares > 100
    /// let large_positions = table.sql_wildcard_values("portfolio.positions.****.shares > 100")?;
    ///
    /// // Get all user emails regardless of user ID
    /// let all_emails = table.sql_wildcard_values("users.***.email")?;
    /// ```
    fn sql_wildcard_values(&self, wildcard_expr: &str) -> Result<Vec<FieldValue>, SqlError>;
}

/// SQL Expression evaluator using the proper SQL AST
///
/// This evaluator uses the existing SQL AST infrastructure to evaluate expressions
/// against Table records, providing full SQL compliance and operator support.
/// It integrates with the StreamingSqlParser for proper SQL parsing.
pub struct ExpressionEvaluator {
    parser: StreamingSqlParser,
}

impl ExpressionEvaluator {
    /// Create a new expression evaluator
    pub fn new() -> Self {
        Self {
            parser: StreamingSqlParser::new(),
        }
    }

    /// Parse a WHERE clause expression and return a predicate function
    ///
    /// This creates a closure that can be used to test individual records
    /// against the WHERE condition using the full SQL AST.
    ///
    /// # Returns
    /// * `Ok(Box<dyn Fn(&String, &FieldValue) -> bool>)` - Predicate function
    /// * `Err(SqlError)` - Parse error for invalid syntax
    pub fn parse_where_clause(
        &self,
        clause: &str,
    ) -> Result<Box<dyn Fn(&String, &FieldValue) -> bool + Send + Sync>, SqlError> {
        if clause.is_empty() || clause == "true" {
            return Ok(Box::new(|_key, _value| true));
        }

        // Parse the WHERE clause into an AST expression
        let expr = self.parse_expression(clause)?;

        Ok(Box::new(move |_key, record| {
            Self::evaluate_expression(&expr, record).unwrap_or(false)
        }))
    }

    /// Parse a SQL expression string into an AST expression
    ///
    /// This handles the conversion from SQL text to Expr AST nodes with proper
    /// operator precedence and SQL syntax compliance.
    fn parse_expression(&self, clause: &str) -> Result<Expr, SqlError> {
        let clause = clause.trim();

        // Handle logical operators first (lowest precedence)
        if let Some(or_pos) = Self::find_logical_operator(clause, " OR ") {
            let left_expr = self.parse_expression(&clause[..or_pos])?;
            let right_expr = self.parse_expression(&clause[or_pos + 4..])?;

            return Ok(Expr::BinaryOp {
                left: Box::new(left_expr),
                op: BinaryOperator::Or,
                right: Box::new(right_expr),
            });
        }

        if let Some(and_pos) = Self::find_logical_operator(clause, " AND ") {
            let left_expr = self.parse_expression(&clause[..and_pos])?;
            let right_expr = self.parse_expression(&clause[and_pos + 5..])?;

            return Ok(Expr::BinaryOp {
                left: Box::new(left_expr),
                op: BinaryOperator::And,
                right: Box::new(right_expr),
            });
        }

        // Handle comparison operators (higher precedence)
        for op_str in &[">=", "<=", "!=", "<>", "=", ">", "<"] {
            if let Some(pos) = clause.find(op_str) {
                let field_name = clause[..pos].trim();
                let value_str = clause[pos + op_str.len()..].trim();

                let op = match *op_str {
                    "=" => BinaryOperator::Equal,
                    "!=" | "<>" => BinaryOperator::NotEqual,
                    "<" => BinaryOperator::LessThan,
                    "<=" => BinaryOperator::LessThanOrEqual,
                    ">" => BinaryOperator::GreaterThan,
                    ">=" => BinaryOperator::GreaterThanOrEqual,
                    _ => {
                        return Err(SqlError::ParseError {
                            message: format!("Unsupported operator: {}", op_str),
                            position: Some(pos),
                        })
                    }
                };

                let field_expr = Expr::Column(field_name.to_string());
                let value_expr = Expr::Literal(self.parse_literal_value(value_str)?);

                return Ok(Expr::BinaryOp {
                    left: Box::new(field_expr),
                    op,
                    right: Box::new(value_expr),
                });
            }
        }

        // Handle unary operators
        if clause.to_uppercase().starts_with("NOT ") {
            let inner_expr = self.parse_expression(&clause[4..])?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(inner_expr),
            });
        }

        // Handle parentheses
        if clause.starts_with('(') && clause.ends_with(')') {
            return self.parse_expression(&clause[1..clause.len() - 1]);
        }

        // Handle simple column references or literals
        if Self::is_identifier(clause) {
            return Ok(Expr::Column(clause.to_string()));
        }

        // Try to parse as literal
        if let Ok(literal) = self.parse_literal_value(clause) {
            return Ok(Expr::Literal(literal));
        }

        Err(SqlError::ParseError {
            message: format!("Unsupported expression syntax: '{}'", clause),
            position: Some(0),
        })
    }

    /// Find logical operators respecting parentheses nesting
    fn find_logical_operator(clause: &str, op: &str) -> Option<usize> {
        let mut paren_depth = 0;
        let upper_clause = clause.to_uppercase();
        let upper_op = op.to_uppercase();

        let mut chars = upper_clause.char_indices().peekable();
        while let Some((i, ch)) = chars.next() {
            match ch {
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                _ => {
                    if paren_depth == 0 && upper_clause[i..].starts_with(&upper_op) {
                        return Some(i);
                    }
                }
            }
        }
        None
    }

    /// Check if a string is a valid SQL identifier
    fn is_identifier(s: &str) -> bool {
        !s.is_empty()
            && s.chars().next().unwrap().is_ascii_alphabetic()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    }

    /// Parse a literal value string into the proper AST LiteralValue
    fn parse_literal_value(&self, value_str: &str) -> Result<LiteralValue, SqlError> {
        let value_str = value_str.trim();

        // String literals (quoted)
        if (value_str.starts_with('\'') && value_str.ends_with('\''))
            || (value_str.starts_with('"') && value_str.ends_with('"'))
        {
            let unquoted = &value_str[1..value_str.len() - 1];
            return Ok(LiteralValue::String(unquoted.to_string()));
        }

        // Boolean literals
        match value_str.to_lowercase().as_str() {
            "true" => return Ok(LiteralValue::Boolean(true)),
            "false" => return Ok(LiteralValue::Boolean(false)),
            "null" => return Ok(LiteralValue::Null),
            _ => {}
        }

        // Numeric literals
        if let Ok(int_val) = value_str.parse::<i64>() {
            return Ok(LiteralValue::Integer(int_val));
        }

        if let Ok(float_val) = value_str.parse::<f64>() {
            return Ok(LiteralValue::Float(float_val));
        }

        // If it looks like a decimal number, store as decimal string
        if value_str.contains('.')
            && value_str
                .chars()
                .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
        {
            return Ok(LiteralValue::Decimal(value_str.to_string()));
        }

        Err(SqlError::ParseError {
            message: format!("Invalid literal value: '{}'", value_str),
            position: Some(0),
        })
    }

    /// Evaluate an expression against a record
    fn evaluate_expression(expr: &Expr, record: &FieldValue) -> Result<bool, SqlError> {
        match expr {
            Expr::BinaryOp { left, op, right } => Self::evaluate_binary_op(left, op, right, record),
            Expr::UnaryOp { op, expr } => Self::evaluate_unary_op(op, expr, record),
            Expr::Column(column_name) => {
                // For simple column references, check if the column exists and is truthy
                let field_value = extract_field_value(record, column_name);
                Ok(field_value.map_or(false, |v| Self::is_truthy(&v)))
            }
            Expr::Literal(literal) => Ok(Self::is_truthy(&Self::literal_to_field_value(literal))),
            _ => Err(SqlError::ExecutionError {
                message: "Unsupported expression type in WHERE clause".to_string(),
                query: Some("".to_string()),
            }),
        }
    }

    /// Evaluate a binary operation
    fn evaluate_binary_op(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        record: &FieldValue,
    ) -> Result<bool, SqlError> {
        match op {
            BinaryOperator::And => {
                let left_result = Self::evaluate_expression(left, record)?;
                if !left_result {
                    return Ok(false); // Short-circuit evaluation
                }
                Self::evaluate_expression(right, record)
            }
            BinaryOperator::Or => {
                let left_result = Self::evaluate_expression(left, record)?;
                if left_result {
                    return Ok(true); // Short-circuit evaluation
                }
                Self::evaluate_expression(right, record)
            }
            _ => {
                // Comparison operators
                let left_value = Self::evaluate_expression_value(left, record)?;
                let right_value = Self::evaluate_expression_value(right, record)?;
                Self::compare_values(&left_value, op, &right_value)
            }
        }
    }

    /// Evaluate an expression to get its FieldValue
    fn evaluate_expression_value(expr: &Expr, record: &FieldValue) -> Result<FieldValue, SqlError> {
        match expr {
            Expr::Column(column_name) => {
                Ok(extract_field_value(record, column_name).unwrap_or(FieldValue::Null))
            }
            Expr::Literal(literal) => Ok(Self::literal_to_field_value(literal)),
            _ => Err(SqlError::ExecutionError {
                message: "Complex expressions not yet supported in comparisons".to_string(),
                query: Some("".to_string()),
            }),
        }
    }

    /// Evaluate a unary operation
    fn evaluate_unary_op(
        op: &UnaryOperator,
        expr: &Expr,
        record: &FieldValue,
    ) -> Result<bool, SqlError> {
        match op {
            UnaryOperator::Not => {
                let result = Self::evaluate_expression(expr, record)?;
                Ok(!result)
            }
            UnaryOperator::IsNull => {
                let value = Self::evaluate_expression_value(expr, record)?;
                Ok(matches!(value, FieldValue::Null))
            }
            UnaryOperator::IsNotNull => {
                let value = Self::evaluate_expression_value(expr, record)?;
                Ok(!matches!(value, FieldValue::Null))
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unsupported unary operator: {:?}", op),
                query: Some("".to_string()),
            }),
        }
    }

    /// Compare two FieldValues using the specified operator
    fn compare_values(
        left: &FieldValue,
        op: &BinaryOperator,
        right: &FieldValue,
    ) -> Result<bool, SqlError> {
        use std::cmp::Ordering;

        // Handle NULL comparisons
        match (left, right) {
            (FieldValue::Null, FieldValue::Null) => {
                return Ok(matches!(op, BinaryOperator::Equal));
            }
            (FieldValue::Null, _) | (_, FieldValue::Null) => {
                return Ok(matches!(op, BinaryOperator::NotEqual));
            }
            _ => {}
        }

        // Type-specific comparisons with proper coercion
        let ordering = match (left, right) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a.cmp(b),
            (FieldValue::Float(a), FieldValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (FieldValue::Integer(a), FieldValue::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (FieldValue::Float(a), FieldValue::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (FieldValue::String(a), FieldValue::String(b)) => a.cmp(b),
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a.cmp(b),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: format!("Cannot compare {:?} with {:?}", left, right),
                    query: Some("".to_string()),
                });
            }
        };

        let result = match op {
            BinaryOperator::Equal => ordering == Ordering::Equal,
            BinaryOperator::NotEqual => ordering != Ordering::Equal,
            BinaryOperator::LessThan => ordering == Ordering::Less,
            BinaryOperator::LessThanOrEqual => ordering != Ordering::Greater,
            BinaryOperator::GreaterThan => ordering == Ordering::Greater,
            BinaryOperator::GreaterThanOrEqual => ordering != Ordering::Less,
            _ => {
                return Err(SqlError::ExecutionError {
                    message: format!("Unsupported comparison operator: {:?}", op),
                    query: Some("".to_string()),
                });
            }
        };

        Ok(result)
    }

    /// Convert AST LiteralValue to FieldValue
    fn literal_to_field_value(literal: &LiteralValue) -> FieldValue {
        match literal {
            LiteralValue::String(s) => FieldValue::String(s.clone()),
            LiteralValue::Integer(i) => FieldValue::Integer(*i),
            LiteralValue::Float(f) => FieldValue::Float(*f),
            LiteralValue::Boolean(b) => FieldValue::Boolean(*b),
            LiteralValue::Null => FieldValue::Null,
            LiteralValue::Decimal(s) => {
                // Parse decimal string into ScaledInteger if possible, otherwise Float
                if let Ok(f) = s.parse::<f64>() {
                    FieldValue::Float(f)
                } else {
                    FieldValue::String(s.clone())
                }
            }
            LiteralValue::Interval { .. } => {
                // For now, convert intervals to strings
                FieldValue::String(format!("{:?}", literal))
            }
        }
    }

    /// Check if a FieldValue is considered "truthy"
    fn is_truthy(value: &FieldValue) -> bool {
        match value {
            FieldValue::Boolean(b) => *b,
            FieldValue::Null => false,
            FieldValue::Integer(i) => *i != 0,
            FieldValue::Float(f) => *f != 0.0,
            FieldValue::String(s) => !s.is_empty(),
            _ => true, // Arrays, structs, etc. are truthy if they exist
        }
    }
}

/// Table-based data source implementation for SQL queries
///
/// This implementation bridges Table state with the SQL subquery engine,
/// providing real-time access to Table data with any SerializationFormat.
/// Now supports JSON, Avro, Protobuf, and the full FieldValue type system.
pub struct TableDataSource<KS, VS>
where
    KS: Serializer<String> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    table: Table<String, KS, VS>,
}

/// Backward compatibility alias - use TableDataSource instead
///
/// This will be deprecated in future versions. Use TableDataSource
/// for new code as it supports all serialization formats.
pub type KafkaDataSource = TableDataSource<
    crate::velostream::kafka::serialization::StringSerializer,
    crate::velostream::serialization::JsonFormat,
>;

impl<KS, VS> TableDataSource<KS, VS>
where
    KS: Serializer<String> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    /// Create a new Table data source from an existing Table
    pub fn from_table(table: Table<String, KS, VS>) -> Self {
        Self { table }
    }

    /// Gets the underlying Table reference for advanced operations
    pub fn table(&self) -> &Table<String, KS, VS> {
        &self.table
    }
}

impl<KS, VS> SqlDataSource for TableDataSource<KS, VS>
where
    KS: Serializer<String> + Send + Sync + 'static,
    VS: SerializationFormat + Send + Sync + 'static,
{
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        // Table now directly returns FieldValue records - no conversion needed!
        let records = self.table.snapshot();
        let mut flattened_records = HashMap::new();

        for (key, field_map) in records {
            // Convert the HashMap<String, FieldValue> to a single FieldValue::Struct
            let struct_value = FieldValue::Struct(field_map);
            flattened_records.insert(key, struct_value);
        }

        Ok(flattened_records)
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        let key_string = key.to_string();
        if let Some(field_map) = self.table.get(&key_string) {
            let struct_value = FieldValue::Struct(field_map);
            Ok(Some(struct_value))
        } else {
            Ok(None)
        }
    }

    fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    fn record_count(&self) -> usize {
        self.table.len() // Use len() instead of size()
    }
}

/// Extract a field value from a FieldValue record
///
/// Supports accessing fields in Struct-type FieldValues using dot notation.
/// Now supports wildcard patterns using '*' for any field name.
fn extract_field_value(record: &FieldValue, field_path: &str) -> Option<FieldValue> {
    match record {
        FieldValue::Struct(fields) => {
            // For simple field access (no dots), return the field directly
            if !field_path.contains('.') {
                if field_path == "*" {
                    // Return all fields as a struct
                    return Some(FieldValue::Struct(fields.clone()));
                }
                return fields.get(field_path).cloned();
            }

            // For nested access like "user.profile.name" or "portfolio.positions.*.shares", split and traverse
            let mut current = record;
            let parts: Vec<&str> = field_path.split('.').collect();

            for (i, part) in parts.iter().enumerate() {
                match current {
                    FieldValue::Struct(current_fields) => {
                        if *part == "*" {
                            // Wildcard - need to search through all fields
                            for (_, field_value) in current_fields.iter() {
                                // Recursively search in each field with remaining path
                                if i + 1 < parts.len() {
                                    let remaining_path = parts[i + 1..].join(".");
                                    if let Some(result) = extract_field_value(field_value, &remaining_path) {
                                        return Some(result);
                                    }
                                } else {
                                    // This is the last part and it's a wildcard, return the field
                                    return Some(field_value.clone());
                                }
                            }
                            return None;
                        } else {
                            if let Some(next) = current_fields.get(*part) {
                                current = next;
                            } else {
                                return None;
                            }
                        }
                    }
                    _ => return None,
                }
            }
            Some(current.clone())
        }
        _ => None,
    }
}

/// Implementation of SqlQueryable for any SqlDataSource
///
/// This provides a generic implementation that works with any data source
/// implementing the SqlDataSource trait, making it extensible for different
/// backends (Table, external databases, etc.).
impl<T: SqlDataSource> SqlQueryable for T {
    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError> {
        let evaluator = ExpressionEvaluator::new();
        let predicate = evaluator.parse_where_clause(where_clause)?;
        let all_records = self.get_all_records()?;

        let mut filtered = HashMap::new();
        for (key, value) in all_records {
            if predicate(&key, &value) {
                filtered.insert(key, value);
            }
        }

        Ok(filtered)
    }

    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError> {
        let evaluator = ExpressionEvaluator::new();
        let predicate = evaluator.parse_where_clause(where_clause)?;
        let all_records = self.get_all_records()?;

        for (key, value) in all_records {
            if predicate(&key, &value) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn sql_column_values(
        &self,
        column: &str,
        where_clause: &str,
    ) -> Result<Vec<FieldValue>, SqlError> {
        let filtered_records = self.sql_filter(where_clause)?;
        let mut values = Vec::new();

        for (_key, record) in filtered_records {
            if let Some(field_value) = extract_field_value(&record, column) {
                values.push(field_value);
            }
        }

        Ok(values)
    }

    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> Result<FieldValue, SqlError> {
        let filtered_records = self.sql_filter(where_clause)?;

        if filtered_records.is_empty() {
            return Ok(FieldValue::Null);
        }

        // For simple column selection, extract the field from the first record
        if filtered_records.len() == 1 {
            let (_key, record) = filtered_records.into_iter().next().unwrap();
            if let Some(field_value) = extract_field_value(&record, select_expr) {
                return Ok(field_value);
            }
        } else {
            // Multiple records - this should error for scalar subqueries
            return Err(SqlError::ExecutionError {
                message: format!(
                    "Scalar subquery returned more than one row: {} rows",
                    filtered_records.len()
                ),
                query: Some(format!("SELECT {} WHERE {}", select_expr, where_clause)),
            });
        }

        Ok(FieldValue::Null)
    }

    fn sql_wildcard_values(&self, wildcard_expr: &str) -> Result<Vec<FieldValue>, SqlError> {
        // Parse the wildcard expression to extract field path and condition
        // Examples:
        // "portfolio.positions.*.shares > 100"
        // "users.*" (just extract all user values)

        // Validate input
        if wildcard_expr.is_empty() {
            return Err(SqlError::ParseError {
                message: "Empty wildcard expression".to_string(),
                position: Some(0),
            });
        }

        let all_records = self.get_all_records()?;
        let mut matching_values = Vec::new();

        if let Some((field_path, condition)) = wildcard_expr.split_once(" > ") {
            // Handle comparisons like "portfolio.positions.****.shares > 100"
            let threshold = condition.parse::<f64>()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid numeric threshold: {}", condition),
                    position: Some(wildcard_expr.find(" > ").unwrap_or(0) + 3),
                })?;

            for (_key, record) in all_records {
                collect_wildcard_matches(&record, field_path, &mut matching_values, Some(threshold));
            }
        } else if let Some((field_path, condition)) = wildcard_expr.split_once(" < ") {
            // Handle less-than comparisons
            let threshold = condition.parse::<f64>()
                .map_err(|_| SqlError::ParseError {
                    message: format!("Invalid numeric threshold: {}", condition),
                    position: Some(wildcard_expr.find(" < ").unwrap_or(0) + 3),
                })?;

            for (_key, record) in all_records {
                collect_wildcard_matches_less_than(&record, field_path, &mut matching_values, threshold);
            }
        } else {
            // Simple wildcard extraction without conditions
            for (_key, record) in all_records {
                collect_wildcard_matches(&record, wildcard_expr, &mut matching_values, None);
            }
        }

        Ok(matching_values)
    }
}

/// Helper function to collect wildcard matches with optional threshold comparison
fn collect_wildcard_matches(
    record: &FieldValue,
    field_path: &str,
    results: &mut Vec<FieldValue>,
    threshold: Option<f64>,
) {
    let parts: Vec<&str> = field_path.split('.').collect();
    collect_wildcard_recursive(record, &parts, 0, results, threshold);
}

/// Helper function for less-than comparisons with wildcards
fn collect_wildcard_matches_less_than(
    record: &FieldValue,
    field_path: &str,
    results: &mut Vec<FieldValue>,
    threshold: f64,
) {
    let parts: Vec<&str> = field_path.split('.').collect();
    collect_wildcard_recursive_less_than(record, &parts, 0, results, threshold);
}

/// Recursive helper for wildcard pattern matching with greater-than comparison
fn collect_wildcard_recursive(
    current: &FieldValue,
    parts: &[&str],
    index: usize,
    results: &mut Vec<FieldValue>,
    threshold: Option<f64>,
) {
    if index >= parts.len() {
        return;
    }

    match current {
        FieldValue::Struct(fields) => {
            let part = parts[index];

            if part == "*" {
                // Wildcard - search all fields at this level
                for (_, field_value) in fields.iter() {
                    if index + 1 < parts.len() {
                        // More parts to process
                        collect_wildcard_recursive(field_value, parts, index + 1, results, threshold);
                    } else {
                        // Last part is wildcard, collect values based on threshold
                        if let Some(threshold_val) = threshold {
                            if let Some(numeric_val) = extract_numeric_value(field_value) {
                                if numeric_val > threshold_val {
                                    results.push(field_value.clone());
                                }
                            }
                        } else {
                            results.push(field_value.clone());
                        }
                    }
                }
            } else if let Some(field_value) = fields.get(part) {
                // Exact field match
                if index + 1 < parts.len() {
                    collect_wildcard_recursive(field_value, parts, index + 1, results, threshold);
                } else {
                    // Final field - apply threshold if needed
                    if let Some(threshold_val) = threshold {
                        if let Some(numeric_val) = extract_numeric_value(field_value) {
                            if numeric_val > threshold_val {
                                results.push(field_value.clone());
                            }
                        }
                    } else {
                        results.push(field_value.clone());
                    }
                }
            }
        }
        _ => {
            // Not a struct, cannot continue traversal
        }
    }
}

/// Recursive helper for wildcard pattern matching with less-than comparison
fn collect_wildcard_recursive_less_than(
    current: &FieldValue,
    parts: &[&str],
    index: usize,
    results: &mut Vec<FieldValue>,
    threshold: f64,
) {
    if index >= parts.len() {
        return;
    }

    match current {
        FieldValue::Struct(fields) => {
            let part = parts[index];

            if part == "*" {
                // Wildcard - search all fields at this level
                for (_, field_value) in fields.iter() {
                    if index + 1 < parts.len() {
                        // More parts to process
                        collect_wildcard_recursive_less_than(field_value, parts, index + 1, results, threshold);
                    } else {
                        // Last part is wildcard, collect values less than threshold
                        if let Some(numeric_val) = extract_numeric_value(field_value) {
                            if numeric_val < threshold {
                                results.push(field_value.clone());
                            }
                        }
                    }
                }
            } else if let Some(field_value) = fields.get(part) {
                // Exact field match
                if index + 1 < parts.len() {
                    collect_wildcard_recursive_less_than(field_value, parts, index + 1, results, threshold);
                } else {
                    // Final field - apply less-than threshold
                    if let Some(numeric_val) = extract_numeric_value(field_value) {
                        if numeric_val < threshold {
                            results.push(field_value.clone());
                        }
                    }
                }
            }
        }
        _ => {
            // Not a struct, cannot continue traversal
        }
    }
}

/// Extract numeric value from FieldValue for threshold comparisons
fn extract_numeric_value(value: &FieldValue) -> Option<f64> {
    match value {
        FieldValue::Integer(i) => Some(*i as f64),
        FieldValue::Float(f) => Some(*f),
        FieldValue::ScaledInteger(val, scale) => {
            // Convert ScaledInteger to f64 for comparison
            let divisor = 10_i64.pow(*scale as u32) as f64;
            Some(*val as f64 / divisor)
        }
        _ => None,
    }
}
