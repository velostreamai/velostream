//! Field-level Runtime Validation
//!
//! Provides validation for field existence and type compatibility during query execution.
//! This complements Phase 1 (aggregate function validation) with runtime checks.

use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord, system_columns};
use std::collections::HashSet;

/// Error type for field validation failures
#[derive(Debug, Clone)]
pub enum FieldValidationError {
    /// Field referenced in query does not exist in record
    FieldNotFound { field_name: String, context: String },
    /// Field exists but has incorrect type for operation
    TypeMismatch {
        field_name: String,
        expected_type: String,
        actual_type: String,
        context: String,
    },
    /// Multiple fields missing from record
    MultipleFieldsMissing {
        field_names: Vec<String>,
        context: String,
    },
}

impl FieldValidationError {
    /// Convert to SqlError for proper error reporting
    pub fn to_sql_error(self) -> SqlError {
        match self {
            FieldValidationError::FieldNotFound {
                field_name,
                context,
            } => SqlError::ExecutionError {
                message: format!(
                    "Field '{}' not found in record during {}",
                    field_name, context
                ),
                query: None,
            },
            FieldValidationError::TypeMismatch {
                field_name,
                expected_type,
                actual_type,
                context,
            } => SqlError::TypeError {
                expected: expected_type,
                actual: actual_type,
                value: Some(field_name),
            },
            FieldValidationError::MultipleFieldsMissing {
                field_names,
                context,
            } => SqlError::ExecutionError {
                message: format!(
                    "Multiple fields not found during {}: {}",
                    context,
                    field_names.join(", ")
                ),
                query: None,
            },
        }
    }
}

/// Validation context for field validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationContext {
    /// Validating GROUP BY fields
    GroupBy,
    /// Validating PARTITION BY fields
    PartitionBy,
    /// Validating SELECT clause fields
    SelectClause,
    /// Validating WHERE clause conditions
    WhereClause,
    /// Validating JOIN ON clause
    JoinCondition,
    /// Validating aggregate functions
    Aggregation,
    /// Validating HAVING clause
    HavingClause,
    /// Validating window frame definitions
    WindowFrame,
    /// Validating ORDER BY clause fields
    OrderByClause,
}

/// Represents known table aliases available in a query
/// Extracted from FROM clause and JOIN clauses
#[derive(Debug, Clone, Default)]
pub struct AliasContext {
    /// Aliases from the FROM source (None if no alias provided)
    pub from_alias: Option<String>,
    /// Aliases from JOIN clauses (right_alias from each JOIN)
    pub join_aliases: HashSet<String>,
}

impl AliasContext {
    /// Create a new empty alias context
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if an alias is known in this context
    pub fn contains_alias(&self, alias: &str) -> bool {
        self.from_alias.as_ref().map(|a| a.as_str()) == Some(alias)
            || self.join_aliases.contains(alias)
    }

    /// Get all known aliases
    pub fn all_aliases(&self) -> HashSet<&str> {
        let mut aliases = HashSet::new();
        if let Some(alias) = &self.from_alias {
            aliases.insert(alias.as_str());
        }
        for alias in &self.join_aliases {
            aliases.insert(alias.as_str());
        }
        aliases
    }
}

impl std::fmt::Display for ValidationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationContext::GroupBy => write!(f, "GROUP BY clause"),
            ValidationContext::PartitionBy => write!(f, "PARTITION BY clause"),
            ValidationContext::SelectClause => write!(f, "SELECT clause"),
            ValidationContext::WhereClause => write!(f, "WHERE clause"),
            ValidationContext::JoinCondition => write!(f, "JOIN condition"),
            ValidationContext::Aggregation => write!(f, "aggregation"),
            ValidationContext::HavingClause => write!(f, "HAVING clause"),
            ValidationContext::WindowFrame => write!(f, "window frame"),
            ValidationContext::OrderByClause => write!(f, "ORDER BY clause"),
        }
    }
}

/// Core field validator for runtime validation
pub struct FieldValidator;

impl AliasContext {
    /// Build an AliasContext from a StreamingQuery's FROM and JOIN clauses
    /// This extracts all known table aliases that can be used in qualified column names
    ///
    /// # Example
    ///
    /// For a query like:
    /// ```sql
    /// SELECT m.price, p.product_name FROM market m
    /// JOIN positions p ON m.symbol = p.symbol
    /// ```
    ///
    /// This returns AliasContext with:
    /// - from_alias: Some("m")
    /// - join_aliases: {"p"}
    pub fn from_streaming_query(query: &crate::velostream::sql::StreamingQuery) -> Self {
        use crate::velostream::sql::StreamingQuery;

        match query {
            StreamingQuery::Select {
                from_alias, joins, ..
            } => {
                let mut context = AliasContext {
                    from_alias: from_alias.clone(),
                    join_aliases: HashSet::new(),
                };

                if let Some(join_clauses) = joins {
                    for join_clause in join_clauses {
                        if let Some(alias) = &join_clause.right_alias {
                            context.join_aliases.insert(alias.clone());
                        }
                    }
                }

                context
            }
            // Other query types don't have FROM/JOIN clauses
            _ => AliasContext::default(),
        }
    }
}

impl FieldValidator {
    /// Validate that a field exists in the record
    ///
    /// # Arguments
    ///
    /// * `record` - The stream record to validate against
    /// * `field_name` - Name of the field to validate
    /// * `context` - Validation context for error messages
    ///
    /// # Returns
    ///
    /// `Ok(())` if field exists, `Err(FieldValidationError)` otherwise
    pub fn validate_field_exists(
        record: &StreamRecord,
        field_name: &str,
        context: ValidationContext,
    ) -> Result<(), FieldValidationError> {
        if !record.fields.contains_key(field_name) {
            return Err(FieldValidationError::FieldNotFound {
                field_name: field_name.to_string(),
                context: context.to_string(),
            });
        }
        Ok(())
    }

    /// Validate that multiple fields exist in the record
    ///
    /// # Arguments
    ///
    /// * `record` - The stream record to validate against
    /// * `field_names` - Names of fields to validate
    /// * `context` - Validation context for error messages
    ///
    /// # Returns
    ///
    /// `Ok(())` if all fields exist, `Err(FieldValidationError)` if any are missing
    pub fn validate_fields_exist(
        record: &StreamRecord,
        field_names: &[&str],
        context: ValidationContext,
    ) -> Result<(), FieldValidationError> {
        let missing: Vec<String> = field_names
            .iter()
            .filter(|name| !record.fields.contains_key(*name as &str))
            .map(|name| name.to_string())
            .collect();

        if !missing.is_empty() {
            return if missing.len() == 1 {
                Err(FieldValidationError::FieldNotFound {
                    field_name: missing[0].clone(),
                    context: context.to_string(),
                })
            } else {
                Err(FieldValidationError::MultipleFieldsMissing {
                    field_names: missing,
                    context: context.to_string(),
                })
            };
        }
        Ok(())
    }

    /// Validate field type compatibility
    ///
    /// # Arguments
    ///
    /// * `field_name` - Name of the field
    /// * `value` - The field value to validate
    /// * `expected_type` - Expected type description (e.g., "numeric", "string")
    /// * `context` - Validation context for error messages
    /// * `type_check_fn` - Function to check if value is compatible type
    ///
    /// # Returns
    ///
    /// `Ok(())` if type is correct, `Err(FieldValidationError)` otherwise
    pub fn validate_field_type<F>(
        field_name: &str,
        value: &FieldValue,
        expected_type: &str,
        context: ValidationContext,
        type_check_fn: F,
    ) -> Result<(), FieldValidationError>
    where
        F: Fn(&FieldValue) -> bool,
    {
        if !type_check_fn(value) {
            return Err(FieldValidationError::TypeMismatch {
                field_name: field_name.to_string(),
                expected_type: expected_type.to_string(),
                actual_type: value.type_name().to_string(),
                context: context.to_string(),
            });
        }
        Ok(())
    }

    /// Extract field names from an expression
    ///
    /// # Arguments
    ///
    /// * `expr` - The expression to extract field names from
    ///
    /// # Returns
    ///
    /// A set of field names referenced in the expression
    pub fn extract_field_names(expr: &Expr) -> HashSet<String> {
        let mut fields = HashSet::new();
        Self::collect_field_names(expr, &mut fields);
        fields
    }

    /// Recursively collect field names from an expression
    fn collect_field_names(expr: &Expr, fields: &mut HashSet<String>) {
        match expr {
            Expr::Column(name) => {
                fields.insert(name.clone());
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::collect_field_names(arg, fields);
                }
            }
            Expr::WindowFunction { args, .. } => {
                for arg in args {
                    Self::collect_field_names(arg, fields);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_field_names(left, fields);
                Self::collect_field_names(right, fields);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_field_names(expr, fields);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_field_names(expr, fields);
                Self::collect_field_names(low, fields);
                Self::collect_field_names(high, fields);
            }
            Expr::List(exprs) => {
                for e in exprs {
                    Self::collect_field_names(e, fields);
                }
            }
            Expr::Case {
                when_clauses,
                else_clause,
            } => {
                for (cond, result) in when_clauses {
                    Self::collect_field_names(cond, fields);
                    Self::collect_field_names(result, fields);
                }
                if let Some(else_expr) = else_clause {
                    Self::collect_field_names(else_expr, fields);
                }
            }
            Expr::Subquery { .. } => {
                // Subqueries have their own field context
            }
            Expr::Literal(_) => {
                // Literals don't reference fields
            }
        }
    }

    /// Extract the base column name from a potentially qualified name (table.column -> column)
    fn normalize_column_name(name: &str) -> &str {
        if let Some(pos) = name.rfind('.') {
            &name[pos + 1..]
        } else {
            name
        }
    }

    /// Validate all fields in expressions with qualified name alias checking
    ///
    /// This enhanced validation:
    /// - Checks that unqualified fields exist in the record
    /// - For qualified names (e.g., "p.product_name"), validates that "p" is a known alias
    /// - Rejects queries with unknown aliases (e.g., "SELECT wrong_alias.id FROM customers")
    ///
    /// # Arguments
    ///
    /// * `record` - The stream record to validate against
    /// * `expressions` - Expressions to validate
    /// * `context` - Validation context for error messages
    /// * `alias_context` - Known table aliases from FROM/JOINs
    ///
    /// # Returns
    ///
    /// `Ok(())` if all fields are valid, `Err(FieldValidationError)` otherwise
    pub fn validate_expressions_with_aliases(
        record: &StreamRecord,
        expressions: &[Expr],
        context: ValidationContext,
        alias_context: &AliasContext,
    ) -> Result<(), FieldValidationError> {
        let mut all_fields = HashSet::new();
        for expr in expressions {
            let expr_fields = Self::extract_field_names(expr);
            all_fields.extend(expr_fields);
        }

        let missing: Vec<String> = all_fields
            .into_iter()
            .filter(|name| {
                // Skip system columns - they're always available
                if system_columns::normalize_if_system_column(name).is_some() {
                    return false;
                }

                // Check if this is a qualified name (contains a dot)
                if let Some(dot_pos) = name.rfind('.') {
                    let alias = &name[..dot_pos];

                    // Validate that the alias is known
                    if !alias_context.contains_alias(alias) {
                        // Unknown alias - this is an error
                        return true; // Include in missing list
                    }

                    // Alias is known, but the field might not be in the record yet (will come from JOIN)
                    // Skip strict validation - the full validation happens after joins
                    false
                } else {
                    // Unqualified name - must exist in the record
                    !record.fields.contains_key(name)
                }
            })
            .collect();

        if !missing.is_empty() {
            return if missing.len() == 1 {
                let field = &missing[0];
                // Check if it's a qualified name with unknown alias
                if let Some(dot_pos) = field.rfind('.') {
                    let alias = &field[..dot_pos];
                    Err(FieldValidationError::FieldNotFound {
                        field_name: format!("unknown table alias '{}' (field: {})", alias, field),
                        context: context.to_string(),
                    })
                } else {
                    Err(FieldValidationError::FieldNotFound {
                        field_name: field.clone(),
                        context: context.to_string(),
                    })
                }
            } else {
                Err(FieldValidationError::MultipleFieldsMissing {
                    field_names: missing,
                    context: context.to_string(),
                })
            };
        }
        Ok(())
    }
}
