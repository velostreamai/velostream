//! Function Metadata and Self-Registration System
//!
//! This module provides the infrastructure for SQL functions to self-register
//! using the inventory pattern. Functions declare their metadata and automatically
//! register themselves at compile time.

use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;
use std::sync::LazyLock;

/// Function handler signature
pub type FunctionHandler = fn(&[Expr], &StreamRecord) -> Result<FieldValue, SqlError>;

/// SQL Function metadata for self-registration
#[derive(Debug, Clone)]
pub struct SqlFunctionDef {
    /// Primary function name (uppercase)
    pub name: &'static str,
    /// Alternative names/aliases for the function
    pub aliases: &'static [&'static str],
    /// Function category for organization
    pub category: FunctionCategory,
    /// Is this an aggregate function (SUM, COUNT, etc.)?
    pub is_aggregate: bool,
    /// Can be used in OVER clauses (window function)?
    pub is_window: bool,
    /// Function implementation handler
    pub handler: FunctionHandler,
}

/// Categories of SQL functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCategory {
    /// Basic aggregation (COUNT, SUM, AVG, MIN, MAX)
    Aggregate,
    /// String manipulation (UPPER, LOWER, SUBSTRING, etc.)
    String,
    /// Mathematical operations (ABS, ROUND, SQRT, etc.)
    Math,
    /// Date/time operations (NOW, EXTRACT, DATE_FORMAT, etc.)
    DateTime,
    /// Conditional logic (CASE, COALESCE, NULLIF, etc.)
    Conditional,
    /// Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
    Window,
    /// JSON operations (JSON_VALUE, JSON_EXTRACT, etc.)
    Json,
    /// Array/Map operations (ARRAY_LENGTH, MAP_KEYS, etc.)
    Array,
    /// Scalar/utility functions (HEADER, CAST, etc.)
    Scalar,
}

// Distributed registration storage for SQL functions
inventory::collect!(SqlFunctionDef);

/// Cached function lookup table for O(1) access.
///
/// Maps uppercase function names (including aliases) to their definitions.
/// Lazily initialized on first access.
static FUNCTION_LOOKUP_CACHE: LazyLock<HashMap<String, &'static SqlFunctionDef>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();

        for func_def in inventory::iter::<SqlFunctionDef> {
            // Insert primary name
            map.insert(func_def.name.to_string(), func_def);

            // Insert all aliases
            for alias in func_def.aliases {
                map.insert((*alias).to_string(), func_def);
            }
        }

        map
    });

/// Macro to register a SQL function with metadata
///
/// # Example
/// ```rust,ignore
/// register_sql_function!(
///     name: "ABS",
///     aliases: [],
///     category: FunctionCategory::Math,
///     aggregate: false,
///     window: false,
///     handler: BuiltinFunctions::abs_function
/// );
/// ```
#[macro_export]
macro_rules! register_sql_function {
    (
        name: $name:expr,
        aliases: [$($alias:expr),*],
        category: $category:expr,
        aggregate: $agg:expr,
        window: $window:expr,
        handler: $handler:path
    ) => {
        inventory::submit! {
            $crate::velostream::sql::execution::expression::function_metadata::SqlFunctionDef {
                name: $name,
                aliases: &[$($alias),*],
                category: $category,
                is_aggregate: $agg,
                is_window: $window,
                handler: $handler,
            }
        }
    };
}

/// Get all registered SQL functions
pub fn all_registered_functions() -> impl Iterator<Item = &'static SqlFunctionDef> {
    inventory::iter::<SqlFunctionDef>.into_iter()
}

/// Find a function by name (case-insensitive).
///
/// Uses a cached HashMap for O(1) lookup performance.
/// The cache is lazily initialized on first access.
pub fn find_function(name: &str) -> Option<&'static SqlFunctionDef> {
    let name_upper = name.to_uppercase();
    FUNCTION_LOOKUP_CACHE.get(&name_upper).copied()
}

/// Get all functions in a specific category
pub fn functions_in_category(category: FunctionCategory) -> Vec<&'static SqlFunctionDef> {
    all_registered_functions()
        .filter(|f| f.category == category)
        .collect()
}

/// Get all aggregate functions
pub fn all_aggregate_functions() -> Vec<&'static SqlFunctionDef> {
    all_registered_functions()
        .filter(|f| f.is_aggregate)
        .collect()
}

/// Get all window functions
pub fn all_window_functions() -> Vec<&'static SqlFunctionDef> {
    all_registered_functions().filter(|f| f.is_window).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_function() {
        // Will be populated as we migrate functions
        // This test validates the infrastructure works
        let _ = find_function("ABS");
    }

    #[test]
    fn test_case_insensitive_lookup() {
        // Functions should be findable regardless of case
        let abs_upper = find_function("ABS");
        let abs_lower = find_function("abs");
        let abs_mixed = find_function("Abs");

        // Either all Some or all None (depending on migration status)
        assert_eq!(abs_upper.is_some(), abs_lower.is_some());
        assert_eq!(abs_upper.is_some(), abs_mixed.is_some());
    }

    #[test]
    fn test_category_filtering() {
        let math_funcs = functions_in_category(FunctionCategory::Math);
        // Should have math functions once migrated
        // For now, just test the API works
        let _ = math_funcs.len();
    }

    #[test]
    fn test_aggregate_filtering() {
        let agg_funcs = all_aggregate_functions();
        for func in agg_funcs {
            assert!(func.is_aggregate);
        }
    }

    #[test]
    fn test_window_filtering() {
        let window_funcs = all_window_functions();
        for func in window_funcs {
            assert!(func.is_window);
        }
    }
}
