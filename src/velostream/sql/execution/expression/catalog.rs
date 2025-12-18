//! SQL Function Catalog Generation
//!
//! This module provides utilities to generate documentation and introspection
//! for all registered SQL functions.

use super::function_metadata::{
    FunctionCategory, SqlFunctionDef, all_aggregate_functions, all_registered_functions,
    all_window_functions, functions_in_category,
};

/// Generates a markdown documentation catalog of all registered SQL functions.
///
/// The catalog is organized by category with full metadata for each function.
///
/// # Example
///
/// ```rust,ignore
/// use velostream::velostream::sql::execution::expression::catalog::generate_function_catalog;
/// let markdown = generate_function_catalog();
/// std::fs::write("docs/sql/FUNCTION_CATALOG.md", markdown).unwrap();
/// ```
pub fn generate_function_catalog() -> String {
    let mut output = String::new();

    output.push_str("# Velostream SQL Function Catalog\n\n");
    output.push_str("Auto-generated catalog of all available SQL functions.\n\n");
    output.push_str("## Table of Contents\n\n");

    // Generate ToC
    let categories = [
        (FunctionCategory::Aggregate, "Aggregate Functions"),
        (FunctionCategory::Math, "Math Functions"),
        (FunctionCategory::String, "String Functions"),
        (FunctionCategory::DateTime, "Date/Time Functions"),
        (FunctionCategory::Conditional, "Conditional Functions"),
        (FunctionCategory::Window, "Window Functions"),
        (FunctionCategory::Json, "JSON Functions"),
        (FunctionCategory::Array, "Array/Map Functions"),
        (FunctionCategory::Scalar, "Scalar/Utility Functions"),
    ];

    for (_, title) in &categories {
        let anchor = title.to_lowercase().replace(' ', "-").replace('/', "");
        output.push_str(&format!("- [{}](#{})\n", title, anchor));
    }
    output.push_str("\n---\n\n");

    // Generate sections for each category
    for (category, title) in &categories {
        let funcs = functions_in_category(*category);
        if funcs.is_empty() {
            continue;
        }

        output.push_str(&format!("## {}\n\n", title));
        output.push_str("| Function | Aliases | Aggregate | Window |\n");
        output.push_str("|----------|---------|-----------|--------|\n");

        for func in funcs {
            let aliases = if func.aliases.is_empty() {
                "-".to_string()
            } else {
                func.aliases.join(", ")
            };
            let agg = if func.is_aggregate { "✓" } else { "-" };
            let win = if func.is_window { "✓" } else { "-" };
            output.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                func.name, aliases, agg, win
            ));
        }
        output.push('\n');
    }

    // Summary statistics
    let total = all_registered_functions().count();
    let aggregates = all_aggregate_functions().len();
    let windows = all_window_functions().len();

    output.push_str("---\n\n");
    output.push_str("## Summary\n\n");
    output.push_str(&format!("- **Total Functions**: {}\n", total));
    output.push_str(&format!("- **Aggregate Functions**: {}\n", aggregates));
    output.push_str(&format!("- **Window Functions**: {}\n", windows));

    output
}

/// Returns all function names (including aliases) as a sorted list.
///
/// Useful for autocomplete, validation, and introspection.
pub fn all_function_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = Vec::new();

    for func in all_registered_functions() {
        names.push(func.name);
        for alias in func.aliases {
            names.push(alias);
        }
    }

    names.sort_unstable();
    names
}

/// Returns functions that match a given pattern (case-insensitive prefix match).
///
/// # Example
///
/// ```rust,ignore
/// use velostream::velostream::sql::execution::expression::catalog::find_functions_by_prefix;
/// let matches = find_functions_by_prefix("DATE");
/// // Returns: ["DATE_FORMAT", "DATEDIFF"]
/// ```
pub fn find_functions_by_prefix(prefix: &str) -> Vec<&'static SqlFunctionDef> {
    let prefix_upper = prefix.to_uppercase();
    all_registered_functions()
        .filter(|f| f.name.starts_with(&prefix_upper))
        .collect()
}

/// Returns a summary of function counts by category.
pub fn function_count_by_category() -> Vec<(FunctionCategory, usize)> {
    let categories = [
        FunctionCategory::Aggregate,
        FunctionCategory::Math,
        FunctionCategory::String,
        FunctionCategory::DateTime,
        FunctionCategory::Conditional,
        FunctionCategory::Window,
        FunctionCategory::Json,
        FunctionCategory::Array,
        FunctionCategory::Scalar,
    ];

    categories
        .iter()
        .map(|&cat| (cat, functions_in_category(cat).len()))
        .collect()
}

// ============================================================================
// FUNCTION TYPE CHECKING HELPERS
// ============================================================================

use super::function_metadata::find_function;

/// Check if a function name is an aggregate function (case-insensitive).
///
/// This replaces hardcoded matches like:
/// ```rust,ignore
/// matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | ...)
/// ```
///
/// With:
/// ```rust,ignore
/// is_aggregate_function(name)
/// ```
pub fn is_aggregate_function(name: &str) -> bool {
    find_function(name).map(|f| f.is_aggregate).unwrap_or(false)
}

/// Check if a function name is a window function (case-insensitive).
///
/// Window functions can be used with OVER clauses.
pub fn is_window_function(name: &str) -> bool {
    find_function(name).map(|f| f.is_window).unwrap_or(false)
}

/// Check if a function name is valid (exists in registry, case-insensitive).
pub fn is_valid_function(name: &str) -> bool {
    find_function(name).is_some()
}

/// Get the category of a function (case-insensitive).
pub fn get_function_category(name: &str) -> Option<FunctionCategory> {
    find_function(name).map(|f| f.category)
}

/// Check if a function belongs to a specific category (case-insensitive).
pub fn is_function_in_category(name: &str, category: FunctionCategory) -> bool {
    find_function(name)
        .map(|f| f.category == category)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_catalog() {
        let catalog = generate_function_catalog();
        assert!(catalog.contains("# Velostream SQL Function Catalog"));
        assert!(catalog.contains("## Aggregate Functions"));
        assert!(catalog.contains("COUNT"));
        assert!(catalog.contains("SUM"));
        assert!(catalog.contains("## Summary"));
    }

    #[test]
    fn test_all_function_names() {
        let names = all_function_names();
        assert!(!names.is_empty());
        // Check for some known functions
        assert!(names.contains(&"COUNT"));
        assert!(names.contains(&"SUM"));
        assert!(names.contains(&"ABS"));
        // Check aliases are included
        assert!(names.contains(&"CEILING")); // alias for CEIL
        assert!(names.contains(&"LEN")); // alias for LENGTH
    }

    #[test]
    fn test_find_functions_by_prefix() {
        let date_funcs = find_functions_by_prefix("DATE");
        assert!(!date_funcs.is_empty());
        for func in &date_funcs {
            assert!(func.name.starts_with("DATE"));
        }
    }

    #[test]
    fn test_function_count_by_category() {
        let counts = function_count_by_category();
        assert!(!counts.is_empty());

        // Aggregate category should have functions
        let agg_count = counts
            .iter()
            .find(|(cat, _)| *cat == FunctionCategory::Aggregate);
        assert!(agg_count.is_some());
        assert!(agg_count.unwrap().1 > 0);
    }

    #[test]
    fn test_is_aggregate_function() {
        // Aggregate functions
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("count")); // case-insensitive
        assert!(is_aggregate_function("SUM"));
        assert!(is_aggregate_function("AVG"));
        assert!(is_aggregate_function("MIN"));
        assert!(is_aggregate_function("MAX"));
        assert!(is_aggregate_function("STDDEV"));
        assert!(is_aggregate_function("VARIANCE"));

        // Non-aggregate functions
        assert!(!is_aggregate_function("ABS"));
        assert!(!is_aggregate_function("UPPER"));
        assert!(!is_aggregate_function("NOW"));

        // Unknown function
        assert!(!is_aggregate_function("UNKNOWN_FUNC"));
    }

    #[test]
    fn test_is_window_function() {
        // Window functions
        assert!(is_window_function("FIRST_VALUE"));
        assert!(is_window_function("LAST_VALUE"));

        // Non-window functions
        assert!(!is_window_function("ABS"));
        assert!(!is_window_function("UPPER"));
    }

    #[test]
    fn test_is_valid_function() {
        assert!(is_valid_function("COUNT"));
        assert!(is_valid_function("abs")); // case-insensitive
        assert!(is_valid_function("CEILING")); // alias
        assert!(!is_valid_function("NOT_A_FUNCTION"));
    }

    #[test]
    fn test_get_function_category() {
        assert_eq!(
            get_function_category("COUNT"),
            Some(FunctionCategory::Aggregate)
        );
        assert_eq!(get_function_category("ABS"), Some(FunctionCategory::Math));
        assert_eq!(
            get_function_category("UPPER"),
            Some(FunctionCategory::String)
        );
        assert_eq!(get_function_category("NOT_A_FUNCTION"), None);
    }

    #[test]
    fn test_is_function_in_category() {
        assert!(is_function_in_category(
            "COUNT",
            FunctionCategory::Aggregate
        ));
        assert!(!is_function_in_category("COUNT", FunctionCategory::Math));
        assert!(is_function_in_category("ABS", FunctionCategory::Math));
        assert!(!is_function_in_category(
            "UNKNOWN",
            FunctionCategory::Aggregate
        ));
    }
}
