//! Function Registry
//!
//! Central registry of all supported SQL functions in Velostream.
//! Used for semantic validation to check if functions exist before runtime.

use std::collections::HashSet;
use std::sync::LazyLock;

/// Registry of all supported scalar and aggregate functions
pub struct FunctionRegistry {
    /// All supported function names (case-insensitive)
    supported_functions: HashSet<String>,
    /// Functions that can be used in OVER clauses (window functions)
    window_functions: HashSet<String>,
    /// Aggregate functions (COUNT, SUM, AVG, etc.)
    aggregate_functions: HashSet<String>,
}

impl FunctionRegistry {
    /// Create a new function registry with all supported functions
    pub fn new() -> Self {
        let mut registry = Self {
            supported_functions: HashSet::new(),
            window_functions: HashSet::new(),
            aggregate_functions: HashSet::new(),
        };

        registry.register_all_functions();
        registry
    }

    /// Check if a function is supported
    pub fn is_function_supported(&self, name: &str) -> bool {
        self.supported_functions.contains(&name.to_uppercase())
    }

    /// Check if a function can be used in OVER clauses
    pub fn is_window_function(&self, name: &str) -> bool {
        self.window_functions.contains(&name.to_uppercase())
    }

    /// Check if a function is an aggregate function
    pub fn is_aggregate_function(&self, name: &str) -> bool {
        self.aggregate_functions.contains(&name.to_uppercase())
    }

    /// Get list of similar function names (for suggestions)
    pub fn find_similar_functions(&self, name: &str, max_results: usize) -> Vec<String> {
        let name_upper = name.to_uppercase();
        let mut similar: Vec<String> = self
            .supported_functions
            .iter()
            .filter(|f| {
                // Simple similarity: starts with same letter or contains substring
                f.starts_with(&name_upper.chars().next().unwrap_or('_').to_string())
                    || f.contains(&name_upper)
                    || name_upper.contains(f.as_str())
            })
            .cloned()
            .collect();

        similar.sort();
        similar.truncate(max_results);
        similar
    }

    // Private registration methods

    fn register_all_functions(&mut self) {
        self.register_aggregate_functions();
        self.register_scalar_functions();
        self.register_string_functions();
        self.register_math_functions();
        self.register_date_time_functions();
        self.register_conditional_functions();
        self.register_window_functions();
        self.register_json_functions();
        self.register_array_functions();
    }

    fn register_aggregate_functions(&mut self) {
        let functions = vec![
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "APPROX_COUNT_DISTINCT",
            "FIRST_VALUE",
            "LAST_VALUE",
            "LISTAGG",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
            self.aggregate_functions.insert(func.to_string());
        }
    }

    fn register_scalar_functions(&mut self) {
        let functions = vec![
            "HEADER",
            "HEADER_KEYS",
            "HAS_HEADER",
            "COALESCE",
            "NULLIF",
            "CAST",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_string_functions(&mut self) {
        let functions = vec![
            "UPPER",
            "LOWER",
            "SUBSTRING",
            "SUBSTR",
            "REPLACE",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "LENGTH",
            "LEN",
            "CONCAT",
            "CONCAT_WS",
            "REGEXP",
            "REGEXP_REPLACE",
            "SPLIT",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_math_functions(&mut self) {
        let functions = vec![
            "ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "SQRT", "POWER", "POW", "MOD", "EXP", "LN",
            "LOG", "LOG10",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_date_time_functions(&mut self) {
        let functions = vec![
            "NOW",
            "CURRENT_TIMESTAMP",
            "CURRENT_TIME",
            "CURRENT_DATE",
            "TIMESTAMP",
            "DATE",
            "TIME",
            "EXTRACT",
            "DATE_TRUNC",
            "TO_TIMESTAMP",
            "TO_DATE",
            "DATE_ADD",
            "DATE_SUB",
            "DATE_DIFF",
            "DATEDIFF",
            // Tumbling window functions
            "TUMBLE_START",
            "TUMBLE_END",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_conditional_functions(&mut self) {
        let functions = vec!["IF", "CASE", "WHEN", "ELSE", "END"];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_window_functions(&mut self) {
        // Functions that can be used in OVER clauses
        let functions = vec![
            // Ranking functions
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "PERCENT_RANK",
            "CUME_DIST",
            "NTILE",
            // Value functions
            "LAG",
            "LEAD",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
            self.window_functions.insert(func.to_string());
        }

        // Note: AVG, SUM, COUNT, MIN, MAX are aggregate functions
        // but are NOT currently supported in OVER clauses
        // This is intentional - they would need special handling
    }

    fn register_json_functions(&mut self) {
        let functions = vec!["JSON_VALUE", "JSON_QUERY", "JSON_OBJECT", "JSON_ARRAY"];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_array_functions(&mut self) {
        let functions = vec![
            "ARRAY_LENGTH",
            "ARRAY_CONTAINS",
            "ARRAY_DISTINCT",
            "MAP_KEYS",
            "MAP_VALUES",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global function registry instance (lazy-initialized)
pub static FUNCTION_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(FunctionRegistry::new);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_functions() {
        let registry = FunctionRegistry::new();

        // Test aggregate functions
        assert!(registry.is_function_supported("COUNT"));
        assert!(registry.is_function_supported("SUM"));
        assert!(registry.is_function_supported("AVG"));

        // Test string functions
        assert!(registry.is_function_supported("UPPER"));
        assert!(registry.is_function_supported("SUBSTRING"));

        // Test date/time functions
        assert!(registry.is_function_supported("TUMBLE_START"));
        assert!(registry.is_function_supported("TUMBLE_END"));

        // Test unsupported functions
        assert!(!registry.is_function_supported("UNKNOWN_FUNC"));
    }

    #[test]
    fn test_window_functions() {
        let registry = FunctionRegistry::new();

        // Test window-only functions
        assert!(registry.is_window_function("ROW_NUMBER"));
        assert!(registry.is_window_function("LAG"));
        assert!(registry.is_window_function("LEAD"));

        // Test that aggregates are NOT window functions
        assert!(!registry.is_window_function("AVG"));
        assert!(!registry.is_window_function("SUM"));
        assert!(!registry.is_window_function("COUNT"));
    }

    #[test]
    fn test_case_insensitive() {
        let registry = FunctionRegistry::new();

        assert!(registry.is_function_supported("count"));
        assert!(registry.is_function_supported("COUNT"));
        assert!(registry.is_function_supported("Count"));
    }

    #[test]
    fn test_find_similar() {
        let registry = FunctionRegistry::new();

        let similar = registry.find_similar_functions("LAST", 5);
        assert!(similar.contains(&"LAST_VALUE".to_string()));
    }

    #[test]
    fn test_aggregate_functions() {
        let registry = FunctionRegistry::new();

        // Test aggregate functions
        assert!(registry.is_aggregate_function("COUNT"));
        assert!(registry.is_aggregate_function("SUM"));
        assert!(registry.is_aggregate_function("AVG"));
        assert!(registry.is_aggregate_function("MIN"));
        assert!(registry.is_aggregate_function("MAX"));
        assert!(registry.is_aggregate_function("APPROX_COUNT_DISTINCT"));

        // Test case insensitivity
        assert!(registry.is_aggregate_function("count"));
        assert!(registry.is_aggregate_function("Count"));

        // Test non-aggregate functions
        assert!(!registry.is_aggregate_function("UPPER"));
        assert!(!registry.is_aggregate_function("SUBSTRING"));
        assert!(!registry.is_aggregate_function("NOW"));

        // Test window functions are NOT aggregates (unless they're also aggregates)
        assert!(!registry.is_aggregate_function("ROW_NUMBER"));
        assert!(!registry.is_aggregate_function("LAG"));
        assert!(!registry.is_aggregate_function("LEAD"));
    }

    #[test]
    fn test_aggregate_and_window_distinction() {
        let registry = FunctionRegistry::new();

        // FIRST_VALUE and LAST_VALUE are both aggregates AND window functions
        assert!(registry.is_aggregate_function("FIRST_VALUE"));
        assert!(registry.is_window_function("FIRST_VALUE"));
        assert!(registry.is_aggregate_function("LAST_VALUE"));
        assert!(registry.is_window_function("LAST_VALUE"));

        // Most aggregates are NOT window functions
        assert!(registry.is_aggregate_function("COUNT"));
        assert!(!registry.is_window_function("COUNT"));
        assert!(registry.is_aggregate_function("SUM"));
        assert!(!registry.is_window_function("SUM"));

        // Most window functions are NOT aggregates
        assert!(registry.is_window_function("ROW_NUMBER"));
        assert!(!registry.is_aggregate_function("ROW_NUMBER"));
        assert!(registry.is_window_function("RANK"));
        assert!(!registry.is_aggregate_function("RANK"));
    }
}
