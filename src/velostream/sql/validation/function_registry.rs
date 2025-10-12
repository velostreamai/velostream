//! Function Registry
//!
//! Central registry of all supported SQL functions in Velostream.
//! Used for semantic validation to check if functions exist before runtime.
//!
//! This registry is now automatically populated from self-registering functions
//! using the inventory pattern. Functions register themselves at compile time.

use crate::velostream::sql::execution::expression::function_metadata;
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
    ///
    /// Functions are automatically collected from the inventory system.
    /// Any function registered with `register_sql_function!` will be included.
    pub fn new() -> Self {
        let mut registry = Self {
            supported_functions: HashSet::new(),
            window_functions: HashSet::new(),
            aggregate_functions: HashSet::new(),
        };

        // First, load from inventory (self-registering functions)
        registry.load_from_inventory();

        // Then, load manually registered functions (for backward compatibility during migration)
        registry.register_all_functions();

        registry
    }

    /// Load functions from the inventory system
    fn load_from_inventory(&mut self) {
        for func_def in function_metadata::all_registered_functions() {
            // Register primary name
            self.supported_functions.insert(func_def.name.to_string());

            // Register aliases
            for alias in func_def.aliases {
                self.supported_functions.insert(alias.to_string());
            }

            // Register aggregate status
            if func_def.is_aggregate {
                self.aggregate_functions.insert(func_def.name.to_string());
            }

            // Register window status
            if func_def.is_window {
                self.window_functions.insert(func_def.name.to_string());
            }
        }
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
            // Basic aggregates
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            // Statistical aggregates
            "STDDEV",
            "STDDEV_SAMP",
            "STDDEV_POP",
            "VARIANCE",
            "VAR_SAMP",
            "VAR_POP",
            "MEDIAN",
            // Percentile functions
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            // Correlation and regression
            "CORR",
            "COVAR_POP",
            "COVAR_SAMP",
            "REGR_SLOPE",
            "REGR_INTERCEPT",
            // Distinct counting
            "COUNT_DISTINCT",
            "APPROX_COUNT_DISTINCT",
            // Positional aggregates
            "FIRST_VALUE",
            "LAST_VALUE",
            // String aggregation
            "LISTAGG",
            "STRING_AGG",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
            self.aggregate_functions.insert(func.to_string());
        }
    }

    fn register_scalar_functions(&mut self) {
        let functions = vec![
            // Header manipulation
            "HEADER",
            "HEADER_KEYS",
            "HAS_HEADER",
            "SET_HEADER",
            "REMOVE_HEADER",
            // Type functions
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
            // Case conversion
            "UPPER",
            "LOWER",
            // Substring extraction
            "SUBSTRING",
            "SUBSTR",
            "LEFT",
            "RIGHT",
            // String modification
            "REPLACE",
            "TRIM",
            "LTRIM",
            "RTRIM",
            // String metrics
            "LENGTH",
            "LEN",
            "POSITION",
            // String concatenation
            "CONCAT",
            "CONCAT_WS",
            "JOIN",
            // Pattern matching
            "REGEXP",
            "REGEXP_REPLACE",
            // String splitting
            "SPLIT",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_math_functions(&mut self) {
        let functions = vec![
            // Basic arithmetic
            "ABS", "MOD", // Rounding
            "ROUND", "CEIL", "CEILING", "FLOOR", // Power and roots
            "POWER", "POW", "SQRT", "EXP", // Logarithms
            "LN", "LOG", "LOG10", // Comparison
            "LEAST", "GREATEST",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_date_time_functions(&mut self) {
        let functions = vec![
            // Current time functions
            "NOW",
            "CURRENT_TIMESTAMP",
            "CURRENT_TIME",
            "CURRENT_DATE",
            "TIMESTAMP",
            // Date/time constructors
            "DATE",
            "TIME",
            // Date extraction and formatting
            "EXTRACT",
            "DATE_FORMAT",
            // Date arithmetic
            "DATE_TRUNC",
            "DATE_ADD",
            "DATE_SUB",
            "DATE_DIFF",
            "DATEDIFF",
            // Date part extraction
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "WEEK",
            "QUARTER",
            "DOW",
            "DOY",
            "EPOCH",
            "MILLISECOND",
            "MICROSECOND",
            "NANOSECOND",
            // Unix timestamp functions
            "UNIX_TIMESTAMP",
            "FROM_UNIXTIME",
            // Conversion functions
            "TO_TIMESTAMP",
            "TO_DATE",
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

        // Note: Statistical aggregate functions (STDDEV, VARIANCE, etc.) can also be used
        // in OVER clauses for window-based calculations, but they are registered as
        // aggregate functions above. The execution engine handles their use in OVER clauses.
    }

    fn register_json_functions(&mut self) {
        let functions = vec![
            "JSON_VALUE",
            "JSON_QUERY",
            "JSON_OBJECT",
            "JSON_ARRAY",
            "JSON_EXTRACT",
        ];

        for func in functions {
            self.supported_functions.insert(func.to_string());
        }
    }

    fn register_array_functions(&mut self) {
        let functions = vec![
            // Array operations
            "ARRAY_LENGTH",
            "ARRAY_CONTAINS",
            "ARRAY_DISTINCT",
            // Map operations
            "MAP_KEYS",
            "MAP_VALUES",
            // Constructors
            "ARRAY",
            "MAP",
            "STRUCT",
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
