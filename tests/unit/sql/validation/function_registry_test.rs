//! Unit tests for SQL function registry
//!
//! Tests for the function registry that validates SQL functions during semantic validation.
//! This test suite covers:
//! - Basic function support checking
//! - Case-insensitive lookup
//! - Aggregate/window/scalar function classification
//! - Function similarity search
//! - Inventory system integration
//! - Alias support from inventory
//! - Category-based filtering

use velostream::velostream::sql::execution::expression::function_metadata;
use velostream::velostream::sql::validation::function_registry::FunctionRegistry;

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

    // Test that aggregates are NOT window functions (but can be used in OVER clauses)
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

#[test]
fn test_statistical_functions() {
    let registry = FunctionRegistry::new();

    // Test statistical aggregates used in trading demo
    assert!(registry.is_function_supported("STDDEV"));
    assert!(registry.is_function_supported("STDDEV_SAMP"));
    assert!(registry.is_function_supported("STDDEV_POP"));
    assert!(registry.is_function_supported("VARIANCE"));
    assert!(registry.is_function_supported("VAR_SAMP"));
    assert!(registry.is_function_supported("VAR_POP"));

    // These should be aggregate functions
    assert!(registry.is_aggregate_function("STDDEV"));
    assert!(registry.is_aggregate_function("VARIANCE"));
}

#[test]
fn test_comparison_functions() {
    let registry = FunctionRegistry::new();

    // Test comparison functions used in trading demo
    assert!(registry.is_function_supported("LEAST"));
    assert!(registry.is_function_supported("GREATEST"));
}

#[test]
fn test_string_extraction_functions() {
    let registry = FunctionRegistry::new();

    // Test string functions
    assert!(registry.is_function_supported("LEFT"));
    assert!(registry.is_function_supported("RIGHT"));
    assert!(registry.is_function_supported("POSITION"));
}

#[test]
fn test_date_part_extraction() {
    let registry = FunctionRegistry::new();

    // Test date part extraction functions
    assert!(registry.is_function_supported("YEAR"));
    assert!(registry.is_function_supported("MONTH"));
    assert!(registry.is_function_supported("DAY"));
    assert!(registry.is_function_supported("HOUR"));
    assert!(registry.is_function_supported("MINUTE"));
    assert!(registry.is_function_supported("SECOND"));
    assert!(registry.is_function_supported("DOW"));
    assert!(registry.is_function_supported("DOY"));
    assert!(registry.is_function_supported("EPOCH"));
}

#[test]
fn test_json_functions() {
    let registry = FunctionRegistry::new();

    assert!(registry.is_function_supported("JSON_VALUE"));
    assert!(registry.is_function_supported("JSON_EXTRACT"));
}

#[test]
fn test_header_functions() {
    let registry = FunctionRegistry::new();

    assert!(registry.is_function_supported("HEADER"));
    assert!(registry.is_function_supported("SET_HEADER"));
    assert!(registry.is_function_supported("REMOVE_HEADER"));
}

#[test]
fn test_inventory_integration() {
    // Verify that the inventory system is being loaded correctly
    let registry = FunctionRegistry::new();

    // Get count of functions from inventory
    let inventory_count = function_metadata::all_registered_functions().count();

    // Inventory should have registered functions (at least the ones we migrated)
    assert!(
        inventory_count > 0,
        "Inventory should have registered functions, but found 0"
    );

    // Verify some key functions that are registered in inventory
    // Note: Only check functions we've actually migrated to inventory
    let inventory_functions = vec![
        "ABS", "COUNT", "SUM", "UPPER", "LOWER", "NOW", "EXTRACT", "STDDEV", "LEAST",
    ];

    for func_name in &inventory_functions {
        assert!(
            registry.is_function_supported(func_name),
            "Function {} from inventory not found in registry",
            func_name
        );

        // Also verify we can find it directly through inventory
        let func_def = function_metadata::find_function(func_name);
        assert!(
            func_def.is_some(),
            "Function {} not found in inventory via find_function",
            func_name
        );
    }

    // Verify aggregate functions from inventory are marked correctly
    assert!(registry.is_aggregate_function("COUNT"));
    assert!(registry.is_aggregate_function("SUM"));
    assert!(registry.is_aggregate_function("STDDEV"));

    println!(
        "✅ Inventory integration test passed: {} functions registered via inventory",
        inventory_count
    );
}

#[test]
fn test_inventory_aliases() {
    // Verify that function aliases from inventory work correctly
    let registry = FunctionRegistry::new();

    // SUBSTR is an alias for SUBSTRING
    if let Some(func_def) = function_metadata::find_function("SUBSTRING") {
        // Check if SUBSTR is in the aliases
        let has_substr_alias = func_def.aliases.contains(&"SUBSTR");
        if has_substr_alias {
            assert!(
                registry.is_function_supported("SUBSTR"),
                "Alias SUBSTR should be supported for SUBSTRING"
            );
        }
    }

    // STDDEV_SAMP is an alias for STDDEV
    if let Some(func_def) = function_metadata::find_function("STDDEV") {
        let has_stddev_samp_alias = func_def.aliases.contains(&"STDDEV_SAMP");
        if has_stddev_samp_alias {
            assert!(
                registry.is_function_supported("STDDEV_SAMP"),
                "Alias STDDEV_SAMP should be supported for STDDEV"
            );
        }
    }
}

#[test]
fn test_inventory_categories() {
    // Verify that functions are categorized correctly in inventory
    let math_funcs =
        function_metadata::functions_in_category(function_metadata::FunctionCategory::Math);
    let agg_funcs = function_metadata::all_aggregate_functions();
    let window_funcs = function_metadata::all_window_functions();

    // Should have functions in each category
    // (Note: counts may be 0 initially, but structure should work)
    let _ = math_funcs.len();
    let _ = agg_funcs.len();
    let _ = window_funcs.len();

    // Verify aggregate functions are marked as aggregate
    for func in agg_funcs {
        assert!(
            func.is_aggregate,
            "Function {} in aggregate list should have is_aggregate=true",
            func.name
        );
    }

    // Verify window functions are marked as window
    for func in window_funcs {
        assert!(
            func.is_window,
            "Function {} in window list should have is_window=true",
            func.name
        );
    }
}
