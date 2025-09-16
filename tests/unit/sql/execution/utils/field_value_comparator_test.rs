/*!
# Field Value Comparator Tests

Tests for ScaledInteger and all other FieldValue comparison operations.
*/

use rust_decimal::Decimal;
use std::collections::HashMap;
use velostream::velostream::sql::ast::BinaryOperator;
use velostream::velostream::sql::execution::utils::field_value_comparator::FieldValueComparator;
use velostream::velostream::sql::execution::FieldValue;

/// Test ScaledInteger equality comparisons
#[test]
fn test_scaled_integer_equality() {
    // Same scale and value - should be equal
    let a = FieldValue::ScaledInteger(12345, 2); // 123.45
    let b = FieldValue::ScaledInteger(12345, 2); // 123.45
    assert!(FieldValueComparator::values_equal(&a, &b));

    // Same value but different scales - should not be equal in exact comparison
    let c = FieldValue::ScaledInteger(1234500, 4); // 123.45
    assert!(!FieldValueComparator::values_equal(&a, &c));
}

/// Test ScaledInteger equality with coercion
#[test]
fn test_scaled_integer_equality_with_coercion() {
    // Same mathematical value but different scales - should be equal with coercion
    let a = FieldValue::ScaledInteger(12345, 2); // 123.45
    let b = FieldValue::ScaledInteger(1234500, 4); // 123.4500
    assert!(FieldValueComparator::values_equal_with_coercion(&a, &b));

    // Different values - should not be equal
    let c = FieldValue::ScaledInteger(12346, 2); // 123.46
    assert!(!FieldValueComparator::values_equal_with_coercion(&a, &c));
}

/// Test ScaledInteger coercion with other numeric types
#[test]
fn test_scaled_integer_coercion_with_numeric_types() {
    let scaled = FieldValue::ScaledInteger(12345, 2); // 123.45
    let integer = FieldValue::Integer(123);
    let float = FieldValue::Float(123.45);

    // ScaledInteger to Integer coercion
    assert!(!FieldValueComparator::values_equal_with_coercion(
        &scaled, &integer
    ));

    // ScaledInteger to Float coercion
    assert!(FieldValueComparator::values_equal_with_coercion(
        &scaled, &float
    ));
    assert!(FieldValueComparator::values_equal_with_coercion(
        &float, &scaled
    ));
}

/// Test ScaledInteger coercion with Decimal
#[test]
fn test_scaled_integer_coercion_with_decimal() {
    let scaled = FieldValue::ScaledInteger(12345, 2); // 123.45
    let decimal = FieldValue::Decimal(Decimal::from(123) + Decimal::from(45) / Decimal::from(100)); // 123.45

    // ScaledInteger to Decimal coercion
    assert!(FieldValueComparator::values_equal_with_coercion(
        &scaled, &decimal
    ));
    assert!(FieldValueComparator::values_equal_with_coercion(
        &decimal, &scaled
    ));
}

/// Test ScaledInteger numeric comparisons (>, <, >=, <=)
#[test]
fn test_scaled_integer_numeric_comparisons() -> Result<(), Box<dyn std::error::Error>> {
    let a = FieldValue::ScaledInteger(12345, 2); // 123.45
    let b = FieldValue::ScaledInteger(12340, 2); // 123.40
    let c = FieldValue::ScaledInteger(12350, 2); // 123.50

    // Same scale comparisons
    assert!(FieldValueComparator::compare_values_for_boolean(
        &c,
        &a,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &b,
        &a,
        &BinaryOperator::LessThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &c,
        &BinaryOperator::LessThan
    )?);

    // Equal comparisons
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &a,
        &BinaryOperator::Equal
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::Equal
    )?);

    Ok(())
}

/// Test ScaledInteger comparisons with different scales
#[test]
fn test_scaled_integer_different_scales_comparisons() -> Result<(), Box<dyn std::error::Error>> {
    let a = FieldValue::ScaledInteger(12345, 2); // 123.45
    let b = FieldValue::ScaledInteger(1234500, 4); // 123.4500 (same value, different scale)
    let c = FieldValue::ScaledInteger(1235, 1); // 123.5

    // Same mathematical value, different scales - should be equal in comparisons
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::Equal
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::NotEqual
    )?);

    // Different values, different scales
    assert!(FieldValueComparator::compare_values_for_boolean(
        &c,
        &a,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &c,
        &BinaryOperator::LessThan
    )?);

    Ok(())
}

/// Test ScaledInteger comparisons with Integer values
#[test]
fn test_scaled_integer_with_integer_comparisons() -> Result<(), Box<dyn std::error::Error>> {
    let scaled = FieldValue::ScaledInteger(12300, 2); // 123.00
    let integer = FieldValue::Integer(123);
    let larger_int = FieldValue::Integer(124);

    // ScaledInteger 123.00 should equal Integer 123
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &integer,
        &BinaryOperator::Equal
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &integer,
        &scaled,
        &BinaryOperator::Equal
    )?);

    // Comparison operations
    assert!(FieldValueComparator::compare_values_for_boolean(
        &larger_int,
        &scaled,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &larger_int,
        &BinaryOperator::LessThan
    )?);

    Ok(())
}

/// Test ScaledInteger comparisons with Float values
#[test]
fn test_scaled_integer_with_float_comparisons() -> Result<(), Box<dyn std::error::Error>> {
    let scaled = FieldValue::ScaledInteger(12345, 2); // 123.45
    let float = FieldValue::Float(123.45);
    let larger_float = FieldValue::Float(123.46);

    // ScaledInteger 123.45 should equal Float 123.45
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &float,
        &BinaryOperator::Equal
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &float,
        &scaled,
        &BinaryOperator::Equal
    )?);

    // Comparison operations
    assert!(FieldValueComparator::compare_values_for_boolean(
        &larger_float,
        &scaled,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &larger_float,
        &BinaryOperator::LessThan
    )?);

    Ok(())
}

/// Test ScaledInteger comparisons with Decimal values
#[test]
fn test_scaled_integer_with_decimal_comparisons() -> Result<(), Box<dyn std::error::Error>> {
    let scaled = FieldValue::ScaledInteger(12345, 2); // 123.45
    let decimal = FieldValue::Decimal(Decimal::from(123) + Decimal::from(45) / Decimal::from(100)); // 123.45
    let larger_decimal =
        FieldValue::Decimal(Decimal::from(123) + Decimal::from(46) / Decimal::from(100)); // 123.46

    // ScaledInteger 123.45 should equal Decimal 123.45
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &decimal,
        &BinaryOperator::Equal
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &decimal,
        &scaled,
        &BinaryOperator::Equal
    )?);

    // Comparison operations
    assert!(FieldValueComparator::compare_values_for_boolean(
        &larger_decimal,
        &scaled,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &larger_decimal,
        &BinaryOperator::LessThan
    )?);

    Ok(())
}

/// Test financial precision scenarios with ScaledInteger
#[test]
fn test_financial_precision_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    // Financial calculations that should maintain exact precision
    let price = FieldValue::ScaledInteger(9999, 2); // $99.99
    let price_plus_cent = FieldValue::ScaledInteger(10000, 2); // $100.00
    let discount = FieldValue::ScaledInteger(199, 2); // $1.99

    // Test precise financial comparisons
    assert!(FieldValueComparator::compare_values_for_boolean(
        &price_plus_cent,
        &price,
        &BinaryOperator::GreaterThan
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &price,
        &discount,
        &BinaryOperator::GreaterThan
    )?);

    // Test exact equality (crucial for financial applications)
    let same_price = FieldValue::ScaledInteger(9999, 2);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &price,
        &same_price,
        &BinaryOperator::Equal
    )?);

    Ok(())
}

/// Test edge cases and error scenarios
#[test]
fn test_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let scaled = FieldValue::ScaledInteger(12345, 2);
    let string_val = FieldValue::String("123.45".to_string());

    // Non-numeric comparisons should fail
    let result = FieldValueComparator::compare_values_for_boolean(
        &scaled,
        &string_val,
        &BinaryOperator::GreaterThan,
    );
    assert!(result.is_err());

    // Zero values
    let zero_scaled = FieldValue::ScaledInteger(0, 2);
    let zero_int = FieldValue::Integer(0);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &zero_scaled,
        &zero_int,
        &BinaryOperator::Equal
    )?);

    // Negative values
    let neg_scaled = FieldValue::ScaledInteger(-12345, 2); // -123.45
    let neg_float = FieldValue::Float(-123.45);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &neg_scaled,
        &neg_float,
        &BinaryOperator::Equal
    )?);

    Ok(())
}

/// Test complex data structure comparisons with ScaledInteger
#[test]
fn test_complex_structures_with_scaled_integer() {
    let mut map_a = HashMap::new();
    map_a.insert("price".to_string(), FieldValue::ScaledInteger(12345, 2));
    map_a.insert("quantity".to_string(), FieldValue::Integer(10));

    let mut map_b = HashMap::new();
    map_b.insert("price".to_string(), FieldValue::ScaledInteger(12345, 2));
    map_b.insert("quantity".to_string(), FieldValue::Integer(10));

    let map_field_a = FieldValue::Map(map_a);
    let map_field_b = FieldValue::Map(map_b);

    // Maps with identical ScaledInteger values should be equal
    assert!(FieldValueComparator::values_equal(
        &map_field_a,
        &map_field_b
    ));

    // Test with coercion (different scales)
    let mut map_c = HashMap::new();
    map_c.insert("price".to_string(), FieldValue::ScaledInteger(1234500, 4)); // Same value, different scale
    map_c.insert("quantity".to_string(), FieldValue::Integer(10));

    let map_field_c = FieldValue::Map(map_c);

    // Should be equal with coercion
    assert!(FieldValueComparator::values_equal_with_coercion(
        &map_field_a,
        &map_field_c
    ));
}

/// Test all comparison operators with ScaledInteger
#[test]
fn test_all_comparison_operators() -> Result<(), Box<dyn std::error::Error>> {
    let a = FieldValue::ScaledInteger(12345, 2); // 123.45
    let b = FieldValue::ScaledInteger(12350, 2); // 123.50

    // Test all binary operators
    assert!(FieldValueComparator::compare_values_for_boolean(
        &b,
        &a,
        &BinaryOperator::GreaterThan
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::GreaterThan
    )?);

    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::LessThan
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &b,
        &a,
        &BinaryOperator::LessThan
    )?);

    assert!(FieldValueComparator::compare_values_for_boolean(
        &b,
        &a,
        &BinaryOperator::GreaterThanOrEqual
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &b,
        &b,
        &BinaryOperator::GreaterThanOrEqual
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::GreaterThanOrEqual
    )?);

    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::LessThanOrEqual
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &a,
        &BinaryOperator::LessThanOrEqual
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &b,
        &a,
        &BinaryOperator::LessThanOrEqual
    )?);

    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &a,
        &BinaryOperator::Equal
    )?);
    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::Equal
    )?);

    assert!(!FieldValueComparator::compare_values_for_boolean(
        &a,
        &a,
        &BinaryOperator::NotEqual
    )?);
    assert!(FieldValueComparator::compare_values_for_boolean(
        &a,
        &b,
        &BinaryOperator::NotEqual
    )?);

    Ok(())
}
