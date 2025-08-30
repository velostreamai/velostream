//! Simple tests for DECIMAL type functionality
//!
//! This module tests basic DECIMAL type implementation that works with
//! the current codebase structure.

use ferrisstreams::ferris::sql::ast::{DataType, Expr, LiteralValue, SelectField, StreamingQuery};
use ferrisstreams::ferris::sql::execution::types::FieldValue;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod decimal_simple_tests {
    use super::*;

    #[test]
    fn test_decimal_datatype_parsing() {
        let parser = StreamingSqlParser::new();

        // Test CREATE TABLE with DECIMAL column
        let sql = "CREATE TABLE financial_data (
            id INTEGER,
            amount DECIMAL,
            price NUMERIC
        ) AS SELECT * FROM orders";

        let result = parser.parse(sql);
        assert!(
            result.is_ok(),
            "Failed to parse CREATE TABLE with DECIMAL: {:?}",
            result.err()
        );

        // Verify the parsed structure
        if let Ok(query) = result {
            match &query {
                StreamingQuery::CreateTable { columns, .. } => {
                    if let Some(cols) = columns {
                        assert_eq!(cols.len(), 3);
                        assert_eq!(cols[0].data_type, DataType::Integer);
                        assert_eq!(cols[1].data_type, DataType::Decimal);
                        assert_eq!(cols[2].data_type, DataType::Decimal);
                    } else {
                        panic!("Expected columns in CreateTable");
                    }
                }
                _ => panic!("Expected CreateTable query, got: {:?}", query),
            }
        }
    }

    #[test]
    fn test_decimal_literal_parsing() {
        let parser = StreamingSqlParser::new();

        let sql = "SELECT 123.45";
        let result = parser.parse(sql);
        assert!(
            result.is_ok(),
            "Failed to parse SELECT with decimal: {:?}",
            result.err()
        );

        if let Ok(query) = result {
            match &query {
                StreamingQuery::Select { fields, .. } => {
                    if let Some(first_field) = fields.first() {
                        match first_field {
                            SelectField::Expression { expr, .. } => {
                                match expr {
                                    Expr::Literal(LiteralValue::Float(value)) => {
                                        assert!((value - 123.45).abs() < f64::EPSILON);
                                    }
                                    _ => panic!("Expected float literal (decimal numbers parse as Float), got: {:?}", expr),
                                }
                            }
                            _ => panic!("Expected expression field, got: {:?}", first_field),
                        }
                    } else {
                        panic!("Expected fields to have at least one item");
                    }
                }
                _ => panic!("Expected SELECT query, got: {:?}", query),
            }
        }
    }

    #[test]
    fn test_scaled_integer_creation() {
        // Test creating ScaledInteger values manually
        let decimal_123_45 = FieldValue::ScaledInteger(12345, 2); // 123.45

        match decimal_123_45 {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(value, 12345);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected ScaledInteger"),
        }
    }

    #[test]
    fn test_scaled_integer_arithmetic() {
        let a = FieldValue::ScaledInteger(12345, 2); // 123.45
        let b = FieldValue::ScaledInteger(6789, 2); // 67.89

        // Test addition
        let sum = a.add(&b).unwrap();
        match sum {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(value, 19134); // 191.34
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected ScaledInteger result, got: {:?}", sum),
        }

        // Test subtraction
        let diff = a.subtract(&b).unwrap();
        match diff {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(value, 5556); // 55.56
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected ScaledInteger result, got: {:?}", diff),
        }

        // Test multiplication
        let product = a.multiply(&b).unwrap();
        match product {
            FieldValue::ScaledInteger(value, scale) => {
                // 123.45 * 67.89 = 8381.0205 (ScaledInteger result)
                assert_eq!(value, 83810205);
                assert_eq!(scale, 4);
            }
            _ => panic!("Expected ScaledInteger result, got: {:?}", product),
        }
    }

    #[test]
    fn test_decimal_casting() {
        let decimal = FieldValue::ScaledInteger(12345, 2); // 123.45

        // Cast to Float
        let float_val = decimal.clone().cast_to("FLOAT").unwrap();
        match float_val {
            FieldValue::Float(f) => {
                assert!((f - 123.45).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Float, got: {:?}", float_val),
        }

        // Cast to Integer (should truncate)
        let int_val = decimal.clone().cast_to("INTEGER").unwrap();
        match int_val {
            FieldValue::Integer(i) => {
                assert_eq!(i, 123);
            }
            _ => panic!("Expected Integer, got: {:?}", int_val),
        }

        // Cast to String
        let string_val = decimal.clone().cast_to("STRING").unwrap();
        match string_val {
            FieldValue::String(s) => {
                assert_eq!(s, "123.45");
            }
            _ => panic!("Expected String, got: {:?}", string_val),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = FieldValue::ScaledInteger(12345, 2); // 123.45

        // Test that ScaledInteger maintains precision internally
        match &original {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(*value, 12345);
                assert_eq!(*scale, 2);

                // Test string conversion maintains precision
                let display = original.to_display_string();
                assert_eq!(display, "123.45");
            }
            _ => panic!("Expected ScaledInteger, got: {:?}", original),
        }

        // Test that clone maintains the same value
        let cloned = original.clone();
        assert_eq!(cloned, original);
    }

    #[test]
    fn test_financial_precision() {
        // Test realistic financial calculation scenarios
        let price = FieldValue::ScaledInteger(12995, 2); // $129.95
        let quantity = FieldValue::Integer(350); // 350 units

        // Calculate total: price * quantity
        let total = price.multiply(&quantity).unwrap();
        match total {
            FieldValue::ScaledInteger(value, scale) => {
                // 129.95 * 350 = 45482.50
                assert_eq!(value, 4548250);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected ScaledInteger result, got: {:?}", total),
        }
    }

    #[test]
    fn test_scale_alignment_in_arithmetic() {
        let a = FieldValue::ScaledInteger(123, 1); // 12.3
        let b = FieldValue::ScaledInteger(4567, 2); // 45.67

        // Addition should align scales automatically
        let sum = a.add(&b).unwrap();
        match sum {
            FieldValue::ScaledInteger(value, scale) => {
                // 12.3 + 45.67 = 57.97
                assert_eq!(value, 5797);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected ScaledInteger result, got: {:?}", sum),
        }
    }

    #[test]
    fn test_decimal_type_name() {
        let decimal = FieldValue::ScaledInteger(12345, 2);
        let type_name = decimal.type_name();
        assert_eq!(type_name, "SCALED_INTEGER");
    }

    #[test]
    fn test_zero_handling() {
        let zero = FieldValue::ScaledInteger(0, 2);
        let number = FieldValue::ScaledInteger(12345, 2);

        // Adding zero
        let result = number.add(&zero).unwrap();
        assert_eq!(result, number);

        // Multiplying by zero
        let result = number.multiply(&zero).unwrap();
        match result {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(value, 0);
                assert_eq!(scale, 4); // Scale adds up in multiplication
            }
            _ => panic!("Expected ScaledInteger result"),
        }
    }

    #[test]
    fn test_financial_conversion() {
        let decimal = FieldValue::ScaledInteger(12345, 2); // 123.45

        // Test conversion to f64 for compatibility
        let f64_val = decimal.to_financial_f64().unwrap();
        assert!((f64_val - 123.45).abs() < f64::EPSILON);

        // Test financial type check
        assert!(decimal.is_financial());

        let integer = FieldValue::Integer(123);
        assert!(!integer.is_financial());
    }
}
