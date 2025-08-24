use crate::ferris::serialization::InternalValue;
use crate::ferris::sql::ast::TimeUnit;
/// Field Value Conversion Utilities
///
/// Handles conversion between FieldValue and InternalValue types.
/// These are pure conversion functions with no engine state dependency.
use crate::ferris::sql::execution::FieldValue;
use std::collections::HashMap;

/// Utility class for converting between FieldValue and InternalValue
pub struct FieldValueConverter;

impl FieldValueConverter {
    /// Convert FieldValue to InternalValue for output serialization
    pub fn field_value_to_internal(value: FieldValue) -> InternalValue {
        match value {
            FieldValue::Integer(i) => InternalValue::Integer(i),
            FieldValue::Float(f) => InternalValue::Number(f),
            FieldValue::String(s) => InternalValue::String(s),
            FieldValue::Boolean(b) => InternalValue::Boolean(b),
            FieldValue::Null => InternalValue::Null,
            FieldValue::Date(d) => InternalValue::String(d.format("%Y-%m-%d").to_string()),
            FieldValue::Timestamp(ts) => {
                InternalValue::String(ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            }
            FieldValue::Decimal(dec) => InternalValue::String(dec.to_string()),
            FieldValue::ScaledInteger(value, scale) => InternalValue::ScaledNumber(value, scale),
            FieldValue::Array(arr) => {
                let internal_arr: Vec<InternalValue> =
                    arr.into_iter().map(Self::field_value_to_internal).collect();
                InternalValue::Array(internal_arr)
            }
            FieldValue::Map(map) => {
                let internal_map: HashMap<String, InternalValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, Self::field_value_to_internal(v)))
                    .collect();
                InternalValue::Object(internal_map)
            }
            FieldValue::Struct(fields) => {
                let internal_map: HashMap<String, InternalValue> = fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::field_value_to_internal(v)))
                    .collect();
                InternalValue::Object(internal_map)
            }
            FieldValue::Interval { value, unit } => {
                // Convert interval to milliseconds for output
                let millis = match unit {
                    TimeUnit::Millisecond => value,
                    TimeUnit::Second => value * 1000,
                    TimeUnit::Minute => value * 60 * 1000,
                    TimeUnit::Hour => value * 60 * 60 * 1000,
                    TimeUnit::Day => value * 24 * 60 * 60 * 1000,
                };
                InternalValue::Integer(millis)
            }
        }
    }

    /// Convert InternalValue to FieldValue
    pub fn internal_to_field_value(value: InternalValue) -> FieldValue {
        match value {
            InternalValue::Integer(i) => FieldValue::Integer(i),
            InternalValue::Number(f) => FieldValue::Float(f),
            InternalValue::String(s) => FieldValue::String(s),
            InternalValue::Boolean(b) => FieldValue::Boolean(b),
            InternalValue::Null => FieldValue::Null,
            InternalValue::ScaledNumber(value, scale) => FieldValue::ScaledInteger(value, scale),
            InternalValue::Array(arr) => {
                let field_arr: Vec<FieldValue> =
                    arr.into_iter().map(Self::internal_to_field_value).collect();
                FieldValue::Array(field_arr)
            }
            InternalValue::Object(map) => {
                let field_map: HashMap<String, FieldValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, Self::internal_to_field_value(v)))
                    .collect();
                FieldValue::Map(field_map)
            }
        }
    }
}
