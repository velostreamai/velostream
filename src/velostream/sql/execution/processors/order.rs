//! ORDER BY Sorting Processor
//!
//! Implements sorting of StreamRecords based on ORDER BY expressions.
//! This processor is used in windowed queries to enforce proper ordering
//! within window partitions before computing window functions.

use crate::velostream::sql::SqlError;
use crate::velostream::sql::ast::OrderByExpr;
use crate::velostream::sql::ast::OrderDirection;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::processors::context::ProcessorContext;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use crate::velostream::sql::execution::utils::FieldValueComparator;
use std::cmp::Ordering;

/// Processor for ORDER BY sorting operations
pub struct OrderProcessor;

impl OrderProcessor {
    /// Process records by sorting them according to ORDER BY expressions
    ///
    /// # Arguments
    ///
    /// * `records` - Vector of records to sort
    /// * `order_by` - ORDER BY expressions specifying sort order
    /// * `context` - Processor context for evaluation
    ///
    /// # Returns
    ///
    /// Sorted vector of records or error if evaluation fails
    pub fn process(
        mut records: Vec<StreamRecord>,
        order_by: &[OrderByExpr],
        context: &ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        if order_by.is_empty() {
            return Ok(records);
        }

        // Sort records using the ORDER BY expressions
        records.sort_by(|a, b| {
            Self::compare_records(a, b, order_by, context).unwrap_or(Ordering::Equal)
        });

        Ok(records)
    }

    /// Compare two records based on ORDER BY expressions
    ///
    /// # Arguments
    ///
    /// * `left` - First record to compare
    /// * `right` - Second record to compare
    /// * `order_by` - ORDER BY expressions in order of priority
    /// * `context` - Processor context for evaluation
    ///
    /// # Returns
    ///
    /// Ordering result (Less, Equal, Greater) or error
    fn compare_records(
        left: &StreamRecord,
        right: &StreamRecord,
        order_by: &[OrderByExpr],
        _context: &ProcessorContext,
    ) -> Result<Ordering, SqlError> {
        // Compare each ORDER BY expression in sequence
        for order_expr in order_by {
            let left_value =
                ExpressionEvaluator::evaluate_expression_value(&order_expr.expr, left)?;
            let right_value =
                ExpressionEvaluator::evaluate_expression_value(&order_expr.expr, right)?;

            let comparison = Self::compare_values(&left_value, &right_value)?;

            // If values are not equal, apply direction and return
            if comparison != Ordering::Equal {
                let result = match &order_expr.direction {
                    OrderDirection::Asc => comparison,
                    OrderDirection::Desc => comparison.reverse(),
                };
                return Ok(result);
            }
            // If equal, continue to next ORDER BY expression
        }

        // All ORDER BY expressions were equal
        Ok(Ordering::Equal)
    }

    /// Compare two FieldValues for ordering
    ///
    /// # Arguments
    ///
    /// * `left` - First value to compare
    /// * `right` - Second value to compare
    ///
    /// # Returns
    ///
    /// Ordering result or error if values are incomparable
    fn compare_values(left: &FieldValue, right: &FieldValue) -> Result<Ordering, SqlError> {
        use FieldValue::*;

        match (left, right) {
            // Handle NULL values (NULL is smallest)
            (Null, Null) => Ok(Ordering::Equal),
            (Null, _) => Ok(Ordering::Less),
            (_, Null) => Ok(Ordering::Greater),

            // Integer comparison
            (Integer(a), Integer(b)) => Ok(a.cmp(b)),

            // Float comparison
            (Float(a), Float(b)) => {
                let cmp_result = if a < b {
                    Ordering::Less
                } else if a > b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }

            // ScaledInteger comparison
            (ScaledInteger(a, scale_a), ScaledInteger(b, scale_b)) => {
                // Normalize to same scale before comparing
                let a_normalized = Self::scale_value(*a, *scale_a, *scale_b);
                let b_normalized = *b;
                Ok(a_normalized.cmp(&b_normalized))
            }

            // String comparison
            (String(a), String(b)) => Ok(a.cmp(b)),

            // Boolean comparison
            (Boolean(a), Boolean(b)) => Ok(a.cmp(b)),

            // Date comparison
            (Date(a), Date(b)) => Ok(a.cmp(b)),

            // Timestamp comparison
            (Timestamp(a), Timestamp(b)) => Ok(a.cmp(b)),

            // Decimal comparison
            (Decimal(a), Decimal(b)) => Ok(a.cmp(b)),

            // Interval comparison
            (
                Interval {
                    value: a,
                    unit: unit_a,
                },
                Interval {
                    value: b,
                    unit: unit_b,
                },
            ) => {
                // Compare intervals of same unit
                if unit_a == unit_b {
                    Ok(a.cmp(b))
                } else {
                    Err(SqlError::TypeError {
                        expected: format!("Interval({:?})", unit_a),
                        actual: format!("Interval({:?})", unit_b),
                        value: None,
                    })
                }
            }

            // Numeric type coercion for comparison
            (Integer(a), Float(b)) => {
                let a_float = *a as f64;
                let cmp_result = if a_float < *b {
                    Ordering::Less
                } else if a_float > *b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }
            (Float(a), Integer(b)) => {
                let b_float = *b as f64;
                let cmp_result = if a < &b_float {
                    Ordering::Less
                } else if a > &b_float {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }

            // ScaledInteger to Integer coercion
            (ScaledInteger(a, scale), Integer(b)) => {
                let a_float = FieldValueComparator::scaled_to_f64(*a, *scale);
                let b_float = *b as f64;
                let cmp_result = if a_float < b_float {
                    Ordering::Less
                } else if a_float > b_float {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }
            (Integer(a), ScaledInteger(b, scale)) => {
                let a_float = *a as f64;
                let b_float = FieldValueComparator::scaled_to_f64(*b, *scale);
                let cmp_result = if a_float < b_float {
                    Ordering::Less
                } else if a_float > b_float {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }

            // ScaledInteger to Float coercion
            (ScaledInteger(a, scale), Float(b)) => {
                let a_float = FieldValueComparator::scaled_to_f64(*a, *scale);
                let cmp_result = if a_float < *b {
                    Ordering::Less
                } else if a_float > *b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }
            (Float(a), ScaledInteger(b, scale)) => {
                let b_float = FieldValueComparator::scaled_to_f64(*b, *scale);
                let cmp_result = if a < &b_float {
                    Ordering::Less
                } else if a > &b_float {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                };
                Ok(cmp_result)
            }

            // Incomparable types
            _ => Err(SqlError::TypeError {
                expected: format!("{:?}", left),
                actual: format!("{:?}", right),
                value: None,
            }),
        }
    }

    /// Scale a ScaledInteger value to a different scale
    ///
    /// # Arguments
    ///
    /// * `value` - The original scaled integer value
    /// * `from_scale` - Original scale
    /// * `to_scale` - Target scale
    ///
    /// # Returns
    ///
    /// Value scaled to the target scale
    fn scale_value(value: i64, from_scale: u8, to_scale: u8) -> i64 {
        if from_scale == to_scale {
            value
        } else if from_scale < to_scale {
            // Scale up (multiply)
            let diff = (to_scale - from_scale) as u32;
            value * 10i64.pow(diff)
        } else {
            // Scale down (divide)
            let diff = (from_scale - to_scale) as u32;
            value / 10i64.pow(diff)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::ast::Expr;
    use std::collections::HashMap;

    #[test]
    fn test_order_processor_simple_integer_asc() {
        let records = vec![
            StreamRecord {
                fields: {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), FieldValue::Integer(3));
                    m
                },
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            },
            StreamRecord {
                fields: {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), FieldValue::Integer(1));
                    m
                },
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            },
            StreamRecord {
                fields: {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), FieldValue::Integer(2));
                    m
                },
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            },
        ];

        let order_by = vec![OrderByExpr {
            expr: Expr::Column("id".to_string()),
            direction: OrderDirection::Asc,
        }];

        let context = ProcessorContext::new("test_order");
        let sorted = OrderProcessor::process(records, &order_by, &context).unwrap();

        assert_eq!(sorted.len(), 3);
        if let FieldValue::Integer(id) = sorted[0].fields.get("id").unwrap() {
            assert_eq!(*id, 1);
        } else {
            panic!("Expected integer");
        }
    }
}
