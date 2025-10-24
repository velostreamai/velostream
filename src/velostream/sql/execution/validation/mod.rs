//! Runtime Field Validation
//!
//! This module provides runtime validation for fields during query execution,
//! ensuring that fields referenced in queries actually exist in records and
//! have appropriate types for the operations being performed.
//!
//! ## Purpose
//!
//! While Phase 1 validates aggregate function usage at parse time, Phase 2
//! validates field availability and type compatibility at runtime, providing
//! early detection of issues that would otherwise fail during record processing.
//!
//! ## Validation Points
//!
//! - **Window Processing**: Validates GROUP BY and PARTITION BY fields exist
//! - **Aggregation**: Validates fields used in aggregate expressions
//! - **SELECT Fields**: Validates all fields in SELECT clause exist
//! - **JOIN Conditions**: Validates ON clause fields from both tables
//! - **HAVING Clause**: Validates fields in HAVING expressions

pub mod field_validator;

pub use field_validator::{FieldValidationError, FieldValidator, ValidationContext};
