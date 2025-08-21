/*!
# DML Operations Test Suite

Comprehensive tests for INSERT, UPDATE, DELETE operations in processors.

## Test Coverage

- INSERT INTO operations (VALUES and SELECT sources)
- UPDATE operations (conditional and unconditional)
- DELETE operations (conditional and unconditional)
- Error handling and validation
- Streaming semantics (tombstones, timestamps, headers)
- Edge cases and boundary conditions

## Test Organization

Tests are organized by DML operation type with comprehensive scenarios for each.
*/

pub mod delete_test;
pub mod insert_test;
pub mod update_test;
