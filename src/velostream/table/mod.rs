/*!
# Table Module

This module provides table functionality for materialized views of Kafka topics.
Tables represent the latest state for each key and support SQL-like queries.

## Core Components

- `table`: Core Table implementation for maintaining materialized state
- `sql`: SQL query interface for Tables with full AST integration

## Re-exports

Public interface for Table functionality.
*/

pub mod sql;
pub mod table;

// Re-export public types
pub use sql::{KafkaDataSource, SqlDataSource, SqlQueryable};
pub use table::{ChangeEvent, Table, TableStats};
