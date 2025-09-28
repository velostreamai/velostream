/*!
# Table Module

This module provides table functionality for materialized views of Kafka topics.
Tables represent the latest state for each key and support SQL-like queries.

## Core Components

- `table`: Core Table implementation for maintaining materialized state
- `compact_table`: Memory-optimized Table for millions of records
- `sql`: SQL query interface for Tables with full AST integration
- `ctas`: CREATE TABLE AS SELECT implementation for table creation

## Re-exports

Public interface for Table functionality.
*/

pub mod compact_table;
pub mod ctas;
pub mod error;
pub mod retry_utils;
pub mod sql;
pub mod streaming;
pub mod table;
pub mod unified_table;

// Re-export public types
pub use compact_table::{CompactTable, MemoryStats};
pub use ctas::{CtasExecutor, CtasResult};
pub use error::{CtasError, CtasResult as CtasErrorResult, TableError, TableResult};
pub use sql::{SqlTable, TableDataSource};
pub use table::{ChangeEvent, Table, TableStats};
pub use unified_table::{OptimizedTableImpl, TableResult as UnifiedTableResult, UnifiedTable};
