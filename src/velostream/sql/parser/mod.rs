/*!
Streaming SQL Parser

This module provides parsing functionality for streaming SQL queries.
The parser is organized into logical submodules but maintains backward compatibility
with the original monolithic structure.
*/

// Submodules (existing)
pub mod annotations;
pub mod validator;

// Core parser implementation (temporary - will be split further)
mod core;

// Re-export public types and structures
pub use core::{StreamingSqlParser, Token, TokenType};
