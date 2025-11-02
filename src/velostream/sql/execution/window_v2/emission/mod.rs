//! Emission Strategy Implementations
//!
//! This module contains concrete implementations of the EmissionStrategy trait.

pub mod emit_changes;
pub mod emit_final;

// Re-exports
pub use emit_changes::EmitChangesStrategy;
pub use emit_final::EmitFinalStrategy;
