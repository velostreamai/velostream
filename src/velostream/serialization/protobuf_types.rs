//! High-performance Protobuf types for financial data serialization
//!
//! This module contains protobuf message definitions optimized for:
//! - Exact financial precision with ScaledInteger -> Decimal message
//! - Maximum performance with direct binary serialization
//! - Cross-system compatibility using industry-standard Decimal format

pub mod financial {
    // Include the generated protobuf code
    include!(concat!(env!("OUT_DIR"), "/velo.serialization.rs"));
}

#[allow(unused_imports)]
pub use financial::*;
