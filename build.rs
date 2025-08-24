use std::io::Result;

fn main() -> Result<()> {
    // Only build protobuf if the feature is enabled
    #[cfg(feature = "protobuf")]
    {
        prost_build::compile_protos(&["src/ferris/serialization/financial.proto"], &["src/"])?;
    }
    Ok(())
}
