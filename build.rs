use std::io::Result;

fn main() -> Result<()> {
    // Always build protobuf definitions
    prost_build::compile_protos(&["src/ferris/serialization/financial.proto"], &["src/"])?;
    Ok(())
}
