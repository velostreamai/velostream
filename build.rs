use std::io::Result;

fn main() -> Result<()> {
    // Always build protobuf definitions
    prost_build::compile_protos(&["src/velostream/serialization/financial.proto"], &["src/"])?;
    Ok(())
}
