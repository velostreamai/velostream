use std::io::Result;

fn main() -> Result<()> {
    let proto_file = "src/velostream/serialization/financial.proto";

    // Tell cargo to only re-run the build script if the proto file changes
    // This prevents unnecessary protobuf regeneration on every build
    println!("cargo:rerun-if-changed={}", proto_file);

    // Build protobuf definitions
    prost_build::compile_protos(&[proto_file], &["src/"])?;
    Ok(())
}
