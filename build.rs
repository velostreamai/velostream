use std::io::Result;
use std::process::Command;

fn main() -> Result<()> {
    // Always build protobuf definitions
    prost_build::compile_protos(&["src/velostream/serialization/financial.proto"], &["src/"])?;

    // Set build timestamp
    let build_time = if let Ok(output) = Command::new("date").arg("+%Y-%m-%d %H:%M:%S UTC").output()
    {
        String::from_utf8(output.stdout)
            .unwrap_or_else(|_| "unknown".to_string())
            .trim()
            .to_string()
    } else {
        "unknown".to_string()
    };

    println!("cargo:rustc-env=BUILD_TIME={}", build_time);

    Ok(())
}
