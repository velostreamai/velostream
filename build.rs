use std::io::Result;
use std::process::Command;

fn main() -> Result<()> {
    // Use vendored protoc for cross-compilation
    // SAFETY: Called once at build script start before any threads exist
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

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

    // Get git commit hash
    let git_hash = if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
    {
        String::from_utf8(output.stdout)
            .unwrap_or_else(|_| "unknown".to_string())
            .trim()
            .to_string()
    } else {
        "unknown".to_string()
    };
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Get git branch
    let git_branch = if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
    {
        String::from_utf8(output.stdout)
            .unwrap_or_else(|_| "unknown".to_string())
            .trim()
            .to_string()
    } else {
        "unknown".to_string()
    };
    println!("cargo:rustc-env=GIT_BRANCH={}", git_branch);

    // Rerun if git HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");

    Ok(())
}
