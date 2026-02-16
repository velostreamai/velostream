# Velostream Installation Guide

Complete guide for installing Velostream pre-built binaries on all supported platforms.

---

## Choose Your Platform

| Platform | Architecture | Download Link |
|----------|--------------|---------------|
| **Linux** | x86_64 (64-bit) | [Download](https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-unknown-linux-musl.tar.gz) |
| **Linux** | ARM64 (Raspberry Pi, AWS Graviton) | [Download](https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-aarch64-unknown-linux-musl.tar.gz) |
| **macOS** | Apple Silicon (M1/M2/M3) | [Download](https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-aarch64-apple-darwin.tar.gz) |
| **macOS** | Intel (x86_64) | [Download](https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-apple-darwin.tar.gz) |
| **Windows** | x86_64 (64-bit) | [Download](https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-pc-windows-msvc.zip) |

**Not sure which to download?** See [Platform Detection](#platform-detection) below.

---

## Linux Installation

### Linux x86_64 (Most Common)

**Works on**: Ubuntu, Debian, CentOS, Fedora, Alpine, and any Linux distro with kernel 3.2+

```bash
# 1. Download
wget https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-unknown-linux-musl.tar.gz

# 2. Verify checksum (recommended)
wget https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-unknown-linux-musl.tar.gz.sha256
sha256sum -c velostream-latest-x86_64-unknown-linux-musl.tar.gz.sha256

# 3. Extract
tar xzf velostream-latest-x86_64-unknown-linux-musl.tar.gz
cd velostream-latest-x86_64-unknown-linux-musl

# 4. Test installation
./bin/velo-sql --version

# 5. (Optional) Add to PATH
source setup-env.sh
velo-sql --version  # Now works without ./bin/
```

**Why musl?** These are **static binaries** with zero runtime dependencies. They work on any Linux distribution without needing specific library versions.

### Linux ARM64 (Raspberry Pi, AWS Graviton)

```bash
# Download ARM64 version
wget https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-aarch64-unknown-linux-musl.tar.gz

# Same steps as x86_64 above
```

---

## macOS Installation

### macOS Apple Silicon (M1/M2/M3/M4)

**Works on**: macOS 11.0+ (Big Sur or later)

```bash
# 1. Download
curl -LO https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-aarch64-apple-darwin.tar.gz

# 2. Verify checksum (recommended)
curl -LO https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-aarch64-apple-darwin.tar.gz.sha256
shasum -a 256 -c velostream-latest-aarch64-apple-darwin.tar.gz.sha256

# 3. Extract
tar xzf velostream-latest-aarch64-apple-darwin.tar.gz
cd velostream-latest-aarch64-apple-darwin

# 4. Test installation
./bin/velo-sql --version

# 5. (Optional) Add to PATH
source setup-env.sh
```

**macOS Security Warning**: First run may show "unidentified developer" warning. See [Troubleshooting](#macos-security-warnings) below.

### macOS Intel (x86_64)

**Works on**: macOS 10.15+ (Catalina or later)

```bash
# Download Intel version
curl -LO https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-apple-darwin.tar.gz

# Same steps as Apple Silicon above
```

---

## Windows Installation

**Works on**: Windows 10, Windows 11 (64-bit)

### PowerShell Method

```powershell
# 1. Download
Invoke-WebRequest -Uri "https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-pc-windows-msvc.zip" -OutFile "velostream.zip"

# 2. Verify checksum (recommended)
Invoke-WebRequest -Uri "https://github.com/velostreamai/velostream/releases/latest/download/velostream-latest-x86_64-pc-windows-msvc.zip.sha256" -OutFile "velostream.zip.sha256"

$expected = (Get-Content velostream.zip.sha256).Split()[0]
$actual = (Get-FileHash velostream.zip -Algorithm SHA256).Hash
if ($expected -eq $actual) {
    Write-Host "✓ Checksum verified"
} else {
    Write-Host "✗ Checksum mismatch - download may be corrupted!"
}

# 3. Extract
Expand-Archive -Path velostream.zip -DestinationPath .
cd velostream-latest-x86_64-pc-windows-msvc

# 4. Test installation
.\bin\velo-sql.exe --version

# 5. (Optional) Add to PATH
.\setup-env.bat
```

### Manual Download

1. Visit [Releases](https://github.com/velostreamai/velostream/releases/latest)
2. Download `velostream-latest-x86_64-pc-windows-msvc.zip`
3. Right-click → Extract All
4. Open extracted folder
5. Run `bin\velo-sql.exe --version`

---

## Platform Detection

### How to Check Your Platform

**Linux**:
```bash
uname -m
# x86_64 → Download x86_64-unknown-linux-musl
# aarch64 → Download aarch64-unknown-linux-musl
```

**macOS**:
```bash
uname -m
# arm64 → Download aarch64-apple-darwin (Apple Silicon)
# x86_64 → Download x86_64-apple-darwin (Intel)
```

**Windows**:
```powershell
systeminfo | findstr /C:"System Type"
# x64-based → Download x86_64-pc-windows-msvc
```

---

## Verification

### Why Verify Downloads?

SHA256 checksums ensure your download:
- ✅ Wasn't corrupted during transfer
- ✅ Hasn't been tampered with
- ✅ Matches the official release

**Recommended for production deployments.**

### Verification Steps

**Linux**:
```bash
sha256sum -c velostream-latest-<platform>.tar.gz.sha256
# Expected: velostream-latest-<platform>.tar.gz: OK
```

**macOS**:
```bash
shasum -a 256 -c velostream-latest-<platform>.tar.gz.sha256
# Expected: velostream-latest-<platform>.tar.gz: OK
```

**Windows** (PowerShell):
```powershell
$expected = (Get-Content velostream.zip.sha256).Split()[0]
$actual = (Get-FileHash velostream.zip -Algorithm SHA256).Hash
if ($expected -eq $actual) { Write-Host "✓ Verified" }
```

---

## What's Included

Each release archive contains:

```
velostream-latest-<platform>/
├── bin/                          # All binaries
│   ├── velo-sql                  # Main streaming SQL server
│   ├── velo-cli                  # Management CLI
│   ├── velo-test                 # Test harness runner
│   ├── velo-sql-batch            # Batch query executor
│   ├── velo-1brc                 # 1BRC demo
│   ├── complete_pipeline_demo    # Complete pipeline demo
│   └── file_processing_demo      # File processing demo
│
├── demo/                         # Working demos
│   ├── 1brc/                     # One Billion Row Challenge
│   ├── trading/                  # Financial trading demo
│   ├── datasource-demo/          # Data source examples
│   ├── test_harness_examples/    # Test harness tiers
│   └── quickstart/               # Quick start examples
│
├── configs/                      # Example configurations
├── target/release/               # Symlinks (for demo compatibility)
├── setup-env.sh (or .bat)        # PATH setup helper
├── README.md                     # Project overview
├── LICENSE                       # Apache 2.0 license
└── INSTALL.md                    # Quick start guide
```

**Total size**: ~80-90 MB (uncompressed)

---

## Next Steps

### Run Your First Demo

```bash
# 1BRC (One Billion Row Challenge)
cd demo/1brc
./run-1brc.sh

# Trading demo (requires Docker)
cd demo/trading
./start-demo.sh --quick
```

### Add to System PATH (Optional)

**Linux/macOS**:
```bash
# Temporary (current session)
source setup-env.sh

# Permanent (add to ~/.bashrc or ~/.zshrc)
echo 'export PATH="/path/to/velostream/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

**Windows**:
```powershell
# Temporary (current session)
.\setup-env.bat

# Permanent (System Settings)
# 1. Search "Environment Variables" in Start Menu
# 2. Edit PATH variable
# 3. Add: C:\path\to\velostream\bin
```

### Explore Documentation

- [Quick Start Guide](../../README.md#quick-start)
- [SQL Reference](../sql/)
- [Demo Guides](../../demo/)
- [Troubleshooting](#troubleshooting)

---

## Troubleshooting

### "Permission denied" Error (Linux/macOS)

**Symptom**:
```bash
./bin/velo-sql --version
# -bash: ./bin/velo-sql: Permission denied
```

**Solution**:
```bash
chmod +x ./bin/velo-sql
# Or make all binaries executable:
chmod +x ./bin/*
```

---

### "Command not found" Error

**Symptom**:
```bash
velo-sql --version
# -bash: velo-sql: command not found
```

**Solution 1**: Use full path
```bash
./bin/velo-sql --version
```

**Solution 2**: Add to PATH
```bash
source setup-env.sh
velo-sql --version  # Now works
```

---

### macOS Security Warnings

**Symptom**:
> "velo-sql cannot be opened because it is from an unidentified developer"

**Solution**:
1. **Option 1**: Right-click binary → Open → Click "Open" again
2. **Option 2**: System Preferences → Security & Privacy → Click "Open Anyway"
3. **Option 3**: Remove quarantine flag:
   ```bash
   xattr -d com.apple.quarantine ./bin/velo-sql
   ```

**Why this happens**: Binaries aren't code-signed yet (planned for future release).

---

### Wrong Architecture Error

**Symptom**:
```bash
./bin/velo-sql --version
# cannot execute binary file: Exec format error
```

**Cause**: Downloaded wrong architecture
- Downloaded ARM64 but running on x86_64
- Downloaded x86_64 but running on ARM64

**Solution**: Download correct version for your platform
```bash
# Check your architecture
uname -m
# Then download matching binary
```

---

### Demo Scripts Don't Work

**Symptom**:
```bash
cd demo/1brc
./run-1brc.sh
# ./target/release/velo-1brc: not found
```

**Cause**: Demo scripts look for binaries in `target/release/` (symlinks to `bin/`) or on your PATH.

**Solution**: Add binaries to your PATH (recommended) or verify symlinks exist:
```bash
# Recommended: add to PATH
source setup-env.sh

# Or verify symlinks
ls -la target/release/
# Should show: velo-* -> ../../bin/*
```

---

### Binary Doesn't Run at All

**Check 1**: Verify download completed
```bash
# File should be ~80-90 MB when extracted
ls -lh velostream-latest-<platform>/
```

**Check 2**: Verify checksum
```bash
sha256sum -c velostream-latest-<platform>.tar.gz.sha256
```

**Check 3**: Re-download if corrupted
```bash
rm velostream-latest-<platform>.tar.gz
# Download again
```

---

### Still Having Issues?

1. **Check GitHub Issues**: [Search existing issues](https://github.com/velostreamai/velostream/issues)
2. **Ask in Discussions**: [GitHub Discussions](https://github.com/velostreamai/velostream/discussions)
3. **File a Bug Report**: [New issue](https://github.com/velostreamai/velostream/issues/new)

Include:
- Operating system and version
- Architecture (`uname -m` output)
- Velostream version (`velo-sql --version`)
- Complete error message
- Steps to reproduce

---

## Build from Source (Alternative)

If you prefer to build from source or binaries don't work for your platform:

**Prerequisites**:
- Rust 1.91+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Protocol Buffers compiler
  - Linux: `apt-get install protobuf-compiler`
  - macOS: `brew install protobuf`
  - Windows: Download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases)

**Build steps**:
```bash
git clone https://github.com/velostreamai/velostream.git
cd velostream
cargo build --release --no-default-features
./target/release/velo-sql --version
```

See [Developer Guide](../developer/) for advanced build options.

---

## System Requirements

### Minimum Requirements

| OS | Requirement |
|----|-------------|
| **Linux** | Kernel 3.2+ (any distro) |
| **macOS** | macOS 10.15+ (Catalina or later) |
| **Windows** | Windows 10 or Windows 11 (64-bit) |
| **Memory** | 512 MB RAM (minimum), 2 GB+ recommended |
| **Disk** | 200 MB for binaries + demos |

### Runtime Dependencies

| Platform | Dependencies |
|----------|--------------|
| **Linux** | ✅ None (static binaries) |
| **macOS** | ✅ System libraries only |
| **Windows** | ✅ MSVC runtime (built-in on Windows 10+) |

**No Kafka installation required** - Velostream connects to existing Kafka clusters. Use Docker for testing:
```bash
docker-compose up -d  # Starts local Kafka
```

---

## Upgrade Guide

### Upgrading to a New Version

```bash
# 1. Download new version
wget https://github.com/velostreamai/velostream/releases/download/v1.1.0/velostream-v1.1.0-<platform>.tar.gz

# 2. Extract to new directory
tar xzf velostream-v1.1.0-<platform>.tar.gz

# 3. Test new version
cd velostream-v1.1.0-<platform>
./bin/velo-sql --version

# 4. Update PATH if needed
# (Or keep old version and switch as needed)
```

**Backward compatibility**: Minor versions (1.0 → 1.1) are backward compatible. Major versions (1.x → 2.x) may have breaking changes - see release notes.

---

## Summary

**Quick reference**:

1. **Download**: [Releases page](https://github.com/velostreamai/velostream/releases/latest)
2. **Extract**: `tar xzf` (Linux/macOS) or `Expand-Archive` (Windows)
3. **Test**: `./bin/velo-sql --version`
4. **Run demo**: `cd demo/1brc && ./run-1brc.sh`
5. **Add to PATH**: `source setup-env.sh` (optional)

**Need help?** See [Troubleshooting](#troubleshooting) or [file an issue](https://github.com/velostreamai/velostream/issues).

---

**Last Updated**: 2024-02-11
**Velostream Version**: v1.0.0+
**Document Version**: 1.0
