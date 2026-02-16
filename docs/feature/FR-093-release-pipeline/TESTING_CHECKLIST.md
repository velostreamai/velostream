# FR-093 Release Pipeline Testing Checklist

## Pre-Release Testing (v0.1.0-beta.1)

### Workflow Trigger
- [ ] Create test tag: `git tag v0.1.0-beta.1 -m "Test release for multi-platform builds"`
- [ ] Push tag: `git push origin v0.1.0-beta.1`
- [ ] Verify workflow triggered: https://github.com/velostreamai/velostream/actions

### Build Monitoring (GitHub Actions)

#### Build Job - Linux x86_64 (musl)
- [ ] Checkout successful
- [ ] cross-rs installation successful
- [ ] Build with cross successful
- [ ] All 9 binaries built (or 7 if optional ones skip)
- [ ] Archive created: `velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz`
- [ ] SHA256 checksum generated
- [ ] Artifacts uploaded

#### Build Job - Linux ARM64 (musl)
- [ ] Checkout successful
- [ ] cross-rs installation successful
- [ ] Build with cross successful
- [ ] All binaries built
- [ ] Archive created: `velostream-v0.1.0-beta.1-aarch64-unknown-linux-musl.tar.gz`
- [ ] SHA256 checksum generated
- [ ] Artifacts uploaded

#### Build Job - macOS Intel (x86_64)
- [ ] Checkout successful
- [ ] Protobuf installation successful (brew)
- [ ] Rust toolchain installed
- [ ] Native build successful
- [ ] All binaries built
- [ ] Archive created: `velostream-v0.1.0-beta.1-x86_64-apple-darwin.tar.gz`
- [ ] SHA256 checksum generated
- [ ] Artifacts uploaded

#### Build Job - macOS Apple Silicon (ARM64)
- [ ] Checkout successful
- [ ] Protobuf installation successful (brew)
- [ ] Rust toolchain installed
- [ ] Native build successful
- [ ] All binaries built
- [ ] Archive created: `velostream-v0.1.0-beta.1-aarch64-apple-darwin.tar.gz`
- [ ] SHA256 checksum generated
- [ ] Artifacts uploaded

#### Build Job - Windows x86_64 (MSVC)
- [ ] Checkout successful
- [ ] Protobuf installation successful (download)
- [ ] Rust toolchain installed
- [ ] Native build successful
- [ ] All binaries built (.exe files)
- [ ] Archive created: `velostream-v0.1.0-beta.1-x86_64-pc-windows-msvc.zip`
- [ ] SHA256 checksum generated
- [ ] Artifacts uploaded

#### Release Job
- [ ] All build jobs completed successfully
- [ ] Artifacts downloaded
- [ ] Release notes generated
- [ ] GitHub Release created
- [ ] All 5 archives uploaded
- [ ] All 5 checksums uploaded
- [ ] Release marked as pre-release (beta)
- [ ] Release notes contain installation instructions

### Platform Verification

#### Linux x86_64 Testing
```bash
# Download
wget https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz
wget https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz.sha256

# Verify checksum
sha256sum -c velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz.sha256

# Extract
tar xzf velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz
cd velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl

# Test binaries
./velo-sql --version
./velo-cli --help
./velo-test --help
./velo-sql-batch --help
./velo-1brc --help

# Check binary sizes
ls -lh velo-* | awk '{print $5, $9}'

# Run demos
cd demo/datasource-demo
../../file_processing_demo
../../complete_pipeline_demo

# Test harness
cd ../test_harness_examples/tier1_basic
../../../velo-test run 01_passthrough.test.yaml --use-testcontainers
```

**Checklist**:
- [ ] Checksum verified successfully
- [ ] Archive extracted without errors
- [ ] All binaries executable (`chmod +x` not needed)
- [ ] `velo-sql --version` shows correct version
- [ ] `velo-cli --help` displays help
- [ ] `velo-test --help` displays help
- [ ] All binaries < 30 MB
- [ ] Demos run successfully
- [ ] Test harness example passes
- [ ] No missing library errors (static linking works)

**Test on multiple distros**:
- [ ] Ubuntu 22.04 LTS
- [ ] Ubuntu 20.04 LTS
- [ ] Debian 12
- [ ] CentOS Stream 9
- [ ] Alpine Linux (musl native)

#### Linux ARM64 Testing
**Platform**: Raspberry Pi 4/5, AWS Graviton, or ARM64 VM

```bash
# Same commands as x86_64, but with aarch64 archive
wget https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-aarch64-unknown-linux-musl.tar.gz
# ... (same test steps)
```

**Checklist**:
- [ ] Checksum verified
- [ ] Binaries run on ARM64 hardware
- [ ] Performance is acceptable
- [ ] Demos complete successfully

#### macOS Intel (x86_64) Testing
**Platform**: Intel-based Mac (macOS 10.15+)

```bash
# Download
curl -LO https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-apple-darwin.tar.gz
curl -LO https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-apple-darwin.tar.gz.sha256

# Verify
shasum -a 256 -c velostream-v0.1.0-beta.1-x86_64-apple-darwin.tar.gz.sha256

# Extract and test
tar xzf velostream-v0.1.0-beta.1-x86_64-apple-darwin.tar.gz
cd velostream-v0.1.0-beta.1-x86_64-apple-darwin
./velo-sql --version
```

**Checklist**:
- [ ] Checksum verified (shasum -a 256)
- [ ] Archive extracted
- [ ] Security warning handled (unsigned binary)
- [ ] Allow binary to run (System Preferences > Security)
- [ ] `velo-sql --version` works
- [ ] Demos run successfully
- [ ] No missing library errors

#### macOS Apple Silicon (ARM64) Testing
**Platform**: M1/M2/M3 Mac (macOS 11.0+)

```bash
# Download ARM64 version
curl -LO https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-aarch64-apple-darwin.tar.gz
# ... (same test steps)
```

**Checklist**:
- [ ] Checksum verified
- [ ] Native ARM64 binary (not Rosetta translation)
- [ ] Security warning handled
- [ ] Binaries run correctly
- [ ] Performance is optimal (native ARM)

**Verify native ARM**:
```bash
file velo-sql  # Should show: Mach-O 64-bit executable arm64
```

#### Windows x86_64 Testing
**Platform**: Windows 10/11 (64-bit)

```powershell
# Download
Invoke-WebRequest -Uri "https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-pc-windows-msvc.zip" -OutFile "velostream.zip"
Invoke-WebRequest -Uri "https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-pc-windows-msvc.zip.sha256" -OutFile "velostream.zip.sha256"

# Verify checksum
$expected = (Get-Content velostream.zip.sha256).Split()[0]
$actual = (Get-FileHash velostream.zip -Algorithm SHA256).Hash
if ($expected -eq $actual) { Write-Host "✓ Checksum verified" } else { Write-Host "✗ Checksum mismatch" }

# Extract
Expand-Archive -Path velostream.zip -DestinationPath .
cd velostream-v0.1.0-beta.1-x86_64-pc-windows-msvc

# Test
.\velo-sql.exe --version
.\velo-cli.exe --help
```

**Checklist**:
- [ ] Checksum verified (PowerShell)
- [ ] Archive extracted
- [ ] Windows Defender SmartScreen handled
- [ ] `velo-sql.exe --version` works
- [ ] All `.exe` files run
- [ ] Demos execute successfully
- [ ] No missing DLL errors

### Binary Size Verification

**Expected sizes**: 8-12 MB per binary

```bash
# Unix
ls -lh velo-* | awk '{print $5, $9}'

# Windows
Get-ChildItem *.exe | Format-Table Name, @{Label="Size (MB)"; Expression={[math]::Round($_.Length / 1MB, 2)}}
```

**Checklist**:
- [ ] All binaries < 30 MB
- [ ] velo-sql ≈ 8-12 MB
- [ ] Other binaries in similar range
- [ ] Stripped binaries (no debug symbols)

### Archive Verification

**Checklist for each archive**:
- [ ] Contains all expected binaries
- [ ] Contains `demo/` directory
- [ ] Contains `configs/` directory
- [ ] Contains `README.md`
- [ ] Contains `LICENSE`
- [ ] Archive is compressed (not uncompressed)
- [ ] Archive name follows convention: `velostream-<version>-<target>.<ext>`
- [ ] Checksum file format: `<hash>  <filename>`

### Release Notes Verification

**Checklist**:
- [ ] Release title is correct (e.g., "v0.1.0-beta.1")
- [ ] Installation instructions for all 5 platforms
- [ ] SHA256 verification instructions
- [ ] List of included binaries
- [ ] Quick start guide
- [ ] System requirements listed
- [ ] Documentation links present
- [ ] Marked as pre-release (beta/alpha/rc)

### Issue Tracking

**If any checks fail, document**:
1. **Platform**: Which platform failed?
2. **Step**: Which step in the checklist?
3. **Error**: Exact error message
4. **Logs**: Link to GitHub Actions logs
5. **Fix**: Proposed solution

**Example issue**:
```markdown
## Build Failure: macOS ARM64

**Step**: Protobuf installation
**Error**: `brew: command not found`
**Logs**: https://github.com/.../actions/runs/12345
**Fix**: Verify Homebrew is available on macos-14 runner
```

## Production Release Testing (v1.0.0)

After successful beta testing, repeat all checks for production release:

- [ ] All beta issues resolved
- [ ] Tag created: `v1.0.0`
- [ ] All 5 platforms build successfully
- [ ] Spot-check 2-3 platforms (full testing not required)
- [ ] Release NOT marked as pre-release
- [ ] Release notes updated for production
- [ ] Documentation links verified

## Performance Benchmarks (Optional)

**Test load times**:
```bash
time ./velo-sql --version
```

**Expected**: < 100ms

**Test demo performance**:
```bash
time ./file_processing_demo
```

**Checklist**:
- [ ] Startup time acceptable
- [ ] Demo execution time reasonable
- [ ] No performance regression vs. debug build

## Security Verification

**Checklist**:
- [ ] SHA256 checksums match
- [ ] No unexpected binaries in archive
- [ ] No suspicious files in archive
- [ ] Binaries are stripped (no debug symbols)
- [ ] Archives are read-only on download
- [ ] GitHub Release created by authorized account

## Documentation Verification

Before announcing release:

- [ ] `docs/developer/releases.md` created
- [ ] `README.md` updated with installation instructions
- [ ] User guides mention binary downloads
- [ ] Links to GitHub Releases page
- [ ] Migration guide (if breaking changes)

## Final Sign-Off

Before announcing v1.0.0:

- [ ] All platforms tested successfully
- [ ] Binary sizes acceptable
- [ ] Checksums verified
- [ ] Demos work on all platforms
- [ ] Documentation complete
- [ ] No critical issues
- [ ] Team approval obtained

**Approved by**: _______________
**Date**: _______________

---

**Testing started**: _______________
**Testing completed**: _______________
**Issues found**: _______________
**Release approved**: _______________
