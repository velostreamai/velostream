#!/bin/bash
set -e

# Build a complete Velostream release archive for the current platform.
# Builds both velostream binaries and velo-test from the sibling directory.
#
# Usage:
#   ./scripts/build-release.sh              # Build release archive
#   ./scripts/build-release.sh v0.2.0       # Build with specific version tag
#
# Prerequisites:
#   - ../velo-test/ must exist (sibling directory)
#   - Rust toolchain installed

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VELOSTREAM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VELO_TEST_DIR="$(cd "$VELOSTREAM_DIR/../velo-test" 2>/dev/null && pwd)" || true

# Determine version
VERSION="${1:-$(grep '^version = ' "$VELOSTREAM_DIR/Cargo.toml" | head -1 | sed 's/version = "\(.*\)"/\1/')}"
VERSION="v${VERSION#v}"  # Ensure v prefix

# Detect platform
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)   TARGET="x86_64-unknown-linux-gnu" ;;
    Linux-aarch64)  TARGET="aarch64-unknown-linux-gnu" ;;
    Darwin-arm64)   TARGET="aarch64-apple-darwin" ;;
    Darwin-x86_64)  TARGET="x86_64-apple-darwin" ;;
    *)              echo -e "${RED}Unsupported platform: $(uname -s)-$(uname -m)${NC}"; exit 1 ;;
esac

ARCHIVE_NAME="velostream-${VERSION}-${TARGET}"
STAGING_DIR="$VELOSTREAM_DIR/release-staging/${ARCHIVE_NAME}"

echo -e "${BLUE}Building Velostream release${NC}"
echo "  Version:  ${VERSION}"
echo "  Target:   ${TARGET}"
echo "  Archive:  ${ARCHIVE_NAME}.tar.gz"
echo ""

# Check velo-test exists
if [ -z "$VELO_TEST_DIR" ] || [ ! -f "$VELO_TEST_DIR/Cargo.toml" ]; then
    echo -e "${RED}Error: velo-test not found at ../velo-test/${NC}"
    echo "Clone it: git clone https://github.com/velostreamai/velo-test.git ../velo-test"
    exit 1
fi

# Build velostream
echo -e "${BLUE}[1/4] Building velostream (release)...${NC}"
cd "$VELOSTREAM_DIR"
cargo build --release --no-default-features
echo -e "${GREEN}  Done${NC}"

# Build velo-test
echo -e "${BLUE}[2/4] Building velo-test (release)...${NC}"
cd "$VELO_TEST_DIR"
cargo build --release
echo -e "${GREEN}  Done${NC}"

# Stage release
echo -e "${BLUE}[3/4] Staging release archive...${NC}"
cd "$VELOSTREAM_DIR"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR/bin"

# Copy velostream binaries
for bin in velo-sql velo-cli velo-sql-batch velo-1brc complete_pipeline_demo file_processing_demo; do
    if [ -f "target/release/$bin" ]; then
        cp "target/release/$bin" "$STAGING_DIR/bin/"
    fi
done

# Copy optional binaries
for bin in velo-schema-generator velo-config-validator; do
    cp "target/release/$bin" "$STAGING_DIR/bin/" 2>/dev/null || true
done

# Copy velo-test binary
cp "$VELO_TEST_DIR/target/release/velo-test" "$STAGING_DIR/bin/"

# Create root symlinks
cd "$STAGING_DIR"
for binary in bin/*; do
    ln -sf "$binary" "$(basename "$binary")"
done
cd "$VELOSTREAM_DIR"

# Create target/release/ symlinks for demo compatibility
mkdir -p "$STAGING_DIR/target/release"
cd "$STAGING_DIR/target/release"
for binary in ../../bin/*; do
    ln -sf "$binary" "$(basename "$binary")"
done
cd "$VELOSTREAM_DIR"

# Copy demo files, docs, license
cp -r demo "$STAGING_DIR/"
cp README.md "$STAGING_DIR/"
cp LICENSE "$STAGING_DIR/"
cp docs/user-guides/INSTALL.md "$STAGING_DIR/" 2>/dev/null || true
mkdir -p "$STAGING_DIR/configs"

# Create setup script
cat > "$STAGING_DIR/setup-env.sh" << 'SETUP_EOF'
#!/bin/bash
export PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/bin" && pwd):$PATH"
echo "Velostream binaries added to PATH"
echo ""
echo "Available commands:"
ls -1 "$(dirname "${BASH_SOURCE[0]}")/bin/" | sed 's/^/  - /'
SETUP_EOF
chmod +x "$STAGING_DIR/setup-env.sh"

echo -e "${GREEN}  Staged $(ls "$STAGING_DIR/bin/" | wc -l | tr -d ' ') binaries${NC}"

# Create archive
echo -e "${BLUE}[4/4] Creating archive...${NC}"
cd "$VELOSTREAM_DIR/release-staging"
tar czf "$VELOSTREAM_DIR/${ARCHIVE_NAME}.tar.gz" "$ARCHIVE_NAME"

# Checksum
cd "$VELOSTREAM_DIR"
if [[ "$OSTYPE" == "darwin"* ]]; then
    shasum -a 256 "${ARCHIVE_NAME}.tar.gz" > "${ARCHIVE_NAME}.tar.gz.sha256"
else
    sha256sum "${ARCHIVE_NAME}.tar.gz" > "${ARCHIVE_NAME}.tar.gz.sha256"
fi

# Clean up staging
rm -rf release-staging

# Summary
ARCHIVE_SIZE=$(ls -lh "${ARCHIVE_NAME}.tar.gz" | awk '{print $5}')
echo ""
echo -e "${GREEN}Release archive created:${NC}"
echo "  ${ARCHIVE_NAME}.tar.gz  (${ARCHIVE_SIZE})"
echo "  ${ARCHIVE_NAME}.tar.gz.sha256"
echo ""
echo -e "${BLUE}Binaries included:${NC}"
tar tzf "${ARCHIVE_NAME}.tar.gz" | grep '/bin/' | sed 's/.*\/bin\//  - /'
echo ""
echo -e "${YELLOW}To test:${NC}"
echo "  tar xzf ${ARCHIVE_NAME}.tar.gz"
echo "  cd ${ARCHIVE_NAME}"
echo "  source setup-env.sh"
echo "  velo-sql --version"
echo "  velo-test --version"
