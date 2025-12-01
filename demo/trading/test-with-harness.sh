#!/bin/bash
# =============================================================================
# Financial Trading Demo - Test Harness Runner
# =============================================================================
# This script runs the velo-test harness against the financial trading SQL app.
#
# Usage:
#   ./test-with-harness.sh              # Run all tests
#   ./test-with-harness.sh validate     # Validate SQL syntax only
#   ./test-with-harness.sh smoke        # Quick smoke test
#   ./test-with-harness.sh stress       # High-volume stress test
#
# Requirements:
#   - velo-test binary (build with: cargo build --release)
#   - Docker (for Kafka testcontainers)
#
# See: docs/feature/FR-084-app-test-harness/README.md
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if velo-test is available
VELO_TEST="${VELO_TEST:-../../target/release/velo-test}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null; then
    echo -e "${RED}Error: velo-test not found${NC}"
    echo "Build it with: cargo build --release"
    echo "Or set VELO_TEST environment variable to the binary path"
    exit 1
fi

# Default values
SQL_FILE="sql/financial_trading.sql"
SPEC_FILE="test_spec.yaml"
SCHEMAS_DIR="schemas/"
OUTPUT_FORMAT="text"
TIMEOUT_MS=60000

# Parse command line arguments
MODE="${1:-run}"
shift || true

while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_MS="$2"
            shift 2
            ;;
        --query)
            QUERY="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  FINANCIAL TRADING DEMO - TEST HARNESS${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

case "$MODE" in
    validate)
        echo -e "${YELLOW}Validating SQL syntax...${NC}"
        echo ""
        "$VELO_TEST" validate "$SQL_FILE"
        echo ""
        echo -e "${GREEN}✅ SQL validation complete${NC}"
        ;;

    smoke)
        echo -e "${YELLOW}Running smoke test (minimal data)...${NC}"
        echo ""
        "$VELO_TEST" run "$SQL_FILE" \
            --spec "$SPEC_FILE" \
            --schemas "$SCHEMAS_DIR" \
            --timeout-ms 30000 \
            --output "$OUTPUT_FORMAT" \
            ${QUERY:+--query "$QUERY"}
        ;;

    stress)
        echo -e "${YELLOW}Running stress test (high volume)...${NC}"
        echo ""
        "$VELO_TEST" run "$SQL_FILE" \
            --spec "$SPEC_FILE" \
            --schemas "$SCHEMAS_DIR" \
            --timeout-ms 120000 \
            --output "$OUTPUT_FORMAT" \
            ${QUERY:+--query "$QUERY"}
        ;;

    run|*)
        echo -e "${YELLOW}Running full test suite...${NC}"
        echo ""
        echo "SQL File:     $SQL_FILE"
        echo "Test Spec:    $SPEC_FILE"
        echo "Schemas:      $SCHEMAS_DIR"
        echo "Timeout:      ${TIMEOUT_MS}ms"
        echo ""

        "$VELO_TEST" run "$SQL_FILE" \
            --spec "$SPEC_FILE" \
            --schemas "$SCHEMAS_DIR" \
            --timeout-ms "$TIMEOUT_MS" \
            --output "$OUTPUT_FORMAT" \
            ${QUERY:+--query "$QUERY"}
        ;;
esac

EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  TEST HARNESS COMPLETE - ALL TESTS PASSED${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
else
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}  TEST HARNESS COMPLETE - SOME TESTS FAILED${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
fi

exit $EXIT_CODE
