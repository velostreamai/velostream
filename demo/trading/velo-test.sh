#!/bin/bash
# =============================================================================
# Financial Trading Demo - velo-test Runner
# =============================================================================
# This script runs the velo-test harness against the financial trading SQL app.
#
# Usage:
#   ./velo-test.sh              # Run all tests (auto-starts Kafka via Docker)
#   ./velo-test.sh validate     # Validate SQL syntax only (no Docker needed)
#   ./velo-test.sh smoke        # Quick smoke test
#   ./velo-test.sh stress       # High-volume stress test
#
# Options:
#   --query <name>        Run only a specific query
#   --output <format>     Output format: text, json, junit
#   --timeout <ms>        Timeout per query in milliseconds
#   --kafka <servers>     Use external Kafka instead of testcontainers
#
# Requirements:
#   - velo-test binary (build with: cargo build --release --features test-support)
#   - Docker (for Kafka testcontainers, unless --kafka is specified)
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
    echo "Build it with: cargo build --release --features test-support"
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

KAFKA_SERVERS=""

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
        --kafka)
            KAFKA_SERVERS="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Build Kafka options
KAFKA_OPTS=""
if [[ -n "$KAFKA_SERVERS" ]]; then
    KAFKA_OPTS="--kafka $KAFKA_SERVERS"
    echo -e "${BLUE}Using external Kafka: $KAFKA_SERVERS${NC}"
else
    # Check if Docker is available (needed for testcontainers)
    if [[ "$MODE" != "validate" ]]; then
        if ! command -v docker &> /dev/null; then
            echo -e "${RED}Error: Docker not found${NC}"
            echo "Testcontainers requires Docker to auto-start Kafka."
            echo "Either install Docker or use --kafka <servers> to specify external Kafka."
            exit 1
        fi
        if ! docker info &> /dev/null 2>&1; then
            echo -e "${RED}Error: Docker is not running${NC}"
            echo "Please start Docker Desktop or the Docker daemon."
            echo "Alternatively, use --kafka <servers> to specify external Kafka."
            exit 1
        fi
        echo -e "${BLUE}Docker detected - will use testcontainers for Kafka${NC}"
        # Force testcontainers since configs may have localhost:9092 which won't work
        KAFKA_OPTS="--use-testcontainers"
    fi
fi

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
            ${KAFKA_OPTS} \
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
            ${KAFKA_OPTS} \
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
            ${KAFKA_OPTS} \
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
