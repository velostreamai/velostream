#!/bin/bash
# =============================================================================
# Test Harness Examples - velo-test Runner
# =============================================================================
# This script runs velo-test against the example SQL applications in this demo.
#
# Usage (from test_harness_examples directory):
#   ./velo-test.sh                 # Run all tiers (auto-starts Kafka via Docker)
#   ./velo-test.sh validate        # Validate all SQL syntax only (no Docker needed)
#   ./velo-test.sh tier1           # Run tier1_basic tests only
#   ./velo-test.sh getting_started # Run getting_started example
#   ./velo-test.sh menu            # Interactive menu to select SQL files only
#   ./velo-test.sh cases           # Interactive: select SQL file, then test case
#
# Usage (from a subdirectory like getting_started):
#   ../velo-test.sh .              # Run current directory as a test tier
#   ../velo-test.sh validate .     # Validate SQL in current directory
#   ../velo-test.sh menu           # Interactive menu for SQL files only
#   ../velo-test.sh cases          # Interactive: select SQL file → test case
#
# Options:
#   --kafka <servers>     Use external Kafka instead of testcontainers
#   --timeout <ms>        Timeout per query in milliseconds (default: 60000)
#   --output <format>     Output format: text, json, junit
#   -q, --query <name>    Run only a specific test case by name
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
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory (where velo-test.sh lives)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Save the original working directory (where the user ran the command from)
ORIGINAL_DIR="$(pwd)"

# Change to script directory for tier lookups
cd "$SCRIPT_DIR"

# Check if velo-test is available
VELO_TEST="${VELO_TEST:-../../target/release/velo-test}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${RED}Error: velo-test not found${NC}"
    echo "Build it with: cargo build --release --features test-support"
    echo "Or set VELO_TEST environment variable to the binary path"
    exit 1
fi

# Convert to absolute path (needed when we cd to different directories)
if [[ -f "$VELO_TEST" ]]; then
    VELO_TEST="$(cd "$(dirname "$VELO_TEST")" && pwd)/$(basename "$VELO_TEST")"
fi

# Default values
OUTPUT_FORMAT="text"
TIMEOUT_MS=60000
KAFKA_SERVERS=""
TARGET_DIR=""
QUERY_FILTER=""

# Parse command line arguments
MODE="${1:-run}"
shift || true

# Check if the next argument is a directory path (not an option)
if [[ $# -gt 0 && "${1:0:1}" != "-" ]]; then
    TARGET_DIR="$1"
    shift
fi

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
        --kafka)
            KAFKA_SERVERS="$2"
            shift 2
            ;;
        --query|-q)
            QUERY_FILTER="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Build query filter option
QUERY_OPTS=""
if [[ -n "$QUERY_FILTER" ]]; then
    QUERY_OPTS="--query $QUERY_FILTER"
    echo -e "${BLUE}Running only query: $QUERY_FILTER${NC}"
fi

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
        KAFKA_OPTS="--use-testcontainers"
    fi
fi

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  TEST HARNESS EXAMPLES - RUNNER${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to get tier directory from tier name (bash 3.x compatible)
get_tier_dir() {
    local tier_name="$1"
    case "$tier_name" in
        getting_started) echo "getting_started" ;;
        tier1) echo "tier1_basic" ;;
        tier2) echo "tier2_aggregations" ;;
        tier3) echo "tier3_joins" ;;
        tier4) echo "tier4_window_functions" ;;
        tier5) echo "tier5_complex" ;;
        tier6) echo "tier6_edge_cases" ;;
        *) echo "" ;;
    esac
}

# Function to run a single SQL file directly
run_sql_file() {
    local sql_file="$1"
    local sql_dir="$(dirname "$sql_file")"
    local tier_dir="$(dirname "$sql_dir")"

    # Look for test_spec.yaml in parent directory
    local spec_file="$tier_dir/test_spec.yaml"
    local schemas_dir="$tier_dir/schemas"

    # Fall back to shared schemas
    if [[ ! -d "$schemas_dir" ]]; then
        schemas_dir="$SCRIPT_DIR/schemas"
    fi

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Running: $(basename "$sql_file")${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo "  SQL File:  $sql_file"

    if [[ -f "$spec_file" ]]; then
        echo "  Spec:      $spec_file"
        echo "  Schemas:   $schemas_dir"
        echo ""
        "$VELO_TEST" run "$sql_file" \
            --spec "$spec_file" \
            --schemas "$schemas_dir" \
            --timeout-ms "$TIMEOUT_MS" \
            --output "$OUTPUT_FORMAT" \
            ${QUERY_OPTS} \
            ${KAFKA_OPTS}
    else
        echo "  (No spec file - running without assertions)"
        echo ""
        "$VELO_TEST" run "$sql_file" \
            --timeout-ms "$TIMEOUT_MS" \
            --output "$OUTPUT_FORMAT" \
            ${QUERY_OPTS} \
            ${KAFKA_OPTS}
    fi
}

# Function to show interactive menu and select SQL file
show_menu() {
    local search_dir="$1"

    # Find all SQL files
    local sql_files=()
    while IFS= read -r -d '' file; do
        sql_files+=("$file")
    done < <(find "$search_dir" -name "*.sql" -type f -print0 | sort -z)

    if [[ ${#sql_files[@]} -eq 0 ]]; then
        echo -e "${RED}No SQL files found in $search_dir${NC}"
        return 1
    fi

    echo -e "${YELLOW}Select a SQL file to run:${NC}"
    echo ""

    local i=1
    for sql_file in "${sql_files[@]}"; do
        # Get relative path for cleaner display
        local rel_path="${sql_file#$search_dir/}"
        echo -e "  ${CYAN}$i)${NC} $rel_path"
        ((i++))
    done

    echo ""
    echo -e "  ${CYAN}0)${NC} Exit"
    echo ""

    while true; do
        echo -n -e "${GREEN}Enter selection [1-${#sql_files[@]}]: ${NC}"
        read -r selection

        if [[ "$selection" == "0" || "$selection" == "q" || "$selection" == "Q" ]]; then
            echo "Exiting."
            return 0
        fi

        if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#sql_files[@]} ]]; then
            local selected_file="${sql_files[$((selection-1))]}"
            echo ""
            run_sql_file "$selected_file"
            return $?
        else
            echo -e "${RED}Invalid selection. Please enter a number between 1 and ${#sql_files[@]}.${NC}"
        fi
    done
}

# Function to show interactive SQL file selection, then test case selection
show_cases_menu() {
    local search_dir="$1"
    local selected_sql=""
    local sql_dir=""
    local spec_file=""

    # Step 1: Find all SQL files
    local sql_files=()
    while IFS= read -r -d '' file; do
        sql_files+=("$file")
    done < <(find "$search_dir" -maxdepth 3 -name "*.sql" -type f -print0 | sort -z)

    if [[ ${#sql_files[@]} -eq 0 ]]; then
        echo -e "${RED}No SQL files found in $search_dir${NC}"
        return 1
    fi

    # Step 2: Select SQL file (if multiple)
    if [[ ${#sql_files[@]} -eq 1 ]]; then
        selected_sql="${sql_files[0]}"
        echo -e "${BLUE}SQL file: $(basename "$selected_sql")${NC}"
    else
        echo -e "${YELLOW}Step 1: Select SQL file to run:${NC}"
        echo ""

        local i=1
        for sql_file in "${sql_files[@]}"; do
            local rel_path="${sql_file#$search_dir/}"
            echo -e "  ${CYAN}$i)${NC} $rel_path"
            ((i++))
        done

        echo ""
        echo -e "  ${CYAN}0)${NC} Exit"
        echo ""

        while true; do
            echo -n -e "${GREEN}Enter SQL file [1-${#sql_files[@]}]: ${NC}"
            read -r selection

            if [[ "$selection" == "0" || "$selection" == "q" || "$selection" == "Q" ]]; then
                echo "Exiting."
                return 0
            fi

            if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#sql_files[@]} ]]; then
                selected_sql="${sql_files[$((selection-1))]}"
                echo ""
                echo -e "${BLUE}Selected: $(basename "$selected_sql")${NC}"
                break
            else
                echo -e "${RED}Invalid selection.${NC}"
            fi
        done
    fi

    # Step 3: Find test_spec.yaml in the SQL file's directory (or parent)
    sql_dir="$(dirname "$selected_sql")"
    spec_file="$sql_dir/test_spec.yaml"

    # Check parent directory if not found (for sql/ subdirectory structure)
    if [[ ! -f "$spec_file" ]]; then
        spec_file="$(dirname "$sql_dir")/test_spec.yaml"
    fi

    # Also check current search directory as fallback
    if [[ ! -f "$spec_file" ]]; then
        spec_file="$search_dir/test_spec.yaml"
    fi

    if [[ ! -f "$spec_file" ]]; then
        echo -e "${YELLOW}No test_spec.yaml found - running SQL directly${NC}"
        echo ""
        run_sql_file "$selected_sql"
        return $?
    fi

    echo -e "${BLUE}Using spec: $spec_file${NC}"

    # Step 4: Extract test case names from test_spec.yaml
    local test_cases=()
    while IFS= read -r name; do
        test_cases+=("$name")
    done < <(grep -E "^[[:space:]]*-[[:space:]]*name:" "$spec_file" | sed 's/.*name:[[:space:]]*//' | tr -d '\r')

    if [[ ${#test_cases[@]} -eq 0 ]]; then
        echo -e "${YELLOW}No test cases found in spec - running SQL directly${NC}"
        run_sql_file "$selected_sql"
        return $?
    fi

    # Step 5: Select test case
    echo ""
    echo -e "${YELLOW}Step 2: Select test case to run:${NC}"
    echo ""

    local i=1
    for case_name in "${test_cases[@]}"; do
        echo -e "  ${CYAN}$i)${NC} $case_name"
        ((i++))
    done

    echo ""
    echo -e "  ${CYAN}a)${NC} Run ALL test cases"
    echo -e "  ${CYAN}0)${NC} Exit"
    echo ""

    while true; do
        echo -n -e "${GREEN}Enter test case [1-${#test_cases[@]}, a, 0]: ${NC}"
        read -r selection

        if [[ "$selection" == "0" || "$selection" == "q" || "$selection" == "Q" ]]; then
            echo "Exiting."
            return 0
        fi

        if [[ "$selection" == "a" || "$selection" == "A" ]]; then
            echo ""
            echo -e "${YELLOW}Running all test cases...${NC}"
            QUERY_FILTER=""
            QUERY_OPTS=""
            run_sql_with_spec "$selected_sql" "$spec_file" "$(dirname "$spec_file")"
            return $?
        fi

        if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#test_cases[@]} ]]; then
            local selected_case="${test_cases[$((selection-1))]}"
            echo ""
            echo -e "${YELLOW}Running test case: $selected_case${NC}"
            QUERY_FILTER="$selected_case"
            QUERY_OPTS="--query $QUERY_FILTER"
            run_sql_with_spec "$selected_sql" "$spec_file" "$(dirname "$spec_file")"
            return $?
        else
            echo -e "${RED}Invalid selection.${NC}"
        fi
    done
}

# Helper function to run SQL with spec file
run_sql_with_spec() {
    local sql_file="$1"
    local spec_file="$2"
    local tier_dir="$3"
    local schemas_dir="$tier_dir/schemas"

    # Fall back to shared schemas
    if [[ ! -d "$schemas_dir" ]]; then
        schemas_dir="$SCRIPT_DIR/schemas"
    fi

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Running: $(basename "$sql_file")${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo "  SQL File:  $sql_file"
    echo "  Spec:      $spec_file"
    echo "  Schemas:   $schemas_dir"
    echo ""

    "$VELO_TEST" run "$sql_file" \
        --spec "$spec_file" \
        --schemas "$schemas_dir" \
        --timeout-ms "$TIMEOUT_MS" \
        --output "$OUTPUT_FORMAT" \
        ${QUERY_OPTS} \
        ${KAFKA_OPTS}
}

# Function to run tests for a single tier
run_tier() {
    local tier_name="$1"
    local tier_dir="$2"

    if [[ ! -d "$tier_dir" ]]; then
        echo -e "${YELLOW}Skipping $tier_name: directory not found${NC}"
        return 0
    fi

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Running: $tier_name${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Find SQL files in the tier
    local sql_files=$(find "$tier_dir" -name "*.sql" -type f 2>/dev/null | head -1)
    local spec_file="$tier_dir/test_spec.yaml"
    local schemas_dir="$tier_dir/schemas"

    # Fall back to shared schemas if tier doesn't have its own
    if [[ ! -d "$schemas_dir" ]]; then
        schemas_dir="schemas"
    fi

    if [[ -z "$sql_files" ]]; then
        echo -e "${YELLOW}  No SQL files found in $tier_dir${NC}"
        return 0
    fi

    if [[ ! -f "$spec_file" ]]; then
        echo -e "${YELLOW}  No test_spec.yaml found in $tier_dir${NC}"
        # Try to just validate the SQL
        for sql_file in $(find "$tier_dir" -name "*.sql" -type f); do
            echo -e "  Validating: $(basename $sql_file)"
            "$VELO_TEST" validate "$sql_file" || return 1
        done
        return 0
    fi

    # Try to find SQL file matching the test_spec application name
    local app_name=$(grep -E "^application:" "$spec_file" 2>/dev/null | sed 's/application:[[:space:]]*//' | tr -d '\r')
    local sql_file=""

    if [[ -n "$app_name" ]]; then
        # Look for SQL file matching application name
        sql_file=$(find "$tier_dir" -name "${app_name}.sql" -type f 2>/dev/null | head -1)
    fi

    # Fall back to first SQL file if no match
    if [[ -z "$sql_file" ]]; then
        sql_file=$(find "$tier_dir" -name "*.sql" -type f | head -1)
    fi
    echo "  SQL File:  $sql_file"
    echo "  Spec:      $spec_file"
    echo "  Schemas:   $schemas_dir"
    echo ""

    if [[ "$MODE" == "validate" ]]; then
        "$VELO_TEST" validate "$sql_file"
    else
        "$VELO_TEST" run "$sql_file" \
            --spec "$spec_file" \
            --schemas "$schemas_dir" \
            --timeout-ms "$TIMEOUT_MS" \
            --output "$OUTPUT_FORMAT" \
            ${QUERY_OPTS} \
            ${KAFKA_OPTS}
    fi
}

# Main execution
case "$MODE" in
    validate)
        if [[ -n "$TARGET_DIR" ]]; then
            # Validate specific directory (e.g., ../velo-test.sh validate .)
            if [[ "$TARGET_DIR" == "." ]]; then
                tier_dir="$ORIGINAL_DIR"
                tier_name="$(basename "$ORIGINAL_DIR")"
            else
                tier_dir="$TARGET_DIR"
                tier_name="$(basename "$TARGET_DIR")"
            fi
            echo -e "${YELLOW}Validating SQL in: $tier_name${NC}"
            echo ""
            cd "$ORIGINAL_DIR"
            run_tier "$tier_name" "$tier_dir"
        else
            # Validate all tiers
            echo -e "${YELLOW}Validating all SQL files...${NC}"
            echo ""
            for tier_name in getting_started tier1 tier2 tier3 tier4 tier5 tier6; do
                tier_dir=$(get_tier_dir "$tier_name")
                run_tier "$tier_name" "$tier_dir"
            done
        fi
        echo ""
        echo -e "${GREEN}✅ All SQL validation complete${NC}"
        ;;

    .)
        # Run current directory (when invoked as ../velo-test.sh . from a subdirectory)
        tier_dir="$ORIGINAL_DIR"
        tier_name="$(basename "$ORIGINAL_DIR")"
        echo -e "${YELLOW}Running tests in current directory: $tier_name${NC}"
        echo ""
        cd "$ORIGINAL_DIR"
        run_tier "$tier_name" "."
        ;;

    getting_started|tier1|tier2|tier3|tier4|tier5|tier6)
        # Run specific tier
        tier_dir=$(get_tier_dir "$MODE")
        if [[ -z "$tier_dir" ]]; then
            echo -e "${RED}Unknown tier: $MODE${NC}"
            exit 1
        fi
        run_tier "$MODE" "$tier_dir"
        ;;

    menu|select)
        # Interactive menu mode (SQL files)
        if [[ -n "$TARGET_DIR" ]]; then
            if [[ "$TARGET_DIR" == "." ]]; then
                search_dir="$ORIGINAL_DIR"
            else
                search_dir="$TARGET_DIR"
            fi
        else
            search_dir="$SCRIPT_DIR"
        fi
        cd "$ORIGINAL_DIR"
        show_menu "$search_dir"
        ;;

    cases|tests)
        # Interactive test case menu (from test_spec.yaml)
        if [[ -n "$TARGET_DIR" ]]; then
            if [[ "$TARGET_DIR" == "." ]]; then
                search_dir="$ORIGINAL_DIR"
            else
                search_dir="$TARGET_DIR"
            fi
        else
            search_dir="$SCRIPT_DIR"
        fi
        cd "$ORIGINAL_DIR"
        show_cases_menu "$search_dir"
        ;;

    run|*)
        echo -e "${YELLOW}Running all test tiers...${NC}"
        echo ""

        FAILED=0
        for tier_name in getting_started tier1 tier2 tier3 tier4 tier5 tier6; do
            tier_dir=$(get_tier_dir "$tier_name")
            if ! run_tier "$tier_name" "$tier_dir"; then
                FAILED=1
                echo -e "${RED}  ❌ $tier_name failed${NC}"
            else
                echo -e "${GREEN}  ✅ $tier_name passed${NC}"
            fi
            echo ""
        done

        if [[ $FAILED -eq 0 ]]; then
            echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
            echo -e "${GREEN}  ALL TESTS PASSED${NC}"
            echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
        else
            echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
            echo -e "${RED}  SOME TESTS FAILED${NC}"
            echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
            exit 1
        fi
        ;;
esac
