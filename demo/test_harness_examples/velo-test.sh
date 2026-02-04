#!/bin/bash
# =============================================================================
# Test Harness Examples - velo-test Runner
# =============================================================================
# This script runs velo-test against the example SQL applications in this demo.
#
# Usage (from test_harness_examples directory):
#   ./velo-test.sh                 # Interactive: select SQL file → test case (default)
#   ./velo-test.sh run             # Run all tiers (auto-starts Kafka via Docker)
#   ./velo-test.sh validate        # Validate all SQL syntax only (no Docker needed)
#   ./velo-test.sh tier1           # Run tier1_basic tests only
#   ./velo-test.sh getting_started # Run getting_started example
#   ./velo-test.sh menu            # Interactive menu to select SQL files only
#   ./velo-test.sh cases           # Interactive: select SQL file, then test case
#
# Usage (from a subdirectory like getting_started):
#   ../velo-test.sh                # Interactive: select SQL file → test case (default)
#   ../velo-test.sh .              # Run current directory as a test tier
#   ../velo-test.sh validate .     # Validate SQL in current directory
#   ../velo-test.sh menu           # Interactive menu for SQL files only
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

# Use Redpanda by default (faster startup: ~3s vs ~10s for Confluent Kafka)
export VELOSTREAM_TEST_CONTAINER="${VELOSTREAM_TEST_CONTAINER:-redpanda}"

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

# Show help function
show_help() {
    echo -e "${CYAN}Usage:${NC} ./velo-test.sh [command] [options]"
    echo ""
    echo -e "${YELLOW}Commands:${NC}"
    echo "  (default)       Interactive: select SQL file → test case"
    echo "  run, --all      Run all tiers (auto-starts Kafka via Docker)"
    echo "  validate        Validate all SQL syntax only (no Docker needed)"
    echo "  health          Check infrastructure health (Docker, Kafka, topics)"
    echo "  tier1-6         Run specific tier tests (e.g., tier1, tier2)"
    echo "  getting_started Run getting_started example"
    echo "  menu            Interactive menu to select SQL files only"
    echo "  cases           Interactive: select SQL file, then test case"
    echo "  .               Run tests in current directory"
    echo "  help, -h, --help Show this help message"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo "  --kafka <servers>   Use external Kafka instead of testcontainers"
    echo "  --reuse             Reuse existing Kafka container (faster for repeated runs)"
    echo "  --rerun-failed      Re-run only the tests that failed in the last run"
    echo "  --timeout <ms>      Timeout per query in milliseconds (default: 60000)"
    echo "  --output <format>   Output format: text, json, junit"
    echo "  -q, --query <name>  Run only a specific test case by name"
    echo ""
    echo -e "${YELLOW}From a subdirectory:${NC}"
    echo "  ../velo-test.sh       Interactive: select SQL file → test case"
    echo "  ../velo-test.sh .     Run current directory as a test tier"
    echo "  ../velo-test.sh validate .  Validate SQL in current directory"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  ./velo-test.sh                           # Interactive selection"
    echo "  ./velo-test.sh run                       # Run all tiers"
    echo "  ./velo-test.sh tier1                     # Run tier1 only"
    echo "  ./velo-test.sh validate                  # Validate SQL syntax"
    echo "  ./velo-test.sh --kafka localhost:9092    # Use external Kafka"
    echo "  ./velo-test.sh tier1 -q test_simple      # Run specific test case"
    echo ""
    echo -e "${CYAN}Requirements:${NC}"
    echo "  • velo-test binary (cargo build --release --features test-support)"
    echo "  • Docker (for testcontainers, unless --kafka is specified)"
    echo ""
    echo "See: docs/feature/FR-084-app-test-harness/README.md"
}

# Check for help flag first (before MODE parsing)
case "${1:-}" in
    -h|--help|help|\?|'?')
        show_help
        exit 0
        ;;
    --all|-all)
        # --all flag runs all tiers (equivalent to 'run' command)
        shift
        MODE="run"
        ;;
    --rerun-failed)
        shift
        RERUN_FAILED=true
        MODE="rerun-failed"
        ;;
    *)
        # Parse command line arguments
        MODE="${1:-cases}"
        shift || true
        ;;
esac

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
        --reuse)
            REUSE_CONTAINERS="--reuse-containers"
            shift
            ;;
        --rerun-failed)
            RERUN_FAILED=true
            shift
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
        if [[ -n "$REUSE_CONTAINERS" ]]; then
            KAFKA_OPTS="$KAFKA_OPTS $REUSE_CONTAINERS"
            echo -e "${BLUE}Container reuse enabled for faster iteration${NC}"
        fi
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
        tier7) echo "tier7_serialization" ;;
        tier8) echo "tier8_fault_tolerance" ;;
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

    # Step 3: Find matching spec file for the SQL file
    sql_dir="$(dirname "$selected_sql")"
    sql_basename="$(basename "$selected_sql" .sql)"
    local parent_dir="$(dirname "$sql_dir")"
    spec_file=""

    # Priority 1: Look for <sql_basename>_spec.yaml in parent dir (e.g., debug_demo_spec.yaml)
    if [[ -f "$parent_dir/${sql_basename}_spec.yaml" ]]; then
        spec_file="$parent_dir/${sql_basename}_spec.yaml"
    # Priority 2: Look for <sql_basename>.spec.yaml in parent dir
    elif [[ -f "$parent_dir/${sql_basename}.spec.yaml" ]]; then
        spec_file="$parent_dir/${sql_basename}.spec.yaml"
    # Priority 3: Look for spec with matching application: field
    elif [[ -z "$spec_file" ]]; then
        for candidate in "$parent_dir"/*spec*.yaml "$sql_dir"/*spec*.yaml "$search_dir"/*spec*.yaml; do
            if [[ -f "$candidate" ]]; then
                local app_name=$(grep -E "^application:" "$candidate" 2>/dev/null | sed 's/application:[[:space:]]*//' | tr -d '\r')
                if [[ "$app_name" == "$sql_basename" ]]; then
                    spec_file="$candidate"
                    break
                fi
            fi
        done
    fi

    # No matching spec found - offer options instead of wrong fallback
    if [[ -z "$spec_file" || ! -f "$spec_file" ]]; then
        echo -e "${YELLOW}No matching spec file for $(basename "$selected_sql")${NC}"
        echo ""
        echo -e "  ${CYAN}1)${NC} Run without assertions (just execute SQL)"
        echo -e "  ${CYAN}2)${NC} Debug interactively (step-by-step)"
        echo -e "  ${CYAN}0)${NC} Go back"
        echo ""

        while true; do
            echo -n -e "${GREEN}Choose option [1-2, 0]: ${NC}"
            read -r choice

            case "$choice" in
                1)
                    echo ""
                    run_sql_file "$selected_sql"
                    return $?
                    ;;
                2)
                    echo ""
                    echo -e "${CYAN}Starting interactive debugger...${NC}"
                    # Build debug command with optional spec for data generation
                    local debug_opts=""

                    # Check if there's a spec file for data generation
                    local debug_spec="${parent_dir}/${sql_basename}_spec.yaml"
                    if [[ -f "$debug_spec" ]]; then
                        debug_opts="--spec $debug_spec"
                        # Add schemas directory
                        local schemas_dir="$parent_dir/schemas"
                        if [[ ! -d "$schemas_dir" ]]; then
                            schemas_dir="$SCRIPT_DIR/schemas"
                        fi
                        if [[ -d "$schemas_dir" ]]; then
                            debug_opts="$debug_opts --schemas $schemas_dir"
                        fi
                        echo -e "${BLUE}Using spec for data generation: $debug_spec${NC}"
                    fi

                    # Add kafka option if external
                    if [[ -n "$KAFKA_SERVERS" ]]; then
                        debug_opts="$debug_opts --kafka $KAFKA_SERVERS"
                    fi

                    "$VELO_TEST" debug "$selected_sql" $debug_opts
                    return $?
                    ;;
                0|q|Q)
                    return 0
                    ;;
                *)
                    echo -e "${RED}Invalid choice${NC}"
                    ;;
            esac
        done
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
    echo -e "  ${CYAN}d)${NC} Debug interactively (step-by-step)"
    echo -e "  ${CYAN}0)${NC} Exit"
    echo ""

    while true; do
        echo -n -e "${GREEN}Enter test case [1-${#test_cases[@]}, a, d, 0]: ${NC}"
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

        if [[ "$selection" == "d" || "$selection" == "D" ]]; then
            echo ""
            echo -e "${CYAN}Starting interactive debugger...${NC}"
            # Build debug command with spec for data generation
            local debug_opts="--spec $spec_file"
            local schemas_dir="$(dirname "$spec_file")/schemas"
            if [[ ! -d "$schemas_dir" ]]; then
                schemas_dir="$SCRIPT_DIR/schemas"
            fi
            if [[ -d "$schemas_dir" ]]; then
                debug_opts="$debug_opts --schemas $schemas_dir"
            fi
            # Add kafka option if external
            if [[ -n "$KAFKA_SERVERS" ]]; then
                debug_opts="$debug_opts --kafka $KAFKA_SERVERS"
            fi
            echo -e "${BLUE}Using spec for data generation: $spec_file${NC}"
            "$VELO_TEST" debug "$selected_sql" $debug_opts
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

# Tracking arrays for summary
PASSED_TESTS=()
FAILED_TESTS=()
SKIPPED_TESTS=()

# Track failed test details for --rerun-failed
FAILED_SQL_FILES=()
FAILED_SPEC_FILES=()
FAILED_SCHEMAS_DIRS=()

# File to persist failed tests for rerun
FAILED_TESTS_FILE="${SCRIPT_DIR}/.failed-tests"

# Start time for total duration
SCRIPT_START_TIME=$(date +%s)

# Parallel arrays for test details (indexed same as PASSED/FAILED order in ALL_TESTS)
ALL_TESTS=()          # test label
ALL_STATUSES=()       # passed/failed/skipped
ALL_DURATIONS=()      # duration string (e.g., "22.8s")
ALL_ASSERTIONS=()     # assertion string (e.g., "3/3")
ALL_QUERIES=()        # query string (e.g., "1/1")

# Temp file for capturing velo-test output
VELO_OUTPUT_TMP=$(mktemp /tmp/velo_test_output.XXXXXX)
trap "rm -f $VELO_OUTPUT_TMP" EXIT

# Parse JSON output from velo-test to extract duration, assertions, queries, and failure details
# Sets: _duration_str, _assertions_str, _queries_str, _failure_details
parse_test_output() {
    local output_file="$1"
    _duration_str=""
    _assertions_str=""
    _queries_str=""
    _failure_details=""

    # Parse duration_ms (format: "duration_ms": 22734)
    local duration_ms=$(grep -o '"duration_ms":[[:space:]]*[0-9]*' "$output_file" 2>/dev/null | head -1 | grep -o '[0-9]*$')
    if [[ -n "$duration_ms" && "$duration_ms" -gt 0 ]]; then
        # Convert to seconds with 1 decimal
        local secs=$(( duration_ms / 1000 ))
        local tenths=$(( (duration_ms % 1000) / 100 ))
        _duration_str="${secs}.${tenths}s"
    fi

    # Parse assertions (format: "total_assertions": 3, "passed_assertions": 3)
    local total_assertions=$(grep -o '"total_assertions":[[:space:]]*[0-9]*' "$output_file" 2>/dev/null | head -1 | grep -o '[0-9]*$')
    local passed_assertions=$(grep -o '"passed_assertions":[[:space:]]*[0-9]*' "$output_file" 2>/dev/null | head -1 | grep -o '[0-9]*$')
    if [[ -n "$total_assertions" && -n "$passed_assertions" ]]; then
        _assertions_str="${passed_assertions}/${total_assertions}"
    fi

    # Parse queries (from "summary": {"total": 1, "passed": 1, ...})
    # These appear early in the summary block
    local total_queries=$(grep -o '"total":[[:space:]]*[0-9]*' "$output_file" 2>/dev/null | head -1 | grep -o '[0-9]*$')
    local passed_queries=$(grep -o '"passed":[[:space:]]*[0-9]*' "$output_file" 2>/dev/null | head -1 | grep -o '[0-9]*$')
    if [[ -n "$total_queries" && -n "$passed_queries" ]]; then
        _queries_str="${passed_queries}/${total_queries}"
    fi

    # Extract failure details from JSON output
    _failure_details=""

    # Extract failed assertion messages from JSON
    # JSON format: "passed": false, "message": "...", "expected": "...", "actual": "..."
    local in_failed=false
    local got_message=false
    while IFS= read -r line; do
        if echo "$line" | grep -q '"passed":[[:space:]]*false'; then
            in_failed=true
            got_message=false
        elif $in_failed; then
            # Check for message
            local msg
            msg=$(echo "$line" | grep -o '"message":[[:space:]]*"[^"]*"' | sed 's/"message":[[:space:]]*"//;s/"$//')
            if [[ -n "$msg" ]]; then
                _failure_details="${_failure_details}      ✗ ${msg}\n"
                got_message=true
            fi
            # Check for expected/actual (come after message)
            local expected
            expected=$(echo "$line" | grep -o '"expected":[[:space:]]*"[^"]*"' | sed 's/"expected":[[:space:]]*"//;s/"$//')
            if [[ -n "$expected" ]]; then
                _failure_details="${_failure_details}        Expected: ${expected}\n"
            fi
            local actual
            actual=$(echo "$line" | grep -o '"actual":[[:space:]]*"[^"]*"' | sed 's/"actual":[[:space:]]*"//;s/"$//')
            if [[ -n "$actual" ]]; then
                _failure_details="${_failure_details}        Actual:   ${actual}\n"
                in_failed=false  # End of this assertion block
            fi
            # End block if we see next assertion type or closing brace
            if $got_message && echo "$line" | grep -q '"type":[[:space:]]*"'; then
                in_failed=false
            fi
        fi
    done < "$output_file"

    # Also extract error-level log lines for context (timeout, topic errors, etc.)
    local errors
    errors=$(grep -iE ' ERROR |error:|timed out|timeout|failed to' "$output_file" 2>/dev/null | grep -v '"passed"' | head -5 | sed 's/^[[:space:]]*//' | sed 's/^/      ⚠ /')
    if [[ -n "$errors" ]]; then
        _failure_details="${_failure_details}${errors}\n"
    fi

    # If no failure details found at all, check for status:error in JSON
    if [[ -z "$_failure_details" ]]; then
        local status_error
        status_error=$(grep -o '"status":[[:space:]]*"error"' "$output_file" 2>/dev/null)
        if [[ -n "$status_error" ]]; then
            _failure_details="      ⚠ Test completed with error status (check logs for details)\n"
        elif [[ -s "$output_file" ]]; then
            # File has content but no parseable failure info - show last few lines
            local tail_lines
            tail_lines=$(tail -3 "$output_file" 2>/dev/null | sed 's/^/      │ /')
            if [[ -n "$tail_lines" ]]; then
                _failure_details="      ⚠ No structured failure details available. Last output:\n${tail_lines}\n"
            fi
        fi
    fi
}

# Run a single test (SQL + spec) and record results
# Usage: run_single_test <test_label> <sql_file> <spec_file> <schemas_dir>
run_single_test() {
    local test_label="$1"
    local sql_file="$2"
    local spec_file="$3"
    local schemas_dir="$4"
    local base_name="$(echo "$test_label" | sed 's|.*/||')"

    echo -n "  ▶ Running ${base_name}... "

    if [[ "$MODE" == "validate" ]]; then
        local start_time=$SECONDS
        if "$VELO_TEST" validate "$sql_file" > /dev/null 2>&1; then
            local elapsed=$(( SECONDS - start_time ))
            echo -e "${GREEN}✅ PASSED${NC} (${elapsed}s)"
            PASSED_TESTS+=("$test_label")
            ALL_TESTS+=("$test_label")
            ALL_STATUSES+=("passed")
            ALL_DURATIONS+=("${elapsed}s")
            ALL_ASSERTIONS+=("-")
            ALL_QUERIES+=("-")
        else
            local elapsed=$(( SECONDS - start_time ))
            echo -e "${RED}❌ FAILED${NC} (${elapsed}s)"
            FAILED_TESTS+=("$test_label")
            ALL_TESTS+=("$test_label")
            ALL_STATUSES+=("failed")
            ALL_DURATIONS+=("${elapsed}s")
            ALL_ASSERTIONS+=("-")
            ALL_QUERIES+=("-")
            return 1
        fi
        return 0
    fi

    # Run test with JSON output to capture details
    "$VELO_TEST" run "$sql_file" \
        --spec "$spec_file" \
        --schemas "$schemas_dir" \
        --timeout-ms "$TIMEOUT_MS" \
        --output json \
        ${QUERY_OPTS} \
        ${KAFKA_OPTS} > "$VELO_OUTPUT_TMP" 2>&1
    local exit_code=$?

    # Parse results from JSON output
    parse_test_output "$VELO_OUTPUT_TMP"

    # Build inline detail string
    local detail=""
    if [[ -n "$_assertions_str" ]]; then
        detail="${_assertions_str} assertions"
    fi
    if [[ -n "$_duration_str" ]]; then
        if [[ -n "$detail" ]]; then
            detail="${detail}, ${_duration_str}"
        else
            detail="${_duration_str}"
        fi
    fi

    if [[ $exit_code -eq 0 ]]; then
        if [[ -n "$detail" ]]; then
            echo -e "${GREEN}✅ PASSED${NC} (${detail})"
        else
            echo -e "${GREEN}✅ PASSED${NC}"
        fi
        PASSED_TESTS+=("$test_label")
        ALL_TESTS+=("$test_label")
        ALL_STATUSES+=("passed")
        ALL_DURATIONS+=("${_duration_str:-?}")
        ALL_ASSERTIONS+=("${_assertions_str:-?}")
        ALL_QUERIES+=("${_queries_str:-?}")
    else
        if [[ -n "$detail" ]]; then
            echo -e "${RED}❌ FAILED${NC} (${detail})"
        else
            echo -e "${RED}❌ FAILED${NC}"
        fi
        # Show failure details inline
        if [[ -n "$_failure_details" ]]; then
            echo -e "$_failure_details"
        fi
        FAILED_TESTS+=("$test_label")
        FAILED_SQL_FILES+=("$sql_file")
        FAILED_SPEC_FILES+=("$spec_file")
        FAILED_SCHEMAS_DIRS+=("$schemas_dir")
        ALL_TESTS+=("$test_label")
        ALL_STATUSES+=("failed")
        ALL_DURATIONS+=("${_duration_str:-?}")
        ALL_ASSERTIONS+=("${_assertions_str:-?}")
        ALL_QUERIES+=("${_queries_str:-?}")
        return 1
    fi
    return 0
}

# Function to run tests for a single tier
# Discovers per-SQL-file test specs (e.g., 01_passthrough.test.yaml) or falls back to test_spec.yaml
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

    local schemas_dir="$tier_dir/schemas"
    # Fall back to shared schemas if tier doesn't have its own
    if [[ ! -d "$schemas_dir" ]]; then
        schemas_dir="schemas"
    fi

    # Strategy 1: Look for per-SQL-file test specs (e.g., 01_passthrough.test.yaml)
    local per_file_specs=()
    while IFS= read -r -d '' spec; do
        per_file_specs+=("$spec")
    done < <(find "$tier_dir" -maxdepth 1 -name "*.test.yaml" -type f -print0 | sort -z)

    if [[ ${#per_file_specs[@]} -gt 0 ]]; then
        local tier_failed=0
        for spec_file in "${per_file_specs[@]}"; do
            local base_name="$(basename "$spec_file" .test.yaml)"
            local sql_file="$tier_dir/${base_name}.sql"
            local test_label="${tier_name}/${base_name}"

            if [[ ! -f "$sql_file" ]]; then
                echo -e "  ${YELLOW}⚠ Skipped: $base_name (no matching .sql file)${NC}"
                SKIPPED_TESTS+=("$test_label")
                ALL_TESTS+=("$test_label")
                ALL_STATUSES+=("skipped")
                ALL_DURATIONS+=("-")
                ALL_ASSERTIONS+=("-")
                ALL_QUERIES+=("-")
                continue
            fi

            run_single_test "$test_label" "$sql_file" "$spec_file" "$schemas_dir" || tier_failed=1
        done
        return $tier_failed
    fi

    # Strategy 2: Fall back to single test_spec.yaml (e.g., getting_started)
    local spec_file="$tier_dir/test_spec.yaml"
    if [[ -f "$spec_file" ]]; then
        local app_name=$(grep -E "^application:" "$spec_file" 2>/dev/null | sed 's/application:[[:space:]]*//' | tr -d '\r')
        local sql_file=""

        if [[ -n "$app_name" ]]; then
            sql_file=$(find "$tier_dir" -name "${app_name}.sql" -type f 2>/dev/null | head -1)
        fi
        if [[ -z "$sql_file" ]]; then
            sql_file=$(find "$tier_dir" -name "*.sql" -type f 2>/dev/null | head -1)
        fi

        if [[ -z "$sql_file" ]]; then
            echo -e "${YELLOW}  No SQL files found in $tier_dir${NC}"
            return 0
        fi

        run_single_test "$tier_name" "$sql_file" "$spec_file" "$schemas_dir"
        return $?
    fi

    # Strategy 3: No specs at all — validate only
    local sql_count=$(find "$tier_dir" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$sql_count" -eq 0 ]]; then
        echo -e "${YELLOW}  No SQL files found in $tier_dir${NC}"
        return 0
    fi

    echo -e "${YELLOW}  No test specs found — validating SQL only${NC}"
    local tier_failed=0
    for sql_file in $(find "$tier_dir" -name "*.sql" -type f | sort); do
        local base_name="$(basename "$sql_file" .sql)"
        echo -n "  ▶ Validating ${base_name}... "
        if "$VELO_TEST" validate "$sql_file" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ VALID${NC}"
        else
            echo -e "${RED}❌ INVALID${NC}"
            FAILED_TESTS+=("${tier_name}/${base_name}")
            tier_failed=1
        fi
    done
    return $tier_failed
}

# Format duration in human readable form
format_duration() {
    local secs=$1
    if [[ $secs -ge 60 ]]; then
        printf "%dm %ds" $((secs / 60)) $((secs % 60))
    else
        printf "%ds" $secs
    fi
}

# Print test summary with details table
print_summary() {
    local total_tests=${#ALL_TESTS[@]}
    local script_end_time
    script_end_time=$(date +%s)
    local total_duration=$((script_end_time - SCRIPT_START_TIME))

    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Test Summary${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Table header
    printf "  %-30s %-12s %-14s %-10s %s\n" "Test" "Queries" "Assertions" "Duration" "Status"
    printf "  %-30s %-12s %-14s %-10s %s\n" "──────────────────────────────" "────────────" "──────────────" "──────────" "──────"

    # Table rows
    local total_assertions_passed=0
    local total_assertions_all=0
    local total_queries_passed=0
    local total_queries_all=0

    for i in $(seq 0 $(( total_tests - 1 ))); do
        local label="${ALL_TESTS[$i]}"
        local status="${ALL_STATUSES[$i]}"
        local duration="${ALL_DURATIONS[$i]}"
        local assertions="${ALL_ASSERTIONS[$i]}"
        local queries="${ALL_QUERIES[$i]}"

        local status_icon=""
        case "$status" in
            passed)  status_icon="${GREEN}✅${NC}" ;;
            failed)  status_icon="${RED}❌${NC}" ;;
            skipped) status_icon="${YELLOW}⚠${NC}" ;;
        esac

        printf "  %-30s %-12s %-14s %-10s " "$label" "$queries" "$assertions" "$duration"
        echo -e "$status_icon"

        # Accumulate assertion/query totals
        if [[ "$assertions" != "-" && "$assertions" != "?" ]]; then
            local a_passed=$(echo "$assertions" | cut -d/ -f1)
            local a_total=$(echo "$assertions" | cut -d/ -f2)
            total_assertions_passed=$(( total_assertions_passed + a_passed ))
            total_assertions_all=$(( total_assertions_all + a_total ))
        fi
        if [[ "$queries" != "-" && "$queries" != "?" ]]; then
            local q_passed=$(echo "$queries" | cut -d/ -f1)
            local q_total=$(echo "$queries" | cut -d/ -f2)
            total_queries_passed=$(( total_queries_passed + q_passed ))
            total_queries_all=$(( total_queries_all + q_total ))
        fi
    done

    # Totals row
    printf "  %-30s %-12s %-14s %-10s %s\n" "──────────────────────────────" "────────────" "──────────────" "──────────" "──────"
    printf "  %-30s %-12s %-14s\n" \
        "Total (${#PASSED_TESTS[@]} passed, ${#FAILED_TESTS[@]} failed)" \
        "${total_queries_passed}/${total_queries_all}" \
        "${total_assertions_passed}/${total_assertions_all}"

    if [[ ${#FAILED_TESTS[@]} -gt 0 ]]; then
        echo ""
        echo -e "${RED}Failed tests:${NC}"
        for t in "${FAILED_TESTS[@]}"; do
            echo -e "  ${RED}✗${NC} $t"
        done

        # Save failed tests to file for --rerun-failed
        true > "$FAILED_TESTS_FILE"
        for i in $(seq 0 $(( ${#FAILED_TESTS[@]} - 1 ))); do
            echo "${FAILED_SQL_FILES[$i]}|${FAILED_SPEC_FILES[$i]}|${FAILED_SCHEMAS_DIRS[$i]}|${FAILED_TESTS[$i]}" >> "$FAILED_TESTS_FILE"
        done
    else
        # Clear the failed tests file on success
        rm -f "$FAILED_TESTS_FILE"
    fi

    echo ""
    echo -e "⏱️  Total Duration: $(format_duration $total_duration)"
    echo ""
    if [[ ${#FAILED_TESTS[@]} -eq 0 ]]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    else
        echo -e "${RED}❌ ${#FAILED_TESTS[@]} test(s) failed${NC}"
        echo -e "${YELLOW}  Rerun failures: ./velo-test.sh --rerun-failed${NC}"
    fi
}

# Handle --rerun-failed: override mode to re-run only previously failed tests
if [[ "${RERUN_FAILED:-}" == "true" ]]; then
    if [[ ! -f "$FAILED_TESTS_FILE" ]]; then
        echo -e "${YELLOW}No failed tests to rerun (.failed-tests file not found)${NC}"
        echo "Run tests first with: ./velo-test.sh --all"
        exit 0
    fi

    failed_count=$(wc -l < "$FAILED_TESTS_FILE" | tr -d ' ')
    echo -e "${YELLOW}Re-running ${failed_count} previously failed test(s)...${NC}"
    echo ""

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Re-running failed tests${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    while IFS='|' read -r sql_file spec_file schemas_dir test_label; do
        run_single_test "$test_label" "$sql_file" "$spec_file" "$schemas_dir" || true
    done < "$FAILED_TESTS_FILE"

    print_summary
    [[ ${#FAILED_TESTS[@]} -eq 0 ]] || exit 1
    exit 0
fi

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
            for tier_name in getting_started tier1 tier2 tier3 tier4 tier5 tier6 tier7 tier8; do
                tier_dir=$(get_tier_dir "$tier_name")
                run_tier "$tier_name" "$tier_dir" || true
            done
        fi
        print_summary
        [[ ${#FAILED_TESTS[@]} -eq 0 ]] || exit 1
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

    getting_started|tier1|tier2|tier3|tier4|tier5|tier6|tier7|tier8)
        # Run specific tier
        tier_dir=$(get_tier_dir "$MODE")
        if [[ -z "$tier_dir" ]]; then
            echo -e "${RED}Unknown tier: $MODE${NC}"
            exit 1
        fi
        run_tier "$MODE" "$tier_dir"
        print_summary
        [[ ${#FAILED_TESTS[@]} -eq 0 ]] || exit 1
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

    health)
        # Run infrastructure health check
        echo -e "${CYAN}Running infrastructure health check...${NC}"
        echo ""
        broker="${KAFKA_SERVERS:-localhost:9092}"
        "$VELO_TEST" health --broker "$broker" --output text
        ;;

    run|*)
        echo -e "${YELLOW}Running all test tiers...${NC}"
        echo ""

        for tier_name in getting_started tier1 tier2 tier3 tier4 tier5 tier6 tier7 tier8; do
            tier_dir=$(get_tier_dir "$tier_name")
            run_tier "$tier_name" "$tier_dir" || true
            echo ""
        done

        print_summary
        [[ ${#FAILED_TESTS[@]} -eq 0 ]] || exit 1
        ;;
esac
