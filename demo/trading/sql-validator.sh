#!/bin/bash

# SQL Validator Script for Financial Trading Demo
# Validates SQL files and their configurations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."

# Velo CLI binary path (comprehensive CLI tool with fixed SQL validation)
VELO_CLI="$PROJECT_ROOT/target/debug/velo-cli"

# Default SQL file to validate
DEFAULT_SQL_FILE="$SCRIPT_DIR/sql/financial_trading.sql"

# Function to print colored output
print_color() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to build Velo CLI if it doesn't exist
build_velo_cli() {
    if [ ! -f "$VELO_CLI" ]; then
        print_color "$YELLOW" "Velo CLI binary not found. Building..."
        cd "$PROJECT_ROOT"
        cargo build --bin velo-cli
        cd "$SCRIPT_DIR"
        print_color "$GREEN" "✓ Velo CLI built successfully"
    fi
}

# Function to validate a SQL file
validate_sql_file() {
    local sql_file=$1
    local verbose=${2:-false}

    if [ ! -f "$sql_file" ]; then
        print_color "$RED" "✗ SQL file not found: $sql_file"
        return 1
    fi

    print_color "$YELLOW" "Validating SQL file: $sql_file"
    print_color "$YELLOW" "════════════════════════════════════════════════════"

    # Run the validator

    # Basic validation
    print_color "$GREEN" "\n► Running SQL validation..."

    # Create temp file for output
    local output_file="/tmp/sql_validator_output_$$.txt"

    # Run validator and capture output
    if $VELO_CLI validate "$sql_file" > "$output_file" 2>&1; then
        print_color "$GREEN" "✓ SQL validation passed"

        # Show summary of results
        print_color "$GREEN" "\n► Validation Results:"
        cat "$output_file"
    else
        print_color "$RED" "✗ SQL validation found issues"

        # Show the errors
        print_color "$RED" "\n► Validation Errors:"
        cat "$output_file"

        # Clean up and exit with error
        rm -f "$output_file"
        return 1
    fi

    # Run with verbose mode if requested
    if [ "$verbose" = true ]; then
        print_color "$YELLOW" "\n► Running verbose validation..."
        $VELO_CLI validate "$sql_file" --verbose 2>&1 || true
    fi

    # Clean up
    rm -f "$output_file"

    print_color "$GREEN" "\n✓ Validation complete for $sql_file"
}

# Function to validate all SQL files in a directory
validate_all_sql_files() {
    local verbose=${1:-false}
    local dir="$SCRIPT_DIR/sql"
    local found_files=false

    print_color "$YELLOW" "Scanning for SQL files in: $dir"

    for sql_file in "$dir"/*.sql; do
        if [ -f "$sql_file" ]; then
            found_files=true
            echo ""
            validate_sql_file "$sql_file" "$verbose"
            echo ""
        fi
    done

    if [ "$found_files" = false ]; then
        print_color "$YELLOW" "No SQL files found in $dir"
    fi
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] [SQL_FILE]

Validates SQL files for the Velostream Financial Trading Demo

Options:
    -h, --help          Show this help message
    -a, --all           Validate all SQL files in the sql/ directory
    -b, --build         Force rebuild of Velo CLI binary
    -v, --verbose       Show detailed validation output

Arguments:
    SQL_FILE            Path to SQL file to validate (default: sql/financial_trading.sql)

Examples:
    $0                              # Validate default financial_trading.sql
    $0 sql/custom_query.sql         # Validate specific SQL file
    $0 --all                        # Validate all SQL files
    $0 --build --all                # Rebuild validator and validate all

EOF
}

# Main execution
main() {
    local sql_file=""
    local validate_all=false
    local force_build=false
    local verbose=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -a|--all)
                validate_all=true
                shift
                ;;
            -b|--build)
                force_build=true
                shift
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            *)
                sql_file="$1"
                shift
                ;;
        esac
    done

    print_color "$GREEN" "╔════════════════════════════════════════════════════╗"
    print_color "$GREEN" "║   Velostream SQL Validator - Financial Trading    ║"
    print_color "$GREEN" "╚════════════════════════════════════════════════════╝"
    echo ""

    # Force rebuild if requested
    if [ "$force_build" = true ]; then
        rm -f "$VELO_CLI"
    fi

    # Build Velo CLI if needed
    build_velo_cli

    # Validate files
    if [ "$validate_all" = true ]; then
        validate_all_sql_files "$verbose"
    elif [ -n "$sql_file" ]; then
        validate_sql_file "$sql_file" "$verbose"
    else
        validate_sql_file "$DEFAULT_SQL_FILE" "$verbose"
    fi

    print_color "$GREEN" "\n✓ SQL validation completed successfully!"
}

# Run main function
main "$@"