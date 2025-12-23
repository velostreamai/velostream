//! Shell script scaffolding for velo-test runner scripts
//!
//! Generates `velo-test.sh` runner scripts based on directory structure detection.
//!
//! # Directory Patterns Detected
//!
//! - **Trading-style**: `apps/*.sql` + `tests/*.yaml` → Simple app-focused runner
//! - **Tiered**: `tier*_*/sql/*.sql` → Multi-tier interactive runner
//! - **Single-file**: Single `*.sql` → Minimal runner
//!
//! # Usage
//!
//! ```bash
//! velo-test scaffold .                              # Auto-detect and generate
//! velo-test scaffold . --style trading              # Force trading style
//! velo-test scaffold . --name "My Project"          # Custom project name
//! velo-test scaffold . --output runner.sh           # Custom output path
//! ```

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Style of the generated runner script
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaffoldStyle {
    /// Simple app-focused runner (apps/*.sql + tests/*.yaml)
    Simple,
    /// Multi-tier interactive runner with menus
    Tiered,
    /// Minimal runner for single SQL file
    Minimal,
}

impl std::fmt::Display for ScaffoldStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScaffoldStyle::Simple => write!(f, "simple"),
            ScaffoldStyle::Tiered => write!(f, "tiered"),
            ScaffoldStyle::Minimal => write!(f, "minimal"),
        }
    }
}

impl std::str::FromStr for ScaffoldStyle {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" | "trading" => Ok(ScaffoldStyle::Simple),
            "tiered" | "multi" | "interactive" => Ok(ScaffoldStyle::Tiered),
            "minimal" | "single" => Ok(ScaffoldStyle::Minimal),
            _ => Err(format!(
                "Unknown style: {}. Use: simple, tiered, or minimal",
                s
            )),
        }
    }
}

/// Configuration for scaffold generation
#[derive(Debug, Clone)]
pub struct ScaffoldConfig {
    /// Directory to analyze and generate script for
    pub directory: PathBuf,
    /// Output path for generated script (default: ./velo-test.sh)
    pub output: PathBuf,
    /// Project name for header (auto-detected from directory name)
    pub name: Option<String>,
    /// Force a specific style (auto-detected if None)
    pub style: Option<ScaffoldStyle>,
    /// Relative path to velo-test binary (default: ../../target/release/velo-test)
    pub velo_test_path: String,
}

impl Default for ScaffoldConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("."),
            output: PathBuf::from("velo-test.sh"),
            name: None,
            style: None,
            velo_test_path: "../../target/release/velo-test".to_string(),
        }
    }
}

/// Detected directory structure
#[derive(Debug)]
pub struct DetectedStructure {
    /// Detected style
    pub style: ScaffoldStyle,
    /// SQL files found
    pub sql_files: Vec<PathBuf>,
    /// Test spec files found
    pub spec_files: Vec<PathBuf>,
    /// Schema directories found
    pub schema_dirs: Vec<PathBuf>,
    /// Tier directories found (for tiered style)
    pub tier_dirs: Vec<String>,
    /// Apps directory (for simple style)
    pub apps_dir: Option<PathBuf>,
    /// Tests directory (for simple style)
    pub tests_dir: Option<PathBuf>,
}

/// Scaffolder for generating velo-test.sh scripts
pub struct Scaffolder {
    config: ScaffoldConfig,
}

impl Scaffolder {
    /// Create a new scaffolder with default config
    pub fn new() -> Self {
        Self {
            config: ScaffoldConfig::default(),
        }
    }

    /// Create scaffolder with specific config
    pub fn with_config(config: ScaffoldConfig) -> Self {
        Self { config }
    }

    /// Set the directory to analyze
    pub fn directory(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.directory = dir.into();
        self
    }

    /// Set the output path
    pub fn output(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.output = path.into();
        self
    }

    /// Set the project name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = Some(name.into());
        self
    }

    /// Force a specific style
    pub fn style(mut self, style: ScaffoldStyle) -> Self {
        self.config.style = Some(style);
        self
    }

    /// Set the velo-test binary path
    pub fn velo_test_path(mut self, path: impl Into<String>) -> Self {
        self.config.velo_test_path = path.into();
        self
    }

    /// Detect the directory structure
    pub fn detect_structure(&self) -> Result<DetectedStructure, String> {
        let dir = &self.config.directory;

        if !dir.exists() {
            return Err(format!("Directory does not exist: {}", dir.display()));
        }

        let mut sql_files = Vec::new();
        let mut spec_files = Vec::new();
        let mut schema_dirs = Vec::new();
        let mut tier_dirs = Vec::new();
        let mut apps_dir = None;
        let mut tests_dir = None;

        // Check for apps/ directory (simple style)
        let apps_path = dir.join("apps");
        if apps_path.is_dir() {
            apps_dir = Some(apps_path.clone());
            // Find SQL files in apps/
            if let Ok(entries) = fs::read_dir(&apps_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "sql") {
                        sql_files.push(path);
                    }
                }
            }
        }

        // Check for tests/ directory (simple style)
        let tests_path = dir.join("tests");
        if tests_path.is_dir() {
            tests_dir = Some(tests_path.clone());
            // Find spec files in tests/
            if let Ok(entries) = fs::read_dir(&tests_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path
                        .extension()
                        .is_some_and(|ext| ext == "yaml" || ext == "yml")
                    {
                        let name = path.file_name().map(|n| n.to_string_lossy().to_string());
                        if name.is_some_and(|n| n.contains("test") || n.contains("spec")) {
                            spec_files.push(path);
                        }
                    }
                }
            }
        }

        // Check for tier directories (tiered style)
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let name = path.file_name().map(|n| n.to_string_lossy().to_string());
                    if let Some(name) = name {
                        // Look for tier*_ or getting_started patterns
                        if name.starts_with("tier") || name == "getting_started" {
                            tier_dirs.push(name.clone());

                            // Find SQL files in tier directories
                            let sql_subdir = path.join("sql");
                            if sql_subdir.is_dir() {
                                if let Ok(sql_entries) = fs::read_dir(&sql_subdir) {
                                    for sql_entry in sql_entries.flatten() {
                                        let sql_path = sql_entry.path();
                                        if sql_path.extension().is_some_and(|ext| ext == "sql") {
                                            sql_files.push(sql_path);
                                        }
                                    }
                                }
                            }

                            // Also check for SQL files directly in tier dir
                            if let Ok(tier_entries) = fs::read_dir(&path) {
                                for tier_entry in tier_entries.flatten() {
                                    let tier_path = tier_entry.path();
                                    if tier_path.extension().is_some_and(|ext| ext == "sql") {
                                        sql_files.push(tier_path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // If no apps/ or tier dirs, look for SQL files directly
        if apps_dir.is_none() && tier_dirs.is_empty() {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "sql") {
                        sql_files.push(path);
                    }
                }
            }

            // Also check sql/ subdirectory
            let sql_subdir = dir.join("sql");
            if sql_subdir.is_dir() {
                if let Ok(entries) = fs::read_dir(&sql_subdir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().is_some_and(|ext| ext == "sql") {
                            sql_files.push(path);
                        }
                    }
                }
            }
        }

        // Find schema directories
        let schemas_path = dir.join("schemas");
        if schemas_path.is_dir() {
            schema_dirs.push(schemas_path);
        }

        // Find spec files if not already found
        if spec_files.is_empty() {
            // Look for spec files in root
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path
                        .extension()
                        .is_some_and(|ext| ext == "yaml" || ext == "yml")
                    {
                        let name = path.file_name().map(|n| n.to_string_lossy().to_string());
                        if name.is_some_and(|n| n.contains("test") || n.contains("spec")) {
                            spec_files.push(path);
                        }
                    }
                }
            }
        }

        // Sort tier dirs naturally
        tier_dirs.sort_by(|a, b| {
            // Put getting_started first
            if a == "getting_started" {
                return std::cmp::Ordering::Less;
            }
            if b == "getting_started" {
                return std::cmp::Ordering::Greater;
            }
            a.cmp(b)
        });

        // Determine style
        let style = if let Some(forced) = self.config.style {
            forced
        } else if !tier_dirs.is_empty() {
            ScaffoldStyle::Tiered
        } else if apps_dir.is_some() && !sql_files.is_empty() {
            ScaffoldStyle::Simple
        } else if sql_files.len() == 1 {
            ScaffoldStyle::Minimal
        } else if !sql_files.is_empty() {
            ScaffoldStyle::Simple
        } else {
            return Err("No SQL files found in directory".to_string());
        };

        Ok(DetectedStructure {
            style,
            sql_files,
            spec_files,
            schema_dirs,
            tier_dirs,
            apps_dir,
            tests_dir,
        })
    }

    /// Generate the shell script
    pub fn generate(&self) -> Result<String, String> {
        let structure = self.detect_structure()?;
        let project_name = self.config.name.clone().unwrap_or_else(|| {
            self.config
                .directory
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "project".to_string())
        });

        match structure.style {
            ScaffoldStyle::Simple => self.generate_simple(&project_name, &structure),
            ScaffoldStyle::Tiered => self.generate_tiered(&project_name, &structure),
            ScaffoldStyle::Minimal => self.generate_minimal(&project_name, &structure),
        }
    }

    /// Write the generated script to the output path
    pub fn write(&self) -> Result<PathBuf, String> {
        let script = self.generate()?;
        let output_path = if self.config.output.is_absolute() {
            self.config.output.clone()
        } else {
            self.config.directory.join(&self.config.output)
        };

        fs::write(&output_path, &script).map_err(|e| format!("Failed to write script: {}", e))?;

        // Make executable on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&output_path)
                .map_err(|e| format!("Failed to get permissions: {}", e))?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&output_path, perms)
                .map_err(|e| format!("Failed to set permissions: {}", e))?;
        }

        Ok(output_path)
    }

    /// Generate simple (trading-style) script
    fn generate_simple(
        &self,
        project_name: &str,
        structure: &DetectedStructure,
    ) -> Result<String, String> {
        let velo_test_path = &self.config.velo_test_path;
        let has_apps_dir = structure.apps_dir.is_some();
        let has_tests_dir = structure.tests_dir.is_some();
        let has_schemas = !structure.schema_dirs.is_empty();

        let sql_dir = if has_apps_dir { "apps" } else { "sql" };
        let spec_dir = if has_tests_dir { "tests" } else { "." };
        let schemas_dir = if has_schemas { "schemas" } else { "." };

        // Collect app names from SQL files
        let app_names: Vec<String> = structure
            .sql_files
            .iter()
            .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let app_list = app_names
            .iter()
            .map(|n| format!("  {}", n))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(format!(
            r##"#!/bin/bash
# =============================================================================
# {project_name} - velo-test Runner
# =============================================================================
# This script runs velo-test against the SQL applications in this project.
#
# Usage:
#   ./velo-test.sh                    # List available apps
#   ./velo-test.sh <app_name>         # Run specific app tests
#   ./velo-test.sh validate           # Validate all SQL syntax
#   ./velo-test.sh all                # Run all app tests
#
# Debug/Step-Through:
#   ./velo-test.sh <app_name> --step           # Step through each query
#   ./velo-test.sh <app_name> --query <name>   # Run single query
#   ./velo-test.sh <app_name> --keep           # Keep Kafka running after test
#   ./velo-test.sh <app_name> -v               # Verbose output
#
# Options:
#   --step                Step through queries one at a time (interactive)
#   --keep                Keep testcontainers running after test (for debugging)
#   -v, --verbose         Enable verbose output
#   --kafka <servers>     Use external Kafka instead of testcontainers
#   --timeout <ms>        Timeout per query in milliseconds (default: 90000)
#   --query <name>        Run only a specific query
#
# Requirements:
#   - velo-test binary (build with: cargo build --release)
#   - Docker (for testcontainers, unless --kafka is specified)
#
# Generated by: velo-test scaffold
# =============================================================================

set -e

# Use Redpanda by default (faster startup: ~3s vs ~10s for Confluent Kafka)
export VELOSTREAM_TEST_CONTAINER="${{VELOSTREAM_TEST_CONTAINER:-redpanda}}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if velo-test is available
VELO_TEST="${{VELO_TEST:-{velo_test_path}}}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${{RED}}Error: velo-test not found${{NC}}"
    echo "Build it with: cargo build --release"
    echo "Or set VELO_TEST environment variable to the binary path"
    exit 1
fi

# Convert to absolute path
if [[ -f "$VELO_TEST" ]]; then
    VELO_TEST="$(cd "$(dirname "$VELO_TEST")" && pwd)/$(basename "$VELO_TEST")"
fi

# Default values
TIMEOUT_MS=90000
KAFKA_SERVERS=""
QUERY_FILTER=""
STEP_MODE=""
KEEP_CONTAINERS=""
VERBOSE=""

# Show help
show_help() {{
    echo -e "${{CYAN}}{project_name} Test Runner${{NC}}"
    echo ""
    echo -e "${{YELLOW}}Usage:${{NC}}"
    echo "  ./velo-test.sh                    List available apps"
    echo "  ./velo-test.sh <app_name>         Run specific app tests"
    echo "  ./velo-test.sh validate           Validate all SQL syntax"
    echo "  ./velo-test.sh all                Run all app tests"
    echo ""
    echo -e "${{YELLOW}}Available Apps:${{NC}}"
    for sql in {sql_dir}/*.sql; do
        name=$(basename "$sql" .sql)
        echo "  $name"
    done
    echo ""
    echo -e "${{YELLOW}}Debug Options:${{NC}}"
    echo "  --step              Step through queries one at a time"
    echo "  --keep              Keep Kafka container running after test"
    echo "  -v, --verbose       Enable verbose output"
    echo "  --query <name>      Run only a specific query"
    echo ""
    echo -e "${{YELLOW}}Other Options:${{NC}}"
    echo "  --kafka <servers>   Use external Kafka instead of testcontainers"
    echo "  --timeout <ms>      Timeout per query (default: 90000)"
    echo "  -h, --help          Show this help"
}}

# Parse arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help|help)
            show_help
            exit 0
            ;;
        --kafka)
            KAFKA_SERVERS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_MS="$2"
            shift 2
            ;;
        --query|-q)
            QUERY_FILTER="$2"
            shift 2
            ;;
        --step)
            STEP_MODE="--step"
            shift
            ;;
        --keep)
            KEEP_CONTAINERS="--keep-containers"
            shift
            ;;
        -v|--verbose)
            VERBOSE="--verbose"
            shift
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# Validate SQL files only
run_validate() {{
    echo -e "${{CYAN}}Validating SQL files...${{NC}}"
    local errors=0
    for sql in {sql_dir}/*.sql; do
        name=$(basename "$sql" .sql)
        echo -n "  Validating $name... "
        if $VELO_TEST validate "$sql" 2>/dev/null; then
            echo -e "${{GREEN}}✓${{NC}}"
        else
            echo -e "${{RED}}✗${{NC}}"
            ((errors++))
        fi
    done

    if [[ $errors -eq 0 ]]; then
        echo -e "${{GREEN}}All SQL files valid!${{NC}}"
    else
        echo -e "${{RED}}$errors file(s) had errors${{NC}}"
        exit 1
    fi
}}

# Run a single app test
run_app() {{
    local app_name="$1"
    local sql_file="{sql_dir}/${{app_name}}.sql"
    local spec_file="{spec_dir}/${{app_name}}.test.yaml"

    # Also try _spec.yaml suffix
    if [[ ! -f "$spec_file" ]]; then
        spec_file="{spec_dir}/${{app_name}}_spec.yaml"
    fi

    if [[ ! -f "$sql_file" ]]; then
        echo -e "${{RED}}Error: SQL file not found: $sql_file${{NC}}"
        exit 1
    fi

    echo -e "${{CYAN}}Running: $app_name${{NC}}"
    echo "  SQL:  $sql_file"
    echo "  Spec: $spec_file"
    echo ""

    # Build command
    local cmd="$VELO_TEST run $sql_file"
    cmd="$cmd --timeout-ms $TIMEOUT_MS"
    cmd="$cmd --schemas {schemas_dir}"

    if [[ -f "$spec_file" ]]; then
        cmd="$cmd --spec $spec_file"
    fi

    if [[ -n "$KAFKA_SERVERS" ]]; then
        cmd="$cmd --kafka $KAFKA_SERVERS"
    else
        cmd="$cmd --use-testcontainers"
    fi

    if [[ -n "$QUERY_FILTER" ]]; then
        cmd="$cmd --query $QUERY_FILTER"
    fi

    if [[ -n "$STEP_MODE" ]]; then
        cmd="$cmd $STEP_MODE"
    fi

    if [[ -n "$KEEP_CONTAINERS" ]]; then
        cmd="$cmd $KEEP_CONTAINERS"
    fi

    if [[ -n "$VERBOSE" ]]; then
        cmd="$cmd $VERBOSE"
    fi

    echo -e "${{BLUE}}$cmd${{NC}}"
    echo ""

    # Run with RUST_LOG=info for readable output (debug for verbose)
    if [[ -n "$VERBOSE" ]]; then
        RUST_LOG=debug $cmd
    else
        RUST_LOG=info $cmd
    fi
}}

# Run all app tests
run_all() {{
    echo -e "${{CYAN}}Running all apps...${{NC}}"
    echo ""

    local passed=0
    local failed=0

    for sql in {sql_dir}/*.sql; do
        name=$(basename "$sql" .sql)
        echo -e "${{YELLOW}}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${{NC}}"
        if run_app "$name"; then
            ((passed++))
        else
            ((failed++))
        fi
        echo ""
    done

    echo -e "${{CYAN}}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${{NC}}"
    echo -e "${{CYAN}}Summary: $passed passed, $failed failed${{NC}}"

    if [[ $failed -gt 0 ]]; then
        exit 1
    fi
}}

# Main
case "$COMMAND" in
    ""|list)
        show_help
        ;;
    validate)
        run_validate
        ;;
    all)
        run_all
        ;;
    *)
        # Check if it's a valid app name
        if [[ -f "{sql_dir}/${{COMMAND}}.sql" ]]; then
            run_app "$COMMAND"
        else
            echo -e "${{RED}}Unknown command or app: $COMMAND${{NC}}"
            echo ""
            show_help
            exit 1
        fi
        ;;
esac
"##,
            project_name = project_name,
            velo_test_path = velo_test_path,
            sql_dir = sql_dir,
            spec_dir = spec_dir,
            schemas_dir = schemas_dir,
        ))
    }

    /// Generate tiered (test_harness_examples-style) script
    fn generate_tiered(
        &self,
        project_name: &str,
        structure: &DetectedStructure,
    ) -> Result<String, String> {
        let velo_test_path = &self.config.velo_test_path;

        // Build tier mapping
        let tier_mapping: Vec<(String, String)> = structure
            .tier_dirs
            .iter()
            .map(|name| {
                let short_name = if name == "getting_started" {
                    "getting_started".to_string()
                } else if name.starts_with("tier") {
                    // Extract tier number: "tier1_basic" -> "tier1"
                    name.split('_').next().unwrap_or(name).to_string()
                } else {
                    name.clone()
                };
                (short_name, name.clone())
            })
            .collect();

        let tier_case_entries = tier_mapping
            .iter()
            .map(|(short, full)| format!("        {}) echo \"{}\" ;;", short, full))
            .collect::<Vec<_>>()
            .join("\n");

        let tier_list = tier_mapping
            .iter()
            .map(|(short, _)| short.clone())
            .collect::<Vec<_>>()
            .join(" ");

        let tier_case_match = tier_mapping
            .iter()
            .map(|(short, _)| short.clone())
            .collect::<Vec<_>>()
            .join("|");

        Ok(format!(
            r##"#!/bin/bash
# =============================================================================
# {project_name} - velo-test Runner
# =============================================================================
# This script runs velo-test against the example SQL applications in this demo.
#
# Usage (from {project_name} directory):
#   ./velo-test.sh                 # Interactive: select SQL file → test case (default)
#   ./velo-test.sh run             # Run all tiers (auto-starts Kafka via Docker)
#   ./velo-test.sh validate        # Validate all SQL syntax only (no Docker needed)
#   ./velo-test.sh tier1           # Run tier1 tests only
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
#   - velo-test binary (build with: cargo build --release)
#   - Docker (for Kafka testcontainers, unless --kafka is specified)
#
# Generated by: velo-test scaffold
# =============================================================================

set -e

# Use Redpanda by default (faster startup: ~3s vs ~10s for Confluent Kafka)
export VELOSTREAM_TEST_CONTAINER="${{VELOSTREAM_TEST_CONTAINER:-redpanda}}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory (where velo-test.sh lives)
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"

# Save the original working directory (where the user ran the command from)
ORIGINAL_DIR="$(pwd)"

# Change to script directory for tier lookups
cd "$SCRIPT_DIR"

# Check if velo-test is available
VELO_TEST="${{VELO_TEST:-{velo_test_path}}}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${{RED}}Error: velo-test not found${{NC}}"
    echo "Build it with: cargo build --release"
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
show_help() {{
    echo -e "${{CYAN}}Usage:${{NC}} ./velo-test.sh [command] [options]"
    echo ""
    echo -e "${{YELLOW}}Commands:${{NC}}"
    echo "  (default)       Interactive: select SQL file → test case"
    echo "  run             Run all tiers (auto-starts Kafka via Docker)"
    echo "  validate        Validate all SQL syntax only (no Docker needed)"
    echo "  {tier_list}     Run specific tier tests"
    echo "  menu            Interactive menu to select SQL files only"
    echo "  cases           Interactive: select SQL file, then test case"
    echo "  .               Run tests in current directory"
    echo "  help, -h, --help Show this help message"
    echo ""
    echo -e "${{YELLOW}}Options:${{NC}}"
    echo "  --kafka <servers>   Use external Kafka instead of testcontainers"
    echo "  --timeout <ms>      Timeout per query in milliseconds (default: 60000)"
    echo "  --output <format>   Output format: text, json, junit"
    echo "  -q, --query <name>  Run only a specific test case by name"
    echo ""
    echo -e "${{YELLOW}}From a subdirectory:${{NC}}"
    echo "  ../velo-test.sh       Interactive: select SQL file → test case"
    echo "  ../velo-test.sh .     Run current directory as a test tier"
    echo "  ../velo-test.sh validate .  Validate SQL in current directory"
}}

# Check for help flag first (before MODE parsing)
case "${{1:-}}" in
    -h|--help|help|\?|'?')
        show_help
        exit 0
        ;;
esac

# Parse command line arguments
MODE="${{1:-cases}}"
shift || true

# Check if the next argument is a directory path (not an option)
if [[ $# -gt 0 && "${{1:0:1}}" != "-" ]]; then
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
            echo -e "${{RED}}Unknown option: $1${{NC}}"
            exit 1
            ;;
    esac
done

# Build query filter option
QUERY_OPTS=""
if [[ -n "$QUERY_FILTER" ]]; then
    QUERY_OPTS="--query $QUERY_FILTER"
    echo -e "${{BLUE}}Running only query: $QUERY_FILTER${{NC}}"
fi

# Build Kafka options
KAFKA_OPTS=""
if [[ -n "$KAFKA_SERVERS" ]]; then
    KAFKA_OPTS="--kafka $KAFKA_SERVERS"
    echo -e "${{BLUE}}Using external Kafka: $KAFKA_SERVERS${{NC}}"
else
    # Check if Docker is available (needed for testcontainers)
    if [[ "$MODE" != "validate" ]]; then
        if ! command -v docker &> /dev/null; then
            echo -e "${{RED}}Error: Docker not found${{NC}}"
            echo "Testcontainers requires Docker to auto-start Kafka."
            echo "Either install Docker or use --kafka <servers> to specify external Kafka."
            exit 1
        fi
        if ! docker info &> /dev/null 2>&1; then
            echo -e "${{RED}}Error: Docker is not running${{NC}}"
            echo "Please start Docker Desktop or the Docker daemon."
            echo "Alternatively, use --kafka <servers> to specify external Kafka."
            exit 1
        fi
        echo -e "${{BLUE}}Docker detected - will use testcontainers for Kafka${{NC}}"
        KAFKA_OPTS="--use-testcontainers"
    fi
fi

echo -e "${{BLUE}}═══════════════════════════════════════════════════════════════${{NC}}"
echo -e "${{BLUE}}  {project_name} - TEST RUNNER${{NC}}"
echo -e "${{BLUE}}═══════════════════════════════════════════════════════════════${{NC}}"
echo ""

# Function to get tier directory from tier name
get_tier_dir() {{
    local tier_name="$1"
    case "$tier_name" in
{tier_case_entries}
        *) echo "" ;;
    esac
}}

# Function to run tests for a single tier
run_tier() {{
    local tier_name="$1"
    local tier_dir="$2"

    if [[ ! -d "$tier_dir" ]]; then
        echo -e "${{YELLOW}}Skipping $tier_name: directory not found${{NC}}"
        return 0
    fi

    echo -e "${{CYAN}}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${{NC}}"
    echo -e "${{CYAN}}  Running: $tier_name${{NC}}"
    echo -e "${{CYAN}}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${{NC}}"

    # Find SQL files in the tier
    local sql_files=$(find "$tier_dir" -name "*.sql" -type f 2>/dev/null | head -1)
    local spec_file="$tier_dir/test_spec.yaml"
    local schemas_dir="$tier_dir/schemas"

    # Fall back to shared schemas if tier doesn't have its own
    if [[ ! -d "$schemas_dir" ]]; then
        schemas_dir="schemas"
    fi

    if [[ -z "$sql_files" ]]; then
        echo -e "${{YELLOW}}  No SQL files found in $tier_dir${{NC}}"
        return 0
    fi

    if [[ ! -f "$spec_file" ]]; then
        echo -e "${{YELLOW}}  No test_spec.yaml found in $tier_dir${{NC}}"
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
        sql_file=$(find "$tier_dir" -name "${{app_name}}.sql" -type f 2>/dev/null | head -1)
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
            ${{QUERY_OPTS}} \
            ${{KAFKA_OPTS}}
    fi
}}

# Function to show interactive menu and select SQL file
show_menu() {{
    local search_dir="$1"

    # Find all SQL files
    local sql_files=()
    while IFS= read -r -d '' file; do
        sql_files+=("$file")
    done < <(find "$search_dir" -name "*.sql" -type f -print0 | sort -z)

    if [[ ${{#sql_files[@]}} -eq 0 ]]; then
        echo -e "${{RED}}No SQL files found in $search_dir${{NC}}"
        return 1
    fi

    echo -e "${{YELLOW}}Select a SQL file to run:${{NC}}"
    echo ""

    local i=1
    for sql_file in "${{sql_files[@]}}"; do
        # Get relative path for cleaner display
        local rel_path="${{sql_file#$search_dir/}}"
        echo -e "  ${{CYAN}}$i)${{NC}} $rel_path"
        ((i++))
    done

    echo ""
    echo -e "  ${{CYAN}}0)${{NC}} Exit"
    echo ""

    while true; do
        echo -n -e "${{GREEN}}Enter selection [1-${{#sql_files[@]}}]: ${{NC}}"
        read -r selection

        if [[ "$selection" == "0" || "$selection" == "q" || "$selection" == "Q" ]]; then
            echo "Exiting."
            return 0
        fi

        if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${{#sql_files[@]}} ]]; then
            local selected_file="${{sql_files[$((selection-1))]}}"
            echo ""
            echo -e "${{CYAN}}Running: $(basename "$selected_file")${{NC}}"
            "$VELO_TEST" run "$selected_file" \
                --timeout-ms "$TIMEOUT_MS" \
                --output "$OUTPUT_FORMAT" \
                ${{QUERY_OPTS}} \
                ${{KAFKA_OPTS}}
            return $?
        else
            echo -e "${{RED}}Invalid selection. Please enter a number between 1 and ${{#sql_files[@]}}.${{NC}}"
        fi
    done
}}

# Main execution
case "$MODE" in
    validate)
        if [[ -n "$TARGET_DIR" ]]; then
            # Validate specific directory
            if [[ "$TARGET_DIR" == "." ]]; then
                tier_dir="$ORIGINAL_DIR"
                tier_name="$(basename "$ORIGINAL_DIR")"
            else
                tier_dir="$TARGET_DIR"
                tier_name="$(basename "$TARGET_DIR")"
            fi
            echo -e "${{YELLOW}}Validating SQL in: $tier_name${{NC}}"
            echo ""
            cd "$ORIGINAL_DIR"
            run_tier "$tier_name" "$tier_dir"
        else
            # Validate all tiers
            echo -e "${{YELLOW}}Validating all SQL files...${{NC}}"
            echo ""
            for tier_name in {tier_list}; do
                tier_dir=$(get_tier_dir "$tier_name")
                run_tier "$tier_name" "$tier_dir"
            done
        fi
        echo ""
        echo -e "${{GREEN}}✅ All SQL validation complete${{NC}}"
        ;;

    .)
        # Run current directory
        tier_dir="$ORIGINAL_DIR"
        tier_name="$(basename "$ORIGINAL_DIR")"
        echo -e "${{YELLOW}}Running tests in current directory: $tier_name${{NC}}"
        echo ""
        cd "$ORIGINAL_DIR"
        run_tier "$tier_name" "."
        ;;

    {tier_case_match})
        # Run specific tier
        tier_dir=$(get_tier_dir "$MODE")
        if [[ -z "$tier_dir" ]]; then
            echo -e "${{RED}}Unknown tier: $MODE${{NC}}"
            exit 1
        fi
        run_tier "$MODE" "$tier_dir"
        ;;

    menu|select)
        # Interactive menu mode
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
        # Interactive test case menu
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

    run|*)
        echo -e "${{YELLOW}}Running all test tiers...${{NC}}"
        echo ""

        FAILED=0
        for tier_name in {tier_list}; do
            tier_dir=$(get_tier_dir "$tier_name")
            if ! run_tier "$tier_name" "$tier_dir"; then
                FAILED=1
                echo -e "${{RED}}  ❌ $tier_name failed${{NC}}"
            else
                echo -e "${{GREEN}}  ✅ $tier_name passed${{NC}}"
            fi
            echo ""
        done

        if [[ $FAILED -eq 0 ]]; then
            echo -e "${{GREEN}}═══════════════════════════════════════════════════════════════${{NC}}"
            echo -e "${{GREEN}}  ALL TESTS PASSED${{NC}}"
            echo -e "${{GREEN}}═══════════════════════════════════════════════════════════════${{NC}}"
        else
            echo -e "${{RED}}═══════════════════════════════════════════════════════════════${{NC}}"
            echo -e "${{RED}}  SOME TESTS FAILED${{NC}}"
            echo -e "${{RED}}═══════════════════════════════════════════════════════════════${{NC}}"
            exit 1
        fi
        ;;
esac
"##,
            project_name = project_name,
            velo_test_path = velo_test_path,
            tier_list = tier_list,
            tier_case_entries = tier_case_entries,
            tier_case_match = tier_case_match,
        ))
    }

    /// Generate minimal script for single SQL file
    fn generate_minimal(
        &self,
        project_name: &str,
        structure: &DetectedStructure,
    ) -> Result<String, String> {
        let velo_test_path = &self.config.velo_test_path;

        let sql_file = structure.sql_files.first().ok_or("No SQL files found")?;

        let sql_filename = sql_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "app.sql".to_string());

        let spec_file = structure
            .spec_files
            .first()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
            .unwrap_or_else(|| "test_spec.yaml".to_string());

        let has_schemas = !structure.schema_dirs.is_empty();

        Ok(format!(
            r##"#!/bin/bash
# =============================================================================
# {project_name} - velo-test Runner
# =============================================================================
# Minimal runner for {sql_filename}
#
# Usage:
#   ./velo-test.sh                 # Run tests
#   ./velo-test.sh validate        # Validate SQL syntax only
#   ./velo-test.sh --step          # Step through queries
#   ./velo-test.sh --debug         # Interactive debug mode
#
# Options:
#   --step              Step through queries one at a time
#   --debug             Start interactive debugger
#   --kafka <servers>   Use external Kafka instead of testcontainers
#   --timeout <ms>      Timeout in milliseconds (default: 60000)
#   -v, --verbose       Enable verbose output
#
# Generated by: velo-test scaffold
# =============================================================================

set -e

export VELOSTREAM_TEST_CONTAINER="${{VELOSTREAM_TEST_CONTAINER:-redpanda}}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
cd "$SCRIPT_DIR"

VELO_TEST="${{VELO_TEST:-{velo_test_path}}}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${{RED}}Error: velo-test not found${{NC}}"
    echo "Build it with: cargo build --release"
    exit 1
fi

if [[ -f "$VELO_TEST" ]]; then
    VELO_TEST="$(cd "$(dirname "$VELO_TEST")" && pwd)/$(basename "$VELO_TEST")"
fi

SQL_FILE="{sql_filename}"
SPEC_FILE="{spec_file}"
TIMEOUT_MS=60000
KAFKA_OPTS=""
EXTRA_OPTS=""
DEBUG_MODE=""
VALIDATE_ONLY=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Usage: ./velo-test.sh [options]"
            echo ""
            echo "Options:"
            echo "  validate          Validate SQL only"
            echo "  --step            Step through queries"
            echo "  --debug           Interactive debugger"
            echo "  --kafka <servers> External Kafka"
            echo "  --timeout <ms>    Timeout (default: 60000)"
            echo "  -v, --verbose     Verbose output"
            exit 0
            ;;
        validate)
            VALIDATE_ONLY="true"
            shift
            ;;
        --step)
            EXTRA_OPTS="$EXTRA_OPTS --step"
            shift
            ;;
        --debug)
            DEBUG_MODE="true"
            shift
            ;;
        --kafka)
            KAFKA_OPTS="--kafka $2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_MS="$2"
            shift 2
            ;;
        -v|--verbose)
            EXTRA_OPTS="$EXTRA_OPTS --verbose"
            shift
            ;;
        *)
            shift
            ;;
    esac
done

if [[ -z "$KAFKA_OPTS" && -z "$VALIDATE_ONLY" ]]; then
    if ! docker info &> /dev/null 2>&1; then
        echo -e "${{RED}}Docker is not running. Start Docker or use --kafka <servers>${{NC}}"
        exit 1
    fi
    KAFKA_OPTS="--use-testcontainers"
fi

echo -e "${{CYAN}}═══════════════════════════════════════${{NC}}"
echo -e "${{CYAN}}  {project_name}${{NC}}"
echo -e "${{CYAN}}═══════════════════════════════════════${{NC}}"
echo ""

if [[ -n "$VALIDATE_ONLY" ]]; then
    echo -e "${{YELLOW}}Validating: $SQL_FILE${{NC}}"
    $VELO_TEST validate "$SQL_FILE"
    echo -e "${{GREEN}}✅ Validation passed${{NC}}"
elif [[ -n "$DEBUG_MODE" ]]; then
    echo -e "${{YELLOW}}Starting debugger for: $SQL_FILE${{NC}}"
    RUST_LOG=info $VELO_TEST debug "$SQL_FILE" \
        --timeout-ms "$TIMEOUT_MS" \
        $KAFKA_OPTS \
        $EXTRA_OPTS
else
    echo -e "${{YELLOW}}Running: $SQL_FILE${{NC}}"

    cmd="$VELO_TEST run $SQL_FILE --timeout-ms $TIMEOUT_MS"

    if [[ -f "$SPEC_FILE" ]]; then
        cmd="$cmd --spec $SPEC_FILE"
    fi

    {schemas_opt}

    cmd="$cmd $KAFKA_OPTS $EXTRA_OPTS"

    echo -e "${{CYAN}}$cmd${{NC}}"
    echo ""

    RUST_LOG=info $cmd
fi
"##,
            project_name = project_name,
            velo_test_path = velo_test_path,
            sql_filename = sql_filename,
            spec_file = spec_file,
            schemas_opt = if has_schemas {
                r#"if [[ -d "schemas" ]]; then
        cmd="$cmd --schemas schemas"
    fi"#
            } else {
                ""
            },
        ))
    }
}

impl Default for Scaffolder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scaffold_style_from_str() {
        assert_eq!(
            "simple".parse::<ScaffoldStyle>().unwrap(),
            ScaffoldStyle::Simple
        );
        assert_eq!(
            "trading".parse::<ScaffoldStyle>().unwrap(),
            ScaffoldStyle::Simple
        );
        assert_eq!(
            "tiered".parse::<ScaffoldStyle>().unwrap(),
            ScaffoldStyle::Tiered
        );
        assert_eq!(
            "minimal".parse::<ScaffoldStyle>().unwrap(),
            ScaffoldStyle::Minimal
        );
    }

    #[test]
    fn test_scaffold_style_display() {
        assert_eq!(ScaffoldStyle::Simple.to_string(), "simple");
        assert_eq!(ScaffoldStyle::Tiered.to_string(), "tiered");
        assert_eq!(ScaffoldStyle::Minimal.to_string(), "minimal");
    }
}
