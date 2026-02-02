#!/bin/bash
#
# Regenerate all trading demo artifacts from SQL @metric annotations.
#
# Usage:
#   ./velo-dashboard-generate.sh [--build]
#
# This script produces all generated artifacts in deploy/:
#
#   deploy/
#   ├── apps/                        # Annotated SQL (deployed by start-demo.sh)
#   │   ├── app_compliance.sql
#   │   └── ...
#   ├── configs -> ../configs        # Symlink (so ../configs/ paths in SQL resolve)
#   ├── schemas -> ../schemas        # Symlink (so ../schemas/ paths in SQL resolve)
#   └── monitoring/                  # Generated monitoring configs
#       ├── prometheus.yml           # Combined scrape config for all apps
#       └── grafana/
#           └── dashboards/          # Per-app Grafana dashboards
#               ├── app_compliance-dashboard.json
#               └── ...
#
# Hand-curated configs in monitoring/ are NOT touched.
# deploy/ is .gitignored and cleaned at the start of each run.
#
# Port assignment matches start-demo.sh: apps are iterated in glob order
# (alphabetical) starting from METRICS_BASE_PORT.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
APPS_DIR="$SCRIPT_DIR/apps"
DEPLOY_DIR="$SCRIPT_DIR/deploy"
DEPLOY_MONITORING_DIR="$DEPLOY_DIR/monitoring"
DEPLOY_APPS_DIR="$DEPLOY_DIR/apps"
VELO_TEST="$PROJECT_ROOT/target/release/velo-test"

METRICS_BASE_PORT=9101

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Build if requested or binary missing
if [[ "${1:-}" == "--build" ]] || [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${YELLOW}Building velo-test (release)...${NC}"
    (cd "$PROJECT_ROOT" && cargo build --release --bin velo-test)
fi

if [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${RED}Error: velo-test binary not found at $VELO_TEST${NC}"
    echo "Run with --build or build manually: cargo build --release --bin velo-test"
    exit 1
fi

echo -e "${BLUE}Regenerating trading demo dashboards${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Clean deploy/ directory (remove stale artifacts)
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_APPS_DIR"
mkdir -p "$DEPLOY_MONITORING_DIR/grafana/dashboards"

# Symlink configs/ so relative paths in SQL files (../configs/) resolve correctly
ln -s "$SCRIPT_DIR/configs" "$DEPLOY_DIR/configs"
# Symlink schemas/ for the same reason
ln -s "$SCRIPT_DIR/schemas" "$DEPLOY_DIR/schemas"

# Clean up any leftover annotated SQL files from previous runs
rm -f "$APPS_DIR"/*.annotated.sql 2>/dev/null || true

# Generate per-app dashboards
METRICS_PORT=$METRICS_BASE_PORT
APP_COUNT=0

for app in "$APPS_DIR"/app_*.sql; do
    app_name=$(basename "$app" .sql)
    echo -e "${GREEN}[$app_name]${NC} telemetry port $METRICS_PORT"

    "$VELO_TEST" annotate \
        "$app" \
        --monitoring "$DEPLOY_MONITORING_DIR" \
        --telemetry-port "$METRICS_PORT" \
        -y 2>&1 | grep -E "^(✅|📊|⚠)" || true

    # Move annotated SQL to deploy/apps/ (what the server should deploy)
    if [ -f "$APPS_DIR/${app_name}.annotated.sql" ]; then
        mv "$APPS_DIR/${app_name}.annotated.sql" "$DEPLOY_APPS_DIR/${app_name}.sql"
    else
        # Fallback: copy raw SQL if annotate didn't produce .annotated.sql
        cp "$app" "$DEPLOY_APPS_DIR/${app_name}.sql"
    fi

    METRICS_PORT=$((METRICS_PORT + 1))
    APP_COUNT=$((APP_COUNT + 1))
    echo ""
done

# Clean extra dirs that velo-test annotate writes (we use our own curated copies)
rm -rf "$DEPLOY_MONITORING_DIR/grafana/provisioning" 2>/dev/null || true
rm -rf "$DEPLOY_MONITORING_DIR/tempo" 2>/dev/null || true
rm -f "$DEPLOY_MONITORING_DIR/grafana/dashboards/dashboard.yml" 2>/dev/null || true

# Build combined prometheus.yml with all telemetry targets
echo -e "${YELLOW}Writing combined prometheus.yml...${NC}"

TELEMETRY_TARGETS=""
METRICS_PORT=$METRICS_BASE_PORT
for app in "$APPS_DIR"/app_*.sql; do
    app_name=$(basename "$app" .sql)
    TELEMETRY_TARGETS="${TELEMETRY_TARGETS}        - 'host.docker.internal:${METRICS_PORT}'  # ${app_name}
"
    METRICS_PORT=$((METRICS_PORT + 1))
done

PROMETHEUS_YML="global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - \"first_rules.yml\"
  # - \"second_rules.yml\"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']
    metrics_path: /metrics
    scrape_interval: 10s

  - job_name: 'velo-sql'
    static_configs:
      - targets: ['host.docker.internal:8080']
    metrics_path: /metrics
    scrape_interval: 10s
    scrape_timeout: 5s

  - job_name: 'velo-sql-telemetry'
    static_configs:
      - targets:
${TELEMETRY_TARGETS}    metrics_path: /metrics
    scrape_interval: 5s
    scrape_timeout: 3s

  - job_name: 'node-exporter'
    static_configs:
      - targets: ['host.docker.internal:9100']
    scrape_interval: 10s
"

echo "$PROMETHEUS_YML" > "$DEPLOY_MONITORING_DIR/prometheus.yml"

echo ""
echo -e "${GREEN}Done!${NC} Generated dashboards for $APP_COUNT apps."
echo ""
echo "Generated files:"
echo "  Annotated SQL: deploy/apps/*.sql"
echo "  Dashboards:    deploy/monitoring/grafana/dashboards/app_*-dashboard.json"
echo "  Prometheus:    deploy/monitoring/prometheus.yml"
echo ""
echo "Restart Grafana to pick up changes:"
echo "  docker restart trading-grafana-1"
