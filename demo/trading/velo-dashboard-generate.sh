#!/bin/bash
#
# Regenerate all trading demo Grafana dashboards from SQL @metric annotations.
#
# Usage:
#   ./velo-dashboard-generate.sh [--build]
#
# This script uses `velo-test annotate` to generate dashboard JSON files
# for each SQL app, then writes a combined prometheus.yml with all
# telemetry targets.
#
# Port assignment matches start-demo.sh: apps are iterated in glob order
# (alphabetical) starting from METRICS_BASE_PORT.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
APPS_DIR="$SCRIPT_DIR/apps"
MONITORING_DIR="$SCRIPT_DIR/monitoring"
GRAFANA_DIR="$SCRIPT_DIR/grafana"
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
        --monitoring "$MONITORING_DIR" \
        --telemetry-port "$METRICS_PORT" \
        -y 2>&1 | grep -E "^(✅|📊|⚠)" || true

    # Clean up annotated SQL file (not needed)
    rm -f "$APPS_DIR/${app_name}.annotated.sql"

    METRICS_PORT=$((METRICS_PORT + 1))
    APP_COUNT=$((APP_COUNT + 1))
    echo ""
done

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

echo "$PROMETHEUS_YML" > "$MONITORING_DIR/prometheus.yml"
echo "$PROMETHEUS_YML" > "$GRAFANA_DIR/prometheus.yml"

echo ""
echo -e "${GREEN}Done!${NC} Generated dashboards for $APP_COUNT apps."
echo ""
echo "Generated files:"
echo "  Dashboards:    $MONITORING_DIR/grafana/dashboards/app_*-dashboard.json"
echo "  Prometheus:    $MONITORING_DIR/prometheus.yml"
echo "  Prometheus:    $GRAFANA_DIR/prometheus.yml"
echo ""
echo "Restart Grafana to pick up changes:"
echo "  docker restart trading-grafana-1"
