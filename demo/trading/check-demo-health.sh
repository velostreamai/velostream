#!/bin/bash
# Health Check Script for Trading Demo
# Validates all components are running and processing data

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

KAFKA_BROKER="localhost:9092"
HEALTH_PASS=true

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Trading Demo Health Check${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check 1: Docker containers
echo -e "${BLUE}1. Docker Containers${NC}"
CONTAINERS=(simple-kafka simple-zookeeper)
for container in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        STATUS=$(docker inspect -f '{{.State.Status}}' $container)
        if [ "$STATUS" == "running" ]; then
            echo -e "  ${GREEN}✓${NC} $container: running"
        else
            echo -e "  ${RED}✗${NC} $container: $STATUS"
            HEALTH_PASS=false
        fi
    else
        echo -e "  ${RED}✗${NC} $container: not found"
        HEALTH_PASS=false
    fi
done
echo ""

# Check 2: Kafka broker connectivity
echo -e "${BLUE}2. Kafka Broker${NC}"
if docker exec simple-kafka kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} Kafka broker is responsive"
else
    echo -e "  ${RED}✗${NC} Kafka broker is not responsive"
    HEALTH_PASS=false
fi
echo ""

# Check 3: Required topics
echo -e "${BLUE}3. Required Topics${NC}"
REQUIRED_TOPICS=(market_data_stream market_data_ts market_data_stream_a market_data_stream_b trading_positions_stream order_book_stream)
EXISTING_TOPICS=$(docker exec simple-kafka kafka-topics --list --bootstrap-server $KAFKA_BROKER 2>/dev/null)

for topic in "${REQUIRED_TOPICS[@]}"; do
    if echo "$EXISTING_TOPICS" | grep -q "^${topic}$"; then
        # Check message count
        MSG_COUNT=$(docker exec simple-kafka kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list $KAFKA_BROKER \
            --topic "$topic" \
            --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

        if [ "$MSG_COUNT" -gt 0 ]; then
            echo -e "  ${GREEN}✓${NC} $topic: exists ($MSG_COUNT messages)"
        else
            echo -e "  ${YELLOW}⚠${NC} $topic: exists but empty"
        fi
    else
        echo -e "  ${RED}✗${NC} $topic: missing"
        HEALTH_PASS=false
    fi
done
echo ""

# Check 4: Data generator (velo-test stress)
echo -e "${BLUE}4. Data Generator (velo-test)${NC}"
if pgrep -f "velo-test.*stress" > /dev/null; then
    PID=$(pgrep -f "velo-test.*stress")
    echo -e "  ${GREEN}✓${NC} Data generator running (PID: $PID)"

    # Check if generator log exists and has recent activity
    if [ -f "/tmp/velo_stress.log" ]; then
        LAST_LINE=$(tail -1 /tmp/velo_stress.log)
        echo -e "  ${BLUE}ℹ${NC} Last log: $LAST_LINE"
    fi
else
    echo -e "  ${RED}✗${NC} Data generator not running"
    HEALTH_PASS=false
fi
echo ""

# Check 5: SQL deployment
echo -e "${BLUE}5. SQL Application${NC}"
if pgrep -f "velo-sql.*deploy-app" > /dev/null; then
    PID=$(pgrep -f "velo-sql.*deploy-app")
    echo -e "  ${GREEN}✓${NC} SQL application running (PID: $PID)"

    # Check deployment log for job count
    if [ -f "/tmp/velo_deployment.log" ]; then
        JOB_COUNT=$(grep -c "Successfully deployed job" /tmp/velo_deployment.log 2>/dev/null || echo "0")
        ACTIVE_JOBS=$(grep "Active jobs:" /tmp/velo_deployment.log 2>/dev/null | tail -1)

        echo -e "  ${BLUE}ℹ${NC} Deployed jobs: $JOB_COUNT"
        if [ -n "$ACTIVE_JOBS" ]; then
            echo -e "  ${BLUE}ℹ${NC} $ACTIVE_JOBS"
        fi

        # Check for record processing
        RECORDS_LINE=$(grep "records processed" /tmp/velo_deployment.log 2>/dev/null | tail -1)
        if [ -n "$RECORDS_LINE" ]; then
            echo -e "  ${BLUE}ℹ${NC} Latest: $RECORDS_LINE"
        fi
    fi
else
    echo -e "  ${RED}✗${NC} SQL application not running"
    HEALTH_PASS=false
fi
echo ""

# Check 6: Consumer groups
echo -e "${BLUE}6. Consumer Groups${NC}"
CONSUMER_GROUPS=$(docker exec simple-kafka kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --list 2>/dev/null | grep "velo-sql" || true)

if [ -n "$CONSUMER_GROUPS" ]; then
    GROUP_COUNT=$(echo "$CONSUMER_GROUPS" | wc -l | tr -d ' ')
    echo -e "  ${GREEN}✓${NC} Active consumer groups: $GROUP_COUNT"

    # Show lag for first group
    FIRST_GROUP=$(echo "$CONSUMER_GROUPS" | head -1)
    echo -e "  ${BLUE}ℹ${NC} Checking lag for: $FIRST_GROUP"
    docker exec simple-kafka kafka-consumer-groups \
        --bootstrap-server $KAFKA_BROKER \
        --group "$FIRST_GROUP" \
        --describe 2>/dev/null | head -5 || true
else
    echo -e "  ${YELLOW}⚠${NC} No active consumer groups found"
fi
echo ""

# Final status
echo -e "${BLUE}========================================${NC}"
if [ "$HEALTH_PASS" = true ]; then
    echo -e "${GREEN}Overall Health: HEALTHY ✓${NC}"
    exit 0
else
    echo -e "${RED}Overall Health: UNHEALTHY ✗${NC}"
    echo -e "${YELLOW}Check logs for details:${NC}"
    echo -e "  tail -f /tmp/velo_deployment.log"
    echo -e "  tail -f /tmp/velo_stress.log"
    exit 1
fi
