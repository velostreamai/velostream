# 🛠️ FerrisStreams CLI Usage Guide

The FerrisStreams CLI provides comprehensive monitoring and management for any FerrisStreams deployment, including stream processing, Kafka clusters, and SQL jobs.

## 🚀 Quick Start

```bash
# Build the CLI tool (creates ./ferris-cli symlink)
./build_cli.sh

# Quick health check of local components
./ferris-cli health

# Connect to remote FerrisStreams server
./ferris-cli --remote --sql-host production.example.com --sql-port 8080 health
```

## 📋 Available Commands

### **Connection Options**
All commands support these global options:
```bash
--sql-host <HOST>          # FerrisStreams SQL server host (default: localhost)
--sql-port <PORT>          # SQL server port (default: 8080)
--kafka-brokers <BROKERS>  # Kafka broker addresses (default: localhost:9092)
--remote                   # Remote mode - skip Docker/process checks
```

### **Health Check**
```bash
# Local deployment
./ferris-cli health

# Remote deployment
./ferris-cli --remote --sql-host prod.example.com health
```
Quick overview of all component health status.

### **System Status**
```bash
# Local deployment - basic status
./ferris-cli status

# Local deployment - detailed verbose output
./ferris-cli status --verbose

# Remote deployment - real-time monitoring
./ferris-cli --remote --sql-host prod.example.com status --refresh 5

# Custom Kafka brokers
./ferris-cli --kafka-brokers kafka1:9092,kafka2:9092 status
```

### **Kafka Monitoring**
```bash
# Basic Kafka info
./ferris-cli kafka

# Show topics
./ferris-cli kafka --topics

# Show consumer groups
./ferris-cli kafka --groups

# Show both topics and groups
./ferris-cli kafka --topics --groups

# Custom broker address
./ferris-cli kafka --brokers localhost:9092 --topics
```

### **Docker Containers**
```bash
# All containers
./ferris-cli docker

# Only FerrisStreams-related containers
./ferris-cli docker --ferris-only
```

### **Process Monitoring**
```bash
# FerrisStreams processes only
./ferris-cli processes

# All processes
./ferris-cli processes --all
```

### **SQL Server Info**
```bash
# Basic SQL server info
./ferris-cli sql

# Show job details
./ferris-cli sql --jobs

# Custom port
./ferris-cli sql --port 8080 --jobs
```

### **Job and Task Monitoring**
```bash
# Show all jobs and tasks (default)
./ferris-cli jobs

# Show only SQL processing details
./ferris-cli jobs --sql

# Show only data generators and producers
./ferris-cli jobs --generators

# Show only topic activity and message counts
./ferris-cli jobs --topics

# Combine options
./ferris-cli jobs --sql --topics
```

### **Demo Management**
```bash
# Start demo (guidance only)
./ferris-cli start --duration 10

# Stop demo (guidance only)
./ferris-cli stop
```

## 🎯 Common Usage Patterns

### **Local Development**
```bash
./build_cli.sh
./ferris-cli health
./ferris-cli status --verbose
```

### **Production Monitoring**
```bash
# Monitor remote production server
./ferris-cli --remote --sql-host prod-ferris.company.com --sql-port 8080 status --refresh 10

# Check production jobs and performance
./ferris-cli --remote --sql-host prod-ferris.company.com jobs --sql --topics

# Monitor with custom Kafka cluster
./ferris-cli --remote --kafka-brokers kafka-prod1:9092,kafka-prod2:9092 kafka --topics
```

### **Multi-Environment Monitoring**
```bash
# Check staging environment
./ferris-cli --remote --sql-host staging-ferris.company.com health

# Check production environment
./ferris-cli --remote --sql-host prod-ferris.company.com health

# Compare environments
./ferris-cli --remote --sql-host staging-ferris.company.com jobs > staging-jobs.txt
./ferris-cli --remote --sql-host prod-ferris.company.com jobs > prod-jobs.txt
```

### **Troubleshoot Issues**
```bash
# Check detailed status
./ferris-cli status --verbose

# Check Kafka specifically
./ferris-cli kafka --topics --groups

# Check Docker containers
./ferris-cli docker --ferris-only

# Check running processes
./ferris-cli processes
```

### **Jobs and Task Monitoring**
```bash
# Check all jobs and tasks
./ferris-cli jobs

# Check only SQL processing
./ferris-cli jobs --sql

# Check data producers/generators
./ferris-cli jobs --generators

# Check topic activity and message counts
./ferris-cli jobs --topics
```

### **Data Flow Verification**
```bash
# Check if topics exist and have data
./ferris-cli kafka --topics

# Monitor Kafka cluster health
./ferris-cli kafka --brokers localhost:9092
```

## 📊 Understanding Output

### **Health Check Colors**
- 🟢 **Green (✅)**: Component healthy
- 🔴 **Red (❌)**: Component has issues
- 🟡 **Yellow (⚠️)**: Some components need attention

### **Status Information**
- **Docker Containers**: Shows running containers with status
- **Kafka Cluster**: Connection status, topic count, and container info
- **SQL Server**: Process status and PIDs
- **SQL Jobs & Tasks**: Active job detection and Kafka topic monitoring
- **FerrisStreams Processes**: Active/idle status

### **Real-Time Monitoring**
- Updates every N seconds (configurable with `--refresh`)
- Clears screen between updates
- Press `Ctrl+C` to stop monitoring

## 🔧 Troubleshooting

### **CLI Not Found**
```bash
# If ./ferris-cli doesn't exist, rebuild it
./build_cli.sh
```

### **Permission Denied**
```bash
# Make sure the script is executable
chmod +x ./build_cli.sh
./build_cli.sh
```

### **Build Failures**
```bash
# Check if you're in the right directory
pwd  # Should be in demo/trading

# Ensure Rust toolchain is available
rustc --version
cargo --version
```

### **Kafka Connection Issues**
```bash
# Check if Kafka is running
./ferris-cli docker --ferris-only

# Check Kafka directly
./ferris-cli kafka --topics
```

## 🎛️ Configuration

The CLI automatically detects standard configurations:
- **Kafka**: localhost:9092
- **SQL Server**: localhost:8080
- **Docker**: Default socket

Override with command-line options:
```bash
./ferris-cli kafka --brokers custom-host:9092
./ferris-cli sql --port 8081
```

## 🚀 Advanced Usage

### **Scripting with CLI**
```bash
# Check if system is healthy before starting demo  
if ./ferris-cli health | grep -q "All systems healthy"; then
    echo "System ready - starting demo"
    ./run_demo.sh
else
    echo "System not ready - check status"
    ./ferris-cli status --verbose
fi
```

### **Log Integration**
```bash
# Monitor while logging output
./ferris-cli status --refresh 10 | tee monitoring.log
```

## 📚 Related Tools

- **Grafana Dashboards**: http://localhost:3000 (visual monitoring)
- **Kafka UI**: http://localhost:8090 (topic browser)
- **Prometheus**: http://localhost:9090 (metrics collection)
- **Python Dashboard**: `python3 dashboard.py` (trading analytics)

---

💡 **Pro Tip**: Use `./ferris-cli --help` or `./ferris-cli <command> --help` for detailed command information!