# 🛠️ Velostream CLI Usage Guide

The Velostream CLI provides comprehensive monitoring and management for any Velostream deployment, including stream processing, Kafka clusters, and SQL jobs.

## 🚀 Quick Start

```bash
# Build the CLI tool (creates ./velo-cli symlink)
./build_cli.sh

# Quick health check of local components
./velo-cli health

# Connect to remote Velostream server
./velo-cli --remote --sql-host production.example.com --sql-port 8080 health
```

## 📋 Available Commands

### **Connection Options**
All commands support these global options:
```bash
--sql-host <HOST>          # Velostream SQL server host (default: localhost)
--sql-port <PORT>          # SQL server port (default: 8080)
--kafka-brokers <BROKERS>  # Kafka broker addresses (default: localhost:9092)
--remote                   # Remote mode - skip Docker/process checks
```

### **Health Check**
```bash
# Local deployment
./velo-cli health

# Remote deployment
./velo-cli --remote --sql-host prod.example.com health
```
Quick overview of all component health status.

### **System Status**
```bash
# Local deployment - basic status
./velo-cli status

# Local deployment - detailed verbose output
./velo-cli status --verbose

# Remote deployment - real-time monitoring
./velo-cli --remote --sql-host prod.example.com status --refresh 5

# Custom Kafka brokers
./velo-cli --kafka-brokers kafka1:9092,kafka2:9092 status
```

### **Kafka Monitoring**
```bash
# Basic Kafka info
./velo-cli kafka

# Show topics
./velo-cli kafka --topics

# Show consumer groups
./velo-cli kafka --groups

# Show both topics and groups
./velo-cli kafka --topics --groups

# Custom broker address
./velo-cli kafka --brokers localhost:9092 --topics
```

### **Docker Containers**
```bash
# All containers
./velo-cli docker

# Only Velostream-related containers
./velo-cli docker --velo-only
```

### **Process Monitoring**
```bash
# Velostream processes only
./velo-cli processes

# All processes
./velo-cli processes --all
```

### **SQL Server Info**
```bash
# Basic SQL server info
./velo-cli sql

# Show job details
./velo-cli sql --jobs

# Custom port
./velo-cli sql --port 8080 --jobs
```

### **Job and Task Monitoring**
```bash
# Show all jobs and tasks (default)
./velo-cli jobs

# Show only SQL processing details
./velo-cli jobs --sql

# Show only data generators and producers
./velo-cli jobs --generators

# Show only topic activity and message counts
./velo-cli jobs --topics

# Combine options
./velo-cli jobs --sql --topics
```

### **Demo Management**
```bash
# Start demo (guidance only)
./velo-cli start --duration 10

# Stop demo (guidance only)
./velo-cli stop
```

## 🎯 Common Usage Patterns

### **Local Development**
```bash
./build_cli.sh
./velo-cli health
./velo-cli status --verbose
```

### **Production Monitoring**
```bash
# Monitor remote production server
./velo-cli --remote --sql-host prod-velo.company.com --sql-port 8080 status --refresh 10

# Check production jobs and performance
./velo-cli --remote --sql-host prod-velo.company.com jobs --sql --topics

# Monitor with custom Kafka cluster
./velo-cli --remote --kafka-brokers kafka-prod1:9092,kafka-prod2:9092 kafka --topics
```

### **Multi-Environment Monitoring**
```bash
# Check staging environment
./velo-cli --remote --sql-host staging-velo.company.com health

# Check production environment
./velo-cli --remote --sql-host prod-velo.company.com health

# Compare environments
./velo-cli --remote --sql-host staging-velo.company.com jobs > staging-jobs.txt
./velo-cli --remote --sql-host prod-velo.company.com jobs > prod-jobs.txt
```

### **Troubleshoot Issues**
```bash
# Check detailed status
./velo-cli status --verbose

# Check Kafka specifically
./velo-cli kafka --topics --groups

# Check Docker containers
./velo-cli docker --velo-only

# Check running processes
./velo-cli processes
```

### **Jobs and Task Monitoring**
```bash
# Check all jobs and tasks
./velo-cli jobs

# Check only SQL processing
./velo-cli jobs --sql

# Check data producers/generators
./velo-cli jobs --generators

# Check topic activity and message counts
./velo-cli jobs --topics
```

### **Data Flow Verification**
```bash
# Check if topics exist and have data
./velo-cli kafka --topics

# Monitor Kafka cluster health
./velo-cli kafka --brokers localhost:9092
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
- **Velostream Processes**: Active/idle status

### **Real-Time Monitoring**
- Updates every N seconds (configurable with `--refresh`)
- Clears screen between updates
- Press `Ctrl+C` to stop monitoring

## 🔧 Troubleshooting

### **CLI Not Found**
```bash
# If ./velo-cli doesn't exist, rebuild it
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
./velo-cli docker --velo-only

# Check Kafka directly
./velo-cli kafka --topics
```

## 🎛️ Configuration

The CLI automatically detects standard configurations:
- **Kafka**: localhost:9092
- **SQL Server**: localhost:8080
- **Docker**: Default socket

Override with command-line options:
```bash
./velo-cli kafka --brokers custom-host:9092
./velo-cli sql --port 8081
```

## 🚀 Advanced Usage

### **Scripting with CLI**
```bash
# Check if system is healthy before starting demo
if ./velo-cli health | grep -q "All systems healthy"; then
    echo "System ready - starting demo"
    ./start-demo.sh
else
    echo "System not ready - check status"
    ./velo-cli status --verbose
fi
```

### **Log Integration**
```bash
# Monitor while logging output
./velo-cli status --refresh 10 | tee monitoring.log
```

## 📚 Related Tools

- **Grafana Dashboards**: http://localhost:3000 (visual monitoring)
- **Kafka UI**: http://localhost:8090 (topic browser)
- **Prometheus**: http://localhost:9090 (metrics collection)
- **Python Dashboard**: `python3 dashboard.py` (trading analytics)

---

💡 **Pro Tip**: Use `./velo-cli --help` or `./velo-cli <command> --help` for detailed command information!