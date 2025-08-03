# FerrisStreams SQL Docker Deployment Guide

## 🚀 Quick Start

This guide covers deploying FerrisStreams SQL servers using Docker containers for production-ready streaming SQL processing.

## 📋 Prerequisites

- **Docker** 20.10+ and **Docker Compose** 2.0+
- **8GB RAM** minimum (recommended: 16GB)
- **20GB disk space** for containers and data volumes
- **Network ports**: 8080-8081, 9090-9093, 3000 (if using monitoring)

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    FerrisStreams SQL Infrastructure          │
├─────────────────────────────────────────────────────────────┤
│  🌐 Kafka UI (8090)     │  📊 Prometheus (9093)             │
│  📊 Grafana (3000)      │  ⚙️  SQL Single (8080)             │
│  ⚙️  SQL Multi (8081)    │  🔧 Data Producer                 │
├─────────────────────────────────────────────────────────────┤
│                    🚀 Kafka Broker (9092)                   │
└─────────────────────────────────────────────────────────────┘
```

## 🎯 Deployment Options

### Option 1: Core Infrastructure (Recommended)

Deploy Kafka + SQL servers for production workloads:

```bash
# Clone the repository
git clone <repository>
cd ferrisstreams

# Start core services
docker-compose up -d kafka kafka-ui ferris-sql-single ferris-sql-multi

# Verify services are running
docker-compose ps
```

### Option 2: Full Infrastructure with Monitoring

Deploy everything including Prometheus and Grafana:

```bash
# Start all services including monitoring
docker-compose --profile monitoring up -d

# Access monitoring interfaces
echo "Grafana: http://localhost:3000 (admin/ferris123)"
echo "Prometheus: http://localhost:9093"
```

### Option 3: Custom Deployment

Deploy specific services as needed:

```bash
# Just Kafka for development
docker-compose up -d kafka kafka-ui

# Add SQL servers when ready
docker-compose up -d ferris-sql-single
docker-compose up -d ferris-sql-multi
```

## 📦 Container Details

### FerrisStreams SQL Server (Single Job)

- **Container**: `ferris-sql-single`
- **Port**: 8080 (API), 9090 (Metrics)
- **Purpose**: Execute single SQL jobs with simple management
- **Use Case**: Development, testing, simple analytics

```bash
# Execute a SQL query
docker exec ferris-sql-single ferris-sql execute \
  --query "SELECT customer_id, amount FROM orders WHERE amount > 100" \
  --topic orders \
  --brokers kafka:9092
```

### FerrisStreams Multi-Job SQL Server

- **Container**: `ferris-sql-multi`
- **Port**: 8081 (API), 9091 (Metrics)
- **Purpose**: Manage multiple concurrent SQL jobs
- **Use Case**: Production, complex analytics, job orchestration

```bash
# Deploy a SQL application
docker exec -it ferris-sql-multi ferris-sql-multi deploy-app \
  --file /app/examples/ecommerce_analytics.sql \
  --brokers kafka:9092 \
  --default-topic orders
```

### Kafka Infrastructure

- **Container**: `ferris-kafka`
- **Port**: 9092 (Broker), 9093 (Controller)
- **Purpose**: Message streaming backbone
- **Configuration**: KRaft mode (no Zookeeper), auto-create topics

### Supporting Services

- **Kafka UI**: Web interface for Kafka management (port 8090)
- **Data Producer**: Helper container for generating test data
- **Prometheus**: Metrics collection (port 9093, optional)
- **Grafana**: Dashboard visualization (port 3000, optional)

## 🔧 Configuration

### Environment Variables

All services support these environment variables:

```bash
# Kafka connection
KAFKA_BROKERS=kafka:9092

# Logging
RUST_LOG=info  # trace, debug, info, warn, error

# SQL Server specific
SQL_MAX_JOBS=20
SQL_MEMORY_LIMIT_MB=1024
SQL_WORKER_THREADS=4
```

### Configuration Files

Mount custom configurations:

```bash
# Custom SQL config
docker run -v $(pwd)/my-sql-config.yaml:/app/sql-config.yaml ferris-sql-single

# Custom examples
docker run -v $(pwd)/my-queries:/app/examples ferris-sql-multi
```

## 🚀 Usage Examples

### 1. Real-Time Order Analytics

```bash
# Start infrastructure
docker-compose up -d

# Wait for services to be ready (30-60 seconds)
docker-compose ps

# Deploy analytics job
docker exec -it ferris-sql-multi ferris-sql-multi deploy-app \
  --file /app/examples/ecommerce_analytics.sql \
  --brokers kafka:9092 \
  --default-topic orders

# Monitor job status
docker logs ferris-sql-multi -f
```

### 2. IoT Sensor Monitoring

```bash
# Start services
docker-compose up -d

# Execute IoT monitoring query
docker exec ferris-sql-single ferris-sql execute \
  --query "
    SELECT 
      JSON_VALUE(payload, '$.device_id') as device_id,
      CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') as temp,
      CASE 
        WHEN CAST(JSON_VALUE(payload, '$.temperature'), 'FLOAT') > 75.0 THEN 'HIGH'
        ELSE 'NORMAL'
      END as alert_level
    FROM sensor_data 
    WHERE JSON_VALUE(payload, '$.sensor_type') = 'temperature'
  " \
  --topic iot_sensors \
  --brokers kafka:9092 \
  --limit 1000
```

### 3. Financial Trading Analytics

```bash
# Deploy financial analytics application
docker exec -it ferris-sql-multi ferris-sql-multi deploy-app \
  --file /app/examples/financial_trading.sql \
  --brokers kafka:9092 \
  --default-topic trades

# Monitor real-time results
docker logs ferris-sql-multi -f | grep "trade_analytics"
```

## 📊 Monitoring and Management

### Service Health Checks

```bash
# Check all services
docker-compose ps

# Check specific service health
docker inspect ferris-sql-single --format='{{.State.Health.Status}}'

# View service logs
docker-compose logs ferris-sql-single
docker-compose logs ferris-sql-multi
docker-compose logs kafka
```

### Kafka Management

```bash
# Access Kafka UI
open http://localhost:8090

# List topics via CLI
docker exec ferris-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create custom topic
docker exec ferris-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic my_custom_topic \
  --partitions 3 \
  --replication-factor 1
```

### SQL Job Management

```bash
# List running jobs (Multi-Job server)
docker exec ferris-sql-multi ferris-sql-multi server --help

# View job metrics through Prometheus (if enabled)
curl http://localhost:9093/metrics | grep ferris_sql

# Access Grafana dashboards (if enabled)
open http://localhost:3000  # admin/ferris123
```

## 🔍 Troubleshooting

### Common Issues

#### 1. Services Not Starting

```bash
# Check Docker daemon
docker info

# Check available resources
docker system df

# View detailed logs
docker-compose logs --tail=50 kafka
docker-compose logs --tail=50 ferris-sql-single
```

#### 2. Port Conflicts

```bash
# Check port usage
netstat -tulpn | grep -E ':(8080|8081|9092|9093)'

# Use different ports if needed
docker-compose -f docker-compose.yml up -d --scale ferris-sql-single=0
```

#### 3. Kafka Connection Issues

```bash
# Test Kafka connectivity
docker exec ferris-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Kafka logs
docker-compose logs kafka | tail -100

# Restart Kafka if needed
docker-compose restart kafka
```

#### 4. SQL Server Connection Issues

```bash
# Test SQL server directly
docker exec ferris-sql-single ferris-sql --help

# Check configuration
docker exec ferris-sql-single cat /app/sql-config.yaml

# Restart SQL servers
docker-compose restart ferris-sql-single ferris-sql-multi
```

### Resource Monitoring

```bash
# Monitor container resource usage
docker stats

# Check disk usage
docker system df

# Clean up if needed
docker system prune -a
```

## 🔒 Security Considerations

### Production Security

```bash
# Use custom networks
networks:
  ferris-network:
    driver: bridge
    internal: true  # Isolate from external access

# Enable authentication (example)
environment:
  KAFKA_SASL_ENABLED: true
  KAFKA_SASL_MECHANISM: PLAIN
  KAFKA_SASL_USERNAME: ferris
  KAFKA_SASL_PASSWORD: secure_password
```

### Access Control

```bash
# Restrict container capabilities
security_opt:
  - no-new-privileges:true
cap_drop:
  - ALL

# Use non-root users (already configured)
user: ferris:ferris
```

## 📈 Performance Tuning

### Resource Limits

```yaml
# Add to docker-compose.yml services
deploy:
  resources:
    limits:
      memory: 2G
      cpus: '1.0'
    reservations:
      memory: 1G
      cpus: '0.5'
```

### Kafka Optimization

```bash
# For high-throughput workloads
environment:
  KAFKA_NUM_PARTITIONS: 6
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_LOG_SEGMENT_BYTES: 1073741824
  KAFKA_LOG_RETENTION_HOURS: 168
```

### SQL Server Optimization

```bash
# Increase job limits for Multi-Job server
command: ["ferris-sql-multi", "server", "--max-jobs", "50", "--brokers", "kafka:9092"]

# Increase memory limits
environment:
  SQL_MEMORY_LIMIT_MB: 4096
  SQL_WORKER_THREADS: 8
```

## 🚚 Production Deployment

### Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yml ferris-sql-stack

# Scale services
docker service scale ferris-sql-stack_ferris-sql-multi=3
```

### Kubernetes

```yaml
# k8s-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ferris-sql-single
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ferris-sql-single
  template:
    metadata:
      labels:
        app: ferris-sql-single
    spec:
      containers:
      - name: ferris-sql
        image: ferris-sql:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_BROKERS
          value: "kafka-service:9092"
        - name: RUST_LOG
          value: "info"
```

### CI/CD Integration

```bash
# Build and push images
docker build -t ferris-sql:latest .
docker build -t ferris-sql-multi:latest -f Dockerfile.multi .

# Push to registry
docker tag ferris-sql:latest registry.company.com/ferris-sql:latest
docker push registry.company.com/ferris-sql:latest
```

## 📚 Additional Resources

### Sample SQL Applications

- **E-commerce Analytics**: `/examples/ecommerce_analytics.sql`
- **IoT Monitoring**: `/examples/iot_monitoring.sql`
- **Financial Trading**: `/examples/financial_trading.sql`
- **Social Media Analytics**: `/examples/social_media_analytics.sql`

### Configuration References

- **SQL Configuration**: `sql-config.yaml`
- **Deployment Script**: `deploy-sql.sh`
- **Performance Testing**: `run_performance_comparison.sh`

### Documentation

- **[SQL Reference Guide](SQL_REFERENCE_GUIDE.md)** - Complete SQL syntax
- **[Multi-Job Guide](../MULTI_JOB_SQL_GUIDE.md)** - Job management patterns
- **[Performance Guide](ADVANCED_PERFORMANCE_OPTIMIZATIONS.md)** - Optimization strategies

## 🎯 Next Steps

1. **Start with core infrastructure**: `docker-compose up -d kafka ferris-sql-single`
2. **Test with sample data**: Use the data-producer container
3. **Deploy SQL applications**: Try the example .sql files
4. **Enable monitoring**: Add `--profile monitoring` for production insights
5. **Scale horizontally**: Add more SQL server instances as needed

## 🤝 Support

- **Logs**: `docker-compose logs <service-name>`
- **Health**: `docker-compose ps`
- **Cleanup**: `docker-compose down -v`
- **Documentation**: Check `/docs` directory for detailed guides

**🎉 You now have a production-ready FerrisStreams SQL infrastructure running in Docker!**