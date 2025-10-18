# Node Naming Patterns - Quick Reference

## Common Prefixes & Patterns

### Environment Prefixes

```sql
-- Production
-- @deployment.node_id: prod-${HOSTNAME}
-- Result: prod-server-1, prod-server-2

-- Staging
-- @deployment.node_id: staging-${HOSTNAME}
-- Result: staging-server-1

-- QA
-- @deployment.node_id: qa-${HOSTNAME}
-- Result: qa-test-1, qa-test-2

-- Development
-- @deployment.node_id: dev-${HOSTNAME}
-- Result: dev-alice-laptop

-- Integration Testing
-- @deployment.node_id: integ-${HOSTNAME}
-- Result: integ-ci-node-1
```

### Cluster/Infrastructure Prefixes

```sql
-- Cluster ID prefix
-- @deployment.node_id: cluster-${CLUSTER_ID}-${HOSTNAME}
-- Result: cluster-prod-1-server-42

-- Region prefix
-- @deployment.node_id: ${REGION}-node-${INSTANCE_NUM}
-- Result: us-east-1-node-01, eu-west-1-node-01

-- Availability zone prefix
-- @deployment.node_id: ${AZ}-${HOSTNAME}
-- Result: us-east-1a-server-1, us-east-1b-server-2

-- Zone prefix (short form)
-- @deployment.node_id: zone${ZONE}-${HOSTNAME}
-- Result: zone1-server-1, zone2-server-2
```

### Application/Service Prefixes

```sql
-- Service prefix
-- @deployment.node_id: trading-${HOSTNAME}
-- Result: trading-server-1

-- Team prefix
-- @deployment.node_id: finance-team-${HOSTNAME}
-- Result: finance-team-server-1

-- Project prefix
-- @deployment.node_id: project-xyz-${HOSTNAME}
-- Result: project-xyz-node-01
```

### Cloud Provider Prefixes

```sql
-- AWS
-- @deployment.node_id: aws-${INSTANCE_ID}
-- Result: aws-i-0abc123def456

-- Azure
-- @deployment.node_id: azure-${VM_NAME}
-- Result: azure-velostream-prod-1

-- GCP
-- @deployment.node_id: gcp-${INSTANCE_NAME}
-- Result: gcp-trading-analytics-001

-- Kubernetes
-- @deployment.node_id: k8s-${POD_NAME}
-- Result: k8s-velostream-0
```

---

## Real-World Examples by Organization

### Tech Startup (Small Team)

```sql
-- @deployment.node_id: prod-${HOSTNAME}
-- @deployment.node_name: ${USER}@${HOSTNAME}

Results:
prod-api-1, prod-api-2
alice@macbook-pro, bob@server-1
```

### Enterprise (Multiple Environments)

```sql
-- @deployment.node_id: ${ENV}-${DC}-${HOSTNAME}

Patterns:
prod-dc1-server-01
prod-dc2-server-01
staging-dc1-test-01
dev-local-laptop-01
```

### SaaS Company (Cloud Native)

```sql
-- @deployment.node_id: ${CUSTOMER_ID}-${REGION}-${INSTANCE_ID}
-- @deployment.node_name: ${CUSTOMER_NAME}-${INSTANCE_TYPE:t3.large}

Results:
cust-123-us-east-1-i-abc123
cust-456-eu-west-1-i-def456

node_name: Acme-Inc-t3.xlarge, Beta-Corp-t3.large
```

### Financial Institution (Regulated)

```sql
-- @deployment.node_id: ${ENVIRONMENT}-${TRADING_DESK}-${REGION}-${SERVER_NUM}

Results:
prod-equity-us-east-1-01
prod-fixed-income-us-west-2-02
staging-risk-us-east-1-01
audit-compliance-us-east-1-01
```

### High-Frequency Trading (HFT)

```sql
-- @deployment.node_id: hft-${EXCHANGE}-${DATACENTER}-${RACK}-${SERVER}

Results:
hft-nyse-ny1-rack3-server42
hft-nasdaq-nj1-rack1-server15
```

---

## Pattern Templates

### Template 1: Simple Environment Prefix

```
Pattern: ${ENV}-${HOSTNAME}
Config:
  ENV = prod, staging, dev, qa
  HOSTNAME = auto-detected

Examples:
  prod-server-1
  staging-server-1
  dev-alice-laptop
  qa-ci-node-1
```

### Template 2: Region + Environment + ID

```
Pattern: ${REGION}-${ENV}-${ID}
Config:
  REGION = us-east-1, eu-west-1, ap-south-1
  ENV = prod, staging
  ID = 01, 02, 03

Examples:
  us-east-1-prod-01
  eu-west-1-staging-02
  ap-south-1-prod-01
```

### Template 3: Cluster + Zone + Instance

```
Pattern: ${CLUSTER_NAME}-${ZONE}-instance${NUM}
Config:
  CLUSTER_NAME = cluster1, cluster2
  ZONE = a, b, c
  NUM = 01, 02, 03

Examples:
  prod-cluster-1-a-instance01
  staging-cluster-2-b-instance03
```

### Template 4: Cloud Provider + Account + Region + Service

```
Pattern: ${PROVIDER}-${ACCOUNT}-${REGION}-${SERVICE}
Config:
  PROVIDER = aws, gcp, azure
  ACCOUNT = prod-acct, dev-acct
  REGION = us-east-1, eu-west-1
  SERVICE = trading, analytics

Examples:
  aws-prod-acct-us-east-1-trading
  gcp-dev-acct-eu-west-1-analytics
  azure-prod-acct-us-east-1-trading
```

### Template 5: Kubernetes Namespace + Pod + Replica

```
Pattern: k8s-${NAMESPACE}-${POD_NAME}-${REPLICA}
Config:
  NAMESPACE = prod, staging, dev
  POD_NAME = velostream (extracted from pod name)
  REPLICA = 0, 1, 2

Examples:
  k8s-prod-velostream-0
  k8s-prod-velostream-1
  k8s-staging-velostream-0
```

---

## Environment Variable Setup Examples

### Docker Compose

```yaml
services:
  velostream:
    environment:
      HOSTNAME: ${HOSTNAME}
      ENV: production
      CLUSTER_ID: cluster-1
      REGION: us-east-1
      # SQL will use: prod-cluster-1-us-east-1-${HOSTNAME}
```

### Docker Run

```bash
docker run \
  -e HOSTNAME=server-1 \
  -e ENV=prod \
  -e CLUSTER_ID=cluster-1 \
  velostream:latest
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velostream
spec:
  template:
    spec:
      containers:
      - name: velostream
        env:
        # Pod name as instance identifier
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name

        # Namespace (environment)
        - name: NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace

        # From ConfigMap
        - name: CLUSTER_ID
          valueFrom:
            configMapKeyRef:
              name: cluster-config
              key: cluster-id

        - name: REGION
          valueFrom:
            configMapKeyRef:
              name: cluster-config
              key: region
```

### AWS EC2 (User Data)

```bash
#!/bin/bash
export INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)
export REGION=$(ec2-metadata --availability-zone | sed 's/[a-z]$//')
export ENVIRONMENT=production
export CLUSTER_ID=prod-cluster-1

./velostream-server
# SQL pattern: prod-cluster-1-${REGION}-${INSTANCE_ID}
# Result: prod-cluster-1-us-east-1-i-0abc123def456
```

### Manual Server Setup

```bash
#!/bin/bash

# Set environment variables for this server
export HOSTNAME=$(hostname)
export ENVIRONMENT=production
export CLUSTER_ID=cluster-1
export DATACENTER=dc1
export RACK_ID=03
export SERVER_NUM=42

./velostream-server
# SQL pattern: ${ENVIRONMENT}-dc${DATACENTER}-rack${RACK_ID}-server${SERVER_NUM}
# Result: production-dc1-rack03-server42
```

---

## SQL Annotation Examples by Use Case

### Use Case 1: Multi-Tenant SaaS

```sql
-- Each customer gets their own prefix
-- @deployment.node_id: cust-${CUSTOMER_ID|DEFAULT_CUSTOMER:generic}-${REGION:us}-node${INSTANCE:1}

# Customer A:
CUSTOMER_ID=acme-inc REGION=us INSTANCE=1 → cust-acme-inc-us-node1

# Customer B:
CUSTOMER_ID=beta-corp REGION=eu INSTANCE=1 → cust-beta-corp-eu-node1
```

### Use Case 2: Blue-Green Deployment

```sql
-- Different prefix for active/standby
-- @deployment.node_id: ${DEPLOYMENT_VERSION}-${HOSTNAME}

# Blue deployment:
DEPLOYMENT_VERSION=blue → blue-server-1, blue-server-2

# Green deployment:
DEPLOYMENT_VERSION=green → green-server-1, green-server-2
```

### Use Case 3: Canary Deployment

```sql
-- Mark canary instances differently
-- @deployment.node_id: ${STABILITY_LEVEL}-${HOSTNAME}

# Stable:
STABILITY_LEVEL=stable → stable-server-1, stable-server-2

# Canary:
STABILITY_LEVEL=canary → canary-server-1, canary-server-2
```

### Use Case 4: Load Balancer Pool Identification

```sql
-- Track which pool this node belongs to
-- @deployment.node_id: pool-${POOL_ID}-${HOSTNAME}

Results:
pool-1-server-1
pool-1-server-2
pool-2-server-1
pool-2-server-2
```

### Use Case 5: Rollout/Deployment Stage

```sql
-- Track deployment progress
-- @deployment.node_id: rollout-${STAGE}-${BATCH}-${HOSTNAME}

Results:
rollout-phase1-batch1-server-01
rollout-phase1-batch2-server-01
rollout-phase2-batch1-server-01
```

---

## Prometheus Queries Examples

### Filter by Environment

```promql
# All production metrics
{node_id=~"prod-.*"}

# Staging only
{node_id=~"staging-.*"}

# QA only
{node_id=~"qa-.*"}
```

### Filter by Cluster

```promql
# Cluster 1
{node_id=~"cluster-1-.*"}

# Region comparison
sum(rate(velo_sql_queries_total[5m])) by (node_id)
```

### Filter by Environment + Comparison

```promql
# Compare prod vs staging
sum(rate(velo_sql_queries_total[5m])) by (node_id)
/ ignoring(node_id) group_left()
sum(rate(velo_sql_queries_total[5m]))
  by (node_id)
  or (label_join(
    on() group_left() count(velo_sql_queries_total{node_id=~"prod-.*"}),
    "node_id",
    "-",
    "node_id"
  ))
```

---

## Migration Path (Existing Systems)

### If You Currently Use Hostnames

```sql
-- Before: Just hostname
-- After: Add environment prefix
-- @deployment.node_id: prod-${HOSTNAME}

# Old metrics:
velo_sql_queries_total{operation="select"} 42

# New metrics (backward compatible):
velo_sql_queries_total{node_id="prod-server-1", operation="select"} 42
```

### If You Currently Use Instance IDs

```sql
-- Before: Just instance ID
-- After: Add context prefix
-- @deployment.node_id: aws-${REGION}-${INSTANCE_ID}

# Old:
velo_sql_queries_total{instance_id="i-0abc123"} 42

# New:
velo_sql_queries_total{node_id="aws-us-east-1-i-0abc123"} 42
```

### If You Currently Use Pod Names (Kubernetes)

```sql
-- Before: Pod name only
-- After: Add namespace context
-- @deployment.node_id: ${NAMESPACE}-${HOSTNAME}

# Old:
velo_sql_queries_total{pod_name="velostream-0"} 42

# New:
velo_sql_queries_total{node_id="prod-velostream-0"} 42
```

---

## Quick Decision Tree

```
Choose your naming convention:

1. Do you have multiple environments? (prod, staging, dev)
   YES → Start with: ${ENV}-${HOSTNAME}
   NO  → Proceed to 2

2. Do you have multiple regions?
   YES → Use: ${REGION}-${ENV}-${HOSTNAME}
   NO  → Proceed to 3

3. Do you have multiple clusters?
   YES → Use: ${CLUSTER_ID}-${ENV}-${HOSTNAME}
   NO  → Proceed to 4

4. Do you use cloud providers?
   YES → Use: ${PROVIDER}-${REGION}-${INSTANCE_ID}
   NO  → Use: ${HOSTNAME}

5. Are you on Kubernetes?
   YES → Use: k8s-${NAMESPACE}-${POD_NAME}
   NO  → Continue above
```

---

## Summary

✅ **Environment prefixes** - `prod-`, `staging-`, `dev-`, `qa-`
✅ **Cluster identification** - `cluster-1-`, `cluster-prod-`
✅ **Region/Zone** - `us-east-1-`, `zone-a-`
✅ **Service/Team** - `trading-`, `finance-team-`
✅ **Cloud provider** - `aws-`, `gcp-`, `azure-`, `k8s-`
✅ **Custom combinations** - Mix and match any above
✅ **Flexible separators** - `-`, `:`, `.` or custom
✅ **Backward compatible** - Optional, no breaking changes

**Key insight:** With `${PREFIX}-${HOSTNAME}` and `${PREFIX}-${CLUSTER_ID}-${REGION}` patterns, you can express virtually any real-world naming convention!
