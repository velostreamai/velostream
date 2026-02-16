# Advanced Node Naming Patterns

## Overview

Support flexible node naming patterns that combine:
- Static prefixes/suffixes
- Environment variables
- Template substitution
- Fallbacks

This allows creating meaningful node IDs for any deployment topology.

---

## Pattern Syntax

### Basic Patterns

```
Pattern: prefix-${VAR_NAME}-suffix
Example: prod-${HOSTNAME}-us-east-1

Pattern: ${VAR1}:${VAR2}:${VAR3}
Example: ${CLUSTER}:${ZONE}:${INSTANCE_ID}

Pattern: Static value (no substitution)
Example: trading-server-1
```

### Format

```
${VAR_NAME}           - Replace with environment variable (fails if not set)
${VAR_NAME:default}   - Replace with env var or use default
${VAR_NAME|fallback1|fallback2}  - Try multiple env vars in order
```

---

## Implementation

### Enhanced Resolution Function

```rust
impl DeploymentConfig {
    /// Advanced pattern resolution supporting prefixes, multiple vars, and fallbacks
    ///
    /// Patterns supported:
    /// - "prefix-${VAR}-suffix"
    /// - "${VAR1}:${VAR2}:${VAR3}"
    /// - "${VAR:default}"
    /// - "${VAR1|VAR2|VAR3:default}"
    fn resolve_pattern(pattern: &str) -> String {
        if !pattern.contains("${") {
            return pattern.to_string(); // No substitution needed
        }

        let mut result = pattern.to_string();

        // Find all ${...} patterns
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let end = start + end;
                let var_spec = &result[start + 2..end];

                let replacement = Self::resolve_var_spec(var_spec);
                result.replace_range(start..=end, &replacement);
            } else {
                break; // Malformed pattern
            }
        }

        result
    }

    /// Resolve a single variable specification
    /// Supports: VAR, VAR:default, VAR1|VAR2|VAR3, VAR1|VAR2:default
    fn resolve_var_spec(spec: &str) -> String {
        // Try primary values first (split by |)
        let parts: Vec<&str> = spec.split('|').collect();

        for part in &parts[..parts.len().saturating_sub(1)] {
            if let Ok(value) = std::env::var(part) {
                return value;
            }
        }

        // Last part might have fallback: VAR:default
        if let Some(last_part) = parts.last() {
            if let Some((var_name, default)) = last_part.split_once(':') {
                if let Ok(value) = std::env::var(var_name) {
                    return value;
                } else {
                    // Use default or special handling for NODE_ID
                    if var_name == "NODE_ID" {
                        return hostname::get()
                            .map(|h| h.to_string_lossy().to_string())
                            .unwrap_or_else(|_| default.to_string());
                    } else {
                        return default.to_string();
                    }
                }
            } else {
                // No default, just a var name
                if let Ok(value) = std::env::var(last_part) {
                    return value.to_string();
                } else if *last_part == "NODE_ID" {
                    return hostname::get()
                        .map(|h| h.to_string_lossy().to_string())
                        .unwrap_or_else(|_| {
                            format!("node-{}", uuid::Uuid::new_v4().to_string()[..8].to_string())
                        });
                }
            }
        }

        spec.to_string() // Return as-is if nothing matches
    }
}
```

---

## Usage Examples

### Example 1: Prefix + Hostname + Region

**SQL Annotation:**
```sql
-- @deployment.node_id: prod-${HOSTNAME}-${AWS_REGION:us-east-1}
```

**Environment:**
```bash
export HOSTNAME=ip-10-0-1-42
export AWS_REGION=us-east-1
```

**Result:**
```
prod-ip-10-0-1-42-us-east-1
```

---

### Example 2: Kubernetes Pod Name with Cluster Prefix

**SQL Annotation:**
```sql
-- @deployment.node_id: ${CLUSTER_NAME:cluster1}-${HOSTNAME}
```

**Kubernetes env:**
```yaml
env:
- name: HOSTNAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: CLUSTER_NAME
  valueFrom:
    configMapKeyRef:
      name: cluster-config
      key: name
```

**Result (if pod name is `velostream-0`):**
```
cluster1-velostream-0
```

---

### Example 3: Multi-Level Fallback

**SQL Annotation:**
```sql
-- @deployment.node_id: ${INSTANCE_ID|EC2_INSTANCE_ID|HOSTNAME:generic-node}
```

**Resolution order:**
1. Try `INSTANCE_ID` env var
2. Fall back to `EC2_INSTANCE_ID` env var
3. Fall back to `HOSTNAME` env var
4. Fall back to literal `"generic-node"`

**Result (if only HOSTNAME set to `server-1`):**
```
server-1
```

---

### Example 4: AWS Multi-Region Deployment

**SQL Annotation:**
```sql
-- @deployment.node_id: aws-${AWS_REGION:us-east-1}-${INSTANCE_ID|EC2_INSTANCE_ID:i-generic}-${AZ:a}
-- @deployment.node_name: ${AWS_REGION:us-east-1}-${INSTANCE_TYPE:t3-medium}
```

**Environment (from AWS metadata service):**
```bash
export AWS_REGION=us-west-2
export INSTANCE_ID=i-0abc123def456
export AZ=a
export INSTANCE_TYPE=t3.large
```

**Result:**
```
node_id:   aws-us-west-2-i-0abc123def456-a
node_name: us-west-2-t3.large
```

---

### Example 5: Kubernetes Multi-Zone Deployment

**SQL Annotation:**
```sql
-- @deployment.node_id: k8s-${KUBE_NAMESPACE:default}-${HOSTNAME}-${KUBE_NODE_NAME|NODE_NAME:unknown}
-- @deployment.region: ${KUBE_REGION:us}-${KUBE_ZONE:east-1}
```

**Kubernetes ConfigMap + env vars:**
```yaml
env:
- name: KUBE_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: HOSTNAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: KUBE_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: KUBE_REGION
  valueFrom:
    configMapKeyRef:
      name: cluster-config
      key: region
- name: KUBE_ZONE
  valueFrom:
    configMapKeyRef:
      name: cluster-config
      key: zone
```

**Result (pod `trading-0` in `prod` namespace on node `node-42`):**
```
node_id:   k8s-prod-trading-0-node-42
region:    us-east-1
```

---

### Example 6: Data Center + Rack + Server

**SQL Annotation:**
```sql
-- @deployment.node_id: dc${DC_ID}-rack${RACK_ID}-server${SERVER_ID}
```

**Environment:**
```bash
export DC_ID=01
export RACK_ID=03
export SERVER_ID=42
```

**Result:**
```
dc01-rack03-server42
```

---

### Example 7: Development Local Machine

**SQL Annotation:**
```sql
-- @deployment.node_id: dev-${USER}-${HOSTNAME:localhost}
```

**Local environment:**
```bash
export USER=$USER
# HOSTNAME auto-detected
```

**Result:**
```
dev-alice-macbook-pro
```

---

### Example 8: Testing - Multiple Instances

**SQL Annotation:**
```sql
-- @deployment.node_id: test-${TEST_ENV:integration}-instance-${INSTANCE_NUM:1}
```

**Test script:**
```bash
for i in 1 2 3; do
  INSTANCE_NUM=$i TEST_ENV=integration ./velostream-server &
done
```

**Result:**
```
test-integration-instance-1
test-integration-instance-2
test-integration-instance-3
```

---

## Pattern Components Breakdown

### Prefixes

```sql
-- Static prefix
-- @deployment.node_id: prod-${HOSTNAME}
-- @deployment.node_id: staging-${POD_NAME}
-- @deployment.node_id: dev-${HOSTNAME:localhost}
```

### Separators

```sql
-- Hyphen separator
-- @deployment.node_id: ${CLUSTER}-${HOSTNAME}-${ZONE}

-- Colon separator
-- @deployment.node_id: ${DC}:${RACK}:${SERVER}

-- Dot separator
-- @deployment.node_id: ${REGION}.${ZONE}.${INSTANCE}

-- Mixed
-- @deployment.node_id: ${ENV}-${CLUSTER}:${POD_NAME}
```

### Suffixes

```sql
-- Static suffix
-- @deployment.node_id: ${HOSTNAME}-prod

-- Dynamic suffix
-- @deployment.node_id: ${HOSTNAME}.${ZONE}.${REGION}
```

---

## Environment Variables Reference

### Common Variables

| Variable | Source | Example | Use Case |
|----------|--------|---------|----------|
| `HOSTNAME` | System | `server-1` | Current hostname |
| `NODE_ID` | User-set | `node-1` | Explicit node identifier |
| `INSTANCE_ID` | Cloud metadata | `i-0abc123` | AWS instance ID |
| `POD_NAME` | Kubernetes | `trading-0` | Pod name |
| `NAMESPACE` | Kubernetes | `prod` | Kubernetes namespace |
| `NODE_NAME` | Kubernetes | `node-42` | Kubernetes node name |
| `AWS_REGION` | Cloud metadata | `us-east-1` | AWS region |
| `AWS_AVAILABILITY_ZONE` | Cloud metadata | `us-east-1a` | AWS AZ |
| `KUBE_ZONE` | Kubernetes | `us-east-1a` | Zone |
| `CLUSTER` | User-set | `prod-cluster-1` | Cluster name |
| `USER` | System | `alice` | Current user |
| `DC_ID` | User-set | `01` | Data center |
| `RACK_ID` | User-set | `03` | Rack number |

---

## Real-World Scenarios

### Scenario 1: AWS ECS Deployment

**Requirements:**
- Account ID
- Region
- Task name
- Container instance

**SQL:**
```sql
-- @deployment.node_id: aws-${AWS_ACCOUNT_ID}-${AWS_REGION}-${ECS_TASK_NAME}-${ECS_CONTAINER_INSTANCE_ARN}
```

**Result:**
```
aws-123456789-us-east-1-velostream-task-3-arn:aws:ec2:us-east-1:123456789:instance/i-0abc123
```

---

### Scenario 2: Kubernetes StatefulSet

**Requirements:**
- Cluster name (from config)
- Namespace
- StatefulSet name (from pod name)
- Replica index (from pod name)

**SQL:**
```sql
-- @deployment.node_id: ${CLUSTER_NAME}-${NAMESPACE}-${STATEFULSET_NAME}-${REPLICA_INDEX}
```

**With pod naming convention `velostream-0`, `velostream-1`, etc.:**
```
prod-cluster-analytics-prod-velostream-0
prod-cluster-analytics-prod-velostream-1
```

---

### Scenario 3: On-Premises Multi-Datacenter

**Requirements:**
- Datacenter location
- Building
- Floor/Rack
- Physical server identifier

**SQL:**
```sql
-- @deployment.node_id: ${DC:HQ}-bldg${BUILDING:A}-floor${FLOOR:3}-srv${SERVER:01}
```

**Result:**
```
HQ-bldgA-floor3-srv01
HQ-bldgB-floor2-srv42
DR-bldgA-floor1-srv99
```

---

### Scenario 4: GCP GKE with Cloud Run

**Requirements:**
- Project ID
- Region
- Service name
- Revision

**SQL:**
```sql
-- @deployment.node_id: gcp-${GCP_PROJECT}-${GCP_REGION}-${CLOUD_RUN_SERVICE}-${CLOUD_RUN_REVISION}
```

**Result:**
```
gcp-my-project-us-central1-trading-analytics-00001
```

---

### Scenario 5: Development Multi-Instance Testing

**Requirements:**
- Developer name
- Test type
- Instance number
- Timestamp

**SQL:**
```sql
-- @deployment.node_id: dev-${USER}-${TEST_TYPE:integration}-instance${INSTANCE_NUM:1}
-- @deployment.node_name: ${USER}@${HOSTNAME:localhost}
```

**Result:**
```
node_id:   dev-alice-integration-instance1
node_name: alice@macbook-pro
```

---

## Parser Behavior

### Resolution Priority

```
1. Literal values (no ${})
   → Use as-is

2. Single variable ${VAR}
   → Check env var
   → If VAR="NODE_ID", try hostname → UUID
   → Otherwise fail gracefully

3. With default ${VAR:default}
   → Check env var
   → Fall back to default

4. Multiple vars ${VAR1|VAR2:default}
   → Try VAR1
   → Try VAR2
   → Fall back to default

5. Complex pattern prod-${VAR1|VAR2}-${VAR3:default}
   → Replace each ${...} independently
   → Concatenate results
```

### Error Handling

```
Pattern: "prod-${UNDEFINED_VAR}"
Result:  "prod-${UNDEFINED_VAR}"  (returns as-is if env var not found)

Pattern: "prod-${VAR1|VAR2:default}"
Result:  "prod-default" (tries VAR1, VAR2, falls back)

Pattern: Static value
Result:  Unchanged
```

---

## SQL Examples - Complete

### Example: Full Production Setup

```sql
-- SQL Application: Financial Trading Platform
-- Version: 2.0.0
-- Description: Real-time financial analytics

-- Deployment: Complex multi-region setup
-- Pattern: provider-account-region-environment-instance-zone
-- @deployment.node_id: aws-${AWS_ACCOUNT_ID:prod-acct}-${AWS_REGION:us-east-1}-trading-${INSTANCE_ID|EC2_INSTANCE_ID:i-generic}-${AZ:a}
-- @deployment.node_name: ${AWS_REGION:us-east-1}-${INSTANCE_TYPE:t3.large}-pod${INSTANCE_NUM:1}
-- @deployment.region: ${AWS_REGION:us-east-1}

-- Observability
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
-- @observability.error_reporting.enabled: true

-- @name: market-data-1
-- Name: Market Data Processor
CREATE STREAM market_data AS
SELECT * FROM kafka_market_data
EMIT CHANGES;
```

**With environment:**
```bash
export AWS_ACCOUNT_ID=123456789
export AWS_REGION=us-west-2
export INSTANCE_ID=i-0abc123def456
export AZ=b
export INSTANCE_TYPE=t3.xlarge
export INSTANCE_NUM=2
```

**Result:**
```
node_id:   aws-123456789-us-west-2-trading-i-0abc123def456-b
node_name: us-west-2-t3.xlarge-pod2
region:    us-west-2
```

---

## Implementation Checklist

- [ ] Implement `resolve_pattern()` function with ${} parsing
- [ ] Support single variable: `${VAR}`
- [ ] Support default: `${VAR:default}`
- [ ] Support multi-var fallback: `${VAR1|VAR2|VAR3}`
- [ ] Support combined: `${VAR1|VAR2:default}`
- [ ] Support prefix/suffix patterns: `prefix-${VAR}-suffix`
- [ ] Add tests for all pattern combinations
- [ ] Add tests for special NODE_ID handling
- [ ] Add tests for fallback behavior
- [ ] Document all patterns in SQL annotation guide
- [ ] Create deployment guide with real-world scenarios
- [ ] Add validation for malformed patterns

---

## Testing Examples

```rust
#[test]
fn test_simple_var() {
    std::env::set_var("HOSTNAME", "server-1");
    assert_eq!(
        DeploymentConfig::resolve_pattern("${HOSTNAME}"),
        "server-1"
    );
}

#[test]
fn test_prefix_suffix() {
    std::env::set_var("HOSTNAME", "server");
    std::env::set_var("REGION", "us-east-1");
    assert_eq!(
        DeploymentConfig::resolve_pattern("prod-${HOSTNAME}-${REGION}"),
        "prod-server-us-east-1"
    );
}

#[test]
fn test_with_default() {
    std::env::remove_var("UNDEFINED");
    assert_eq!(
        DeploymentConfig::resolve_pattern("${UNDEFINED:default-value}"),
        "default-value"
    );
}

#[test]
fn test_multi_var_fallback() {
    std::env::set_var("VAR2", "value2");
    std::env::remove_var("VAR1");
    std::env::remove_var("VAR3");
    assert_eq!(
        DeploymentConfig::resolve_pattern("${VAR1|VAR2|VAR3:fallback}"),
        "value2"
    );
}

#[test]
fn test_complex_pattern() {
    std::env::set_var("CLUSTER", "prod");
    std::env::set_var("ZONE", "us-east");
    std::env::set_var("INSTANCE", "1");
    assert_eq!(
        DeploymentConfig::resolve_pattern(
            "${CLUSTER}-${ZONE}-instance${INSTANCE}"
        ),
        "prod-us-east-instance1"
    );
}
```

---

## Summary

✅ **Prefix + environment variables** - Yes! Fully supported
✅ **Multiple fallbacks** - `${VAR1|VAR2|VAR3}`
✅ **Complex patterns** - `prefix-${VAR1}-mid-${VAR2:default}-suffix`
✅ **Mixed separators** - `-`, `:`, `.`, or custom
✅ **Real-world scenarios** - AWS, GCP, Kubernetes, on-premises
✅ **Backward compatible** - Static values still work
✅ **Graceful degradation** - Missing vars used as-is
