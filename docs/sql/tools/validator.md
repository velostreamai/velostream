# SQL Validator Tool Guide

## Overview

Velostream provides comprehensive SQL validation through an integrated validation system that checks SQL queries and applications for:

- **Parsing Correctness**: Validates SQL syntax compatibility with Velostream parser
- **Configuration Completeness**: Ensures all required source/sink configurations are present
- **Performance Warnings**: Identifies potential performance issues in streaming queries
- **Syntax Compatibility**: Flags SQL constructs that may not be fully supported
- **Pre-deployment Validation**: Prevents deployment of invalid SQL applications

## Unified Validation Architecture

As of the latest update, SQL validation is now integrated directly into Velostream's primary tools:

### 🎯 **Primary Validation Methods**

#### 1. **CLI Validation** (Development & CI/CD)
```bash
# Validate a single SQL file
velo-cli validate path/to/file.sql

# Validate all SQL files in a directory
velo-cli validate path/to/sql/

# Strict mode (fail on warnings)
velo-cli validate path/to/sql/ --strict

# Verbose output with recommendations
velo-cli validate path/to/file.sql --verbose

# JSON output for CI/CD integration
velo-cli validate path/to/sql/ --format json
```

#### 2. **Pre-deployment Validation** (Production)
```bash
# Automatic validation before deployment
velo-sql-multi deploy-app --file production.sql

# Validation occurs automatically and blocks deployment if invalid
# ❌ Deployment blocked if validation fails
# ✅ Deployment proceeds only if validation passes
```

## Installation & Setup

### Building the Tools

```bash
# Build the Velostream CLI with validation support
cargo build --bin velo-cli

# Build the SQL multi-job server with integrated validation
cargo build --bin velo-sql-multi

# No longer needed (deprecated):
# ❌ cargo build --bin sql-validator
# ❌ cargo build --bin velo-config-validator
```

## Validation Features

### 1. **Application-Level Validation** ✅

The validator now understands SQL applications as complete units:

```bash
velo-cli validate trading.sql --verbose

📊 Validation Results
====================
📄 trading.sql
  📦 Application: Real-Time Trading Analytics
  📊 Queries: 5 total, 4 valid
  ✅ Valid (with warnings)
```

### 2. **Query-Level Analysis** ✅

Detailed per-query validation with line numbers:

```
❌ Query #2 (Line 45):
  📝 Parsing Errors:
    • SQL parse error at position 234
  ⚙️ Configuration Errors:
    • Missing datasource.consumer_config.bootstrap.servers
  📥 Missing Source Configs:
    • market_data: bootstrap.servers, topic
```

### 3. **Configuration Validation** ✅

#### **YAML Inheritance Support**
```yaml
# market_data_source.yaml
extends: common_kafka_source.yaml
topic:
  name: "market_data"
```

The validator correctly handles:
- `extends:` inheritance chains
- Nested configuration merging
- Property flattening (e.g., `datasource.consumer_config.bootstrap.servers`)

### 4. **Performance Analysis** ✅

```
⚡ Performance Warnings:
  • Stream-to-stream JOINs without time windows can be expensive
  • GROUP BY with DISTINCT operations can be memory-intensive
  • Consider batch configuration for multi-source queries
```

## Command Line Options

### `velo-cli validate`

| Option | Description | Example |
|--------|-------------|---------|
| `--verbose, -v` | Show detailed validation output | `velo-cli validate file.sql -v` |
| `--strict, -s` | Fail on warnings (exit code 1) | `velo-cli validate file.sql --strict` |
| `--format, -f` | Output format (text/json) | `velo-cli validate file.sql -f json` |

### JSON Output Format

Perfect for CI/CD integration:

```json
{
  "total_files": 1,
  "valid_files": 0,
  "results": [{
    "file": "trading.sql",
    "application_name": "Real-Time Trading Analytics",
    "valid": false,
    "total_queries": 5,
    "valid_queries": 3,
    "errors": [...],
    "recommendations": [
      "Add time windows to JOIN operations",
      "Consider batch configuration for throughput"
    ]
  }]
}
```

## Integration Scenarios

### 1. **Development Workflow**

```bash
# During development, validate as you write
velo-cli validate my_query.sql --verbose

# Check all queries in your project
velo-cli validate sql/ --verbose
```

### 2. **CI/CD Pipeline**

```yaml
# GitHub Actions example
- name: Validate SQL
  run: |
    velo-cli validate sql/ --strict --format json > validation.json
    if [ $? -ne 0 ]; then
      echo "SQL validation failed"
      cat validation.json | jq '.'
      exit 1
    fi
```

### 3. **Pre-commit Hook**

```bash
#!/bin/bash
# .git/hooks/pre-commit
velo-cli validate $(git diff --cached --name-only | grep '.sql$') --strict
if [ $? -ne 0 ]; then
  echo "❌ SQL validation failed. Please fix errors before committing."
  exit 1
fi
```

### 4. **Production Deployment**

```bash
# Automatic validation during deployment
velo-sql-multi deploy-app --file production.sql

# Output:
Starting deployment from file: production.sql
Reading SQL file: production.sql
Validating SQL application...
✅ SQL validation passed!
📦 Application: Trading Analytics v1.0.0
📊 3 queries validated successfully
Deploying application...
```

## Validation Rules

### ✅ **Required Configurations**

#### Kafka Sources
- `datasource.consumer_config.bootstrap.servers`
- `datasource.consumer_config.topic` or topic specification
- `datasource.consumer_config.group.id` (recommended)

#### Kafka Sinks
- `datasink.producer_config.bootstrap.servers`
- `datasink.producer_config.topic`
- `datasink.producer_config.value.format`

#### File Sources
- `datasource.path`
- `datasource.format` (csv/json/jsonlines)

### ⚠️ **Performance Warnings**

1. **Memory Concerns**
   - `ORDER BY` without `LIMIT`
   - `GROUP BY` with `DISTINCT`
   - Unbounded state accumulation

2. **JOIN Performance**
   - Stream-to-stream JOINs without time windows
   - Large table lookups without indexes
   - Cartesian products

3. **Throughput Optimization**
   - Missing batch configuration for multi-source queries
   - Inefficient serialization formats
   - Missing compression settings

## Migration Guide

### From Standalone Validators

#### **Old Way** (Deprecated)
```bash
# ❌ Don't use these anymore:
sql-validator path/to/file.sql
velo-config-validator config.yaml
```

#### **New Way** (Recommended)
```bash
# ✅ Use integrated validation:
velo-cli validate path/to/file.sql
# Config validation happens automatically within SQL validation
```

### Benefits of New System

1. **Unified Interface**: Single tool for all validation needs
2. **Context-Aware**: Understands complete applications, not just queries
3. **Pre-deployment Safety**: Automatic validation prevents bad deployments
4. **Better Error Messages**: Application-level context in error reporting
5. **YAML Inheritance**: Full support for configuration inheritance

## Troubleshooting

### Common Issues

#### 1. **Missing Configuration Files**
```
❌ Configuration error: Failed to load config file 'configs/source.yaml'
```
**Solution**: Ensure config files exist relative to where you run validation

#### 2. **YAML Inheritance Issues**
```
❌ Circular dependency detected in YAML inheritance
```
**Solution**: Check for circular `extends:` references

#### 3. **Property Name Mismatches**
```
❌ Missing required config: datasource.consumer_config.bootstrap.servers
```
**Solution**: Use dot notation for nested properties (not underscores)

## Best Practices

### 1. **Validate Early and Often**
```bash
# Add to your development workflow
alias vsql='velo-cli validate --verbose'
vsql my_query.sql
```

### 2. **Use Strict Mode in CI/CD**
```bash
# Fail fast on any issues
velo-cli validate sql/ --strict
```

### 3. **Leverage JSON for Automation**
```bash
# Parse validation results programmatically
velo-cli validate sql/ --format json | jq '.results[] | select(.valid == false)'
```

### 4. **Document SQL Applications**
```sql
-- SQL Application: Trading Analytics
-- Description: Real-time trading analysis pipeline
-- Version: 1.0.0

-- Your SQL queries here...
```

## Current Status

### ✅ **Production Ready**
- Configuration validation
- Performance analysis
- Pre-deployment validation
- YAML inheritance support
- CI/CD integration

### 🚀 **Recent Improvements**
- Unified validation architecture
- Application-level understanding
- Enhanced error messages
- JSON output support
- Automatic pre-deployment validation

### 🔧 **Known Limitations**
- Complex nested subqueries may have limited support
- Some advanced window functions still being implemented
- Schema inference for complex types in progress

## Conclusion

The Velostream SQL Validator is now fully integrated into the core toolchain, providing comprehensive validation at every stage from development to deployment. The unified architecture ensures consistency and prevents invalid SQL from reaching production.

**Key Commands**:
- `velo-cli validate` - Development and CI/CD validation
- `velo-sql-multi deploy-app` - Automatic pre-deployment validation

For more information, see the [Velostream CLI Guide](../cli-guide.md) and [SQL Deployment Guide](../deployment-guide.md).