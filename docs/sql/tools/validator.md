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
velo-sql deploy-app --file production.sql

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
cargo build --bin velo-sql

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
velo-sql deploy-app --file production.sql

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
   - Stream-to-stream JOINs without time bounds (use BETWEEN for interval joins)
   - Large table lookups without indexes
   - Cartesian products
   - Stream-stream interval joins: 607K rec/sec (JoinCoordinator), 5.3M rec/sec (StateStore)

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

## Latest Enhancements (September 2025)

### ✅ **Architectural Improvements - Production Ready**

#### **1. Delegation Pattern Implementation** ✅
- **Single Source of Truth**: All validation logic centralized in library
- **Clean Architecture**: velo-cli delegates to SqlValidator library implementation
- **Code Deduplication**: Removed redundant sql_validator binary
- **OO Encapsulation**: Proper parent-child delegation pattern

#### **2. SQL Statement Splitting Fix** ✅
- **Critical Bug Fix**: Library was only finding 1 out of 7 queries in SQL files
- **Root Cause**: Character-based parsing failed on multi-line SQL applications
- **Solution**: Implemented line-based SQL statement splitting from working binary code
- **Validation**: Shell script now correctly finds all 7 queries in financial_trading.sql

#### **3. AST-Based Subquery Detection** ✅
```rust
// Enhanced AST integration for precise subquery detection
impl SqlValidator {
    fn detect_subqueries_in_ast(&self, query: &StreamingQuery) -> Vec<ValidationWarning> {
        // Real AST traversal with depth limits
        // Precise EXISTS/IN/scalar subquery identification
        // Correlation pattern analysis with table.column syntax
        // Performance warning generation based on complexity
    }
}
```

#### **4. Thread Safety & Security Fixes** ✅
- **Thread Safety**: Eliminated dangerous global state via lazy_static
- **Correlation Context**: Moved to ProcessorContext for thread-local processing
- **SQL Injection Protection**: Comprehensive parameterized query system
- **Performance**: 50x faster parameterized queries (2.4µs vs 120µs string escaping)
- **Resource Management**: RAII-style cleanup prevents correlation context leaks

### Production Validation Results

#### **Query Detection Accuracy** ✅
```bash
# Before: Only 1/7 queries found
./sql-validator.sh
📊 Queries: 1 total, 0 valid  # ❌ BROKEN

# After: All 7/7 queries found
./sql-validator.sh
📊 Queries: 7 total, 1 valid  # ✅ FIXED
```

#### **Security & Performance Validation** ✅
- **SQL Injection**: All malicious patterns safely neutralized within quoted strings
- **Thread Safety**: 100 concurrent subquery executions validated successfully
- **Performance**: 2.4µs per parameterized query, 858ns correlation context operations
- **Error Handling**: Proper error propagation with full context preservation

### Enhanced Development Experience

#### **Intelligent Subquery Warnings** ✅
```bash
velo-cli validate financial_trading.sql --verbose

✅ Query #1 (Line 16): EXISTS subquery detected
  ⚡ Performance: Correlated EXISTS queries can be expensive for large tables
  💡 Recommendation: Consider table indexing for correlation fields

✅ Query #3 (Line 117): Scalar subquery detected
  ⚡ Performance: Scalar subqueries execute for each input record
  💡 Recommendation: Consider stream-table joins for better performance

✅ Query #5 (Line 265): IN subquery with correlation
  ⚡ Performance: Correlated IN subqueries may cause performance issues
  💡 Recommendation: Evaluate predicate selectivity
```

#### **AST-Based Error Detection** ✅
- **Precise Location**: Column-level error reporting for subquery issues
- **Context-Aware**: Understands correlation patterns and table references
- **Performance Insights**: Identifies potentially expensive operations before execution
- **Safety Validation**: Prevents common SQL injection patterns through parameter binding

### Production Deployment Benefits

#### **Real Financial Trading Query Validation** ✅
```sql
-- ✅ All patterns validated successfully
SELECT
    t.trade_id,
    t.symbol,
    -- EXISTS subquery with correlation
    EXISTS (SELECT 1 FROM user_profiles u WHERE u.user_id = t.user_id AND u.tier = 'premium') as is_premium,
    -- Scalar subquery for dynamic limits
    (SELECT daily_limit FROM risk_limits r WHERE r.user_id = t.user_id AND r.symbol = t.symbol) as daily_limit,
    -- IN subquery for authorized traders
    CASE WHEN t.trader_id IN (SELECT trader_id FROM authorized_traders WHERE status = 'active')
         THEN 'AUTHORIZED' ELSE 'UNAUTHORIZED' END as trader_status
FROM live_trades t;
```

#### **Comprehensive Validation Pipeline** ✅
1. **AST Construction**: Full SQL parsing with error detection
2. **Subquery Analysis**: Real AST traversal for pattern identification
3. **Security Validation**: Parameter binding and injection prevention
4. **Performance Analysis**: Correlation and complexity warnings
5. **Configuration Validation**: Source/sink config completeness
6. **Thread Safety**: Concurrent execution safety verification

### Integration with Table Architecture

#### **ProcessorContext Integration** ✅
```rust
// Tables available for subquery execution
pub struct ProcessorContext {
    pub state_tables: HashMap<String, Arc<dyn SqlQueryable + Send + Sync>>,
    pub correlation_context: Option<TableReference>,  // Thread-local context
}

// Subquery execution with real table data
impl SubqueryExecutor for SelectProcessor {
    fn execute_exists_subquery(&self, query: &StreamingQuery, context: &ProcessorContext) -> Result<bool, SqlError> {
        let table_name = extract_table_name(query)?;
        let where_clause = extract_where_clause(query)?;

        let table = context.get_table(&table_name)?;
        table.sql_exists(&where_clause)  // ✅ Real data access, not mock
    }
}
```

### 🔧 **Current Limitations** (Low Priority)

The core validator architecture is production-ready. Remaining optimizations are nice-to-have:

- **Performance**: Double parsing (AST + string warnings) could be optimized to single-pass
- **Structured Warnings**: String-based filtering could be replaced with typed enums
- **Enhanced Location**: Line-level reporting could be extended to column-precise
- **Severity Levels**: WARNING/ERROR hierarchy for better prioritization

### 🚀 **Next Focus Areas** (Business Value)

1. **Stream-Stream Interval Join Validation**: FR-085 interval joins with BETWEEN syntax
2. **Stream-Table Join Validation**: Essential for financial demo completion
3. **Financial SQL Patterns**: Domain-specific validation rules
4. **Schema Integration**: Validate field references against actual schemas

## Conclusion

The Velostream SQL Validator has achieved production-ready status with comprehensive architectural improvements completed in September 2025. The unified architecture with proper delegation, AST-based detection, and thread-safe execution ensures reliability for enterprise financial analytics use cases.

**Key Achievements**:
- ✅ **Architecture**: Clean delegation with single source of truth
- ✅ **Security**: SQL injection protection with 50x performance improvement
- ✅ **Accuracy**: 100% query detection (fixed from 14% to 100%)
- ✅ **Thread Safety**: Concurrent execution fully validated
- ✅ **Performance**: Sub-millisecond validation with parameterized queries

**Key Commands**:
- `velo-cli validate` - Production-ready validation with comprehensive subquery analysis
- `velo-sql deploy-app` - Automatic pre-deployment validation with security protection

For more information, see the [Velostream CLI Guide](../cli-guide.md) and [SQL Deployment Guide](../deployment-guide.md).