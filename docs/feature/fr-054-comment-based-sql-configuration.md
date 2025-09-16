# Feature Request: Comment-Based SQL Configuration

## Overview

Add support for comment-based configuration syntax in VeloStream SQL files to enable inline configuration directly within SQL scripts, making them more self-contained and easier to deploy.

## Current State

VeloStream currently supports configuration through:
- WITH clause configuration within SQL statements
- External YAML configuration files
- Configuration inheritance via extends pattern

However, comment-based configuration syntax (documented in `MULTI_JOB_SQL_GUIDE.md`) is not implemented in the SQL parser.

## Proposed Feature

### Comment-Based Configuration Syntax

Enable SQL files to contain configuration directives in comments using the `-- CONFIG:` prefix:

```sql
-- PERFORMANCE SETTINGS
-- CONFIG: max_memory_mb = 8192           -- Total memory limit for all jobs
-- CONFIG: worker_threads = 16            -- Number of worker threads
-- CONFIG: batch_size = 1000              -- Processing batch size
-- CONFIG: flush_interval_ms = 100        -- Output flush interval

-- CONSUMER SETTINGS  
-- CONFIG: group_prefix = "myapp_"        -- Prefix for consumer group names
-- CONFIG: job_prefix = "job_"            -- Prefix for job names
-- CONFIG: session_timeout_ms = 30000     -- Kafka session timeout
-- CONFIG: heartbeat_interval_ms = 10000  -- Kafka heartbeat interval

-- RELIABILITY SETTINGS
-- CONFIG: timeout_ms = 60000             -- Default job timeout
-- CONFIG: retry_attempts = 3             -- Number of retry attempts on failure
-- CONFIG: restart_policy = "on-failure"  -- Job restart policy (always/never/on-failure)

-- SERIALIZATION SETTINGS
-- CONFIG: default_format = "json"        -- Default serialization format
-- CONFIG: financial_precision = true     -- Enable financial precision arithmetic
-- CONFIG: decimal_places = 4             -- Default decimal precision

-- MONITORING SETTINGS
-- CONFIG: metrics_enabled = true         -- Enable detailed metrics collection
-- CONFIG: health_check_interval_ms = 30000 -- Health check frequency

-- SQL EXECUTION
SELECT transaction_id, customer_id, amount, 
       CASE WHEN amount > 1000 THEN 'HIGH_VALUE' ELSE 'NORMAL' END as risk_category
FROM financial_transactions
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY risk_category
INTO processed_transactions;
```

### Job-Level Configuration

Support job-specific configuration with `-- JOB:` prefix:

```sql
-- GLOBAL CONFIG
-- CONFIG: max_memory_mb = 8192
-- CONFIG: worker_threads = 16

-- JOB: fraud_detection
-- JOB: memory_limit_mb = 2048
-- JOB: timeout_ms = 30000
-- JOB: restart_policy = "always"
START JOB fraud_detection AS
SELECT * FROM transactions WHERE risk_score > 0.8;

-- JOB: analytics_pipeline  
-- JOB: memory_limit_mb = 4096
-- JOB: batch_size = 5000
START JOB analytics_pipeline AS
SELECT customer_id, SUM(amount) FROM transactions GROUP BY customer_id;
```

## Benefits

### 1. **Self-Contained Deployment**
- SQL files contain all necessary configuration
- No need to manage separate YAML configuration files
- Easier deployment and version control

### 2. **Improved Developer Experience**
- Configuration visible alongside SQL logic
- No context switching between SQL and YAML files
- Clear documentation of performance tuning decisions

### 3. **Enhanced Maintainability**
- Configuration changes tracked with SQL changes in version control
- Easier to understand configuration impact on specific queries
- Reduced configuration file sprawl

### 4. **Production-Ready Features**
- Fine-grained control over job resources
- Performance tuning at the SQL level
- Monitoring and reliability settings co-located with business logic

## Technical Implementation

### 1. **Parser Enhancement**
Extend the SQL parser (`src/velo/sql/parser.rs`) to:
- Extract configuration comments before parsing SQL statements
- Parse `-- CONFIG:` and `-- JOB:` directives
- Build configuration objects from parsed directives

### 2. **Configuration Integration**
Integrate with existing configuration system:
- Merge comment-based config with WITH clause config
- Support precedence rules (comment-based < WITH clause < command-line)
- Validate configuration values and types

### 3. **Error Handling**
- Clear error messages for invalid configuration syntax
- Line number reporting for configuration errors  
- Validation of configuration value types and ranges

### 4. **Backward Compatibility**
- Maintain full compatibility with existing WITH clause syntax
- Support mixing comment-based and WITH clause configuration
- Graceful fallback for unsupported configuration keys

## Implementation Plan

### Phase 1: Core Parser Support
- [ ] Extend SQL parser to extract configuration comments
- [ ] Implement comment parsing logic for `-- CONFIG:` directives
- [ ] Add configuration validation and type checking
- [ ] Basic integration with existing configuration system

### Phase 2: Job-Level Configuration  
- [ ] Add support for `-- JOB:` directives
- [ ] Implement job-specific configuration inheritance
- [ ] Support for multi-job configuration in single files
- [ ] Integration with job management system

### Phase 3: Advanced Features
- [ ] Configuration precedence and merging logic
- [ ] Environment variable expansion in comment configuration
- [ ] Configuration validation with detailed error reporting
- [ ] Documentation and examples

### Phase 4: Testing and Documentation
- [ ] Comprehensive test suite for comment-based configuration
- [ ] Update documentation and examples
- [ ] Performance testing for parser enhancement
- [ ] Migration guide for existing deployments

## Configuration Categories

### Performance Configuration
```sql
-- CONFIG: max_memory_mb = 8192
-- CONFIG: worker_threads = 16  
-- CONFIG: batch_size = 1000
-- CONFIG: flush_interval_ms = 100
-- CONFIG: backpressure_threshold = 10000
```

### Kafka Configuration
```sql
-- CONFIG: group_prefix = "myapp_"
-- CONFIG: session_timeout_ms = 30000
-- CONFIG: heartbeat_interval_ms = 10000
-- CONFIG: auto_offset_reset = "earliest"
-- CONFIG: enable_auto_commit = false
```

### Reliability Configuration  
```sql
-- CONFIG: timeout_ms = 60000
-- CONFIG: retry_attempts = 3
-- CONFIG: restart_policy = "on-failure"
-- CONFIG: health_check_interval_ms = 30000
-- CONFIG: circuit_breaker_threshold = 5
```

### Serialization Configuration
```sql
-- CONFIG: default_format = "json"
-- CONFIG: financial_precision = true
-- CONFIG: decimal_places = 4
-- CONFIG: compression = "gzip"
-- CONFIG: schema_validation = true
```

## Examples

### Financial Analytics Pipeline
```sql
-- High-performance financial processing configuration
-- CONFIG: max_memory_mb = 16384
-- CONFIG: worker_threads = 32
-- CONFIG: financial_precision = true
-- CONFIG: decimal_places = 6
-- CONFIG: batch_size = 500
-- CONFIG: timeout_ms = 120000

-- Real-time fraud detection
SELECT transaction_id, customer_id, amount,
       calculate_risk_score(amount, merchant_category, customer_history) as risk_score
FROM financial_transactions
WHERE timestamp > NOW() - INTERVAL '5 minutes'
  AND amount > 100.00
HAVING risk_score > 0.7
INTO fraud_alerts WITH (format = 'avro');
```

### Multi-Job Analytics
```sql
-- Global settings
-- CONFIG: max_memory_mb = 8192
-- CONFIG: metrics_enabled = true

-- JOB: order_processing
-- JOB: memory_limit_mb = 2048
-- JOB: batch_size = 1000
START JOB order_processing AS
SELECT * FROM orders WHERE status = 'PENDING';

-- JOB: customer_analytics  
-- JOB: memory_limit_mb = 4096
-- JOB: timeout_ms = 300000
START JOB customer_analytics AS
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_spend
FROM orders GROUP BY customer_id;
```

## Success Criteria

- [ ] SQL files can be deployed with embedded configuration
- [ ] Configuration syntax is intuitive and well-documented
- [ ] Performance impact on SQL parsing is minimal (<5% overhead)
- [ ] Full backward compatibility with existing configuration methods
- [ ] Comprehensive error reporting for invalid configurations
- [ ] Integration with existing monitoring and metrics systems

## References

- Current WITH clause implementation: `src/velo/sql/config/with_clause_parser.rs`
- SQL parser: `src/velo/sql/parser.rs`  
- Configuration system: `src/velo/sql/config/`
- Multi-job documentation: `docs/MULTI_JOB_SQL_GUIDE.md`