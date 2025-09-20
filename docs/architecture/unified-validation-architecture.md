# Unified SQL Validation Architecture

## Overview

Velostream implements a unified validation architecture that provides comprehensive SQL and configuration validation across all deployment pathways. The system prevents invalid SQL applications from reaching production by implementing mandatory validation gates in CLI tools, deployment pipelines, and pre-deployment checks.

This architecture replaces the previous fragmented validation approach with a centralized, delegated system that ensures consistency and reliability across development, CI/CD, and production environments.

## Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Unified Validation System                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────┐    ┌─────────────────────────┐                   │
│  │      velo-cli           │    │   velo-sql-multi        │                   │
│  │                         │    │                         │                   │
│  │ • validate command      │    │ • Pre-deployment        │                   │
│  │ • Directory validation  │    │   validation gate       │                   │
│  │ • JSON/text output      │    │ • Deployment blocking   │                   │
│  │ • CI/CD integration     │    │ • Error reporting       │                   │
│  └─────────────────────────┘    └─────────────────────────┘                   │
│              │                              │                                  │
│              └──────────────┬───────────────┘                                  │
│                             │ DELEGATION                                       │
│                             ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    SqlValidator (Core Engine)                           │   │
│  │                                                                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │ SQL Parser  │  │ Query       │  │ Config      │  │ Error       │   │   │
│  │  │             │  │ Analyzer    │  │ Validator   │  │ Aggregator  │   │   │
│  │  │ • Syntax    │  │             │  │             │  │             │   │   │
│  │  │   validation│  │ • Source/   │  │ • YAML      │  │ • Context   │   │   │
│  │  │ • AST       │  │   Sink      │  │   loading   │  │   building  │   │   │
│  │  │   parsing   │  │   detection │  │ • Property  │  │ • Error     │   │   │
│  │  │ • Structure │  │ • Property  │  │   flattening│  │   chaining  │   │   │
│  │  │   validation│  │   extraction│  │ • Inheritance│  │ • Line/col  │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  │                                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │                  Application-Level Validation                   │   │   │
│  │  │                                                                 │   │   │
│  │  │  • Complete SQL application understanding                       │   │   │
│  │  │  • Cross-query dependency analysis                              │   │   │
│  │  │  • Global configuration inheritance validation                  │   │   │
│  │  │  • Performance warning detection                                │   │   │
│  │  │  • Schema compatibility checking                                │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
               ┌─────────────────────────────────────────┐
               │          Validation Outputs              │
               │                                         │
               │  • Human-readable error reports         │
               │  • JSON output for CI/CD integration    │
               │  • Line/column error positioning        │
               │  • Configuration inheritance context    │
               │  • Performance recommendations          │
               └─────────────────────────────────────────┘
```

## Core Components

### 1. SqlValidator (Central Engine)

**Purpose**: Centralized validation engine providing comprehensive SQL application validation

**Key Features**:
- **Application-Level Understanding**: Validates complete SQL applications, not just individual queries
- **Configuration Integration**: Handles YAML inheritance, property flattening, and global configurations
- **Multi-Format Output**: Supports both human-readable and JSON output formats
- **Error Context**: Provides precise line/column information with inheritance context
- **Performance Analysis**: Detects potential performance issues and provides recommendations

**Core Methods**:
```rust
impl SqlValidator {
    pub fn new() -> Self;
    pub fn validate_application(&self, file_path: &Path) -> ApplicationValidationResult;
    pub fn validate_directory(&self, dir_path: &Path) -> Vec<ApplicationValidationResult>;
}
```

**Validation Pipeline**:
```
SQL File → Parse Application → Extract Queries → Analyze Each Query → Validate Config → Aggregate Results
```

### 2. CLI Tool Integration

#### velo-cli (Development & CI/CD)

**Purpose**: Developer-focused validation with comprehensive output options

**Command Structure**:
```bash
velo-cli validate <path> [OPTIONS]
  --verbose, -v        Detailed validation output
  --strict, -s         Fail on warnings (exit code 1)
  --format, -f         Output format (text/json)
```

**Delegation Pattern**:
```rust
// Complete delegation to SqlValidator
let validator = SqlValidator::new();
let results = if path_obj.is_file() {
    vec![validator.validate_application(path_obj)]
} else if path_obj.is_dir() {
    validator.validate_directory(path_obj)
} else {
    // Error handling
};
```

**Output Formats**:
- **Text**: Human-readable with emojis and formatting for terminal display
- **JSON**: Structured output for CI/CD integration and automated processing

#### velo-sql-multi (Production Deployment)

**Purpose**: Pre-deployment validation gate preventing invalid SQL from reaching production

**Automatic Validation**:
```rust
// Mandatory validation before deployment
let validator = SqlValidator::new();
let validation_result = validator.validate_application(std::path::Path::new(&file_path));

if !validation_result.is_valid {
    return Err("SQL validation failed. Please fix the errors above before deployment.".into());
}
```

**Error Blocking**: Deployment is **completely blocked** if validation fails, ensuring no invalid SQL reaches production.

## Validation Flow Architecture

### Development Workflow

```
Developer writes SQL → IDE validation (future) → Local validation (velo-cli) → Git commit → CI/CD validation → Production deployment
```

### CI/CD Integration Flow

```
1. Code Commit
   ↓
2. CI/CD Pipeline Trigger
   ↓
3. velo-cli validate --strict --format json
   ↓
4. Validation Gate
   ├─ Pass → Continue to deployment
   └─ Fail → Block deployment + Generate report
   ↓
5. Automated deployment (if validation passes)
```

### Production Deployment Flow

```
1. velo-sql-multi deploy-app --file app.sql
   ↓
2. Automatic Pre-deployment Validation
   ├─ SqlValidator.validate_application()
   ├─ Parse SQL application
   ├─ Validate all queries and configurations
   └─ Check for errors/warnings
   ↓
3. Validation Gate
   ├─ Valid → Proceed with deployment
   └─ Invalid → Block deployment + Detailed error report
   ↓
4. SQL Application Deployment (only if valid)
```

## Validation Scope & Coverage

### 1. SQL Syntax Validation
- **Complete SQL parsing**: Full AST validation for streaming SQL syntax
- **Streaming SQL features**: Windows, aggregations, joins, subqueries
- **Syntax compatibility**: Ensures SQL is compatible with Velostream engine
- **Structure validation**: Proper CREATE STREAM syntax, INTO clauses, WITH clauses

### 2. Configuration Validation
- **YAML inheritance**: Validates `extends` chains and property inheritance
- **Property flattening**: Converts nested YAML to dot-notation properties
- **Required properties**: Ensures all mandatory source/sink configurations are present
- **Property validation**: Type checking, format validation, range validation

### 3. Source/Sink Validation
- **Kafka sources**: Bootstrap servers, topics, consumer configuration
- **Kafka sinks**: Producer configuration, topic validation, serialization settings
- **File sources**: Path validation, format checking, security validation
- **File sinks**: Output path validation, compression settings, buffer configuration

### 4. Performance Analysis
- **Memory concerns**: ORDER BY without LIMIT, unbounded state accumulation
- **JOIN performance**: Stream-to-stream JOINs without time windows
- **Throughput optimization**: Missing batch configuration, inefficient serialization
- **Resource validation**: Buffer sizes, thread limits, timeout settings

### 5. Cross-Query Dependencies
- **Application coherence**: Ensures queries work together as a complete application
- **Global property consistency**: Validates global configurations across all components
- **Schema compatibility**: Checks for schema mismatches between queries
- **Resource conflicts**: Prevents conflicting configurations

## Error Handling & Reporting

### Error Classification

```rust
#[derive(Debug, Clone)]
pub enum ValidationError {
    SyntaxError { line: usize, column: usize, message: String },
    ConfigurationError { property: String, message: String, context: String },
    InheritanceError { property: String, chain: Vec<String>, message: String },
    PerformanceWarning { query_index: usize, message: String, recommendation: String },
}
```

### Error Context Enhancement

**Rich Error Information**:
- **Precise location**: Line and column numbers for all errors
- **Configuration context**: Shows inheritance chain for configuration errors
- **Property source**: Indicates whether error comes from file, global, or inline config
- **Suggestions**: Provides actionable recommendations for fixing errors

**Example Error Output**:
```
❌ Query #2 (Line 45):
  📝 Parsing Errors:
    • SQL parse error at position 234: Expected SELECT but found FROM
  ⚙️ Configuration Errors:
    • Missing datasource.consumer_config.bootstrap.servers
  📥 Missing Source Configs:
    • market_data: bootstrap.servers, topic

💡 Recommendations:
  • Add time windows to JOIN operations for better performance
  • Consider batch configuration for high-throughput scenarios
```

### JSON Output Structure

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

## Integration Patterns

### 1. Development Integration

**Local Development**:
```bash
# Quick validation during development
velo-cli validate my_query.sql --verbose

# Validate entire project
velo-cli validate sql/ --verbose
```

**Pre-commit Hook**:
```bash
#!/bin/bash
# .git/hooks/pre-commit
velo-cli validate $(git diff --cached --name-only | grep '.sql$') --strict
if [ $? -ne 0 ]; then
  echo "❌ SQL validation failed. Please fix errors before committing."
  exit 1
fi
```

### 2. CI/CD Integration

**GitHub Actions**:
```yaml
- name: Validate SQL
  run: |
    velo-cli validate sql/ --strict --format json > validation.json
    if [ $? -ne 0 ]; then
      echo "SQL validation failed"
      cat validation.json | jq '.'
      exit 1
    fi
```

**GitLab CI**:
```yaml
validate:
  script:
    - velo-cli validate sql/ --strict --format json
  artifacts:
    reports:
      junit: validation.json
```

### 3. Production Integration

**Automatic Pre-deployment Validation**:
```bash
# Validation happens automatically
velo-sql-multi deploy-app --file production.sql

# Output:
# Starting deployment from file: production.sql
# Reading SQL file: production.sql
# Validating SQL application...
# ✅ SQL validation passed!
# 📦 Application: Trading Analytics v1.0.0
# 📊 3 queries validated successfully
# Deploying application...
```

## Migration from Legacy System

### Before: Fragmented Validation

```bash
# Multiple separate tools
cargo build --bin sql-validator
cargo build --bin velo-config-validator
sql-validator file.sql
velo-config-validator config.yaml
```

**Problems**:
- Validation logic scattered across multiple binaries
- Inconsistent error reporting and formatting
- No application-level understanding
- Manual validation steps prone to being skipped
- No integration with deployment process

### After: Unified Validation

```bash
# Single unified tool
cargo build --bin velo-cli
velo-cli validate file.sql         # Combined SQL + config validation
velo-cli validate --format json    # CI/CD integration
velo-sql-multi deploy-app --file demo.sql  # Automatic pre-deployment validation
```

**Benefits**:
- ✅ **Single source of truth** for all validation logic
- ✅ **Consistent error reporting** across all tools
- ✅ **Application-level understanding** of SQL projects
- ✅ **Automatic pre-deployment validation** prevents invalid deployments
- ✅ **CI/CD ready** with JSON output and strict modes

### Migration Path

1. **Phase 1**: Introduce unified validation alongside legacy tools
2. **Phase 2**: Update all documentation and examples to use new system
3. **Phase 3**: Mark legacy tools as deprecated
4. **Phase 4**: Remove legacy validation binaries

## Performance Characteristics

### Validation Performance Targets

| Operation | Target Time | Typical Time |
|-----------|-------------|--------------|
| Single SQL file validation | < 100ms | 20-80ms |
| Directory validation (10 files) | < 500ms | 200-400ms |
| Configuration inheritance resolution | < 50ms | 10-30ms |
| Error report generation | < 20ms | 5-15ms |
| JSON output serialization | < 10ms | 2-8ms |

### Memory Usage

- **Base validator**: ~5MB memory footprint
- **Per-file processing**: ~1-2MB additional memory
- **Configuration cache**: ~1MB for typical project
- **Total system**: ~10MB for typical validation workload

### Scalability

- **File count**: Tested with 100+ SQL files
- **File size**: Supports SQL files up to 10MB
- **Configuration complexity**: Handles inheritance chains up to 10 levels deep
- **Concurrent validation**: Thread-safe for parallel processing

## Security Considerations

### Input Validation
- **Path traversal prevention**: Validates file paths for security
- **Configuration injection**: Prevents malicious YAML injection
- **Resource limits**: Prevents DoS through large file processing
- **Access control**: Respects file system permissions

### Error Information Disclosure
- **Sensitive data**: Ensures error messages don't leak sensitive configuration
- **Path sanitization**: Normalizes paths in error messages
- **Configuration masking**: Masks sensitive properties in error outputs

## Future Enhancements

### Planned Features

**IDE Integration**:
- Language Server Protocol support for real-time validation
- VS Code extension with validation and autocompletion
- IntelliJ plugin for comprehensive SQL development support

**Advanced Analysis**:
- Schema evolution impact analysis
- Performance prediction based on query patterns
- Resource usage estimation for deployment planning

**Enhanced CI/CD**:
- GitHub App integration for PR validation comments
- GitLab merge request integration
- Slack/Teams notifications for validation failures

### Extensibility

**Custom Validators**:
```rust
pub trait CustomValidator {
    fn validate(&self, sql: &str, config: &HashMap<String, String>) -> ValidationResult;
    fn supports_query_type(&self, query_type: &str) -> bool;
}
```

**Plugin Architecture**:
- Support for domain-specific validation rules
- Custom error message formatting
- Extended configuration validation

## Testing Strategy

### Unit Tests
- **SQL parsing validation**: Comprehensive syntax validation tests
- **Configuration handling**: YAML inheritance and property flattening tests
- **Error reporting**: Validation error formatting and context tests
- **Integration points**: CLI tool delegation and output formatting tests

### Integration Tests
- **End-to-end validation**: Complete SQL application validation workflows
- **CLI integration**: Command-line interface testing with various input scenarios
- **Pre-deployment testing**: velo-sql-multi integration validation
- **Error scenarios**: Comprehensive error handling and recovery testing

### Performance Tests
- **Large file handling**: Validation performance with large SQL files
- **Directory processing**: Batch validation performance testing
- **Memory usage**: Resource consumption monitoring during validation
- **Concurrent processing**: Thread safety and parallel validation testing

## Monitoring & Observability

### Metrics Collection
- **Validation frequency**: Number of validations per day/hour
- **Error rates**: Percentage of files that fail validation
- **Performance metrics**: Average validation time per file size
- **Error categories**: Distribution of error types and frequencies

### Alerting
- **High error rates**: Alert when validation failure rate exceeds threshold
- **Performance degradation**: Alert when validation time exceeds targets
- **System errors**: Alert on validation system failures or crashes

### Logging
- **Validation events**: Structured logging of all validation activities
- **Error details**: Comprehensive error logging with context
- **Performance tracking**: Detailed timing and resource usage logs

## Conclusion

The Unified SQL Validation Architecture provides a robust, scalable, and developer-friendly validation system that ensures SQL application quality across all stages of development and deployment. By centralizing validation logic in the SqlValidator component and providing consistent delegation patterns across CLI tools, the system eliminates validation inconsistencies while providing comprehensive error reporting and CI/CD integration.

This architecture establishes a foundation for advanced validation features including IDE integration, performance analysis, and custom validation rules, while maintaining the flexibility to evolve with changing requirements in streaming SQL processing.

**Key Benefits Delivered**:
- 🛡️ **Production Safety**: Automatic pre-deployment validation prevents invalid SQL from reaching production
- 🎯 **Developer Experience**: Consistent, detailed error reporting with actionable recommendations
- 🔄 **CI/CD Integration**: JSON output and strict modes enable seamless automation
- 📊 **Application Understanding**: Validates complete SQL applications, not just individual queries
- ⚡ **Performance**: Sub-100ms validation for typical SQL files with comprehensive analysis

The unified validation system represents a significant architectural improvement that enhances both developer productivity and production reliability.