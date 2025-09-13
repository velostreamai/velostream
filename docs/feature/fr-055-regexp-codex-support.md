# Feature Request: RegExp Code Serialization Format

**Status**: RFC (Request for Comments)  
**Priority**: Medium  
**Target Release**: 0.3.0  
**Created**: 2025-09-03  

## Executive Summary

Proposal to add a new serialization format called **RegExp Code** that enables structured field extraction from text-based data streams using regular expressions with named capture groups. This format would complement existing Avro, Protobuf, and JSON support by providing efficient parsing of semi-structured log data, CSV-like formats, and other text-based protocols.

## Problem Statement

### Current Limitations

FerrisStreams currently supports structured serialization formats (Avro, Protobuf, JSON) but lacks native support for extracting structured data from text-based or semi-structured sources such as:

- **Application Logs**: Apache access logs, application debug logs, syslog formats
- **Legacy Data Formats**: Fixed-width files, custom delimited formats, proprietary log formats  
- **IoT Data Streams**: Sensor readings in custom text formats
- **Financial Data**: Trade reports, market data feeds with custom formatting

### Text-Based Data Processing Challenges

Current serialization codecs (JSON, Avro, Protobuf) expect structured data, but many real-world streams contain semi-structured text that requires parsing:

1. **Pattern Extraction Complexity**: Manual string parsing is error-prone and inefficient
2. **Type Safety**: Raw string parsing lacks type validation and conversion
3. **Performance Constraints**: Repeated pattern compilation impacts streaming throughput  
4. **Maintenance Burden**: Hard-coded parsing logic is difficult to maintain and evolve

### Use Case Examples

```regex
# Apache Access Log Pattern
^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\d+)

# Financial Trade Data Pattern  
(?P<symbol>[A-Z]{3,5}),(?P<price>\d+\.\d{4}),(?P<volume>\d+),(?P<timestamp>\d{10}),(?P<side>[BS])

# IoT Sensor Data Pattern
SENSOR:(?P<device_id>\w+):TEMP:(?P<temperature>-?\d+\.\d+):HUMIDITY:(?P<humidity>\d+\.\d+):TIME:(?P<timestamp>\d+)
```

## Proposed Solution

### RegExp Code Codec Implementation

Implement a new serialization codec that uses regular expressions with named capture groups to deserialize text-based data into structured `StreamRecord` objects. This codec would integrate with FerrisStreams' existing serialization framework alongside JSON, Avro, and Protobuf codecs.

#### Schema Definition
```yaml
# regexp_schema.yaml
format: regexp
version: "1.0"
pattern: |
  ^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\d+)

fields:
  ip:
    type: string
    description: "Client IP address"
    
  timestamp:
    type: timestamp
    format: "%d/%b/%Y:%H:%M:%S %z"
    description: "Request timestamp"
    
  method:
    type: string
    enum: ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]
    
  path:
    type: string
    description: "Request path"
    
  protocol:
    type: string
    pattern: "HTTP/\\d\\.\\d"
    
  status:
    type: integer
    range: [100, 599]
    
  size:
    type: integer
    min: 0
    description: "Response size in bytes"

performance:
  compile_once: true
  max_backtrack_limit: 1000000
  timeout_ms: 100
```

#### Rust Implementation Structure

```rust
// RegExp Code serialization format implementation
pub struct RegexpCodec {
    pattern: Regex,
    field_mappings: HashMap<String, FieldMapping>,
    performance_config: RegexpPerformanceConfig,
}

#[derive(Debug, Clone)]
pub struct FieldMapping {
    pub field_name: String,
    pub field_type: RegexpFieldType,
    pub validation: Option<FieldValidation>,
    pub transformation: Option<FieldTransformation>,
}

#[derive(Debug, Clone)]
pub enum RegexpFieldType {
    String,
    Integer,
    Float,
    ScaledInteger { scale: u8 },  // For financial precision
    Timestamp { format: String },
    Boolean,
    IpAddress,
    Url,
}

#[derive(Debug, Clone)]
pub struct RegexpPerformanceConfig {
    pub compile_once: bool,
    pub max_backtrack_limit: u64,
    pub timeout_ms: u64,
    pub cache_compiled_patterns: bool,
}

```

### Core Features

#### 1. Named Capture Groups → FieldValue Mapping
```rust
impl RegexpCodec {
    pub fn deserialize(&self, input: &str) -> Result<StreamRecord, SerializationError> {
        let captures = self.pattern.captures(input)
            .ok_or(SerializationError::parse_error("Pattern did not match"))?;
            
        let mut fields = HashMap::new();
        
        for (name, mapping) in &self.field_mappings {
            if let Some(captured_value) = captures.name(name) {
                let field_value = self.convert_captured_value(
                    captured_value.as_str(), 
                    &mapping.field_type
                )?;
                fields.insert(name.clone(), field_value);
            }
        }
        
        Ok(StreamRecord {
            fields,
            timestamp: self.extract_timestamp(&captures)?,
            offset: 0, // Will be set by data source
            partition: 0,
            headers: HashMap::new(),
        })
    }
    
    fn convert_captured_value(&self, value: &str, field_type: &RegexpFieldType) -> Result<FieldValue, SerializationError> {
        match field_type {
            RegexpFieldType::String => Ok(FieldValue::String(value.to_string())),
            RegexpFieldType::Integer => {
                value.parse::<i64>()
                    .map(FieldValue::Integer)
                    .map_err(|e| SerializationError::type_conversion_error(
                        format!("Failed to parse integer: {}", e),
                        "string", "integer", Some(Box::new(e))
                    ))
            },
            RegexpFieldType::ScaledInteger { scale } => {
                // Financial precision support
                let parsed = value.parse::<f64>()?;
                let scaled = (parsed * 10_f64.powi(*scale as i32)) as i64;
                Ok(FieldValue::ScaledInteger(scaled, *scale))
            },
            RegexpFieldType::Timestamp { format } => {
                let parsed = chrono::NaiveDateTime::parse_from_str(value, format)?;
                Ok(FieldValue::Timestamp(parsed))
            },
            // ... other type conversions
        }
    }
}
```

#### 2. Performance Optimizations
```rust
pub struct RegexpPerformanceOptimizer {
    compiled_patterns: LruCache<String, Regex>,
    metrics: RegexpMetrics,
}

impl RegexpPerformanceOptimizer {
    pub fn compile_with_optimizations(&mut self, pattern: &str, config: &RegexpPerformanceConfig) -> Result<Regex, regex::Error> {
        if config.cache_compiled_patterns {
            if let Some(cached) = self.compiled_patterns.get(pattern) {
                return Ok(cached.clone());
            }
        }
        
        let mut builder = RegexBuilder::new(pattern);
        builder
            .backtrack_limit(config.max_backtrack_limit)
            .build()
            .map(|regex| {
                if config.cache_compiled_patterns {
                    self.compiled_patterns.put(pattern.to_string(), regex.clone());
                }
                regex
            })
    }
    
    pub fn validate_performance(&self, pattern: &str) -> PerformanceReport {
        // Analyze pattern complexity and provide optimization suggestions
        let complexity_score = self.analyze_pattern_complexity(pattern);
        let optimization_suggestions = self.suggest_optimizations(pattern, complexity_score);
        
        PerformanceReport {
            complexity_score,
            optimization_suggestions,
            estimated_throughput: self.estimate_throughput(complexity_score),
        }
    }
}
```

## Technical Architecture

### Codec Integration
```rust
// Add to existing serialization format enum
#[derive(Debug, Clone)]
pub enum SerializationFormat {
    Json,
    Avro,
    Protobuf,
    RegexpCode,  // New format
}

// Codec factory integration
impl SerializationCodec {
    pub fn create_regexp_codec(schema: RegexpSchema) -> Result<Box<dyn SerializationCodec>, SerializationError> {
        Ok(Box::new(RegexpCodec::new(schema)?))
    }
}
```

### Kafka Integration
```yaml
# Kafka topic configuration with RegExp Code
datasource:
  type: kafka
  topic: "apache-access-logs"
  
  schema:
    format: regexp
    pattern: |
      ^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\d+)
    
    fields:
      ip: { type: string }
      timestamp: { type: timestamp, format: "%d/%b/%Y:%H:%M:%S %z" }
      method: { type: string }
      path: { type: string }
      protocol: { type: string }
      status: { type: integer }
      size: { type: integer }
```

### SQL Engine Integration
```sql
-- FerrisStreams SQL with RegExp Code source
CREATE STREAM apache_logs (
    ip STRING,
    timestamp TIMESTAMP,
    method STRING,
    path STRING,
    status INTEGER,
    size INTEGER
) WITH (
    'format' = 'regexp',
    'pattern' = '^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\d+)',
    'kafka.topic' = 'apache-access-logs'
);

-- Query with financial precision
SELECT 
    ip,
    timestamp,
    CASE WHEN status >= 400 THEN 'error' ELSE 'success' END as status_category,
    size::SCALED_INTEGER(2) as size_kb  -- Convert to KB with 2 decimal precision
FROM apache_logs
WHERE method = 'POST' 
  AND timestamp > NOW() - INTERVAL '1 HOUR';
```

## Performance Considerations

### Industry Benchmarks and Reference Implementations

The Vector project provides comprehensive regex parsing performance tests that serve as important benchmarks for this feature: [Vector Regex Parsing Performance Tests](https://github.com/vectordotdev/vector-test-harness/tree/master/cases/regex_parsing_performance). These tests demonstrate real-world performance characteristics for regex-based log parsing in high-throughput streaming scenarios.

Key insights from Vector's regex performance testing:
- **Pattern Complexity Impact**: Simple patterns (5-10 capture groups) significantly outperform complex patterns (15+ groups)
- **Compilation Overhead**: Pattern compilation time becomes critical in high-throughput scenarios
- **Memory Allocation Patterns**: Regex engines with efficient memory reuse show 2-3x better performance
- **Backtracking Limits**: Catastrophic backtracking can cause severe performance degradation

### Benchmarks and Expectations

#### Target Performance Metrics
| Scenario | Expected Throughput | Latency (P95) | Memory Usage |
|----------|-------------------|---------------|--------------|
| Simple patterns (5-10 groups) | 50K+ records/sec | <2ms | <1MB/1K records |
| Complex patterns (15-20 groups) | 15K+ records/sec | <10ms | <2MB/1K records |
| Financial precision patterns | 25K+ records/sec | <5ms | <1.5MB/1K records |

#### Optimization Strategies

Based on Vector's regex performance analysis and our streaming architecture:

1. **Pattern Compilation Caching**: Pre-compile and cache regex patterns
   - *Insight from Vector*: Compilation overhead is significant for complex patterns
   - *FerrisStreams Approach*: LRU cache with configurable size limits

2. **Backtrack Limiting**: Prevent catastrophic backtracking in complex patterns
   - *Vector Finding*: Unbounded backtracking can cause 100x+ performance degradation
   - *Our Solution*: Configurable backtrack limits with timeout handling

3. **Field Type Optimization**: Direct conversion to FieldValue types without intermediate allocations
   - *Performance Goal*: Avoid String intermediate allocations for numeric types
   - *Financial Precision*: Direct conversion to ScaledInteger for exact arithmetic

4. **Batch Processing**: Process multiple records with same pattern efficiently
   - *Vector Insight*: Amortized pattern compilation cost across batches
   - *Streaming Optimization*: Reuse compiled patterns within batch boundaries

#### Vector Test Harness Compatibility

Our implementation targets compatibility with Vector's regex parsing test suite to ensure industry-standard performance:

```rust
// Based on Vector's performance test methodology
#[cfg(test)]
mod vector_compatibility_tests {
    use super::*;
    
    #[test]
    fn test_apache_common_log_performance() {
        // Pattern from Vector's test suite
        let pattern = r#"^(?P<host>[^ ]*) [^ ]* (?P<user>[^ ]*) \[(?P<timestamp>[^\]]*)\] "(?P<method>\S+)(?: +(?P<path>[^\"]*?)(?: +\S*)?)?" (?P<status>[^ ]*) (?P<size>[^ ]*)(?: "(?P<referer>[^\"]*)" "(?P<agent>[^\"]*)")?.*$"#;
        
        let schema = create_apache_schema(pattern);
        let codec = RegexpCodec::new(schema).unwrap();
        
        // Test with Vector's sample data
        let test_logs = load_vector_test_data("apache_common.log");
        
        let start = Instant::now();
        for log_line in &test_logs {
            let _record = codec.deserialize(log_line).expect("Parse failed");
        }
        let elapsed = start.elapsed();
        
        let throughput = test_logs.len() as f64 / elapsed.as_secs_f64();
        
        // Target: Match or exceed Vector's performance benchmarks
        assert!(throughput > 30_000.0, "Throughput {} below Vector baseline", throughput);
    }
}

### Memory Management
```rust
pub struct RegexpMemoryManager {
    record_pool: ObjectPool<StreamRecord>,
    string_pool: ObjectPool<String>,
    field_map_pool: ObjectPool<HashMap<String, FieldValue>>,
}

impl RegexpMemoryManager {
    pub fn deserialize_with_pooling(&mut self, input: &str, codec: &RegexpCodec) -> Result<StreamRecord, SerializationError> {
        let mut record = self.record_pool.get_or_create();
        let mut fields = self.field_map_pool.get_or_create();
        
        // Reuse existing allocations for field parsing
        codec.deserialize_into(input, &mut record, &mut fields)?;
        
        Ok(record)
    }
}
```


## Implementation Plan

### Phase 1: Core Implementation (4 weeks)
- [ ] **Week 1**: Basic RegexpCodec implementation with named capture groups
- [ ] **Week 2**: FieldValue type conversion and validation framework
- [ ] **Week 3**: Integration with existing serialization framework
- [ ] **Week 4**: Basic performance optimizations and testing

### Phase 2: Advanced Features (3 weeks)  
- [ ] **Week 5**: Advanced field types and validation framework
- [ ] **Week 6**: Schema evolution and migration support
- [ ] **Week 7**: Advanced validation and transformation features

### Phase 3: Performance & Production (3 weeks)
- [ ] **Week 8**: Memory management and object pooling
- [ ] **Week 9**: Performance benchmarking and optimization
- [ ] **Week 10**: Production testing and documentation

### Phase 4: Documentation & Community (2 weeks)
- [ ] **Week 11**: Complete documentation and examples
- [ ] **Week 12**: Pattern library and community integration

## Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_apache_log_parsing() {
        let schema = RegexpSchema {
            pattern: r#"^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" (?P<status>\d+) (?P<size>\d+)"#.to_string(),
            fields: create_apache_log_fields(),
            // ...
        };
        
        let codec = RegexpCodec::new(schema).unwrap();
        let log_line = r#"192.168.1.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#;
        
        let record = codec.deserialize(log_line).unwrap();
        
        assert_eq!(record.fields.get("ip").unwrap(), &FieldValue::String("192.168.1.1".to_string()));
        assert_eq!(record.fields.get("status").unwrap(), &FieldValue::Integer(200));
        assert_eq!(record.fields.get("size").unwrap(), &FieldValue::Integer(2326));
    }
    
    #[test]
    fn test_financial_precision_parsing() {
        let schema = RegexpSchema {
            pattern: r"(?P<symbol>[A-Z]{3,5}),(?P<price>\d+\.\d{4}),(?P<volume>\d+)".to_string(),
            fields: hashmap! {
                "price".to_string() => FieldMapping {
                    field_type: RegexpFieldType::ScaledInteger { scale: 4 },
                    // ...
                }
            },
        };
        
        let codec = RegexpCodec::new(schema).unwrap();
        let trade_data = "AAPL,150.2500,1000";
        
        let record = codec.deserialize(trade_data).unwrap();
        
        // Verify exact financial precision
        assert_eq!(record.fields.get("price").unwrap(), &FieldValue::ScaledInteger(1502500, 4));
    }
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_kafka_regexp_integration() {
    let kafka_config = KafkaSourceConfig {
        topic: "test-apache-logs".to_string(),
        schema: SerializationConfig::RegexpCode(regexp_schema()),
        // ...
    };
    
    let mut reader = KafkaDataReader::new(kafka_config).await.unwrap();
    
    // Test full pipeline with regexp deserialization
    let records = reader.read().await.unwrap();
    assert!(!records.is_empty());
    
    // Verify field extraction worked correctly
    let first_record = &records[0];
    assert!(first_record.fields.contains_key("ip"));
    assert!(first_record.fields.contains_key("status"));
}
```

### Performance Tests
```rust
#[test]
fn benchmark_regexp_throughput() {
    let codec = create_apache_log_codec();
    let test_data = generate_apache_log_lines(10000);
    
    let start = Instant::now();
    let mut processed = 0;
    
    for line in &test_data {
        let _record = codec.deserialize(line).unwrap();
        processed += 1;
    }
    
    let elapsed = start.elapsed();
    let throughput = processed as f64 / elapsed.as_secs_f64();
    
    println!("Throughput: {:.0} records/second", throughput);
    assert!(throughput > 25_000.0, "Throughput below target: {}", throughput);
}
```

## Documentation Plan

### User Documentation
- **Getting Started Guide**: Basic RegExp Code usage with common examples
- **Schema Reference**: Complete field type and validation options
- **Performance Tuning Guide**: Optimization best practices and troubleshooting
- **Flink Integration Guide**: Compatibility patterns and migration strategies

### Developer Documentation
- **API Reference**: Complete RegexpCodec API documentation
- **Extension Guide**: How to add custom field types and transformations
- **Performance Internals**: Memory management and optimization details
- **Testing Guide**: How to test RegExp Code schemas effectively

## Risk Assessment

### High Risk Items
1. **Regex Complexity**: Complex patterns could cause performance degradation
2. **Flink Compatibility**: Java Pattern syntax differences may cause issues
3. **Memory Usage**: Large capture groups could increase memory consumption

### Mitigation Strategies
1. **Pattern Validation**: Analyze and warn about complex patterns during schema validation
2. **Performance Testing**: Comprehensive test suite against Vector benchmarks  
3. **Memory Monitoring**: Built-in memory usage tracking and alerting

### Success Criteria
- [ ] Parse common log formats at 25K+ records/second
- [ ] Maintain <10ms P95 latency for typical patterns
- [ ] Match or exceed Vector test harness performance benchmarks
- [ ] Memory usage <2MB per 1K records for complex patterns
- [ ] Zero precision loss for financial data parsing
- [ ] Seamless integration with existing FerrisStreams codec framework

## Future Enhancements

### Planned Features
1. **Schema Evolution**: Support for pattern migration and versioning
2. **Advanced Validation**: Custom validation functions and cross-field dependencies
3. **Machine Learning Integration**: Automatic pattern discovery from sample data
4. **Multi-line Support**: Support for multi-line record patterns (e.g., stack traces)

### Community Integration
1. **Pattern Library**: Curated collection of common patterns (Apache logs, nginx, syslog)
2. **Schema Registry**: Share and discover regex patterns for common log formats  
3. **IDE Support**: Language server and IDE extensions for pattern development

---

**RFC Feedback Period**: 30 days  
**Implementation Target**: FerrisStreams v0.3.0  
**Maintainer**: @ferris-streams/core-team  

Please provide feedback on:
1. Codec API design and usability
2. Performance requirements and expectations  
3. Integration with existing serialization framework
4. Additional use cases and pattern requirements

---

*This feature request is part of the FerrisStreams performance and compatibility enhancement initiative.*