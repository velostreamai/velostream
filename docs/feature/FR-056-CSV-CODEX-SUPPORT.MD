# Feature Request FR-056: CSV Codec Support

**Status:** Draft  
**Priority:** Medium-High  
**Complexity:** Medium  
**Estimated Effort:** 2-3 weeks  
**Target Version:** v1.2.0  

## Executive Summary

Replace the current basic CSV string-splitting implementation with a production-grade CSV codec that provides RFC 4180 compliance, automatic schema inference, financial precision support, and enhanced error handling. This enhancement will significantly improve data quality, performance, and production readiness for financial CSV processing.

## Problem Statement

### Current Limitations

The existing CSV file processing in `src/ferris/datasource/file/reader.rs` uses basic string splitting:

```rust
// Current implementation (line 226)
let fields: Vec<&str> = line.split(self.config.csv_delimiter).collect();
// Comment: "Simple CSV parsing - in production, use csv crate"
```

**Critical Issues:**
- ❌ **No RFC 4180 compliance** - Cannot handle standard CSV edge cases
- ❌ **No escape sequence handling** - Fails on quoted fields with delimiters
- ❌ **No multi-line field support** - Cannot parse fields spanning multiple lines
- ❌ **Manual type conversion** - Inefficient downstream type inference
- ❌ **Poor error reporting** - Limited debugging information for malformed data
- ❌ **Performance bottlenecks** - String allocation overhead in hot paths

### Business Impact

For financial data processing, these limitations create:
- **Data corruption risks** from malformed CSV parsing
- **Performance degradation** in high-throughput scenarios
- **Type inference delays** affecting SQL query execution
- **Production deployment blockers** for enterprise customers

## Proposed Solution

### Overview

Implement a comprehensive CSV codec (`CsvCodec`) that integrates with FerrisStreams' existing serialization architecture, providing:

1. **RFC 4180 Compliant Parsing**
2. **Automatic Schema Inference** 
3. **Financial Precision Detection**
4. **Enhanced Error Reporting**
5. **Performance Optimizations**

### Technical Architecture

```
src/ferris/serialization/
├── csv_codec.rs              # Main CSV codec implementation
├── csv_config.rs             # Configuration structures  
├── csv_inference.rs          # Schema inference logic
└── csv_financial.rs          # Financial data detection
```

### Core Components

#### 1. CsvCodec Implementation

```rust
pub struct CsvCodec {
    // RFC 4180 compliant parser
    reader_builder: AsyncReaderBuilder,
    
    // Schema inference
    headers: Vec<String>,
    type_mapping: HashMap<String, DataType>,
    metadata: Option<csv_sniffer::Metadata>,
    
    // FerrisStreams integration
    financial_precision: bool,
    timestamp_formats: Vec<String>,
    error_recovery: CsvErrorRecovery,
}

#[async_trait]
impl SerializationCodec for CsvCodec {
    async fn deserialize(&self, data: &[u8]) -> Result<StreamRecord, SerializationError>;
    async fn serialize(&self, record: &StreamRecord) -> Result<Vec<u8>, SerializationError>;
    fn get_schema(&self) -> Result<Schema, SerializationError>;
}
```

#### 2. Financial-Aware Type Detection

```rust
impl CsvCodec {
    fn detect_financial_fields(&self, headers: &[String]) -> HashMap<String, FinancialType> {
        // Automatic detection of:
        // - Monetary amounts (price, cost, amount, fee, etc.)
        // - Percentages (rate, percent, ratio, etc.) 
        // - Quantities (volume, shares, units, etc.)
        // - Account numbers (account, id with patterns)
    }
    
    fn convert_financial_field(&self, value: &str, field_type: FinancialType) -> FieldValue {
        match field_type {
            FinancialType::MonetaryAmount => {
                // Remove currency symbols, parse as ScaledInteger
                let cleaned = value.replace(['$', '€', '£', ','], "");
                let amount = cleaned.parse::<f64>().unwrap() * 10000.0; // 4 decimal places
                FieldValue::ScaledInteger(amount as i64, 4)
            },
            FinancialType::Percentage => {
                // Handle percentage values with proper scaling
            },
            // ... other financial types
        }
    }
}
```

#### 3. Enhanced Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum CsvError {
    #[error("CSV parse error at line {line}, column {column}: {message}")]
    ParseError {
        line: usize,
        column: usize, 
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Schema inference failed: {message}")]
    SchemaInferenceError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Type conversion error for field '{field}': {message}")]
    TypeConversionError {
        field: String,
        message: String,
        from_type: String,
        to_type: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}
```

### Configuration Extensions

#### Enhanced FileSourceConfig

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvCodecConfig {
    // RFC 4180 compliance
    pub delimiter: char,
    pub quote: char, 
    pub escape: Option<char>,
    pub has_headers: bool,
    
    // Schema inference
    pub auto_infer_schema: bool,
    pub sample_size_for_inference: usize,
    pub confidence_threshold: f64,
    
    // Financial data processing
    pub enable_financial_detection: bool,
    pub currency_symbols: Vec<String>,
    pub decimal_separator: char,
    pub thousands_separator: Option<char>,
    
    // Performance tuning
    pub buffer_size: usize,
    pub enable_simd: bool,
    pub parallel_parsing: bool,
    
    // Error handling
    pub error_recovery: CsvErrorRecovery,
    pub max_parse_errors: Option<usize>,
    pub skip_malformed_records: bool,
}

#[derive(Debug, Clone)]
pub enum CsvErrorRecovery {
    Strict,           // Fail on first error
    SkipRecord,       // Skip malformed records, continue processing  
    BestEffort,       // Attempt to salvage partial data
    DeadLetterQueue,  // Route errors to DLQ for manual review
}
```

## Implementation Plan

### Phase 1: Foundation (Week 1)

**Deliverables:**
- [ ] `CsvCodec` struct and basic RFC 4180 parsing
- [ ] Integration with existing `SerializationCodec` trait
- [ ] Enhanced `SerializationError::CsvError` variants
- [ ] Unit tests for basic CSV parsing scenarios

**Acceptance Criteria:**
- Parse standard CSV files correctly
- Handle quoted fields with embedded delimiters
- Process multi-line quoted fields
- 100% compatibility with existing `FileReader` interface

### Phase 2: Schema Inference (Week 2)

**Deliverables:**
- [ ] Automatic column type detection using `csv-sniffer`
- [ ] Smart financial field recognition
- [ ] `ScaledInteger` conversion for monetary values
- [ ] Configurable confidence thresholds
- [ ] Schema validation and correction

**Acceptance Criteria:**
- Correctly identify data types for 95% of standard financial CSV formats
- Automatically detect monetary fields and convert to `ScaledInteger`
- Generate accurate `Schema` objects for downstream SQL processing
- Handle ambiguous type scenarios gracefully

### Phase 3: Performance & Production Features (Week 3)

**Deliverables:**
- [ ] Streaming parser for large files
- [ ] Memory pool optimization for `StreamRecord` allocation  
- [ ] Error recovery mechanisms and dead letter queue support
- [ ] Comprehensive benchmarking and performance testing
- [ ] Production deployment documentation

**Acceptance Criteria:**
- 2-3x performance improvement over current string-splitting approach
- Memory usage within 10% of current implementation
- Handle files with millions of records without memory pressure
- Comprehensive error reporting for debugging malformed data

## Dependencies

### External Crates

```toml
[dependencies]
csv = "1.3"              # RFC 4180 compliant CSV parsing
csv-async = "1.2"        # Async streaming support  
csv-sniffer = "0.2"      # Automatic schema inference
chrono = { version = "0.4", features = ["serde"] }  # Date/time parsing
thiserror = "1.0"        # Enhanced error types
```

### Internal Integration Points

- **SerializationCodec trait** - Implement CSV codec interface
- **FileReader** - Replace string splitting with codec calls  
- **SerializationError** - Add CSV-specific error variants
- **Schema inference** - Integrate with existing schema system
- **FieldValue types** - Enhanced type conversion logic

## Performance Expectations

### Benchmarks (Target Metrics)

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| **Parse Speed** | 50MB/s | 150MB/s | 3x faster |
| **Memory Usage** | Baseline | -20% | Lower allocation |
| **Type Inference** | Manual | Automatic | 100% coverage |
| **Error Detection** | Basic | Detailed | Full context |

### Load Testing Scenarios

- **Small Files**: 1K-100K records, latency-sensitive
- **Large Files**: 1M+ records, throughput-optimized  
- **Real-time Streaming**: Continuous file watching with sub-second updates
- **Malformed Data**: Graceful handling of corrupted CSV files

## Risk Assessment

### Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| **Breaking Changes** | High | Low | Maintain 100% API compatibility |
| **Performance Regression** | Medium | Low | Comprehensive benchmarking |
| **Memory Usage** | Medium | Medium | Memory profiling and optimization |
| **Type Inference Accuracy** | Medium | Medium | Configurable confidence thresholds |

### Operational Risks

- **Production Deployment**: Staged rollout with feature flags
- **Data Migration**: Backward compatibility with existing CSV files
- **Error Handling**: Graceful degradation for malformed data

## Testing Strategy

### Unit Tests
- RFC 4180 compliance test suite
- Financial data type detection
- Error handling and recovery scenarios
- Schema inference accuracy

### Integration Tests  
- End-to-end CSV file processing
- Performance benchmarking
- Memory usage profiling
- Error recovery workflows

### Compatibility Tests
- Existing CSV file format compatibility
- FileReader API compatibility  
- SerializationCodec trait compliance

## Success Metrics

### Functional Requirements
- [ ] 100% RFC 4180 compliance
- [ ] Automatic schema inference for 95% of financial CSV formats
- [ ] Zero breaking changes to existing API
- [ ] Enhanced error reporting with full context

### Performance Requirements  
- [ ] 3x parsing speed improvement
- [ ] 20% reduction in memory usage
- [ ] Sub-millisecond latency for small files
- [ ] Sustained throughput for large files

### Quality Requirements
- [ ] 95% test coverage
- [ ] Zero critical bugs in production
- [ ] Comprehensive documentation and examples
- [ ] Performance benchmarking suite

## Future Enhancements

### Version 1.3.0+
- **SIMD optimizations** for delimiter detection
- **Parallel CSV parsing** for multi-core systems  
- **Column-based storage** integration
- **Custom type plugins** for domain-specific parsing

### Long-term Vision
- **Machine learning** type inference
- **Advanced financial data** recognition (CUSIP, ISIN, etc.)
- **Real-time schema evolution** detection
- **Integration** with schema registries

## References

- [RFC 4180: Common Format and MIME Type for CSV Files](https://tools.ietf.org/html/rfc4180)
- [csv crate documentation](https://docs.rs/csv/)
- [csv-sniffer crate documentation](https://docs.rs/csv-sniffer/)
- [FerrisStreams SerializationCodec Architecture](../architecture/SERIALIZATION_ARCHITECTURE.md)
- [Financial Precision Implementation Guide](../guides/FINANCIAL_PRECISION_GUIDE.md)

---

**Created:** 2025-01-03  
**Author:** Development Team  
**Reviewers:** Architecture Team, Performance Team  
**Approval:** Pending