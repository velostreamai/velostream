# TODO: Work In Progress Items

This document tracks current work-in-progress items and technical debt that needs to be addressed in the FerrisStreams project.

## 🔧 Multi-Job SQL Server Issues

### High Priority

- [ ] **Test Multi-Job SQL Server Functionality**
  - Verify `ferris-sql-multi` server starts correctly
  - Test job deployment and execution
  - Validate job lifecycle management (start/stop/pause/resume)
  - Test concurrent job execution
  - Verify job isolation and resource management

- [ ] **Implement Batch Processing Support**
  - Add batch size configuration for multi-job server
  - Implement batching logic in datasource reading
  - Add batch timeout support
  - Test batch vs streaming performance characteristics
  - Document batch processing configuration options

- [ ] **Implement Exactly-Once Semantics**
  - Add transactional support with commit-only-on-success
  - Implement proper offset management for Kafka sources
  - Add rollback capability on processing failures
  - Test failure scenarios and recovery
  - Validate exactly-once delivery guarantees
  - Add configuration for delivery semantics (at-least-once vs exactly-once)

### Medium Priority

- [ ] **Enhance Error Handling and Recovery**
  - Improve error propagation from datasource readers
  - Add circuit breaker patterns for failing datasources
  - Implement retry logic with exponential backoff
  - Add dead letter queue support for failed records
  - Test error scenarios and recovery mechanisms

- [ ] **Kafka Consumer Performance Optimization**
  - Replace consumer.poll() with stream-based consumption
  - Benchmark performance difference between poll() and streams
  - Implement async stream processing for improved throughput
  - Measure CPU and memory usage improvements
  - Test backpressure handling with stream approach

## ⚡ Performance Analysis & Optimization

### Critical Performance Items

- [ ] **Analyze StreamRecord → InternalValue Conversion Performance**
  - **Current Implementation**: Uses `FieldValueConverter::field_value_to_internal()`
  - **Questions to Answer**:
    - Is the conversion zero-copy or does it allocate new memory?
    - What is the per-record conversion overhead in nanoseconds?
    - How does it scale with record size and field count?
    - Can we optimize for common field types (String, Integer, Float)?
  - **Action Items**:
    - Create micro-benchmarks for conversion performance
    - Profile memory allocations during conversion
    - Compare with direct memory mapping approaches
    - Identify bottlenecks and optimization opportunities
  - **Target**: <100ns per field conversion, zero-copy for large strings/bytes

- [ ] **Benchmark Multi-Job Server vs Single-Job Performance**
  - Compare resource usage (CPU, memory) per job
  - Measure job isolation overhead
  - Test concurrent job performance scaling
  - Analyze context switching and scheduling overhead
  - Document performance characteristics and recommendations

### Memory Management Analysis

- [ ] **Profile Memory Usage Patterns**
  - Analyze memory allocation patterns in datasource reading loop
  - Identify potential memory leaks in long-running jobs
  - Test garbage collection pressure under high load
  - Measure memory overhead per active job
  - Optimize memory pool usage for record processing

- [ ] **Zero-Copy Optimization Opportunities**
  - Investigate zero-copy paths for large field values
  - Analyze string/bytes field handling for copy avoidance
  - Profile serialization/deserialization allocations
  - Consider memory mapping for file-based sources
  - Benchmark against other streaming systems (Kafka Streams, Flink)

## 🏗️ Architecture & Technical Debt

### Code Quality

- [ ] **Refactor Large Functions**
  - `src/bin/ferris-sql-multi.rs`: Job deployment logic is still complex
  - `src/ferris/sql/multi_job.rs`: Consider splitting process_datasource_records
  - Extract configuration building into dedicated structs
  - Improve error types and error context

- [ ] **Improve Test Coverage**
  - Add integration tests for multi-job server endpoints
  - Test concurrent job scenarios
  - Add performance regression tests
  - Test failure scenarios and edge cases
  - Mock datasource tests for unit testing

### Configuration & Deployment

- [ ] **Enhanced Configuration Management**
  - Support for job-specific configuration overrides
  - Environment-based configuration profiles
  - Configuration validation and schema enforcement
  - Hot-reload configuration support
  - Configuration templating and inheritance

- [ ] **Production Readiness**
  - Add comprehensive logging with structured output
  - Implement health checks and readiness probes
  - Add metrics export (Prometheus, OpenTelemetry)
  - Document deployment best practices
  - Add resource usage monitoring and alerting

## 📊 Specific Performance Investigations

### Conversion Performance Analysis

**Current Code Path**:
```rust
// In src/ferris/sql/multi_job.rs:257-262
let record_fields: HashMap<String, InternalValue> = record
    .fields
    .into_iter()
    .map(|(k, v)| (k, FieldValueConverter::field_value_to_internal(v)))
    .collect();
```

**Performance Questions**:
1. **Memory Allocation**: Does `FieldValueConverter::field_value_to_internal()` allocate?
2. **Copy Semantics**: Are string/bytes fields copied or referenced?
3. **HashMap Overhead**: Is HashMap the most efficient container for record fields?
4. **Batching Opportunity**: Can we process multiple records in a batch to amortize costs?

**Investigation Tasks**:
- [x] Create benchmark comparing conversion approaches:
  - ✅ Current FieldValueConverter performance measured
  - [ ] Direct field mapping alternatives
  - [ ] Batch conversion optimization
  - [ ] Zero-copy alternatives investigation
- [x] Profile with different record sizes (10 fields vs 100 fields)
- [x] Test with different field types (primitives vs large strings)
- [ ] Measure impact on end-to-end query latency

**✅ PERFORMANCE ANALYSIS RESULTS** (Completed August 28, 2025):

**Per-Field Conversion Performance**:
- **Current**: ~520ns per field (5x slower than target)
- **Target**: <100ns per field
- **Finding**: HashMap collection overhead dominates individual conversion cost

**Individual Type Performance**:
- String: 42.3ns per conversion
- Integer: 12.1ns per conversion  
- Float: 12.3ns per conversion
- ScaledInteger: 12.3ns per conversion

**Key Insights**:
1. **Individual conversions are fast** - meet performance targets
2. **HashMap collection creates overhead** - 5x performance hit
3. **String conversions need allocation** - not zero-copy
4. **Scaling is roughly linear** - 516ns/field regardless of record size

**🚨 OPTIMIZATION OPPORTUNITIES**:
1. **Replace HashMap collection** with direct field array processing
2. **Implement batch processing** to amortize collection overhead
3. **Consider zero-copy string handling** for large payloads
4. **Profile end-to-end latency impact** on real workloads

### Exactly-Once Implementation Strategy

**Current Gap**: Multi-job server processes records but doesn't guarantee exactly-once semantics.

**Requirements**:
- [ ] **Offset Management**: Proper Kafka offset handling per job
- [ ] **Transactional Processing**: Atomic record processing with rollback
- [ ] **Idempotency**: Handle duplicate record processing gracefully
- [ ] **State Management**: Persist job processing state for recovery
- [ ] **Error Handling**: Distinguish between retryable and fatal errors

**Implementation Approach**:
1. Add transaction boundaries around record processing
2. Implement checkpointing for processing progress
3. Add configuration for exactly-once vs at-least-once modes
4. Test with simulated failures and network partitions

## 📈 Success Criteria

### Performance Targets
- **Conversion Overhead**: <100ns per field (current: unknown)
- **Multi-Job Throughput**: >10K records/sec per job (current: unknown)
- **Memory Efficiency**: <100MB per active job (current: unknown)
- **Exactly-Once Latency**: <10ms additional latency for guarantees

### Reliability Targets
- **Job Uptime**: 99.9% availability during normal operations
- **Recovery Time**: <30s job recovery after failures
- **Data Consistency**: 100% exactly-once delivery when enabled
- **Resource Isolation**: No job can affect others' performance

## 🗓️ Priority Timeline

### Week 1: Performance Analysis
- [ ] Set up conversion performance benchmarks
- [ ] Profile current multi-job server performance
- [ ] Identify top 3 performance bottlenecks

### Week 2: Multi-Job Server Functionality
- [ ] Test and fix multi-job server basic operations
- [ ] Implement basic batch processing support
- [ ] Add comprehensive error handling

### Week 3: Exactly-Once Semantics
- [ ] Design exactly-once architecture
- [ ] Implement transactional processing
- [ ] Add offset management and checkpointing

### Week 4: Integration & Testing
- [ ] Integration testing of all features
- [ ] Performance regression testing
- [ ] Documentation and deployment guides

---

**Last Updated**: August 28, 2025  
**Status**: Initial creation - items need investigation and prioritization