# Architectural Decoupling Plan: Preparing for Pluggable Data Sources

**Duration**: 1-2 weeks  
**Priority**: Critical  
**Purpose**: Decouple Kafka from core SQL engine to enable pluggable data sources  
**Target**: Zero breaking changes, full backward compatibility

---

## 📋 Week 1: Core Decoupling (Days 1-5)

### **Day 1: Audit & Analysis** ✅ **COMPLETED**
**Goal**: Identify all Kafka coupling points

#### Morning (4 hours) ✅ **COMPLETED**
- ✅ **Audit Kafka Dependencies**
  ```bash
  # Found 23 files with Kafka references
  # Most are in /kafka/ module (expected)
  # Core SQL engine is already clean!
  ```
  
- ✅ **Document Coupling Points**
  - ✅ Processors have NO direct Kafka dependencies
  - ✅ Schema assumptions are generic (StreamRecord)
  - ✅ ProcessorContext is already source-agnostic
  - ✅ Only peripheral components need updates

#### Afternoon (4 hours) ✅ **COMPLETED**
- ✅ **Create Dependency Map**
  ```
  SQL Engine Core ✅ CLEAN
  ├── Parser (Generic StreamRecord ✓)
  ├── Processors
  │   ├── SelectProcessor → No Kafka ✅
  │   ├── InsertProcessor → No Kafka ✅
  │   └── JoinProcessor → ProcessorContext (Clean) ✅
  ├── Execution Engine → Minimal coupling (comments only) ✅
  └── Types (Fully generic) ✅
  ```

- ✅ **Impact Assessment Document**
  - ✅ Core components need NO refactoring
  - ✅ Risk assessment: LOW (additive changes only)
  - ✅ Testing: Existing tests should pass unchanged

**Deliverable**: ✅ `KAFKA_COUPLING_AUDIT.md` completed

**Key Discovery**: 🎉 **Architecture is already 90% ready for pluggable sources!**

---

### **Day 2: Define Abstraction Layer** ✅ **COMPLETED**
**Goal**: Design traits and interfaces for heterogeneous data sources

#### Morning (4 hours) ✅ **COMPLETED**
- ✅ **Create Core Traits (Separate Input/Output)**
  ```rust
  // IMPLEMENTED: src/ferris/sql/datasource/mod.rs ✅
  
  // ✅ Created DataSource trait for input sources
  pub trait DataSource: Send + Sync + 'static {
      type Error: Error + Send + Sync + 'static;
      
      async fn initialize(&mut self, config: SourceConfig) -> Result<(), Self::Error>;
      async fn fetch_schema(&self) -> Result<Schema, Self::Error>;
      async fn create_reader(&self) -> Result<Box<dyn DataReader>, Self::Error>;
      fn supports_streaming(&self) -> bool;
      fn supports_batch(&self) -> bool;
      fn metadata(&self) -> SourceMetadata;
  }
  
  // ✅ Created DataSink trait for output destinations
  pub trait DataSink: Send + Sync + 'static {
      type Error: Error + Send + Sync + 'static;
      
      async fn initialize(&mut self, config: SinkConfig) -> Result<(), Self::Error>;
      async fn validate_schema(&self, schema: &Schema) -> Result<(), Self::Error>;
      async fn create_writer(&self) -> Result<Box<dyn DataWriter>, Self::Error>;
      fn supports_transactions(&self) -> bool;
      fn supports_upsert(&self) -> bool;
      fn metadata(&self) -> SinkMetadata;
  }
  
  // ✅ Created DataReader trait for consuming from any source
  pub trait DataReader: Send + Sync {
      async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>>;
      async fn read_batch(&mut self, size: usize) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>>;
      async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>>;
  }
  
  // ✅ Created DataWriter trait for publishing to any sink
  pub trait DataWriter: Send + Sync {
      async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn write_batch(&mut self, records: Vec<StreamRecord>) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn update(&mut self, key: &str, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
      async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
  }
  ```

#### Afternoon (4 hours) ✅ **COMPLETED**
- ✅ **Design Configuration System**
  ```rust
  // IMPLEMENTED: src/ferris/sql/datasource/config.rs ✅
  
  // ✅ Created comprehensive SourceConfig enum
  pub enum SourceConfig {
      Kafka { brokers: String, topic: String, group_id: String, ... },
      File { path: String, format: FileFormat, watch: bool, ... },
      S3 { bucket: String, prefix: String, region: String, ... },
      Database { connection_string: String, table: String, ... },
      Iceberg { catalog_uri: String, namespace: String, table: String },
      Custom(HashMap<String, String>),
  }
  
  // ✅ Created ConnectionString parser with full URI support  
  pub struct ConnectionString {
      pub scheme: String,
      pub host: Option<String>,
      pub port: Option<u16>,
      pub path: String,
      pub query_params: HashMap<String, String>,
  }
  
  impl ConnectionString {
      pub fn parse(uri: &str) -> Result<Self, ConfigError> // ✅ IMPLEMENTED
      pub fn to_source_config(&self) -> Result<SourceConfig, ConfigError> // ✅ IMPLEMENTED
      pub fn to_sink_config(&self) -> Result<SinkConfig, ConfigError> // ✅ IMPLEMENTED
  }
  ```

- ✅ **Schema Management Interface**
  ```rust
  // ✅ Added to DataSource/DataSink traits:
  async fn fetch_schema(&self) -> Result<Schema, Self::Error>;
  async fn validate_schema(&self, schema: &Schema) -> Result<(), Self::Error>;
  
  // ✅ Created metadata system for capabilities discovery
  pub struct SourceMetadata {
      pub source_type: String,
      pub version: String,
      pub supports_streaming: bool,
      pub supports_batch: bool,
      pub supports_schema_evolution: bool,
      pub capabilities: Vec<String>,
  }
  ```

- ✅ **Registry System Created**
  ```rust
  // IMPLEMENTED: src/ferris/sql/datasource/registry.rs ✅
  pub struct DataSourceRegistry {
      source_factories: HashMap<String, SourceFactory>,
      sink_factories: HashMap<String, SinkFactory>,
  }
  
  // ✅ Factory pattern with URI-based creation
  pub fn create_source(uri: &str) -> Result<Box<dyn DataSource<Error = ...>>> // ✅ IMPLEMENTED
  pub fn create_sink(uri: &str) -> Result<Box<dyn DataSink<Error = ...>>> // ✅ IMPLEMENTED
  ```

**Deliverable**: ✅ Complete datasource abstraction layer with:
- ✅ `src/ferris/sql/datasource/mod.rs` - Core traits 
- ✅ `src/ferris/sql/datasource/config.rs` - Configuration system
- ✅ `src/ferris/sql/datasource/registry.rs` - Factory registry
- ✅ Full compilation success with comprehensive tests

---

### **Day 3: Implement Kafka Adapter**
**Goal**: Wrap existing Kafka code with new traits

#### Morning (4 hours)
- [ ] **Create Kafka DataSource Implementation**
  ```rust
  // src/ferris/sql/datasource/kafka/mod.rs
  pub struct KafkaDataSource {
      brokers: String,
      config: HashMap<String, String>,
  }
  
  impl DataSource for KafkaDataSource {
      type Record = KafkaRecord;
      type Error = KafkaError;
      
      async fn create_consumer(&self) -> Result<Box<dyn DataConsumer>> {
          // Wrap existing KafkaConsumer
          Ok(Box::new(KafkaConsumerAdapter::new(&self.brokers)?))
      }
  }
  ```

#### Afternoon (4 hours)
- [ ] **Implement Consumer/Producer Adapters**
  ```rust
  struct KafkaConsumerAdapter {
      inner: KafkaConsumer,
  }
  
  impl DataConsumer for KafkaConsumerAdapter {
      async fn poll(&mut self) -> Result<Option<StreamRecord>> {
          // Adapt existing poll logic
      }
  }
  ```

- [ ] **Add Backward Compatibility Layer**
  ```rust
  // Ensure old code still works
  pub fn create_kafka_source(config: &Config) -> Box<dyn DataSource> {
      Box::new(KafkaDataSource::from_legacy_config(config))
  }
  ```

**Deliverable**: `src/ferris/sql/datasource/kafka/` fully implemented

---

### **Day 4: Refactor ProcessorContext**
**Goal**: Support heterogeneous input/output in ProcessorContext

#### Morning (4 hours)
- [ ] **Abstract ProcessorContext for Mixed Sources**
  ```rust
  // Before (coupled to Kafka)
  pub struct ProcessorContext {
      kafka_consumer: Option<KafkaConsumer>,
      kafka_producer: Option<KafkaProducer>,
  }
  
  // After (heterogeneous source/sink support)
  pub struct ProcessorContext {
      // Multiple input sources (e.g., Kafka + S3)
      data_readers: HashMap<String, Box<dyn DataReader>>,
      // Multiple output sinks (e.g., Iceberg + Kafka)
      data_writers: HashMap<String, Box<dyn DataWriter>>,
      // Active source/sink for current operation
      active_reader: Option<String>,
      active_writer: Option<String>,
  }
  ```

- [ ] **Update Context Methods for Multi-Source**
  ```rust
  impl ProcessorContext {
      // Read from specific source
      pub async fn read_from(&mut self, source: &str) -> Result<Option<StreamRecord>> {
          let reader = self.data_readers.get_mut(source)
              .ok_or(SqlError::SourceNotFound(source.to_string()))?;
          reader.read().await
      }
      
      // Write to specific sink
      pub async fn write_to(&mut self, sink: &str, record: StreamRecord) -> Result<()> {
          let writer = self.data_writers.get_mut(sink)
              .ok_or(SqlError::SinkNotFound(sink.to_string()))?;
          writer.write(record).await
      }
      
      // Read from active source
      pub async fn read(&mut self) -> Result<Option<StreamRecord>> {
          let source = self.active_reader.as_ref()
              .ok_or(SqlError::NoActiveSource)?;
          self.read_from(source).await
      }
      
      // Write to active sink
      pub async fn write(&mut self, record: StreamRecord) -> Result<()> {
          let sink = self.active_writer.as_ref()
              .ok_or(SqlError::NoActiveSink)?;
          self.write_to(sink, record).await
      }
  }
  ```

#### Afternoon (4 hours)
- [ ] **Update All Processors**
  - SelectProcessor: Use context.read_record()
  - InsertProcessor: Use context.write_record()
  - UpdateProcessor: Use context abstractions
  - DeleteProcessor: Use context abstractions
  - JoinProcessor: Use context for both sides

**Deliverable**: All processors using abstracted ProcessorContext

---

### **Day 5: Integration & Testing**
**Goal**: Ensure everything works with abstractions

#### Morning (4 hours)
- [ ] **Update SQL Engine**
  ```rust
  impl SqlEngine {
      pub async fn execute_with_source(
          &mut self,
          query: &str,
          source: Box<dyn DataSource>,
      ) -> Result<QueryResult> {
          let context = ProcessorContext::new_with_source(source);
          // Execute query with new context
      }
  }
  ```

- [ ] **Add Source Registry**
  ```rust
  pub struct DataSourceRegistry {
      sources: HashMap<String, Box<dyn Fn() -> Box<dyn DataSource>>>,
  }
  
  impl DataSourceRegistry {
      pub fn register(&mut self, scheme: &str, factory: impl Fn() -> Box<dyn DataSource>) {
          self.sources.insert(scheme.to_string(), Box::new(factory));
      }
      
      pub fn create_from_uri(&self, uri: &str) -> Result<Box<dyn DataSource>> {
          let parsed = ConnectionString::parse(uri)?;
          let factory = self.sources.get(&parsed.scheme)
              .ok_or(Error::UnknownScheme)?;
          Ok(factory())
      }
  }
  ```

#### Afternoon (4 hours)
- [ ] **Run Comprehensive Tests**
  ```bash
  # Ensure no regressions
  cargo test --all-features
  cargo test --no-default-features
  
  # Run integration tests
  cargo test integration::
  ```

- [ ] **Performance Validation**
  - Ensure no performance degradation
  - Measure overhead of abstraction layer
  - Verify memory usage is unchanged

**Deliverable**: All tests passing with new abstractions

---

## 📋 Week 2: Advanced Features & Polish (Days 6-10)

### **Day 6: Schema Management System**
**Goal**: Implement source-agnostic schema handling

#### Morning (4 hours)
- [ ] **Create Schema Registry**
  ```rust
  pub struct SchemaRegistry {
      schemas: HashMap<String, SchemaInfo>,
      providers: Vec<Box<dyn SchemaProvider>>,
  }
  
  impl SchemaRegistry {
      pub async fn discover(&mut self, source: &str) -> Result<SchemaInfo> {
          // Try each provider until one succeeds
          for provider in &self.providers {
              if let Ok(schema) = provider.discover_schema(source).await {
                  self.schemas.insert(source.to_string(), schema.clone());
                  return Ok(schema);
              }
          }
          Err(Error::SchemaNotFound)
      }
  }
  ```

#### Afternoon (4 hours)
- [ ] **Implement Schema Evolution**
  ```rust
  pub trait SchemaEvolution {
      fn can_evolve(&self, from: &SchemaInfo, to: &SchemaInfo) -> bool;
      fn evolve(&self, record: StreamRecord, target: &SchemaInfo) -> Result<StreamRecord>;
  }
  ```

- [ ] **Add Schema Caching**
  - TTL-based cache invalidation
  - Schema version tracking
  - Backward/forward compatibility checks

**Deliverable**: Complete schema management system

---

### **Day 7: Error Handling & Recovery**
**Goal**: Create unified error handling for all sources

#### Morning (4 hours)
- [ ] **Define Source-Agnostic Errors**
  ```rust
  pub enum DataSourceError {
      ConnectionFailed(String),
      SchemaError(String),
      ReadError(String),
      WriteError(String),
      Timeout(Duration),
      Recoverable(Box<dyn Error>),
      Fatal(Box<dyn Error>),
  }
  ```

- [ ] **Implement Retry Logic**
  ```rust
  pub struct RetryPolicy {
      max_retries: u32,
      backoff: BackoffStrategy,
      timeout: Duration,
  }
  
  impl DataConsumer for RetryableConsumer {
      async fn poll(&mut self) -> Result<Option<StreamRecord>> {
          retry_with_backoff(|| self.inner.poll()).await
      }
  }
  ```

#### Afternoon (4 hours)
- [ ] **Add Circuit Breaker**
  ```rust
  pub struct CircuitBreaker {
      failure_threshold: u32,
      recovery_timeout: Duration,
      state: CircuitState,
  }
  ```

- [ ] **Implement Health Checks**
  ```rust
  pub trait HealthCheck {
      async fn is_healthy(&self) -> bool;
      async fn check_connectivity(&self) -> Result<()>;
  }
  ```

**Deliverable**: Robust error handling across all sources

---

### **Day 8: Configuration & URI Parsing**
**Goal**: Support flexible data source configuration

#### Morning (4 hours)
- [ ] **Implement URI Parser**
  ```rust
  impl ConnectionString {
      pub fn parse(uri: &str) -> Result<Self> {
          // Parse: kafka://broker1:9092,broker2:9092/topic?group_id=test
          // Parse: s3://bucket/prefix/*.parquet?region=us-west-2
          // Parse: file:///path/to/data.json?watch=true
      }
  }
  ```

- [ ] **Add Configuration Validation**
  ```rust
  pub trait ConfigValidator {
      fn validate(&self, config: &SourceConfig) -> Result<()>;
      fn validate_uri(&self, uri: &str) -> Result<()>;
  }
  ```

#### Afternoon (4 hours)
- [ ] **Create Configuration Builder**
  ```rust
  pub struct DataSourceBuilder {
      scheme: Option<String>,
      params: HashMap<String, String>,
  }
  
  impl DataSourceBuilder {
      pub fn new() -> Self { ... }
      pub fn scheme(mut self, scheme: &str) -> Self { ... }
      pub fn param(mut self, key: &str, value: &str) -> Self { ... }
      pub fn build(self) -> Result<Box<dyn DataSource>> { ... }
  }
  ```

**Deliverable**: Flexible configuration system

---

### **Day 9: Documentation & Examples**
**Goal**: Document the new architecture

#### Morning (4 hours)
- [ ] **Write Architecture Documentation**
  - Architecture overview diagram
  - Trait relationships
  - Migration guide from old to new
  - Performance considerations

- [ ] **Create Developer Guide**
  ```markdown
  # Adding a New Data Source
  
  1. Implement DataSource trait
  2. Implement DataConsumer/DataProducer
  3. Register with DataSourceRegistry
  4. Add tests
  ```

#### Afternoon (4 hours)
- [ ] **Create Example Implementations**
  ```rust
  // examples/file_source.rs
  // Simple file-based data source example
  
  // examples/mock_source.rs  
  // Mock source for testing
  
  // examples/custom_source.rs
  // Template for custom implementations
  ```

**Deliverable**: Complete documentation package

---

### **Day 10: Performance & Optimization**
**Goal**: Ensure zero performance regression

#### Morning (4 hours)
- [ ] **Performance Benchmarks**
  ```rust
  #[bench]
  fn bench_kafka_direct() { ... }
  
  #[bench]
  fn bench_kafka_through_abstraction() { ... }
  
  #[bench]
  fn bench_schema_discovery() { ... }
  ```

- [ ] **Profile Memory Usage**
  - Ensure no memory leaks
  - Check allocation patterns
  - Verify cleanup on drop

#### Afternoon (4 hours)
- [ ] **Optimize Hot Paths**
  - Remove unnecessary allocations
  - Use zero-copy where possible
  - Optimize trait dispatch

- [ ] **Final Testing**
  ```bash
  # Full test suite
  cargo test --all
  
  # Performance tests
  cargo bench
  
  # Integration tests
  cargo test integration:: -- --test-threads=1
  ```

**Deliverable**: Performance-validated implementation

---

## 📊 Success Criteria

### **Week 1 Completion**
- ✅ All Kafka dependencies abstracted
- ✅ ProcessorContext is source-agnostic
- ✅ Kafka adapter implements new traits
- ✅ All existing tests pass
- ✅ Zero breaking changes

### **Week 2 Completion**
- ✅ Schema management system operational
- ✅ Robust error handling in place
- ✅ URI-based configuration working
- ✅ Documentation complete
- ✅ Performance benchmarks show no regression

---

## 🚀 Next Steps After Completion

With this foundation in place, you can:

1. **Start implementing new data sources** in parallel
2. **Begin Phase 1 of pluggable sources** (File systems)
3. **Community can contribute** data source implementations
4. **Existing code continues to work** without modification

---

## 📝 Daily Checklist Template

```markdown
## Day X Checklist - [Date]

### Morning
- [ ] Task 1 (Status: ⏳/✅/❌)
- [ ] Task 2 (Status: ⏳/✅/❌)
- [ ] Task 3 (Status: ⏳/✅/❌)

### Afternoon  
- [ ] Task 4 (Status: ⏳/✅/❌)
- [ ] Task 5 (Status: ⏳/✅/❌)
- [ ] Task 6 (Status: ⏳/✅/❌)

### Blockers
- None / [Describe blocker]

### Notes
- [Any important observations]

### Tomorrow's Priority
- [Top priority for next day]
```

---

## 🎯 Risk Mitigation

### **Risk 1: Breaking Changes**
- **Mitigation**: Keep old interfaces, add adapters
- **Testing**: Run full test suite after each change
- **Rollback**: Git branch for each day's work

### **Risk 2: Performance Regression**
- **Mitigation**: Benchmark before and after
- **Testing**: Run performance suite daily
- **Optimization**: Profile and optimize hot paths

### **Risk 3: Scope Creep**
- **Mitigation**: Stick to defined daily goals
- **Focus**: Only decouple, don't add features
- **Timeline**: Hard stop at 2 weeks

---

## 📊 Tracking Progress

Use this progress tracker:

```
Week 1: Core Decoupling
[▓▓▓▓░░░░░░] 40% - Day 2/5 Complete ✅

Week 2: Advanced Features  
[░░░░░░░░░░] 0% - Not Started

Overall Progress
[▓▓░░░░░░░░] 20% - 2/10 Days Complete ✅
```

---

*This plan ensures a clean, systematic decoupling that prepares FerrisStreams for pluggable data sources while maintaining 100% backward compatibility.*