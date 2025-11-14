# Week 9: JobProcessor Configuration Guide

## Overview

Week 9 introduces a flexible configuration system for selecting between different job processing architectures at runtime:

- **V1 Architecture**: Single-threaded baseline for comparison
- **V2 Architecture**: Multi-partition parallel execution for production

This guide explains how to configure and use these architectures.

## Configuration System

### JobProcessorConfig Enum

The `JobProcessorConfig` enum allows you to specify which architecture to use:

```rust
pub enum JobProcessorConfig {
    V1,  // Single-partition baseline
    V2 {
        num_partitions: Option<usize>,
        enable_core_affinity: bool,
    },
}
```

### Supported Configuration Formats

Configuration can be specified as strings in multiple formats:

| Format | Description | Example |
|--------|-------------|---------|
| `v1` | V1 architecture (single-threaded) | `"v1"` |
| `v2` | V2 with default partitions (CPU count) | `"v2"` |
| `v2:N` | V2 with N partitions | `"v2:8"` |
| `v2:affinity` | V2 with core affinity | `"v2:affinity"` |
| `v2:N:affinity` | V2 with N partitions and core affinity | `"v2:8:affinity"` |

## Usage Examples

### Programmatic Configuration

```rust
use velostream::velostream::server::processors::{JobProcessorFactory, JobProcessorConfig};

// Create V1 processor (baseline)
let processor = JobProcessorFactory::create(JobProcessorConfig::V1);
assert_eq!(processor.num_partitions(), 1);
assert_eq!(processor.processor_version(), "V1");

// Create V2 with default partitions (auto-detected)
let processor = JobProcessorFactory::create(JobProcessorConfig::V2 {
    num_partitions: None,
    enable_core_affinity: false,
});

// Create V2 with 8 partitions
let processor = JobProcessorFactory::create(JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: false,
});

// Create V2 with 8 partitions and core affinity
let processor = JobProcessorFactory::create(JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: true,
});
```

### String-Based Configuration

```rust
use velostream::velostream::server::processors::JobProcessorFactory;

// Parse and create from string
let processor = JobProcessorFactory::create_from_str("v1")?;
let processor = JobProcessorFactory::create_from_str("v2")?;
let processor = JobProcessorFactory::create_from_str("v2:8")?;
let processor = JobProcessorFactory::create_from_str("v2:8:affinity")?;
```

### Factory Convenience Methods

```rust
use velostream::velostream::server::processors::JobProcessorFactory;

// Create V1
let processor = JobProcessorFactory::create_v1();

// Create V2 with defaults
let processor = JobProcessorFactory::create_v2_default();

// Create V2 with specific partition count
let processor = JobProcessorFactory::create_v2_with_partitions(8);
```

## Architecture Characteristics

### V1 Architecture (Single-Threaded Baseline)

**Configuration:**
```
JobProcessorConfig::V1
```

**Characteristics:**
- Single-threaded processing
- Single partition (num_partitions = 1)
- Sequential record processing
- Baseline throughput: ~23.7K rec/sec
- Baseline overhead: 95-98%
- Use case: Comparison, testing, low-concurrency

**Benefits:**
- Simplicity (baseline for comparison)
- Easy to understand and debug
- Minimal resource usage
- Perfect for validation testing

**When to Use:**
- Establishing performance baselines
- Running on single-core systems
- Testing query logic before scaling
- Comparing against V2 improvements

### V2 Architecture (Multi-Partition Parallel)

**Configuration:**
```
JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: false,
}
```

**Characteristics:**
- Multi-threaded parallel processing
- Configurable partitions (typically = CPU count)
- Pluggable PartitioningStrategy:
  - **StickyPartition**: ~0% overhead (uses Kafka `__partition__` field)
  - **SmartRepartition**: 0% aligned, ~8% misaligned
  - **AlwaysHash**: Safe default with consistent routing
  - **RoundRobin**: Maximum throughput
- Target throughput: ~190K rec/sec on 8 cores
- Expected scaling: 8x on 8 cores (100% linear)
- Use case: Production, multi-core systems

**Benefits:**
- Linear scaling with core count (8x on 8 cores)
- Lock-free atomic operations
- Pluggable routing strategies for workload optimization
- Per-partition metrics and observability
- Production-ready backpressure handling

**When to Use:**
- Production deployments
- Multi-core systems (4+ cores)
- High-throughput requirements
- When 8x scaling is needed

## Automatic Partition Detection

V2 uses automatic CPU count detection if no partition count is specified:

```rust
let config = JobProcessorConfig::V2 {
    num_partitions: None,  // Auto-detect
    enable_core_affinity: false,
};
// On 8-core system: 8 partitions created
// On 4-core system: 4 partitions created
// On 16-core system: 16 partitions created
```

## Core Affinity (Advanced)

For maximum performance on systems with NUMA or high CPU counts, enable core affinity:

```rust
let config = JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: true,  // Pin partitions to cores
};
```

**Effects:**
- Improves cache locality on multi-socket systems
- Can reduce context switching overhead
- Reduces NUMA traffic (on NUMA systems)
- Best for 8+ core systems

**Trade-offs:**
- Slightly less flexibility in load balancing
- May not improve performance on single-socket systems
- Enable only if benchmarks show improvement

## Expected Performance

### Baseline Comparison (Week 9)

| Architecture | Partitions | Throughput | Scaling |
|---|---|---|---|
| **Direct SQL** | 1 | 500K-1.6M rec/sec | N/A (theoretical max) |
| **V1** | 1 | ~23.7K rec/sec | Baseline (1x) |
| **V2 (8 cores)** | 8 | ~190K rec/sec | 8x from V1 |
| **V2 (4 cores)** | 4 | ~95K rec/sec | 4x from V1 |

### Phase 6-7 Improvements (Future)

| Phase | Optimization | Expected Improvement |
|---|---|---|
| Phase 5 (Current) | Hash partitioning | 8x on 8 cores |
| Phase 6 | Lock-free structures | 2-3x per core |
| Phase 7 | SIMD vectorization | 1.0-1.5x additional |
| **Target** | All combined | **65x total** |

## Real-World Examples

### High-Throughput Production System

```rust
// For a production system with 8 CPU cores
let config = JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: true,
};
let processor = JobProcessorFactory::create(config);
// Expected: ~190K-200K rec/sec
```

### Development/Testing System

```rust
// For development or testing
let config = JobProcessorConfig::V2 {
    num_partitions: Some(4),
    enable_core_affinity: false,
};
let processor = JobProcessorFactory::create(config);
// Expected: ~95-100K rec/sec on 4 cores
```

### Baseline Comparison

```rust
// To compare V1 vs V2 performance
let v1_processor = JobProcessorFactory::create_v1();
let v2_processor = JobProcessorFactory::create_v2_with_partitions(8);

// Run identical workload on both
// Expected: V2 is ~8x faster than V1
```

### From String Configuration

```rust
// Load from config file or environment
let config_str = std::env::var("PROCESSOR_CONFIG")
    .unwrap_or_else(|_| "v2:8".to_string());

let processor = JobProcessorFactory::create_from_str(&config_str)?;
```

## Configuration in StreamJobServer

The configuration system integrates with StreamJobServer for runtime architecture selection:

```rust
use velostream::velostream::server::processors::JobProcessorConfig;

// Future: StreamJobServer will accept JobProcessorConfig
// in WITH clauses or as configuration parameter

// Example SQL (future support):
// CREATE STREAM my_stream AS
// SELECT * FROM input_topic
// WITH (processor = 'v2:8', enable_affinity = true)
```

## Debugging and Monitoring

### Configuration Description

Get a human-readable description of the configuration:

```rust
let config = JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: false,
};

println!("{}", config.description());
// Output: V2 (Multi-partition: 8, 8x baseline expected)
```

### Configuration Display

Display configuration as string:

```rust
let config = JobProcessorConfig::V2 {
    num_partitions: Some(8),
    enable_core_affinity: true,
};

println!("{}", config);
// Output: v2:8:affinity
```

### Processor Metadata

```rust
let processor = JobProcessorFactory::create(config);

println!("Name: {}", processor.processor_name());
println!("Version: {}", processor.processor_version());
println!("Partitions: {}", processor.num_partitions());
```

## Best Practices

### 1. Choose Architecture Based on Hardware

- **Single/dual-core**: Use V1 (simpler)
- **4+ cores**: Use V2 (linear scaling)
- **8+ cores**: Use V2 with affinity (cache optimization)

### 2. Benchmark Your Workload

```rust
// Run identical query on both architectures
// V1: Baseline
let v1 = JobProcessorFactory::create_v1();
// run_benchmark(v1);

// V2: Expected 8x on 8 cores
let v2 = JobProcessorFactory::create_v2_with_partitions(8);
// run_benchmark(v2);
```

### 3. Monitor Partition Utilization

- Check per-partition metrics
- Look for load imbalance (one partition much busier)
- If imbalanced: Check GROUP BY key distribution

### 4. Adjust Based on Results

```
If throughput < expected:
  1. Check CPU utilization (should be ~100% on all cores for V2)
  2. Check partition balance
  3. Consider enabling core affinity
  4. Profile to find bottleneck

If throughput is good:
  1. Increase record batch size slightly
  2. Monitor latency (should stay <5ms p95)
  3. Keep system stable
```

### 5. Version Control Configuration

Store configuration in version control:

```yaml
# config/production.yaml
processor:
  architecture: "v2:8:affinity"
  backpressure_enabled: true
  queue_threshold: 1000

# config/development.yaml
processor:
  architecture: "v2:4"
  backpressure_enabled: true
  queue_threshold: 500

# config/testing.yaml
processor:
  architecture: "v1"  # Baseline for comparison
```

## Troubleshooting

### Configuration Parse Errors

```rust
// Invalid format
let result = "v2:x".parse::<JobProcessorConfig>();
// Error: "Unknown V2 parameter: x"

// Valid formats
let config: JobProcessorConfig = "v2:8".parse()?;  // ✓
let config: JobProcessorConfig = "v2:affinity".parse()?;  // ✓
let config: JobProcessorConfig = "v2:8:affinity".parse()?;  // ✓
```

### Unexpected Partition Count

```rust
let config = JobProcessorConfig::V2 {
    num_partitions: None,
};
let processor = JobProcessorFactory::create(config);

// If you expect 8 partitions but get 4:
// → Check CPU count on your system (std::thread::available_parallelism())
// → Explicitly set num_partitions: Some(8) if needed
```

### Performance Regression

```
V2 showing less than 8x improvement on 8 cores:

1. Check GROUP BY key distribution
   - If skewed: Some partitions idle, others overloaded
   - Solution: Check data pattern, may not be parallelizable

2. Check backpressure
   - If channels are blocking: Data is backing up
   - Solution: Increase buffer size, check downstream

3. Check lock contention
   - If single core at 100%, others low: Contention
   - Solution: This is Phase 6 optimization (not yet)
```

## Migration Path

### Phase 1: Validation (Week 9)

1. Run V1 baseline: `JobProcessorConfig::V1`
2. Run V2 baseline: `JobProcessorConfig::V2 { num_partitions: Some(8), .. }`
3. Compare performance
4. Validate 8x scaling

### Phase 2: Optimization (Week 10-12)

1. Phase 6: Lock-free optimization (2-3x per core)
2. Phase 7: SIMD vectorization (1.0-1.5x additional)
3. Measure cumulative improvements

### Phase 3: Production (After Phase 7)

1. Deploy V2 with all optimizations
2. Monitor metrics continuously
3. Adjust configuration based on workload

## References

- **[FR-082 Schedule](./FR-082-SCHEDULE.md)** - Overall project timeline
- **[Pluggable Strategies](./PLUGGABLE-PARTITIONING-STRATEGIES.md)** - Strategy details
- **[Performance Benchmarks](./WEEK9-PERFORMANCE-BENCHMARKS.md)** - Baseline results
