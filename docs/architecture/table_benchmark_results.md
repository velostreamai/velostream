# Table Performance Benchmark Results

## Executive Summary

This benchmark compares CompactTable vs Standard Table performance across different dataset sizes and query patterns to optimize table selection in CTAS queries.

## Benchmark Results

| Dataset | Records | Fields | Insert Time (µs) | Query Time (µs) | Memory Usage (bytes) | Memory Savings |
|---------|---------|---------|------------------|------------------|---------------------|----------------|
| Small Dataset - Simple | 1.0K | 5 | CT: 8778, ST: 1829 | CT: 8041, ST: 2046 | CT: 26.9K, ST: 96.0K | 72.0% |
| Small Dataset - Complex | 10.0K | 20 | CT: 248882, ST: 24023 | CT: 18694, ST: 2236 | CT: 1.3M, ST: 960.0K | -40.5% |
| Medium Dataset - Financial | 100.0K | 10 | CT: 1092565, ST: 176062 | CT: 5920, ST: 1008 | CT: 5.5M, ST: 9.6M | 43.1% |
| Medium Dataset - Analytics | 500.0K | 15 | CT: 9890965, ST: 1385751 | CT: 3685, ST: 676 | CT: 49.3M, ST: 48.0M | -2.8% |
| Large Dataset - IoT | 100.0K | 8 | CT: 795431, ST: 156965 | CT: 528, ST: 109 | CT: 4.2M, ST: 9.6M | 56.5% |
| Large Dataset - User Analytics | 250.0K | 12 | CT: 5750422, ST: 534298 | CT: 327, ST: 84 | CT: 31.1M, ST: 24.0M | -29.6% |


## Performance Analysis

### Memory Efficiency
**Key Findings:**
- **Small Dataset - Simple**: 72.0% memory reduction with CompactTable
- **Small Dataset - Complex**: -40.5% memory reduction with CompactTable
- **Medium Dataset - Financial**: 43.1% memory reduction with CompactTable
- **Medium Dataset - Analytics**: -2.8% memory reduction with CompactTable
- **Large Dataset - IoT**: 56.5% memory reduction with CompactTable
- **Large Dataset - User Analytics**: -29.6% memory reduction with CompactTable


### Query Performance
**Performance Impact:**
- **Small Dataset - Simple**: CompactTable ⚠️ 3.9x slower
- **Small Dataset - Complex**: CompactTable ⚠️ 8.4x slower
- **Medium Dataset - Financial**: CompactTable ⚠️ 5.9x slower
- **Medium Dataset - Analytics**: CompactTable ⚠️ 5.5x slower
- **Large Dataset - IoT**: CompactTable ⚠️ 4.8x slower
- **Large Dataset - User Analytics**: CompactTable ⚠️ 3.9x slower


### Insert Performance
**Insert Performance:**
- **Small Dataset - Simple**: CompactTable ⚠️ 4.8x slower
- **Small Dataset - Complex**: CompactTable ⚠️ 10.4x slower
- **Medium Dataset - Financial**: CompactTable ⚠️ 6.2x slower
- **Medium Dataset - Analytics**: CompactTable ⚠️ 7.1x slower
- **Large Dataset - IoT**: CompactTable ⚠️ 5.1x slower
- **Large Dataset - User Analytics**: CompactTable ⚠️ 10.8x slower


## Recommendations

### Use CompactTable When:
**Based on benchmark results:**


### Use Standard Table When:
**Based on benchmark results:**
- **Small Dataset - Simple**: Query performance penalty: 3.9x, Memory savings: 72.0%
- **Small Dataset - Complex**: Query performance penalty: 8.4x, Memory savings: -40.5%
- **Medium Dataset - Financial**: Query performance penalty: 5.9x, Memory savings: 43.1%
- **Medium Dataset - Analytics**: Query performance penalty: 5.5x, Memory savings: -2.8%
- **Large Dataset - IoT**: Query performance penalty: 4.8x, Memory savings: 56.5%
- **Large Dataset - User Analytics**: Query performance penalty: 3.9x, Memory savings: -29.6%


## Methodology

- **Insert Benchmark**: Time to insert all records into the table
- **Query Benchmark**: Time for 1000 random key lookups per test
- **Memory Measurement**: Actual memory usage (CompactTable) vs estimated (Standard Table)
- **Test Environment**: Rust release build with optimizations

Generated: 2025-09-25 07:36:51 UTC
