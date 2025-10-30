/*!
# Financial Precision Benchmark

This benchmark demonstrates the precision issues with f64 for financial calculations
and compares performance/accuracy of different numeric representations:

- f64 (current FieldValue::Float)
- i64 scaled integers (4 decimal places)
- RustDecimal (exact decimal arithmetic)
- i128 scaled integers (high precision)

Issue: https://github.com/bluemonk3y/velo_streams/issues/19
*/

use rust_decimal::Decimal;
use rust_decimal::prelude::*; // Includes ToPrimitive trait for to_f64()
use std::time::Instant;

/// Financial amount with 4 decimal places precision using i64
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScaledI64 {
    /// Value scaled by 10,000 (4 decimal places)
    scaled_value: i64,
}

impl ScaledI64 {
    const SCALE: i64 = 10_000;

    pub fn from_f64(value: f64) -> Self {
        Self {
            scaled_value: (value * Self::SCALE as f64).round() as i64,
        }
    }

    pub fn from_cents(cents: i64) -> Self {
        Self {
            scaled_value: cents * 100, // Convert cents to 4 decimal places
        }
    }

    pub fn to_f64(self) -> f64 {
        self.scaled_value as f64 / Self::SCALE as f64
    }

    pub fn add(self, other: Self) -> Self {
        Self {
            scaled_value: self.scaled_value + other.scaled_value,
        }
    }

    pub fn multiply(self, factor: f64) -> Self {
        Self {
            scaled_value: (self.scaled_value as f64 * factor).round() as i64,
        }
    }
}

/// High precision financial amount using i128
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScaledI128 {
    /// Value scaled by 10^8 (8 decimal places)
    scaled_value: i128,
}

impl ScaledI128 {
    const SCALE: i128 = 100_000_000;

    pub fn from_f64(value: f64) -> Self {
        Self {
            scaled_value: (value * Self::SCALE as f64).round() as i128,
        }
    }

    pub fn to_f64(self) -> f64 {
        self.scaled_value as f64 / Self::SCALE as f64
    }

    pub fn add(self, other: Self) -> Self {
        Self {
            scaled_value: self.scaled_value + other.scaled_value,
        }
    }

    pub fn multiply(self, factor: f64) -> Self {
        Self {
            scaled_value: (self.scaled_value as f64 * factor).round() as i128,
        }
    }
}

/// Generate realistic financial test data (stock prices, trade amounts, etc.)
fn generate_financial_data(count: usize) -> Vec<f64> {
    let mut data = Vec::with_capacity(count);
    let mut price = 100.0; // Starting price

    for i in 0..count {
        // Simulate realistic financial values with small increments
        let change = 0.01 * ((i as f64 * 0.1).sin()); // Small price movements
        price += change;

        // Add some realistic financial amounts
        match i % 5 {
            0 => data.push(price),          // Stock prices: $100.xx
            1 => data.push(price * 1000.0), // Large trades: $100,000.xx
            2 => data.push(0.0125),         // Interest rates: 1.25%
            3 => data.push(price / 100.0),  // Small amounts: $1.xx
            4 => data.push(price * 0.001),  // Fractional shares: $0.1xx
            _ => unreachable!(),
        }
    }

    data
}

#[cfg(test)]
mod precision_tests {
    use super::*;

    #[test]
    fn test_f64_precision_loss_demonstration() {
        println!("\n=== F64 PRECISION LOSS DEMONSTRATION ===");

        // Demonstrate classic floating point precision issues
        let mut sum_f64 = 0.0f64;
        let increment = 0.01f64; // 1 cent

        // Add 1 cent 10,000 times = should equal $100.00
        for _ in 0..10_000 {
            sum_f64 += increment;
        }

        let expected = 100.0;
        let error = (sum_f64 - expected).abs();

        println!("Adding 0.01 ten thousand times:");
        println!("f64 result: {:.10}", sum_f64);
        println!("Expected:   {:.10}", expected);
        println!("Error:      {:.10}", error);
        println!("Equal?:     {}", sum_f64 == expected);

        // This will fail due to floating point precision
        assert!(error > 0.0, "f64 should have precision errors");
        assert!(error < 1e-10, "Error should be small but present");
    }

    #[test]
    fn test_scaled_i64_precision() {
        println!("\n=== SCALED I64 PRECISION TEST ===");

        let mut sum_i64 = ScaledI64::from_cents(0);
        let increment = ScaledI64::from_f64(0.01); // 1 cent

        // Add 1 cent 10,000 times
        for _ in 0..10_000 {
            sum_i64 = sum_i64.add(increment);
        }

        let result = sum_i64.to_f64();
        let expected = 100.0;

        println!("Adding 0.01 ten thousand times with scaled i64:");
        println!("i64 result: {:.10}", result);
        println!("Expected:   {:.10}", expected);
        println!("Equal?:     {}", result == expected);

        // Scaled integers should be exact
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rust_decimal_precision() {
        println!("\n=== RUST DECIMAL PRECISION TEST ===");

        let mut sum_decimal = Decimal::ZERO;
        let increment = Decimal::from_f64(0.01).unwrap();

        // Add 1 cent 10,000 times
        for _ in 0..10_000 {
            sum_decimal += increment;
        }

        let result = sum_decimal.to_f64().unwrap();
        let expected = 100.0;

        println!("Adding 0.01 ten thousand times with Decimal:");
        println!("Decimal result: {:.10}", result);
        println!("Expected:       {:.10}", expected);
        println!("Equal?:         {}", result == expected);

        // Decimal should be exact for this case
        assert_eq!(result, expected);
    }
}

#[cfg(test)]
mod performance_benchmarks {
    use super::*;

    const BENCHMARK_SIZE: usize = 1_000_000;

    #[test]
    fn benchmark_f64_aggregation() {
        let data = generate_financial_data(BENCHMARK_SIZE);
        println!("\n=== F64 AGGREGATION BENCHMARK ===");

        let start = Instant::now();
        let mut sum = 0.0f64;
        let mut count = 0u64;
        let mut min = f64::MAX;
        let mut max = f64::MIN;

        for &value in &data {
            sum += value;
            count += 1;
            if value < min {
                min = value;
            }
            if value > max {
                max = value;
            }
        }

        let avg = sum / count as f64;
        let duration = start.elapsed();

        println!("Records: {}", BENCHMARK_SIZE);
        println!("Time: {:?}", duration);
        println!(
            "Throughput: {:.0} records/sec",
            BENCHMARK_SIZE as f64 / duration.as_secs_f64()
        );
        println!("Sum: {:.4}", sum);
        println!("Avg: {:.4}", avg);
        println!("Min: {:.4}", min);
        println!("Max: {:.4}", max);
    }

    #[test]
    fn benchmark_scaled_i64_aggregation() {
        let data = generate_financial_data(BENCHMARK_SIZE);
        println!("\n=== SCALED I64 AGGREGATION BENCHMARK ===");

        let start = Instant::now();
        let mut sum = ScaledI64::from_cents(0);
        let mut count = 0u64;
        let mut min = ScaledI64::from_f64(f64::MAX);
        let mut max = ScaledI64::from_f64(f64::MIN);

        for &value in &data {
            let scaled_value = ScaledI64::from_f64(value);
            sum = sum.add(scaled_value);
            count += 1;
            if scaled_value.scaled_value < min.scaled_value {
                min = scaled_value;
            }
            if scaled_value.scaled_value > max.scaled_value {
                max = scaled_value;
            }
        }

        let avg = ScaledI64::from_f64(sum.to_f64() / count as f64);
        let duration = start.elapsed();

        println!("Records: {}", BENCHMARK_SIZE);
        println!("Time: {:?}", duration);
        println!(
            "Throughput: {:.0} records/sec",
            BENCHMARK_SIZE as f64 / duration.as_secs_f64()
        );
        println!("Sum: {:.4}", sum.to_f64());
        println!("Avg: {:.4}", avg.to_f64());
        println!("Min: {:.4}", min.to_f64());
        println!("Max: {:.4}", max.to_f64());
    }

    #[test]
    fn benchmark_scaled_i128_aggregation() {
        let data = generate_financial_data(BENCHMARK_SIZE);
        println!("\n=== SCALED I128 AGGREGATION BENCHMARK ===");

        let start = Instant::now();
        let mut sum = ScaledI128::from_f64(0.0);
        let mut count = 0u64;
        let mut min = ScaledI128::from_f64(f64::MAX);
        let mut max = ScaledI128::from_f64(f64::MIN);

        for &value in &data {
            let scaled_value = ScaledI128::from_f64(value);
            sum = sum.add(scaled_value);
            count += 1;
            if scaled_value.scaled_value < min.scaled_value {
                min = scaled_value;
            }
            if scaled_value.scaled_value > max.scaled_value {
                max = scaled_value;
            }
        }

        let avg = ScaledI128::from_f64(sum.to_f64() / count as f64);
        let duration = start.elapsed();

        println!("Records: {}", BENCHMARK_SIZE);
        println!("Time: {:?}", duration);
        println!(
            "Throughput: {:.0} records/sec",
            BENCHMARK_SIZE as f64 / duration.as_secs_f64()
        );
        println!("Sum: {:.4}", sum.to_f64());
        println!("Avg: {:.4}", avg.to_f64());
        println!("Min: {:.4}", min.to_f64());
        println!("Max: {:.4}", max.to_f64());
    }

    #[test]
    fn benchmark_rust_decimal_aggregation() {
        let data = generate_financial_data(BENCHMARK_SIZE);
        println!("\n=== RUST DECIMAL AGGREGATION BENCHMARK ===");

        let start = Instant::now();
        let mut sum = Decimal::ZERO;
        let mut count = 0u64;
        let mut min = Decimal::MAX;
        let mut max = Decimal::MIN;

        for &value in &data {
            let decimal_value = Decimal::from_f64(value).unwrap_or(Decimal::ZERO);
            sum += decimal_value;
            count += 1;
            if decimal_value < min {
                min = decimal_value;
            }
            if decimal_value > max {
                max = decimal_value;
            }
        }

        let avg = sum / Decimal::new(count as i64, 0);
        let duration = start.elapsed();

        println!("Records: {}", BENCHMARK_SIZE);
        println!("Time: {:?}", duration);
        println!(
            "Throughput: {:.0} records/sec",
            BENCHMARK_SIZE as f64 / duration.as_secs_f64()
        );
        println!("Sum: {:.4}", sum.to_f64().unwrap_or(0.0));
        println!("Avg: {:.4}", avg.to_f64().unwrap_or(0.0));
        println!("Min: {:.4}", min.to_f64().unwrap_or(0.0));
        println!("Max: {:.4}", max.to_f64().unwrap_or(0.0));
    }

    #[test]
    fn benchmark_precision_comparison() {
        println!("\n=== PRECISION COMPARISON ===");

        // Test with values that commonly cause precision issues
        let problematic_values = vec![
            0.1,
            0.2,
            0.3,       // Classic decimal representation issues
            1.0 / 3.0, // Repeating decimal
            99.999,    // Near boundary values
            0.0001,
            0.0002,
            0.0003, // Small values
        ];

        let mut f64_sum = 0.0f64;
        let mut i64_sum = ScaledI64::from_cents(0);
        let mut i128_sum = ScaledI128::from_f64(0.0);
        let mut decimal_sum = Decimal::ZERO;

        for &value in &problematic_values {
            f64_sum += value;
            i64_sum = i64_sum.add(ScaledI64::from_f64(value));
            i128_sum = i128_sum.add(ScaledI128::from_f64(value));
            decimal_sum += Decimal::from_f64(value).unwrap_or(Decimal::ZERO);
        }

        println!("Summing problematic floating point values:");
        println!("f64:      {:.10}", f64_sum);
        println!("i64:      {:.10}", i64_sum.to_f64());
        println!("i128:     {:.10}", i128_sum.to_f64());
        println!("Decimal:  {:.10}", decimal_sum.to_f64().unwrap_or(0.0));

        // Check if results are equal (they likely won't be for f64)
        let expected = problematic_values.iter().sum::<f64>();
        println!("Expected: {:.10}", expected);

        println!(
            "f64 == expected:     {}",
            (f64_sum - expected).abs() < 1e-15
        );
        println!(
            "i64 ~= expected:     {}",
            (i64_sum.to_f64() - expected).abs() < 1e-4
        );
        println!(
            "i128 ~= expected:    {}",
            (i128_sum.to_f64() - expected).abs() < 1e-8
        );
        println!(
            "Decimal ~= expected: {}",
            (decimal_sum.to_f64().unwrap() - expected).abs() < 1e-15
        );
    }

    #[test]
    fn benchmark_financial_calculation_patterns() {
        println!("\n=== FINANCIAL CALCULATION PATTERNS ===");

        // Test patterns common in financial calculations
        let prices = [123.45, 67.89, 234.56, 89.12, 345.67];
        let quantities = vec![1000, 500, 750, 1200, 300];

        println!("Testing trade calculations (price * quantity):");

        let start = Instant::now();
        let mut f64_total = 0.0f64;
        for (price, qty) in prices.iter().zip(&quantities) {
            f64_total += price * (*qty as f64);
        }
        let f64_duration = start.elapsed();

        let start = Instant::now();
        let mut i64_total = ScaledI64::from_cents(0);
        for (price, qty) in prices.iter().zip(&quantities) {
            let scaled_price = ScaledI64::from_f64(*price);
            let trade_value = scaled_price.multiply(*qty as f64);
            i64_total = i64_total.add(trade_value);
        }
        let i64_duration = start.elapsed();

        let start = Instant::now();
        let mut decimal_total = Decimal::ZERO;
        for (price, qty) in prices.iter().zip(&quantities) {
            let decimal_price = Decimal::from_f64(*price).unwrap();
            let decimal_qty = Decimal::new(*qty as i64, 0);
            decimal_total += decimal_price * decimal_qty;
        }
        let decimal_duration = start.elapsed();

        println!(
            "f64 total:     ${:.2} (time: {:?})",
            f64_total, f64_duration
        );
        println!(
            "i64 total:     ${:.2} (time: {:?})",
            i64_total.to_f64(),
            i64_duration
        );
        println!(
            "Decimal total: ${:.2} (time: {:?})",
            decimal_total.to_f64().unwrap(),
            decimal_duration
        );

        // Performance comparison
        println!("\nPerformance relative to f64:");
        println!(
            "i64 overhead:     {:.2}x",
            i64_duration.as_nanos() as f64 / f64_duration.as_nanos() as f64
        );
        println!(
            "Decimal overhead: {:.2}x",
            decimal_duration.as_nanos() as f64 / f64_duration.as_nanos() as f64
        );
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Test how different numeric types would integrate with Velostream aggregations
    #[test]
    fn test_aggregation_accuracy_real_world() {
        println!("\n=== REAL-WORLD FINANCIAL AGGREGATION TEST ===");

        // Simulate a day of small transactions (like micropayments or fees)
        let mut transactions = Vec::new();
        for i in 0..10_000 {
            // Small fee calculations that often cause precision issues
            let fee = 0.001 + (i as f64 * 0.0001) % 0.01; // Fees from $0.001 to $0.011
            transactions.push(fee);
        }

        println!(
            "Processing {} small financial transactions",
            transactions.len()
        );

        // f64 aggregation
        let start = Instant::now();
        let f64_sum: f64 = transactions.iter().sum();
        let f64_avg = f64_sum / transactions.len() as f64;
        let f64_time = start.elapsed();

        // i64 scaled aggregation
        let start = Instant::now();
        let mut i64_sum = ScaledI64::from_cents(0);
        for &tx in &transactions {
            i64_sum = i64_sum.add(ScaledI64::from_f64(tx));
        }
        let i64_avg = ScaledI64::from_f64(i64_sum.to_f64() / transactions.len() as f64);
        let i64_time = start.elapsed();

        // Decimal aggregation
        let start = Instant::now();
        let mut decimal_sum = Decimal::ZERO;
        for &tx in &transactions {
            decimal_sum += Decimal::from_f64(tx).unwrap_or(Decimal::ZERO);
        }
        let decimal_avg = decimal_sum / Decimal::new(transactions.len() as i64, 0);
        let decimal_time = start.elapsed();

        println!("\nResults:");
        println!(
            "f64 - Sum: ${:.6}, Avg: ${:.6}, Time: {:?}",
            f64_sum, f64_avg, f64_time
        );
        println!(
            "i64 - Sum: ${:.6}, Avg: ${:.6}, Time: {:?}",
            i64_sum.to_f64(),
            i64_avg.to_f64(),
            i64_time
        );
        println!(
            "Dec - Sum: ${:.6}, Avg: ${:.6}, Time: {:?}",
            decimal_sum.to_f64().unwrap_or(0.0),
            decimal_avg.to_f64().unwrap_or(0.0),
            decimal_time
        );

        println!("\nDifferences from f64 baseline:");
        println!("i64 sum diff:   ${:.6}", (i64_sum.to_f64() - f64_sum).abs());
        println!(
            "decimal sum diff: ${:.6}",
            (decimal_sum.to_f64().unwrap_or(0.0) - f64_sum).abs()
        );

        println!("\nPerformance vs f64:");
        println!(
            "i64 performance: {:.2}x",
            i64_time.as_nanos() as f64 / f64_time.as_nanos() as f64
        );
        println!(
            "Decimal performance: {:.2}x",
            decimal_time.as_nanos() as f64 / f64_time.as_nanos() as f64
        );

        // Verify we have measurable differences (proving the precision issue exists)
        let sum_difference = (i64_sum.to_f64() - f64_sum).abs();
        if sum_difference > 1e-6 {
            println!(
                "\n⚠️  PRECISION ISSUE DETECTED: f64 has ${:.6} error in sum",
                sum_difference
            );
        }
    }
}
