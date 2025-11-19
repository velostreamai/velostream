//! JobProcessorConfig - Configuration for runtime architecture selection
//!
//! This module provides configuration types for selecting between different
//! job processing architectures (V1, V2, etc.) at runtime.
//!
//! For detailed information on Dead Letter Queue (DLQ) and metrics configuration,
//! see the [DLQ & Metrics Guide](../../../docs/data-sources/dlq-and-metrics-guide.md).

use crate::velostream::server::v2::PartitionedJobConfig;
use std::str::FromStr;

/// Configuration for selecting and configuring job processors
///
/// This enum allows selecting between different processing modes:
/// - Simple: Single-threaded, best-effort delivery
/// - Transactional: Single-threaded, at-least-once delivery with ACID transactions
/// - Adaptive: Multi-partition parallel execution with adaptive partitioning
///
/// ## Usage
///
/// ```rust,no_run
/// use velostream::velostream::server::processors::JobProcessorConfig;
///
/// // Adaptive mode (multi-partition, 8 cores)
/// let adaptive_config = JobProcessorConfig::Adaptive {
///     num_partitions: Some(8),
///     enable_core_affinity: false,
/// };
/// ```
#[derive(Clone, Debug)]
pub enum JobProcessorConfig {
    /// Simple Mode: Single-threaded, non-transactional, best-effort
    ///
    /// Characteristics:
    /// - Single-threaded batch processing
    /// - Best-effort semantics (LogAndContinue by default)
    /// - Optimized for throughput
    /// - Use case: Simple pipelines, throughput-oriented, acceptable data loss
    Simple,

    /// Transactional Mode: Single-threaded, transactional, at-least-once
    ///
    /// Characteristics:
    /// - Single-threaded batch processing with transaction boundaries
    /// - At-least-once delivery semantics
    /// - ACID transaction support per batch
    /// - Use case: Data consistency required, acceptable lower throughput
    Transactional,

    /// Adaptive Mode: Multi-partition parallel execution with auto-scaling
    ///
    /// Characteristics:
    /// - Multi-threaded parallel processing with automatic partitioning
    /// - Configurable partition count (default: CPU count)
    /// - Adaptive PartitioningStrategy selection (StickyPartition, SmartRepartition, etc.)
    /// - Target throughput: ~190K rec/sec on 8 cores (8x linear scaling)
    /// - Scaling efficiency: 100% (linear scaling expected)
    /// - Use case: High-throughput, multi-core systems, adaptive workloads
    ///
    /// ## Parameters
    /// - `num_partitions`: Number of independent partitions (defaults to CPU count)
    /// - `enable_core_affinity`: Pin partitions to CPU cores (advanced optimization)
    Adaptive {
        num_partitions: Option<usize>,
        enable_core_affinity: bool,
    },
}

impl JobProcessorConfig {
    /// Create Simple mode configuration
    pub fn simple() -> Self {
        JobProcessorConfig::Simple
    }

    /// Create Transactional mode configuration
    pub fn transactional() -> Self {
        JobProcessorConfig::Transactional
    }

    /// Create Adaptive mode configuration with default settings
    pub fn adaptive_default() -> Self {
        JobProcessorConfig::Adaptive {
            num_partitions: None,
            enable_core_affinity: false,
        }
    }

    /// Create Adaptive mode configuration with specific partition count
    pub fn adaptive_with_partitions(num_partitions: usize) -> Self {
        JobProcessorConfig::Adaptive {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        }
    }

    /// Convert this config to PartitionedJobConfig for Adaptive mode
    pub fn to_partitioned_job_config(&self) -> PartitionedJobConfig {
        match self {
            JobProcessorConfig::Simple | JobProcessorConfig::Transactional => {
                // Simple/Transactional configs don't need PartitionedJobConfig, but return defaults
                PartitionedJobConfig {
                    num_partitions: Some(1),
                    enable_core_affinity: false,
                    ..Default::default()
                }
            }
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => PartitionedJobConfig {
                num_partitions: *num_partitions,
                enable_core_affinity: *enable_core_affinity,
                ..Default::default()
            },
        }
    }

    /// Get a description of this configuration
    pub fn description(&self) -> String {
        match self {
            JobProcessorConfig::Simple => {
                "Simple (Single-threaded, best-effort delivery)".to_string()
            }
            JobProcessorConfig::Transactional => {
                "Transactional (Single-threaded, at-least-once delivery)".to_string()
            }
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => {
                let partitions = num_partitions
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "CPU count".to_string());
                let affinity = if *enable_core_affinity {
                    " with core affinity"
                } else {
                    ""
                };
                format!(
                    "Adaptive (Multi-partition: {}{}, {}x baseline expected)",
                    partitions,
                    affinity,
                    num_partitions.unwrap_or_else(|| {
                        std::thread::available_parallelism()
                            .map(|n| n.get())
                            .unwrap_or(8)
                    })
                )
            }
        }
    }
}

impl Default for JobProcessorConfig {
    fn default() -> Self {
        // Default to Adaptive with automatic partition detection
        JobProcessorConfig::Adaptive {
            num_partitions: None,
            enable_core_affinity: false,
        }
    }
}

impl FromStr for JobProcessorConfig {
    type Err = String;

    /// Parse JobProcessorConfig from string representation
    ///
    /// Supported formats:
    /// - "simple" → JobProcessorConfig::Simple
    /// - "transactional" → JobProcessorConfig::Transactional
    /// - "adaptive" → JobProcessorConfig::Adaptive with defaults
    /// - "adaptive:4" → JobProcessorConfig::Adaptive with 4 partitions
    /// - "adaptive:affinity" → JobProcessorConfig::Adaptive with core affinity
    /// - "adaptive:8:affinity" → Adaptive with 8 partitions and affinity
    ///
    /// ## Examples
    ///
    /// ```rust,ignore
    /// use std::str::FromStr;
    /// use velostream::velostream::server::processors::JobProcessorConfig;
    ///
    /// let simple = "simple".parse::<JobProcessorConfig>()?;
    /// let tx = "transactional".parse::<JobProcessorConfig>()?;
    /// let adaptive = "adaptive".parse::<JobProcessorConfig>()?;
    /// let adaptive_8 = "adaptive:8".parse::<JobProcessorConfig>()?;
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.trim().to_lowercase();

        // Handle simple mode names
        match s_lower.as_str() {
            "simple" => return Ok(JobProcessorConfig::Simple),
            "transactional" => return Ok(JobProcessorConfig::Transactional),
            "adaptive" => {
                return Ok(JobProcessorConfig::Adaptive {
                    num_partitions: None,
                    enable_core_affinity: false,
                });
            }
            _ => {}
        }

        // Handle "adaptive:" prefix with parameters
        if s_lower.starts_with("adaptive:") {
            let mut num_partitions = None;
            let mut enable_core_affinity = false;

            if let Some(params) = s_lower.strip_prefix("adaptive:") {
                for param in params.split(':') {
                    if param == "affinity" {
                        enable_core_affinity = true;
                    } else if let Ok(n) = param.parse::<usize>() {
                        if n > 0 {
                            num_partitions = Some(n);
                        } else {
                            return Err(format!("Invalid partition count: {}", n));
                        }
                    } else if !param.is_empty() {
                        return Err(format!("Unknown parameter: {}", param));
                    }
                }
            }

            return Ok(JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            });
        }

        Err(format!(
            "Unknown mode: '{}'. Supported: 'simple', 'transactional', 'adaptive', 'adaptive:8', 'adaptive:8:affinity'",
            s
        ))
    }
}

impl std::fmt::Display for JobProcessorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobProcessorConfig::Simple => write!(f, "simple"),
            JobProcessorConfig::Transactional => write!(f, "transactional"),
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => {
                write!(f, "adaptive")?;
                if let Some(n) = num_partitions {
                    write!(f, ":{}", n)?;
                }
                if *enable_core_affinity {
                    write!(f, ":affinity")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_config() {
        let config = JobProcessorConfig::Simple;
        assert_eq!(config.to_string(), "simple");
        assert!(
            config
                .description()
                .contains("Single-threaded, best-effort")
        );
    }

    #[test]
    fn test_transactional_config() {
        let config = JobProcessorConfig::Transactional;
        assert_eq!(config.to_string(), "transactional");
        assert!(
            config
                .description()
                .contains("Single-threaded, at-least-once")
        );
    }

    #[test]
    fn test_adaptive_config_default() {
        let config = JobProcessorConfig::Adaptive {
            num_partitions: None,
            enable_core_affinity: false,
        };
        assert_eq!(config.to_string(), "adaptive");
    }

    #[test]
    fn test_adaptive_config_with_partitions() {
        let config = JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: false,
        };
        assert_eq!(config.to_string(), "adaptive:8");
    }

    #[test]
    fn test_adaptive_config_with_affinity() {
        let config = JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: true,
        };
        assert_eq!(config.to_string(), "adaptive:8:affinity");
    }

    #[test]
    fn test_parse_simple() {
        let config: JobProcessorConfig = "simple".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::Simple));
    }

    #[test]
    fn test_parse_transactional() {
        let config: JobProcessorConfig = "transactional".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::Transactional));
    }

    #[test]
    fn test_parse_adaptive() {
        let config: JobProcessorConfig = "adaptive".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::Adaptive { .. }));
    }

    #[test]
    fn test_parse_adaptive_with_partitions() {
        let config: JobProcessorConfig = "adaptive:8".parse().unwrap();
        if let JobProcessorConfig::Adaptive {
            num_partitions: Some(n),
            ..
        } = config
        {
            assert_eq!(n, 8);
        } else {
            panic!("Expected Adaptive with 8 partitions");
        }
    }

    #[test]
    fn test_parse_adaptive_with_affinity() {
        let config: JobProcessorConfig = "adaptive:affinity".parse().unwrap();
        if let JobProcessorConfig::Adaptive {
            enable_core_affinity,
            ..
        } = config
        {
            assert!(enable_core_affinity);
        } else {
            panic!("Expected Adaptive with affinity");
        }
    }

    #[test]
    fn test_parse_adaptive_with_both() {
        let config: JobProcessorConfig = "adaptive:8:affinity".parse().unwrap();
        if let JobProcessorConfig::Adaptive {
            num_partitions: Some(n),
            enable_core_affinity,
        } = config
        {
            assert_eq!(n, 8);
            assert!(enable_core_affinity);
        } else {
            panic!("Expected Adaptive with 8 partitions and affinity");
        }
    }

    #[test]
    fn test_parse_invalid() {
        let result: Result<JobProcessorConfig, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_default_is_adaptive() {
        let config = JobProcessorConfig::default();
        assert!(matches!(config, JobProcessorConfig::Adaptive { .. }));
    }

    #[test]
    fn test_description() {
        let adaptive = JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: false,
        };
        assert!(adaptive.description().contains("Adaptive"));
        assert!(adaptive.description().contains("8"));
    }

    #[test]
    fn test_to_partitioned_job_config_adaptive() {
        let config = JobProcessorConfig::Adaptive {
            num_partitions: Some(8),
            enable_core_affinity: true,
        };
        let pjc = config.to_partitioned_job_config();
        assert_eq!(pjc.num_partitions, Some(8));
        assert!(pjc.enable_core_affinity);
    }
}
