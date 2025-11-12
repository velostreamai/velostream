//! JobProcessorConfig - Configuration for runtime architecture selection
//!
//! This module provides configuration types for selecting between different
//! job processing architectures (V1, V2, etc.) at runtime.

use crate::velostream::server::v2::PartitionedJobConfig;
use std::str::FromStr;

/// Configuration for selecting and configuring job processors
///
/// This enum allows selecting between different processing architectures
/// at runtime via configuration.
///
/// ## Usage
///
/// ```rust,no_run
/// use velostream::velostream::server::processors::JobProcessorConfig;
///
/// // V2 architecture (multi-partition, 8 cores)
/// let v2_config = JobProcessorConfig::V2 {
///     num_partitions: Some(8),
///     enable_core_affinity: false,
/// };
/// ```
#[derive(Clone, Debug)]
pub enum JobProcessorConfig {
    /// V1 Simple Architecture: Single-threaded, non-transactional
    ///
    /// Characteristics:
    /// - Single-threaded batch processing
    /// - Best-effort semantics (LogAndContinue by default)
    /// - Optimized for throughput
    /// - Use case: Simple pipelines, throughput-oriented
    V1Simple,

    /// V1 Transactional Architecture: Single-threaded, transactional
    ///
    /// Characteristics:
    /// - Single-threaded batch processing with transaction boundaries
    /// - At-least-once delivery semantics
    /// - ACID transaction support per batch
    /// - Use case: Data consistency required, lower throughput acceptable
    V1Transactional,

    /// V2 Architecture: Multi-partition, parallel execution
    ///
    /// Characteristics:
    /// - Multi-threaded parallel processing
    /// - Configurable partition count (default: CPU count)
    /// - Pluggable PartitioningStrategy (StickyPartition, SmartRepartition, etc.)
    /// - Target throughput: ~190K rec/sec on 8 cores (8x scaling)
    /// - Scaling efficiency: 100% (linear scaling expected)
    /// - Use case: High-throughput, multi-core systems
    ///
    /// ## Parameters
    /// - `num_partitions`: Number of independent partitions (defaults to CPU count)
    /// - `enable_core_affinity`: Pin partitions to CPU cores (advanced optimization)
    V2 {
        num_partitions: Option<usize>,
        enable_core_affinity: bool,
    },
}

impl JobProcessorConfig {
    /// Create V1 Simple configuration
    pub fn v1_simple() -> Self {
        JobProcessorConfig::V1Simple
    }

    /// Create V1 Transactional configuration
    pub fn v1_transactional() -> Self {
        JobProcessorConfig::V1Transactional
    }

    /// Create V2 configuration with default settings
    pub fn v2_default() -> Self {
        JobProcessorConfig::V2 {
            num_partitions: None,
            enable_core_affinity: false,
        }
    }

    /// Create V2 configuration with specific partition count
    pub fn v2_with_partitions(num_partitions: usize) -> Self {
        JobProcessorConfig::V2 {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        }
    }

    /// Convert this config to PartitionedJobConfig for V2
    pub fn to_partitioned_job_config(&self) -> PartitionedJobConfig {
        match self {
            JobProcessorConfig::V1Simple | JobProcessorConfig::V1Transactional => {
                // V1 configs don't need PartitionedJobConfig, but return defaults
                PartitionedJobConfig {
                    num_partitions: Some(1),
                    enable_core_affinity: false,
                    ..Default::default()
                }
            }
            JobProcessorConfig::V2 {
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
            JobProcessorConfig::V1Simple => {
                "V1 Simple (Single-threaded, best-effort semantics)".to_string()
            }
            JobProcessorConfig::V1Transactional => {
                "V1 Transactional (Single-threaded, at-least-once semantics)".to_string()
            }
            JobProcessorConfig::V2 {
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
                    "V2 (Multi-partition: {}{}, {}x baseline expected)",
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
        // Default to V2 with automatic partition detection
        JobProcessorConfig::V2 {
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
    /// - "v1:simple" → JobProcessorConfig::V1Simple
    /// - "v1:transactional" → JobProcessorConfig::V1Transactional
    /// - "v2" or "V2" → JobProcessorConfig::V2 with defaults
    /// - "v2:4" or "V2:4" → JobProcessorConfig::V2 with 4 partitions
    /// - "v2:affinity" → JobProcessorConfig::V2 with core affinity
    /// - "v2:8:affinity" → JobProcessorConfig::V2 with 8 partitions and affinity
    ///
    /// ## Examples
    ///
    /// ```rust,ignore
    /// use std::str::FromStr;
    /// use velostream::velostream::server::processors::JobProcessorConfig;
    ///
    /// let v1_simple = "v1:simple".parse::<JobProcessorConfig>()?;
    /// let v1_tx = "v1:transactional".parse::<JobProcessorConfig>()?;
    /// let v2 = "v2".parse::<JobProcessorConfig>()?;
    /// let v2_8 = "v2:8".parse::<JobProcessorConfig>()?;
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.trim().to_lowercase();

        if s_lower.starts_with("v1:") {
            if let Some(variant) = s_lower.strip_prefix("v1:") {
                match variant {
                    "simple" => return Ok(JobProcessorConfig::V1Simple),
                    "transactional" => return Ok(JobProcessorConfig::V1Transactional),
                    _ => {
                        return Err(format!(
                            "Unknown V1 variant: '{}'. Supported: 'simple', 'transactional'",
                            variant
                        ));
                    }
                }
            }
        }

        if s_lower.starts_with("v2") {
            let mut num_partitions = None;
            let mut enable_core_affinity = false;

            // Parse optional parameters after "v2:"
            if let Some(params_str) = s_lower.strip_prefix("v2:") {
                for param in params_str.split(':') {
                    if param == "affinity" {
                        enable_core_affinity = true;
                    } else if let Ok(n) = param.parse::<usize>() {
                        if n > 0 {
                            num_partitions = Some(n);
                        } else {
                            return Err(format!("Invalid partition count: {}", n));
                        }
                    } else if !param.is_empty() {
                        return Err(format!("Unknown V2 parameter: {}", param));
                    }
                }
            }

            return Ok(JobProcessorConfig::V2 {
                num_partitions,
                enable_core_affinity,
            });
        }

        Err(format!(
            "Unknown processor config: '{}'. Supported: 'v1:simple', 'v1:transactional', 'v2', 'v2:8', 'v2:affinity'",
            s
        ))
    }
}

impl std::fmt::Display for JobProcessorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobProcessorConfig::V1Simple => write!(f, "v1:simple"),
            JobProcessorConfig::V1Transactional => write!(f, "v1:transactional"),
            JobProcessorConfig::V2 {
                num_partitions,
                enable_core_affinity,
            } => {
                write!(f, "v2")?;
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
    fn test_v1_simple_config() {
        let config = JobProcessorConfig::V1Simple;
        assert_eq!(config.to_string(), "v1:simple");
        assert!(config.description().contains("V1 Simple"));
    }

    #[test]
    fn test_v1_transactional_config() {
        let config = JobProcessorConfig::V1Transactional;
        assert_eq!(config.to_string(), "v1:transactional");
        assert!(config.description().contains("V1 Transactional"));
    }

    #[test]
    fn test_v2_config_default() {
        let config = JobProcessorConfig::V2 {
            num_partitions: None,
            enable_core_affinity: false,
        };
        assert_eq!(config.to_string(), "v2");
    }

    #[test]
    fn test_v2_config_with_partitions() {
        let config = JobProcessorConfig::V2 {
            num_partitions: Some(8),
            enable_core_affinity: false,
        };
        assert_eq!(config.to_string(), "v2:8");
    }

    #[test]
    fn test_v2_config_with_affinity() {
        let config = JobProcessorConfig::V2 {
            num_partitions: Some(8),
            enable_core_affinity: true,
        };
        assert_eq!(config.to_string(), "v2:8:affinity");
    }

    #[test]
    fn test_parse_v1_simple() {
        let config: JobProcessorConfig = "v1:simple".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V1Simple));

        let config: JobProcessorConfig = "V1:SIMPLE".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V1Simple));
    }

    #[test]
    fn test_parse_v1_transactional() {
        let config: JobProcessorConfig = "v1:transactional".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V1Transactional));

        let config: JobProcessorConfig = "V1:TRANSACTIONAL".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V1Transactional));
    }

    #[test]
    fn test_parse_v2() {
        let config: JobProcessorConfig = "v2".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V2 { .. }));

        let config: JobProcessorConfig = "V2".parse().unwrap();
        assert!(matches!(config, JobProcessorConfig::V2 { .. }));
    }

    #[test]
    fn test_parse_v2_with_partitions() {
        let config: JobProcessorConfig = "v2:8".parse().unwrap();
        if let JobProcessorConfig::V2 {
            num_partitions: Some(n),
            ..
        } = config
        {
            assert_eq!(n, 8);
        } else {
            panic!("Expected V2 with 8 partitions");
        }
    }

    #[test]
    fn test_parse_v2_with_affinity() {
        let config: JobProcessorConfig = "v2:affinity".parse().unwrap();
        if let JobProcessorConfig::V2 {
            enable_core_affinity,
            ..
        } = config
        {
            assert!(enable_core_affinity);
        } else {
            panic!("Expected V2 with affinity");
        }
    }

    #[test]
    fn test_parse_v2_with_both() {
        let config: JobProcessorConfig = "v2:8:affinity".parse().unwrap();
        if let JobProcessorConfig::V2 {
            num_partitions: Some(n),
            enable_core_affinity,
        } = config
        {
            assert_eq!(n, 8);
            assert!(enable_core_affinity);
        } else {
            panic!("Expected V2 with 8 partitions and affinity");
        }
    }

    #[test]
    fn test_parse_invalid() {
        let result: Result<JobProcessorConfig, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_default_is_v2() {
        let config = JobProcessorConfig::default();
        assert!(matches!(config, JobProcessorConfig::V2 { .. }));
    }

    #[test]
    fn test_description() {
        let v2 = JobProcessorConfig::V2 {
            num_partitions: Some(8),
            enable_core_affinity: false,
        };
        assert!(v2.description().contains("V2"));
        assert!(v2.description().contains("8"));
    }

    #[test]
    fn test_to_partitioned_job_config_v2() {
        let config = JobProcessorConfig::V2 {
            num_partitions: Some(8),
            enable_core_affinity: true,
        };
        let pjc = config.to_partitioned_job_config();
        assert_eq!(pjc.num_partitions, Some(8));
        assert!(pjc.enable_core_affinity);
    }
}
