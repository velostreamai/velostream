use std::collections::HashMap;
use std::time::Duration;

/// Environment variable name for broker address family configuration
pub const BROKER_ADDRESS_FAMILY_ENV: &str = "VELOSTREAM_BROKER_ADDRESS_FAMILY";

/// Valid values for broker address family configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerAddressFamily {
    /// Force IPv4 only (useful for testcontainers, Docker)
    V4,
    /// Force IPv6 only
    V6,
    /// Allow both IPv4 and IPv6 (librdkafka default)
    Any,
}

impl BrokerAddressFamily {
    /// Get the librdkafka configuration value
    pub fn as_librdkafka_value(&self) -> &'static str {
        match self {
            BrokerAddressFamily::V4 => "v4",
            BrokerAddressFamily::V6 => "v6",
            BrokerAddressFamily::Any => "any",
        }
    }

    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "v4" | "ipv4" => BrokerAddressFamily::V4,
            "v6" | "ipv6" => BrokerAddressFamily::V6,
            "any" | "both" => BrokerAddressFamily::Any,
            _ => {
                log::warn!(
                    "Invalid broker address family '{}', using default 'v4'. \
                     Valid values: v4, v6, any",
                    s
                );
                BrokerAddressFamily::V4
            }
        }
    }

    /// Returns true if this family should be explicitly configured
    /// (i.e., not the librdkafka default "any")
    pub fn should_configure(&self) -> bool {
        *self != BrokerAddressFamily::Any
    }
}

impl Default for BrokerAddressFamily {
    fn default() -> Self {
        // Default to v4 to avoid common IPv6 issues with containers
        BrokerAddressFamily::V4
    }
}

/// Get the configured broker address family from environment
///
/// Reads from `VELOSTREAM_BROKER_ADDRESS_FAMILY` environment variable.
/// Valid values:
/// - `v4` or `ipv4` - Force IPv4 only (default, best for Docker/testcontainers)
/// - `v6` or `ipv6` - Force IPv6 only
/// - `any` or `both` - Allow both (librdkafka default)
///
/// If not set or invalid, defaults to `v4`.
///
/// This is useful when:
/// - Running with testcontainers where localhost may resolve to IPv6
/// - Brokers advertise hostnames that resolve differently on IPv4/IPv6
/// - Network environment has IPv6 connectivity issues
pub fn get_broker_address_family() -> BrokerAddressFamily {
    std::env::var(BROKER_ADDRESS_FAMILY_ENV)
        .map(|v| BrokerAddressFamily::from_str(&v))
        .unwrap_or_default()
}

/// Apply broker address family configuration to a ClientConfig
///
/// This is a convenience function that applies the broker address family
/// configuration from the environment to a given ClientConfig.
pub fn apply_broker_address_family(config: &mut rdkafka::config::ClientConfig) {
    let family = get_broker_address_family();
    if family.should_configure() {
        config.set("broker.address.family", family.as_librdkafka_value());
    }
}

/// Common configuration fields shared between producer and consumer
///
/// This struct contains all the configuration fields that are identical
/// between KafkaProducer and KafkaConsumer configurations, eliminating
/// duplication and ensuring consistency.
#[derive(Debug, Clone)]
pub struct CommonKafkaConfig {
    /// Kafka broker list (e.g., "localhost:9092" or "broker1:9092,broker2:9092")
    pub brokers: String,
    /// Client ID for this producer/consumer instance
    pub client_id: Option<String>,
    /// Request timeout for Kafka operations
    pub request_timeout: Duration,
    /// Retry backoff time between failed requests
    pub retry_backoff: Duration,
    /// Additional custom configuration properties
    pub custom_config: HashMap<String, String>,
}

impl Default for CommonKafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            client_id: None,
            request_timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(100),
            custom_config: HashMap::new(),
        }
    }
}

impl CommonKafkaConfig {
    /// Create a new common configuration with brokers
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            ..Default::default()
        }
    }

    /// Set client ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set retry backoff
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    /// Add custom configuration property
    pub fn custom_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_config.insert(key.into(), value.into());
        self
    }

    /// Add multiple custom properties
    pub fn custom_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.custom_config.extend(properties);
        self
    }

    /// Clear all custom properties
    pub fn clear_custom_properties(mut self) -> Self {
        self.custom_config.clear();
        self
    }

    /// Get a reference to custom properties
    pub fn custom_properties_ref(&self) -> &HashMap<String, String> {
        &self.custom_config
    }

    /// Apply common development-friendly settings
    pub fn apply_development_preset(&mut self) {
        self.request_timeout = Duration::from_secs(10);
        self.retry_backoff = Duration::from_millis(50);
    }

    /// Apply common production settings
    pub fn apply_production_preset(&mut self) {
        self.request_timeout = Duration::from_secs(60);
        self.retry_backoff = Duration::from_millis(200);
    }

    /// Apply common high-reliability settings
    pub fn apply_high_reliability_preset(&mut self) {
        self.request_timeout = Duration::from_secs(90);
        self.retry_backoff = Duration::from_millis(500);
    }
}

/// Trait for configuration types that contain common Kafka configuration
pub trait HasCommonConfig {
    /// Get a reference to the common configuration
    fn common_config(&self) -> &CommonKafkaConfig;

    /// Get a mutable reference to the common configuration
    fn common_config_mut(&mut self) -> &mut CommonKafkaConfig;

    /// Apply common configuration from another config
    fn apply_common_config(&mut self, common: CommonKafkaConfig) {
        *self.common_config_mut() = common;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CommonKafkaConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert!(config.client_id.is_none());
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.retry_backoff, Duration::from_millis(100));
        assert!(config.custom_config.is_empty());
    }

    #[test]
    fn test_builder_pattern() {
        let config = CommonKafkaConfig::new("broker1:9092,broker2:9092")
            .client_id("test-client")
            .request_timeout(Duration::from_secs(45))
            .retry_backoff(Duration::from_millis(200))
            .custom_property("security.protocol", "SSL")
            .custom_property("ssl.truststore.location", "/path/to/truststore");

        assert_eq!(config.brokers, "broker1:9092,broker2:9092");
        assert_eq!(config.client_id, Some("test-client".to_string()));
        assert_eq!(config.request_timeout, Duration::from_secs(45));
        assert_eq!(config.retry_backoff, Duration::from_millis(200));
        assert_eq!(config.custom_config.len(), 2);
        assert_eq!(
            config.custom_config.get("security.protocol"),
            Some(&"SSL".to_string())
        );
    }

    #[test]
    fn test_presets() {
        let mut config = CommonKafkaConfig::default();

        config.apply_development_preset();
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.retry_backoff, Duration::from_millis(50));

        config.apply_production_preset();
        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert_eq!(config.retry_backoff, Duration::from_millis(200));

        config.apply_high_reliability_preset();
        assert_eq!(config.request_timeout, Duration::from_secs(90));
        assert_eq!(config.retry_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_custom_properties_management() {
        let mut config = CommonKafkaConfig::new("localhost:9092")
            .custom_property("prop1", "value1")
            .custom_property("prop2", "value2");

        assert_eq!(config.custom_properties_ref().len(), 2);

        let mut additional_props = HashMap::new();
        additional_props.insert("prop3".to_string(), "value3".to_string());
        config = config.custom_properties(additional_props);

        assert_eq!(config.custom_config.len(), 3);

        config = config.clear_custom_properties();
        assert!(config.custom_config.is_empty());
    }

    #[test]
    fn test_broker_address_family_parsing() {
        assert_eq!(BrokerAddressFamily::from_str("v4"), BrokerAddressFamily::V4);
        assert_eq!(
            BrokerAddressFamily::from_str("ipv4"),
            BrokerAddressFamily::V4
        );
        assert_eq!(BrokerAddressFamily::from_str("V4"), BrokerAddressFamily::V4);
        assert_eq!(BrokerAddressFamily::from_str("v6"), BrokerAddressFamily::V6);
        assert_eq!(
            BrokerAddressFamily::from_str("ipv6"),
            BrokerAddressFamily::V6
        );
        assert_eq!(
            BrokerAddressFamily::from_str("any"),
            BrokerAddressFamily::Any
        );
        assert_eq!(
            BrokerAddressFamily::from_str("both"),
            BrokerAddressFamily::Any
        );
        // Invalid values default to v4
        assert_eq!(
            BrokerAddressFamily::from_str("invalid"),
            BrokerAddressFamily::V4
        );
    }

    #[test]
    fn test_broker_address_family_librdkafka_value() {
        assert_eq!(BrokerAddressFamily::V4.as_librdkafka_value(), "v4");
        assert_eq!(BrokerAddressFamily::V6.as_librdkafka_value(), "v6");
        assert_eq!(BrokerAddressFamily::Any.as_librdkafka_value(), "any");
    }

    #[test]
    fn test_broker_address_family_should_configure() {
        assert!(BrokerAddressFamily::V4.should_configure());
        assert!(BrokerAddressFamily::V6.should_configure());
        assert!(!BrokerAddressFamily::Any.should_configure());
    }

    #[test]
    fn test_broker_address_family_default() {
        assert_eq!(BrokerAddressFamily::default(), BrokerAddressFamily::V4);
    }
}
