use std::collections::HashMap;
use std::time::Duration;

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
}
