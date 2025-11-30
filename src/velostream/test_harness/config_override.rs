//! Configuration override system for test infrastructure
//!
//! Intercepts source/sink configurations and overrides:
//! - bootstrap.servers → testcontainers Kafka
//! - topic names → test_{run_id}_{original}
//! - file paths → temp directory paths

use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration overrides for test execution
#[derive(Debug, Clone)]
pub struct ConfigOverrides {
    /// Bootstrap servers override (testcontainers Kafka)
    pub bootstrap_servers: Option<String>,

    /// Topic name prefix (test_{run_id}_)
    pub topic_prefix: String,

    /// Whether to apply topic prefix (default: true)
    pub apply_topic_prefix: bool,

    /// Temp directory for file sinks
    pub temp_dir: Option<PathBuf>,

    /// Additional property overrides
    pub properties: HashMap<String, String>,
}

impl ConfigOverrides {
    /// Create new overrides with run ID
    pub fn new(run_id: &str) -> Self {
        Self {
            bootstrap_servers: None,
            topic_prefix: format!("test_{}_", run_id),
            apply_topic_prefix: true,
            temp_dir: None,
            properties: HashMap::new(),
        }
    }

    /// Disable topic prefix (use exact topic names from config)
    pub fn without_topic_prefix(mut self) -> Self {
        self.apply_topic_prefix = false;
        self
    }

    /// Set bootstrap servers override
    pub fn with_bootstrap_servers(mut self, servers: &str) -> Self {
        self.bootstrap_servers = Some(servers.to_string());
        self
    }

    /// Set temp directory for file sinks
    pub fn with_temp_dir(mut self, dir: PathBuf) -> Self {
        self.temp_dir = Some(dir);
        self
    }

    /// Add a property override
    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    /// Override a topic name with the test prefix (if enabled)
    pub fn override_topic(&self, original: &str) -> String {
        if self.apply_topic_prefix {
            format!("{}{}", self.topic_prefix, original)
        } else {
            original.to_string()
        }
    }

    /// Override a file path to use temp directory
    pub fn override_file_path(&self, original: &str) -> PathBuf {
        if let Some(ref temp_dir) = self.temp_dir {
            // Extract filename from original path
            let filename = std::path::Path::new(original)
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_else(|| original.replace(['/', '\\'], "_"));
            temp_dir.join(filename)
        } else {
            PathBuf::from(original)
        }
    }

    /// Apply overrides to a configuration map
    pub fn apply_to_config(&self, config: &mut HashMap<String, String>) {
        // Override bootstrap.servers if set
        if let Some(ref servers) = self.bootstrap_servers {
            // Handle various bootstrap.servers key formats
            let bootstrap_keys = [
                "bootstrap.servers",
                "bootstrap_servers",
                "kafka.bootstrap.servers",
            ];
            for key in bootstrap_keys {
                if config.contains_key(key) {
                    config.insert(key.to_string(), servers.clone());
                }
            }
            // Also add standard key if none exists
            config.insert("bootstrap.servers".to_string(), servers.clone());
        }

        // Override topic names
        let topic_keys: Vec<String> = config
            .keys()
            .filter(|k| k.contains("topic"))
            .cloned()
            .collect();
        for key in topic_keys {
            if let Some(original) = config.get(&key) {
                let overridden = self.override_topic(original);
                config.insert(key, overridden);
            }
        }

        // Override file paths
        let file_keys: Vec<String> = config
            .keys()
            .filter(|k| k.contains("path") || k.contains("file") || k.contains("output"))
            .cloned()
            .collect();
        for key in file_keys {
            if let Some(original) = config.get(&key) {
                let overridden = self.override_file_path(original);
                config.insert(key, overridden.display().to_string());
            }
        }

        // Apply additional property overrides
        for (key, value) in &self.properties {
            config.insert(key.clone(), value.clone());
        }
    }

    /// Apply overrides to SQL WITH clause properties
    pub fn apply_to_sql_properties(&self, sql: &str) -> String {
        let mut result = sql.to_string();

        // Override bootstrap.servers in SQL
        if let Some(ref servers) = self.bootstrap_servers {
            // Pattern: 'bootstrap.servers' = 'original_value'
            let patterns = [
                (
                    r#"'bootstrap.servers'\s*=\s*'[^']*'"#,
                    format!("'bootstrap.servers' = '{}'", servers),
                ),
                (
                    r#"'bootstrap_servers'\s*=\s*'[^']*'"#,
                    format!("'bootstrap_servers' = '{}'", servers),
                ),
            ];

            for (pattern, replacement) in patterns {
                if let Ok(re) = regex::Regex::new(pattern) {
                    result = re.replace_all(&result, replacement.as_str()).to_string();
                }
            }
        }

        // Override topic names in SQL WITH clauses
        // Pattern: '<source_name>.topic' = '<topic_name>' or 'topic' = '<topic_name>'
        if self.apply_topic_prefix {
            // Match patterns like 'market_data.topic' = 'my_topic' or 'topic' = 'my_topic'
            if let Ok(re) = regex::Regex::new(r#"'([^']*\.)?topic'\s*=\s*'([^']*)'"#) {
                result = re
                    .replace_all(&result, |caps: &regex::Captures| {
                        let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                        let topic_name = &caps[2];
                        let new_topic = self.override_topic(topic_name);
                        format!("'{}topic' = '{}'", prefix, new_topic)
                    })
                    .to_string();
            }
        }

        result
    }
}

/// Builder for creating config overrides from test infrastructure
pub struct ConfigOverrideBuilder {
    overrides: ConfigOverrides,
}

impl ConfigOverrideBuilder {
    /// Create new builder with run ID
    pub fn new(run_id: &str) -> Self {
        Self {
            overrides: ConfigOverrides::new(run_id),
        }
    }

    /// Set bootstrap servers from test infrastructure
    pub fn bootstrap_servers(mut self, servers: &str) -> Self {
        self.overrides.bootstrap_servers = Some(servers.to_string());
        self
    }

    /// Set temp directory from test infrastructure
    pub fn temp_dir(mut self, dir: PathBuf) -> Self {
        self.overrides.temp_dir = Some(dir);
        self
    }

    /// Add custom property override
    pub fn property(mut self, key: &str, value: &str) -> Self {
        self.overrides
            .properties
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Disable topic prefix (use exact topic names from config)
    pub fn no_topic_prefix(mut self) -> Self {
        self.overrides.apply_topic_prefix = false;
        self
    }

    /// Build the config overrides
    pub fn build(self) -> ConfigOverrides {
        self.overrides
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_override() {
        let overrides = ConfigOverrides::new("abc123");
        assert_eq!(
            overrides.override_topic("market_data"),
            "test_abc123_market_data"
        );
    }

    #[test]
    fn test_file_path_override() {
        let overrides = ConfigOverrides::new("abc123").with_temp_dir(PathBuf::from("/tmp/test"));

        let overridden = overrides.override_file_path("/data/output.jsonl");
        assert_eq!(overridden, PathBuf::from("/tmp/test/output.jsonl"));
    }

    #[test]
    fn test_apply_to_config() {
        let overrides = ConfigOverrides::new("abc123").with_bootstrap_servers("localhost:9092");

        let mut config = HashMap::new();
        config.insert("bootstrap.servers".to_string(), "original:9092".to_string());
        config.insert("topic".to_string(), "my_topic".to_string());

        overrides.apply_to_config(&mut config);

        assert_eq!(
            config.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(
            config.get("topic"),
            Some(&"test_abc123_my_topic".to_string())
        );
    }
}
