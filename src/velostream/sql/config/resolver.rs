//! Property Resolution with Environment Variable Support
//!
//! Provides a unified `PropertyResolver` for resolving configuration properties
//! with a consistent precedence chain: ENV_VAR → config property → default.
//!
//! ## Usage
//!
//! ```rust
//! use velostream::velostream::sql::config::resolver::PropertyResolver;
//! use std::collections::HashMap;
//!
//! let resolver = PropertyResolver::new("VELOSTREAM_");
//! let props: HashMap<String, String> = HashMap::new();
//!
//! // Resolves: VELOSTREAM_KAFKA_BROKERS → props["brokers"] → props["bootstrap.servers"] → default
//! let brokers = resolver.resolve(
//!     "KAFKA_BROKERS",
//!     &["brokers", "bootstrap.servers"],
//!     &props,
//!     "localhost:9092".to_string(),
//! );
//! ```

use std::collections::HashMap;
use std::str::FromStr;

/// PropertyResolver provides type-safe property resolution with environment variable support.
///
/// Resolution chain (highest to lowest priority):
/// 1. Environment variable: `{prefix}{env_key}` (e.g., `VELOSTREAM_KAFKA_BROKERS`)
/// 2. Property from props HashMap (tries each key in order)
/// 3. Default value
#[derive(Debug, Clone)]
pub struct PropertyResolver {
    /// Environment variable prefix (e.g., "VELOSTREAM_")
    prefix: String,
}

impl Default for PropertyResolver {
    fn default() -> Self {
        Self::new("VELOSTREAM_")
    }
}

impl PropertyResolver {
    /// Create a new PropertyResolver with the given prefix.
    ///
    /// # Arguments
    /// * `prefix` - Prefix for environment variables (e.g., "VELOSTREAM_")
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Resolve a property value with type conversion.
    ///
    /// Resolution order:
    /// 1. Environment variable: `{prefix}{env_key}`
    /// 2. Properties HashMap (tries each prop_key in order)
    /// 3. Default value
    ///
    /// # Arguments
    /// * `env_key` - Environment variable suffix (without prefix)
    /// * `prop_keys` - Property keys to try in order
    /// * `props` - Properties HashMap to search
    /// * `default` - Default value if not found
    ///
    /// # Example
    /// ```ignore
    /// let brokers = resolver.resolve(
    ///     "KAFKA_BROKERS",                    // → VELOSTREAM_KAFKA_BROKERS
    ///     &["brokers", "bootstrap.servers"],  // prop keys to try
    ///     &props,
    ///     "localhost:9092".to_string(),
    /// );
    /// ```
    pub fn resolve<T: FromStr>(
        &self,
        env_key: &str,
        prop_keys: &[&str],
        props: &HashMap<String, String>,
        default: T,
    ) -> T {
        self.resolve_inner(env_key, prop_keys, props, default, false)
    }

    /// Resolve a property value with logging when env var overrides.
    ///
    /// Same as `resolve()` but logs at debug level when:
    /// - An environment variable overrides config
    /// - A property is found in the HashMap
    pub fn resolve_with_log<T: FromStr>(
        &self,
        env_key: &str,
        prop_keys: &[&str],
        props: &HashMap<String, String>,
        default: T,
    ) -> T {
        self.resolve_inner(env_key, prop_keys, props, default, true)
    }

    /// Internal resolution logic
    fn resolve_inner<T: FromStr>(
        &self,
        env_key: &str,
        prop_keys: &[&str],
        props: &HashMap<String, String>,
        default: T,
        log_enabled: bool,
    ) -> T {
        let full_env_key = format!("{}{}", self.prefix, env_key);

        // 1. Try environment variable first (highest priority)
        if let Ok(env_value) = std::env::var(&full_env_key) {
            if let Ok(parsed) = env_value.parse::<T>() {
                if log_enabled {
                    log::debug!(
                        "PropertyResolver: {} = '{}' (from env var)",
                        full_env_key,
                        env_value
                    );
                }
                return parsed;
            } else if log_enabled {
                log::warn!(
                    "PropertyResolver: {} = '{}' could not be parsed, using fallback",
                    full_env_key,
                    env_value
                );
            }
        }

        // 2. Try property keys in order
        for prop_key in prop_keys {
            if let Some(prop_value) = props.get(*prop_key) {
                if let Ok(parsed) = prop_value.parse::<T>() {
                    if log_enabled {
                        log::debug!(
                            "PropertyResolver: {} = '{}' (from prop '{}')",
                            full_env_key,
                            prop_value,
                            prop_key
                        );
                    }
                    return parsed;
                }
            }
        }

        // 3. Return default
        if log_enabled {
            log::debug!("PropertyResolver: {} using default value", full_env_key);
        }
        default
    }

    /// Resolve an optional property (returns None if not found).
    ///
    /// Resolution order:
    /// 1. Environment variable: `{prefix}{env_key}`
    /// 2. Properties HashMap (tries each prop_key in order)
    /// 3. None
    pub fn resolve_optional<T: FromStr>(
        &self,
        env_key: &str,
        prop_keys: &[&str],
        props: &HashMap<String, String>,
    ) -> Option<T> {
        let full_env_key = format!("{}{}", self.prefix, env_key);

        // 1. Try environment variable first
        if let Ok(env_value) = std::env::var(&full_env_key) {
            if let Ok(parsed) = env_value.parse::<T>() {
                return Some(parsed);
            }
        }

        // 2. Try property keys in order
        for prop_key in prop_keys {
            if let Some(prop_value) = props.get(*prop_key) {
                if let Ok(parsed) = prop_value.parse::<T>() {
                    return Some(parsed);
                }
            }
        }

        // 3. Not found
        None
    }

    /// Resolve a boolean property.
    ///
    /// Accepts: "true", "1", "yes", "on" (case-insensitive) as true
    /// Accepts: "false", "0", "no", "off" (case-insensitive) as false
    pub fn resolve_bool(
        &self,
        env_key: &str,
        prop_keys: &[&str],
        props: &HashMap<String, String>,
        default: bool,
    ) -> bool {
        let full_env_key = format!("{}{}", self.prefix, env_key);

        // 1. Try environment variable first
        if let Ok(env_value) = std::env::var(&full_env_key) {
            return parse_bool(&env_value).unwrap_or(default);
        }

        // 2. Try property keys in order
        for prop_key in prop_keys {
            if let Some(prop_value) = props.get(*prop_key) {
                if let Some(parsed) = parse_bool(prop_value) {
                    return parsed;
                }
            }
        }

        // 3. Return default
        default
    }

    /// Get the configured prefix
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Build the full environment variable name
    pub fn env_var_name(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

/// Parse a boolean from various string representations
fn parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Global default resolver with VELOSTREAM_ prefix
pub fn default_resolver() -> PropertyResolver {
    PropertyResolver::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_resolve_from_default() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let props: HashMap<String, String> = HashMap::new();

        let result: String = resolver.resolve(
            "NONEXISTENT_KEY",
            &["nonexistent"],
            &props,
            "default_value".to_string(),
        );

        assert_eq!(result, "default_value");
    }

    #[test]
    fn test_resolve_from_props() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("my_key".to_string(), "prop_value".to_string());

        let result: String = resolver.resolve(
            "MY_KEY",
            &["my_key", "fallback_key"],
            &props,
            "default".to_string(),
        );

        assert_eq!(result, "prop_value");
    }

    #[test]
    fn test_resolve_from_env() {
        let resolver = PropertyResolver::new("TEST_RESOLVER_");

        // Set env var (unsafe in Rust 2024 due to thread safety)
        // SAFETY: This test runs in isolation and we clean up after
        unsafe {
            env::set_var("TEST_RESOLVER_MY_VAR", "env_value");
        }

        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("my_var".to_string(), "prop_value".to_string());

        let result: String = resolver.resolve("MY_VAR", &["my_var"], &props, "default".to_string());

        // Env var should win over props
        assert_eq!(result, "env_value");

        // Cleanup
        // SAFETY: Cleaning up the env var we set
        unsafe {
            env::remove_var("TEST_RESOLVER_MY_VAR");
        }
    }

    #[test]
    fn test_resolve_prop_key_order() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("second_key".to_string(), "second_value".to_string());
        props.insert("first_key".to_string(), "first_value".to_string());

        // Should find first_key first since it's listed first
        let result: String = resolver.resolve(
            "TEST",
            &["first_key", "second_key"],
            &props,
            "default".to_string(),
        );

        assert_eq!(result, "first_value");
    }

    #[test]
    fn test_resolve_numeric() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("port".to_string(), "9092".to_string());

        let result: u16 = resolver.resolve("PORT", &["port"], &props, 8080);

        assert_eq!(result, 9092);
    }

    #[test]
    fn test_resolve_bool() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("enabled".to_string(), "true".to_string());
        props.insert("disabled".to_string(), "false".to_string());
        props.insert("one".to_string(), "1".to_string());
        props.insert("yes".to_string(), "yes".to_string());

        assert!(resolver.resolve_bool("ENABLED", &["enabled"], &props, false));
        assert!(!resolver.resolve_bool("DISABLED", &["disabled"], &props, true));
        assert!(resolver.resolve_bool("ONE", &["one"], &props, false));
        assert!(resolver.resolve_bool("YES", &["yes"], &props, false));
    }

    #[test]
    fn test_resolve_optional_found() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("key".to_string(), "value".to_string());

        let result: Option<String> = resolver.resolve_optional("KEY", &["key"], &props);

        assert_eq!(result, Some("value".to_string()));
    }

    #[test]
    fn test_resolve_optional_not_found() {
        let resolver = PropertyResolver::new("TEST_PREFIX_");
        let props: HashMap<String, String> = HashMap::new();

        let result: Option<String> =
            resolver.resolve_optional("NONEXISTENT", &["nonexistent"], &props);

        assert_eq!(result, None);
    }

    #[test]
    fn test_env_var_name() {
        let resolver = PropertyResolver::new("VELOSTREAM_");
        assert_eq!(
            resolver.env_var_name("KAFKA_BROKERS"),
            "VELOSTREAM_KAFKA_BROKERS"
        );
    }

    #[test]
    fn test_default_resolver() {
        let resolver = default_resolver();
        assert_eq!(resolver.prefix(), "VELOSTREAM_");
    }
}
