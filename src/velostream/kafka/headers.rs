use rdkafka::message::Headers as KafkaHeaders;
use std::collections::HashMap;
use std::sync::Arc;

/// Custom headers type that provides a clean API for Kafka message headers
///
/// `Headers` wraps an `Arc<HashMap<String, Option<String>>>` to provide an ergonomic interface
/// for working with Kafka message headers. It supports both valued headers and null headers,
/// and provides builder-pattern methods for easy construction.
///
/// **Performance Note:** Headers uses `Arc` for shared ownership, making cloning very cheap
/// (O(1) atomic increment). This is beneficial when messages are cloned or shared across
/// multiple consumers.
///
/// # Examples
///
/// ## Creating Headers
/// ```rust
/// # use velostream::Headers;
/// let headers = Headers::new()
///     .insert("source", "web-api")
///     .insert("version", "1.2.3")
///     .insert("trace-id", "abc-123-def")
///     .insert_null("optional-field");
/// ```
///
/// ## Querying Headers
/// ```rust
/// # use velostream::Headers;
/// # let headers = Headers::new().insert("source", "web-api");
/// // Get a header value
/// if let Some(source) = headers.get("source") {
///     println!("Source: {}", source);
/// }
///
/// // Check if header exists
/// if headers.contains_key("source") {
///     println!("Has source header");
/// }
///
/// // Iterate over all headers
/// for (key, value) in headers.iter() {
///     match value {
///         Some(v) => println!("{}: {}", key, v),
///         None => println!("{}: <null>", key),
///     }
/// }
/// ```
///
/// ## Integration with Messages
/// ```rust,no_run
/// use velostream::{KafkaConsumer, JsonSerializer};
/// use std::time::Duration;
///
/// fn handle_user_created(_value: &String) {}
/// fn handle_user_updated(_value: &String) {}
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let consumer = KafkaConsumer::<String, String, _, _>::new(
///         "localhost:9092",
///         "group",
///         JsonSerializer,
///         JsonSerializer
///     )?;
///     
///     let message = consumer.poll(Duration::from_secs(5)).await?;
///
///     // Access message headers
///     let headers = message.headers();
///     if let Some(event_type) = headers.get("event-type") {
///         match event_type {
///             "user-created" => handle_user_created(message.value()),
///             "user-updated" => handle_user_updated(message.value()),
///             _ => println!("Unknown event type: {}", event_type),
///         }
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Headers {
    inner: Arc<HashMap<String, Option<String>>>,
}

impl Headers {
    /// Creates a new empty headers collection
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
        }
    }

    /// Creates a new headers collection with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Inserts a header with a value
    ///
    /// This method uses copy-on-write semantics when the Arc is shared
    pub fn insert(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        // Copy-on-write: clone HashMap only if Arc is shared
        Arc::make_mut(&mut self.inner).insert(key.into(), Some(value.into()));
        self
    }

    /// Inserts a header with no value (null header)
    ///
    /// This method uses copy-on-write semantics when the Arc is shared
    pub fn insert_null(mut self, key: impl Into<String>) -> Self {
        // Copy-on-write: clone HashMap only if Arc is shared
        Arc::make_mut(&mut self.inner).insert(key.into(), None);
        self
    }

    /// Gets a header value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).and_then(|v| v.as_deref())
    }

    /// Gets a header value by key, including null values
    pub fn get_optional(&self, key: &str) -> Option<&Option<String>> {
        self.inner.get(key)
    }

    /// Checks if a header exists (regardless of value)
    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    /// Returns the number of headers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no headers
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates over all headers
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        self.inner.as_ref().iter()
    }

    /// Converts to rdkafka OwnedHeaders for internal use
    pub(crate) fn to_rdkafka_headers(&self) -> rdkafka::message::OwnedHeaders {
        let mut headers = rdkafka::message::OwnedHeaders::new_with_capacity(self.inner.len());

        for (key, value) in self.inner.as_ref() {
            let header = rdkafka::message::Header {
                key,
                value: value.as_deref(),
            };
            headers = headers.insert(header);
        }

        headers
    }

    /// Creates Headers from rdkafka headers
    pub(crate) fn from_rdkafka_headers<H: KafkaHeaders>(kafka_headers: &H) -> Self {
        let mut headers = HashMap::with_capacity(kafka_headers.count());

        for i in 0..kafka_headers.count() {
            let header = kafka_headers.get(i);
            let key = header.key.to_string();
            let value = header.value.map(|v| {
                // Convert bytes to string, using lossy conversion if needed
                String::from_utf8_lossy(v).into_owned()
            });
            headers.insert(key, value);
        }

        Self {
            inner: Arc::new(headers),
        }
    }

    /// Converts the headers into a standard HashMap, discarding any null values.
    /// This method attempts to take ownership of the inner HashMap if possible,
    /// otherwise it clones the data.
    pub fn into_map(self) -> HashMap<String, String> {
        // Try to unwrap Arc if we're the only owner, otherwise clone
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner
            .into_iter()
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .collect()
    }
}

impl Default for Headers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_creation() {
        let headers = Headers::new()
            .insert("source", "test")
            .insert("version", "1.0")
            .insert_null("optional");

        assert_eq!(headers.get("source"), Some("test"));
        assert_eq!(headers.get("version"), Some("1.0"));
        assert_eq!(headers.get("optional"), None);
        assert!(headers.contains_key("optional"));
        assert_eq!(headers.len(), 3);
    }

    #[test]
    fn test_headers_iteration() {
        let headers = Headers::new()
            .insert("key1", "value1")
            .insert("key2", "value2");

        let mut count = 0;
        for (key, value) in headers.iter() {
            assert!(key == "key1" || key == "key2");
            assert!(value.is_some());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_headers_cheap_clone() {
        // Create headers with multiple entries
        let headers = Headers::new()
            .insert("source", "test-system")
            .insert("trace-id", "abc-123-def-456")
            .insert("span-id", "xyz-789")
            .insert("priority", "high");

        // Clone is cheap (O(1) Arc increment)
        let cloned1 = headers.clone();
        let cloned2 = headers.clone();
        let cloned3 = cloned1.clone();

        // All clones share the same underlying data
        assert_eq!(headers.get("source"), Some("test-system"));
        assert_eq!(cloned1.get("source"), Some("test-system"));
        assert_eq!(cloned2.get("source"), Some("test-system"));
        assert_eq!(cloned3.get("source"), Some("test-system"));

        // They are equal
        assert_eq!(headers, cloned1);
        assert_eq!(cloned1, cloned2);
        assert_eq!(cloned2, cloned3);
    }

    #[test]
    fn test_headers_copy_on_write() {
        // Create headers
        let headers = Headers::new().insert("key1", "value1");

        // Clone headers
        let cloned = headers.clone();

        // Modify cloned headers (triggers copy-on-write)
        let modified = cloned.insert("key2", "value2");

        // Original is unchanged (COW worked)
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("key1"), Some("value1"));
        assert_eq!(headers.get("key2"), None);

        // Modified has both
        assert_eq!(modified.len(), 2);
        assert_eq!(modified.get("key1"), Some("value1"));
        assert_eq!(modified.get("key2"), Some("value2"));
    }
}
