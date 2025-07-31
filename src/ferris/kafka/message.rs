use crate::ferris::kafka::headers::Headers;

/// A message containing deserialized key, value, and headers
///
/// This struct represents a complete Kafka message with type-safe access to all components:
/// - **Key**: Optional deserialized key of type `K`
/// - **Value**: Deserialized message payload of type `V`
/// - **Headers**: Message metadata as a `Headers` collection
///
/// # Examples
///
/// ```rust,no_run
/// # use ferrisstreams::{Message, Headers};
/// # let message = Message::new(Some("key".to_string()), "value".to_string(), Headers::new());
/// // Access by reference (borrowing)
/// println!("Key: {:?}", message.key());
/// println!("Value: {}", message.value());
/// println!("Headers: {:?}", message.headers());
///
/// // Check for specific headers
/// if let Some(source) = message.headers().get("source") {
///     println!("Message originated from: {}", source);
/// }
///
/// // Consume the message (take ownership)
/// let (key, value, headers) = message.into_parts();
/// // Now you own the key, value, and headers
/// ```
///
/// See the [consumer with headers example](https://github.com/your-repo/examples/consumer_with_headers.rs)
/// for a comprehensive demonstration.
#[derive(Debug)]
pub struct Message<K, V> {
    pub key: Option<K>,
    pub value: V,
    pub headers: Headers,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

impl<K, V> Message<K, V> {
    /// Creates a new message with the given key, value, headers and partition
    pub fn new(
        key: Option<K>,
        value: V,
        headers: Headers,
        partition: i32,
        offset: i64,
        timestamp: Option<i64>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
            partition,
            offset,
            timestamp,
        }
    }

    /// Returns a reference to the message key
    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    /// Returns a reference to the message value
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Returns a reference to the message headers
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Returns the partition ID this message was consumed from
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset of this message in its partition
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp of this message if available
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns a human-readable timestamp string if available
    pub fn timestamp_string(&self) -> Option<String> {
        self.timestamp.map(|ts| {
            chrono::DateTime::from_timestamp_millis(ts)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                .unwrap_or_else(|| format!("{} (invalid datetime)", ts))
        })
    }

    /// Returns true if this message is the first in its partition (offset 0)
    pub fn is_first(&self) -> bool {
        self.offset == 0
    }

    /// Returns a human-readable string containing all metadata about this message
    pub fn metadata_string(&self) -> String {
        let mut parts = vec![
            format!("Partition: {}", self.partition),
            format!("Offset: {}", self.offset),
        ];

        if let Some(ts) = self.timestamp_string() {
            parts.push(format!("Timestamp: {}", ts));
        }

        if self.is_first() {
            parts.push("First message in partition".to_string());
        }

        parts.join(", ")
    }

    /// Consumes the message and returns the owned key
    pub fn into_key(self) -> Option<K> {
        self.key
    }

    /// Consumes the message and returns the owned value
    pub fn into_value(self) -> V {
        self.value
    }

    /// Consumes the message and returns the owned headers
    pub fn into_headers(self) -> Headers {
        self.headers
    }

    /// Consumes the message and returns all components as a tuple
    pub fn into_parts(self) -> (Option<K>, V, Headers) {
        (self.key, self.value, self.headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let headers = Headers::new().insert("source", "test");
        let message = Message::new(
            Some("key".to_string()),
            "value".to_string(),
            headers,
            0,
            42,
            Some(1633046400000),
        );

        assert_eq!(message.key(), Some(&"key".to_string()));
        assert_eq!(message.value(), &"value".to_string());
        assert_eq!(message.headers().get("source"), Some("test"));
        assert_eq!(message.partition(), 0);
        assert_eq!(message.offset(), 42);
        assert_eq!(message.timestamp(), Some(1633046400000));
        assert_eq!(
            message.timestamp_string(),
            Some("2021-10-01 00:00:00.000".to_string())
        );
    }

    #[test]
    fn test_message_consumption() {
        let headers = Headers::new().insert("source", "test");
        let message = Message::new(
            Some("key".to_string()),
            "value".to_string(),
            headers,
            0,
            42,
            Some(1633046400000),
        );

        let (key, value, headers) = message.into_parts();
        assert_eq!(key, Some("key".to_string()));
        assert_eq!(value, "value".to_string());
        assert_eq!(headers.get("source"), Some("test"));
    }

    #[test]
    fn test_message_selective_consumption() {
        let headers = Headers::new().insert("source", "test");
        let message = Message::new(
            Some("key".to_string()),
            "value".to_string(),
            headers,
            0,
            42,
            Some(1633046400000),
        );

        let value = message.into_value();
        assert_eq!(value, "value".to_string());
    }
}
