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
}

impl<K, V> Message<K, V> {
    /// Creates a new message with the given key, value, and headers
    pub fn new(key: Option<K>, value: V, headers: Headers) -> Self {
        Self { key, value, headers }
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
        let message = Message::new(Some("key".to_string()), "value".to_string(), headers);

        assert_eq!(message.key(), Some(&"key".to_string()));
        assert_eq!(message.value(), &"value".to_string());
        assert_eq!(message.headers().get("source"), Some("test"));
    }

    #[test]
    fn test_message_consumption() {
        let headers = Headers::new().insert("source", "test");
        let message = Message::new(Some("key".to_string()), "value".to_string(), headers);

        let (key, value, headers) = message.into_parts();
        assert_eq!(key, Some("key".to_string()));
        assert_eq!(value, "value".to_string());
        assert_eq!(headers.get("source"), Some("test"));
    }

    #[test]
    fn test_message_selective_consumption() {
        let headers = Headers::new().insert("source", "test");
        let message = Message::new(Some("key".to_string()), "value".to_string(), headers);

        let value = message.into_value();
        assert_eq!(value, "value".to_string());
    }
}