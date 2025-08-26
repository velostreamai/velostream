//! Comprehensive tests for Phase 2 Enhanced Kafka Serialization Format System
//!
//! These tests validate the configurable serialization functionality that enables
//! runtime format selection via SQL WITH clauses.

use ferrisstreams::ferris::kafka::serialization_format::{
    SerializationConfig, SerializationFactory, SerializationFormat
};
use ferrisstreams::ferris::kafka::serialization::SerializationError;
use std::collections::HashMap;

#[cfg(test)]
mod serialization_format_tests {
    use super::*;

    #[test]
    fn test_serialization_format_parsing_all_variants() {
        // Test all supported format strings
        assert_eq!("json".parse::<SerializationFormat>().unwrap(), SerializationFormat::Json);
        assert_eq!("JSON".parse::<SerializationFormat>().unwrap(), SerializationFormat::Json);
        assert_eq!("bytes".parse::<SerializationFormat>().unwrap(), SerializationFormat::Bytes);
        assert_eq!("raw".parse::<SerializationFormat>().unwrap(), SerializationFormat::Bytes);
        assert_eq!("string".parse::<SerializationFormat>().unwrap(), SerializationFormat::String);
        assert_eq!("text".parse::<SerializationFormat>().unwrap(), SerializationFormat::String);

        // Test feature-gated formats
        #[cfg(feature = "avro")]
        {
            let avro_format = "avro".parse::<SerializationFormat>().unwrap();
            assert!(matches!(avro_format, SerializationFormat::Avro { .. }));
        }

        #[cfg(feature = "protobuf")]
        {
            let proto_format = "protobuf".parse::<SerializationFormat>().unwrap();
            assert!(matches!(proto_format, SerializationFormat::Protobuf { .. }));
            
            let proto_format_alt = "proto".parse::<SerializationFormat>().unwrap();
            assert!(matches!(proto_format_alt, SerializationFormat::Protobuf { .. }));
        }
    }

    #[test]
    fn test_serialization_format_parsing_invalid() {
        // Test invalid formats
        assert!("invalid_format".parse::<SerializationFormat>().is_err());
        assert!("xml".parse::<SerializationFormat>().is_err());
        assert!("yaml".parse::<SerializationFormat>().is_err());
        assert!("".parse::<SerializationFormat>().is_err());
        
        // Verify error message contains helpful information
        let err = "unsupported".parse::<SerializationFormat>().unwrap_err();
        let err_msg = format!("{}", err);
        assert!(err_msg.contains("Unsupported serialization format"));
        assert!(err_msg.contains("json"));
        assert!(err_msg.contains("bytes"));
        assert!(err_msg.contains("string"));
    }

    #[test]
    fn test_serialization_format_display() {
        assert_eq!(SerializationFormat::Json.to_string(), "json");
        assert_eq!(SerializationFormat::Bytes.to_string(), "bytes");
        assert_eq!(SerializationFormat::String.to_string(), "string");

        #[cfg(feature = "avro")]
        {
            let avro_format = SerializationFormat::Avro {
                schema_registry_url: "http://localhost:8081".to_string(),
                subject: "test-subject".to_string(),
            };
            assert_eq!(avro_format.to_string(), "avro");
        }

        #[cfg(feature = "protobuf")]
        {
            let proto_format = SerializationFormat::Protobuf {
                message_type: "MyMessage".to_string(),
            };
            assert_eq!(proto_format.to_string(), "protobuf");
        }
    }

    #[test]
    fn test_serialization_config_empty() {
        let config = SerializationConfig::new();
        assert!(config.key_format.is_none());
        assert!(config.value_format.is_none());
        assert!(config.schema_registry_url.is_none());
        assert!(config.custom_properties.is_empty());
        
        // Test defaults
        assert_eq!(config.key_format(), SerializationFormat::Json);
        assert_eq!(config.value_format(), SerializationFormat::Json);
    }

    #[test]
    fn test_serialization_config_from_sql_params_basic() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());
        sql_params.insert("custom.property".to_string(), "custom_value".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        
        assert_eq!(config.key_format(), SerializationFormat::String);
        assert_eq!(config.value_format(), SerializationFormat::Json);
        assert_eq!(config.custom_properties.get("custom.property"), Some(&"custom_value".to_string()));
    }

    #[test]
    fn test_serialization_config_from_sql_params_alternative_keys() {
        // Test both key.serializer and key_serializer work
        let mut sql_params = HashMap::new();
        sql_params.insert("key_serializer".to_string(), "bytes".to_string());
        sql_params.insert("value_serializer".to_string(), "string".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        
        assert_eq!(config.key_format(), SerializationFormat::Bytes);
        assert_eq!(config.value_format(), SerializationFormat::String);
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_serialization_config_avro_parameters() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "avro".to_string());
        sql_params.insert("value.serializer".to_string(), "avro".to_string());
        sql_params.insert("schema.registry.url".to_string(), "http://localhost:8081".to_string());
        sql_params.insert("key.subject".to_string(), "orders-key".to_string());
        sql_params.insert("value.subject".to_string(), "orders-value".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        
        // Verify Avro configuration
        if let SerializationFormat::Avro { schema_registry_url, subject } = config.key_format() {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "orders-key");
        } else {
            panic!("Expected Avro format for key");
        }

        if let SerializationFormat::Avro { schema_registry_url, subject } = config.value_format() {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "orders-value");
        } else {
            panic!("Expected Avro format for value");
        }

        assert_eq!(config.schema_registry_url, Some("http://localhost:8081".to_string()));
        assert_eq!(config.key_subject, Some("orders-key".to_string()));
        assert_eq!(config.value_subject, Some("orders-value".to_string()));
    }

    #[test]
    fn test_serialization_config_validation_valid() {
        // Test valid configurations
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "json".to_string());
        sql_params.insert("value.serializer".to_string(), "string".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        assert!(config.validate().is_ok());
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_serialization_config_validation_avro_missing_registry() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "avro".to_string());
        // Missing schema.registry.url and key.subject

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        let validation_result = config.validate();
        
        assert!(validation_result.is_err());
        let err_msg = format!("{}", validation_result.unwrap_err());
        assert!(err_msg.contains("Schema Registry URL required"));
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_serialization_config_validation_avro_missing_subject() {
        let mut sql_params = HashMap::new();
        sql_params.insert("value.serializer".to_string(), "avro".to_string());
        sql_params.insert("schema.registry.url".to_string(), "http://localhost:8081".to_string());
        // Missing value.subject

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        let validation_result = config.validate();
        
        assert!(validation_result.is_err());
        let err_msg = format!("{}", validation_result.unwrap_err());
        assert!(err_msg.contains("Value subject required"));
    }

    #[test]
    fn test_serialization_factory_validate_format_json() {
        assert!(SerializationFactory::validate_format(&SerializationFormat::Json).is_ok());
        assert!(SerializationFactory::validate_format(&SerializationFormat::Bytes).is_ok());
        assert!(SerializationFactory::validate_format(&SerializationFormat::String).is_ok());
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_serialization_factory_validate_format_avro_valid() {
        let avro_format = SerializationFormat::Avro {
            schema_registry_url: "http://localhost:8081".to_string(),
            subject: "test-subject".to_string(),
        };
        assert!(SerializationFactory::validate_format(&avro_format).is_ok());
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_serialization_factory_validate_format_avro_invalid() {
        // Empty schema registry URL
        let avro_format = SerializationFormat::Avro {
            schema_registry_url: "".to_string(),
            subject: "test-subject".to_string(),
        };
        let result = SerializationFactory::validate_format(&avro_format);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Schema Registry URL cannot be empty"));

        // Empty subject
        let avro_format = SerializationFormat::Avro {
            schema_registry_url: "http://localhost:8081".to_string(),
            subject: "".to_string(),
        };
        let result = SerializationFactory::validate_format(&avro_format);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Subject cannot be empty"));
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_serialization_factory_validate_format_protobuf_valid() {
        let proto_format = SerializationFormat::Protobuf {
            message_type: "com.example.MyMessage".to_string(),
        };
        assert!(SerializationFactory::validate_format(&proto_format).is_ok());
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_serialization_factory_validate_format_protobuf_invalid() {
        let proto_format = SerializationFormat::Protobuf {
            message_type: "".to_string(),
        };
        let result = SerializationFactory::validate_format(&proto_format);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Message type cannot be empty"));
    }

    #[cfg(not(feature = "avro"))]
    #[test]
    fn test_serialization_factory_avro_feature_not_enabled() {
        // This test only runs when avro feature is NOT enabled
        // We can't create Avro format without the feature, but we can test the error case
        let result = "avro".parse::<SerializationFormat>();
        // Should fail because avro feature is not enabled
        assert!(result.is_err());
    }

    #[cfg(not(feature = "protobuf"))]
    #[test]
    fn test_serialization_factory_protobuf_feature_not_enabled() {
        // This test only runs when protobuf feature is NOT enabled
        let result = "protobuf".parse::<SerializationFormat>();
        // Should fail because protobuf feature is not enabled  
        assert!(result.is_err());
    }

    #[test]
    fn test_serialization_config_complex_sql_scenario() {
        // Test a complex, realistic SQL WITH clause scenario
        let mut sql_params = HashMap::new();
        sql_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());
        sql_params.insert("compression.type".to_string(), "snappy".to_string());
        sql_params.insert("acks".to_string(), "all".to_string());
        sql_params.insert("enable.idempotence".to_string(), "true".to_string());
        sql_params.insert("custom.interceptor.classes".to_string(), "com.example.MyInterceptor".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        
        // Verify serialization config
        assert_eq!(config.key_format(), SerializationFormat::String);
        assert_eq!(config.value_format(), SerializationFormat::Json);
        
        // Verify non-serialization properties are captured
        assert_eq!(config.custom_properties.get("bootstrap.servers"), Some(&"localhost:9092".to_string()));
        assert_eq!(config.custom_properties.get("compression.type"), Some(&"snappy".to_string()));
        assert_eq!(config.custom_properties.get("acks"), Some(&"all".to_string()));
        assert_eq!(config.custom_properties.get("enable.idempotence"), Some(&"true".to_string()));
        assert_eq!(config.custom_properties.get("custom.interceptor.classes"), Some(&"com.example.MyInterceptor".to_string()));
        
        // Verify validation passes
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_serialization_factory_json_serializer_creation() {
        use ferrisstreams::ferris::kafka::serialization::JsonSerializer;
        
        let _serializer = SerializationFactory::json_serializer::<serde_json::Value>();
        
        // Verify it's the right type (this is a compile-time check mostly)
        let serializer: JsonSerializer = SerializationFactory::json_serializer::<String>();
        
        // Basic functionality test
        use ferrisstreams::ferris::kafka::serialization::Serializer;
        let test_data = "Hello, World!".to_string();
        let serialized = serializer.serialize(&test_data).unwrap();
        let deserialized: String = serializer.deserialize(&serialized).unwrap();
        assert_eq!(test_data, deserialized);
    }

    #[test]
    fn test_serialization_format_equality() {
        // Test format comparison
        assert_eq!(SerializationFormat::Json, SerializationFormat::Json);
        assert_eq!(SerializationFormat::Bytes, SerializationFormat::Bytes);
        assert_eq!(SerializationFormat::String, SerializationFormat::String);
        
        assert_ne!(SerializationFormat::Json, SerializationFormat::Bytes);
        assert_ne!(SerializationFormat::String, SerializationFormat::Json);

        #[cfg(feature = "avro")]
        {
            let avro1 = SerializationFormat::Avro {
                schema_registry_url: "http://localhost:8081".to_string(),
                subject: "test".to_string(),
            };
            let avro2 = SerializationFormat::Avro {
                schema_registry_url: "http://localhost:8081".to_string(),
                subject: "test".to_string(),
            };
            let avro3 = SerializationFormat::Avro {
                schema_registry_url: "http://localhost:8081".to_string(),
                subject: "different".to_string(),
            };
            
            assert_eq!(avro1, avro2);
            assert_ne!(avro1, avro3);
        }
    }

    #[test]
    fn test_serialization_config_edge_cases() {
        // Empty parameters
        let config = SerializationConfig::from_sql_params(&HashMap::new()).unwrap();
        assert_eq!(config.key_format(), SerializationFormat::Json);
        assert_eq!(config.value_format(), SerializationFormat::Json);

        // Case sensitivity
        let mut sql_params = HashMap::new();
        sql_params.insert("KEY.SERIALIZER".to_string(), "string".to_string()); // Won't match
        sql_params.insert("key.serializer".to_string(), "json".to_string()); // Will match

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        assert_eq!(config.key_format(), SerializationFormat::Json); // Uses the matching key

        // Multiple custom properties
        let mut sql_params = HashMap::new();
        for i in 1..=10 {
            sql_params.insert(format!("custom.property.{}", i), format!("value_{}", i));
        }
        sql_params.insert("key.serializer".to_string(), "string".to_string());

        let config = SerializationConfig::from_sql_params(&sql_params).unwrap();
        assert_eq!(config.custom_properties.len(), 10);
        for i in 1..=10 {
            assert_eq!(
                config.custom_properties.get(&format!("custom.property.{}", i)), 
                Some(&format!("value_{}", i))
            );
        }
    }
}