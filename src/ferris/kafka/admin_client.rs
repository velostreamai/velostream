//! Admin client utilities for Kafka topic management
//!
//! This module provides functionality to create and manage Kafka topics
//! using the rdkafka admin client.

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::time::Duration;

/// Admin client for managing Kafka topics
pub struct KafkaAdminClient {
    admin: AdminClient<DefaultClientContext>,
}

impl KafkaAdminClient {
    /// Create a new admin client
    pub fn new(bootstrap_servers: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("client.id", "ferris-admin-client")
            .create()?;

        Ok(Self { admin })
    }

    /// Create a topic with specified configuration
    pub async fn create_topic(
        &self,
        topic_name: &str,
        partitions: i32,
        replication_factor: Option<i32>,
        config: Option<HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let replication = match replication_factor {
            Some(factor) => TopicReplication::Fixed(factor),
            None => TopicReplication::Fixed(1), // Default to replication factor of 1
        };

        let mut new_topic = NewTopic::new(topic_name, partitions, replication);

        // Add topic-level configurations if provided
        if let Some(ref topic_config) = config {
            for (key, value) in topic_config {
                new_topic = new_topic.set(key, value);
            }
        }

        let admin_opts = AdminOptions::new()
            .operation_timeout(Some(Duration::from_secs(30)))
            .request_timeout(Some(Duration::from_secs(30)));

        let results = self.admin.create_topics(&[new_topic], &admin_opts).await?;

        for result in results {
            match result {
                Ok(topic) => {
                    println!("✅ Created topic: {} with {} partitions", topic, partitions);
                }
                Err((topic, error)) => {
                    if error.to_string().contains("already exists") {
                        println!("ℹ️ Topic {} already exists, continuing...", topic);
                    } else {
                        return Err(format!("Failed to create topic {}: {}", topic, error).into());
                    }
                }
            }
        }

        Ok(())
    }

    /// Create a performance test topic with optimized settings
    pub async fn create_performance_topic(
        &self,
        topic_name: &str,
        partitions: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut config = HashMap::new();

        // Performance-optimized topic configurations
        config.insert("compression.type".to_string(), "lz4".to_string());
        config.insert("cleanup.policy".to_string(), "delete".to_string());
        config.insert("retention.ms".to_string(), "3600000".to_string()); // 1 hour
        config.insert("segment.ms".to_string(), "600000".to_string()); // 10 minutes
        config.insert("max.message.bytes".to_string(), "67108864".to_string()); // 64MB
        config.insert("min.insync.replicas".to_string(), "1".to_string());

        self.create_topic(topic_name, partitions, Some(1), Some(config))
            .await
    }

    /// Delete a topic (useful for cleanup)
    pub async fn delete_topic(&self, topic_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let admin_opts = AdminOptions::new()
            .operation_timeout(Some(Duration::from_secs(30)))
            .request_timeout(Some(Duration::from_secs(30)));

        let results = self.admin.delete_topics(&[topic_name], &admin_opts).await?;

        for result in results {
            match result {
                Ok(topic) => {
                    println!("🗑️ Deleted topic: {}", topic);
                }
                Err((topic, error)) => {
                    if error.to_string().contains("does not exist") {
                        println!("ℹ️ Topic {} does not exist, nothing to delete", topic);
                    } else {
                        return Err(format!("Failed to delete topic {}: {}", topic, error).into());
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a topic exists
    pub async fn topic_exists(&self, topic_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(Some(topic_name), Duration::from_secs(10))?;

        Ok(metadata
            .topics()
            .iter()
            .any(|topic| topic.name() == topic_name))
    }

    /// Get topic partition count
    pub async fn get_partition_count(
        &self,
        topic_name: &str,
    ) -> Result<i32, Box<dyn std::error::Error>> {
        let metadata = self
            .admin
            .inner()
            .fetch_metadata(Some(topic_name), Duration::from_secs(10))?;

        for topic in metadata.topics() {
            if topic.name() == topic_name {
                return Ok(topic.partitions().len() as i32);
            }
        }

        Err(format!("Topic {} not found", topic_name).into())
    }
}
