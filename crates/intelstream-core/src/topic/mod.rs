//! # Topic Management
//!
//! Topics are the primary abstraction for organizing message streams.
//! Each topic consists of one or more partitions distributed across brokers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{IntelStreamError, Result};
use crate::message::Compression;
use crate::storage::RetentionPolicy;

/// Configuration for creating a new topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Topic name (must be unique within the cluster).
    pub name: String,
    /// Number of partitions.
    pub partition_count: u32,
    /// Replication factor (number of replicas including the leader).
    pub replication_factor: u32,
    /// Retention policy for this topic.
    pub retention: RetentionPolicy,
    /// Compression codec for this topic's messages.
    pub compression: Compression,
    /// Whether log compaction is enabled (retains only the latest value per key).
    pub compact: bool,
    /// Custom topic-level settings.
    pub settings: HashMap<String, String>,
}

impl TopicConfig {
    /// Create a new topic configuration with sensible defaults.
    pub fn new(name: impl Into<String>, partition_count: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            partition_count,
            replication_factor,
            retention: RetentionPolicy::default(),
            compression: Compression::None,
            compact: false,
            settings: HashMap::new(),
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(IntelStreamError::Config(
                "Topic name must not be empty".to_string(),
            ));
        }
        if self.partition_count == 0 {
            return Err(IntelStreamError::InvalidPartitionCount(0));
        }
        if self.replication_factor == 0 {
            return Err(IntelStreamError::Config(
                "Replication factor must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Runtime metadata for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic configuration.
    pub config: TopicConfig,
    /// Partition assignment: partition_id -> list of broker IDs (first = leader).
    pub partition_assignments: HashMap<u32, Vec<u32>>,
    /// Creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Whether the topic is marked for deletion.
    pub marked_for_deletion: bool,
}

/// In-memory topic registry managing all known topics.
pub struct TopicRegistry {
    topics: HashMap<String, TopicMetadata>,
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    /// Create a new topic if it does not already exist.
    pub fn create_topic(&mut self, config: TopicConfig) -> Result<&TopicMetadata> {
        config.validate()?;

        if self.topics.contains_key(&config.name) {
            return Err(IntelStreamError::TopicAlreadyExists(config.name.clone()));
        }

        let name = config.name.clone();
        let metadata = TopicMetadata {
            config,
            partition_assignments: HashMap::new(),
            created_at: chrono::Utc::now(),
            marked_for_deletion: false,
        };

        self.topics.insert(name.clone(), metadata);
        Ok(self.topics.get(&name).unwrap())
    }

    /// Look up a topic by name.
    pub fn get_topic(&self, name: &str) -> Result<&TopicMetadata> {
        self.topics
            .get(name)
            .ok_or_else(|| IntelStreamError::TopicNotFound(name.to_string()))
    }

    /// Delete a topic by name.
    pub fn delete_topic(&mut self, name: &str) -> Result<TopicMetadata> {
        self.topics
            .remove(name)
            .ok_or_else(|| IntelStreamError::TopicNotFound(name.to_string()))
    }

    /// List all topic names.
    pub fn list_topics(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
    }

    /// Number of registered topics.
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_topic() {
        let mut registry = TopicRegistry::new();
        let config = TopicConfig::new("orders", 6, 3);
        registry.create_topic(config).unwrap();

        let topic = registry.get_topic("orders").unwrap();
        assert_eq!(topic.config.partition_count, 6);
        assert_eq!(topic.config.replication_factor, 3);
    }

    #[test]
    fn test_duplicate_topic_rejected() {
        let mut registry = TopicRegistry::new();
        registry
            .create_topic(TopicConfig::new("orders", 3, 1))
            .unwrap();
        let result = registry.create_topic(TopicConfig::new("orders", 3, 1));
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_partitions_rejected() {
        let config = TopicConfig::new("bad-topic", 0, 1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_delete_topic() {
        let mut registry = TopicRegistry::new();
        registry
            .create_topic(TopicConfig::new("temp", 1, 1))
            .unwrap();
        assert_eq!(registry.topic_count(), 1);

        registry.delete_topic("temp").unwrap();
        assert_eq!(registry.topic_count(), 0);
    }
}
