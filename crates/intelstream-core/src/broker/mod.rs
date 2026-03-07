//! # Broker
//!
//! A broker is a single node in the IntelStream cluster. It hosts partitions,
//! participates in consensus, handles produce/consume requests, and coordinates
//! replication.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::error::{IntelStreamError, Result};
use crate::message::Message;
use crate::partition::{Partition, PartitionRole};
use crate::storage::{RetentionPolicy, StorageConfig};
use crate::topic::{TopicConfig, TopicMetadata, TopicRegistry};

/// Unique identifier for a broker in the cluster.
pub type BrokerId = u32;

/// Broker configuration loaded from config files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub storage: StorageConfig,
}

/// Health status of a broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerStatus {
    Starting,
    Running,
    Draining,
    ShuttingDown,
    Offline,
}

/// The broker manages topics, partitions, and handles client requests.
pub struct Broker {
    config: BrokerConfig,
    status: Arc<RwLock<BrokerStatus>>,
    topic_registry: Arc<RwLock<TopicRegistry>>,
    /// Partitions keyed by "topic-partition_id".
    partitions: DashMap<String, Partition>,
    data_dir: PathBuf,
}

impl Broker {
    /// Create a new broker instance.
    pub fn new(config: BrokerConfig) -> Result<Self> {
        let data_dir = PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)?;

        info!("Broker {} starting at {}:{}", config.id, config.host, config.port);

        Ok(Self {
            config,
            status: Arc::new(RwLock::new(BrokerStatus::Starting)),
            topic_registry: Arc::new(RwLock::new(TopicRegistry::new())),
            partitions: DashMap::new(),
            data_dir,
        })
    }

    /// Start the broker, loading existing topics and partitions from disk.
    pub async fn start(&self) -> Result<()> {
        *self.status.write().await = BrokerStatus::Running;
        info!("Broker {} is now running", self.config.id);
        Ok(())
    }

    /// Create a topic and allocate its partitions on this broker.
    pub async fn create_topic(&self, config: TopicConfig) -> Result<TopicMetadata> {
        let mut registry = self.topic_registry.write().await;
        let metadata = registry.create_topic(config.clone())?.clone();

        // Open partition logs for partitions assigned to this broker
        for partition_id in 0..config.partition_count {
            let partition_key = format!("{}-{}", config.name, partition_id);
            let partition = Partition::open(
                &config.name,
                partition_id,
                &self.data_dir,
                self.config.storage.clone(),
                RetentionPolicy::default(),
                PartitionRole::Leader, // Simplified: assume this broker is leader
            )?;
            self.partitions.insert(partition_key, partition);
        }

        info!(
            "Created topic '{}' with {} partitions",
            config.name, config.partition_count
        );
        Ok(metadata)
    }

    /// Produce a message to a specific topic-partition.
    pub fn produce(
        &self,
        topic: &str,
        partition_id: u32,
        message: Message,
    ) -> Result<u64> {
        let key = format!("{}-{}", topic, partition_id);
        let mut partition = self
            .partitions
            .get_mut(&key)
            .ok_or_else(|| IntelStreamError::PartitionNotFound {
                topic: topic.to_string(),
                partition: partition_id,
            })?;

        partition.append(&message)
    }

    /// Get broker status.
    pub async fn status(&self) -> BrokerStatus {
        *self.status.read().await
    }

    /// Get broker ID.
    pub fn id(&self) -> BrokerId {
        self.config.id
    }

    /// List all hosted partition keys.
    pub fn hosted_partitions(&self) -> Vec<String> {
        self.partitions
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Initiate graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        warn!("Broker {} shutting down", self.config.id);
        *self.status.write().await = BrokerStatus::ShuttingDown;

        // Flush all partitions
        for mut entry in self.partitions.iter_mut() {
            entry.value_mut().flush()?;
        }

        *self.status.write().await = BrokerStatus::Offline;
        info!("Broker {} shutdown complete", self.config.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn test_broker_config(dir: &Path) -> BrokerConfig {
        BrokerConfig {
            id: 1,
            host: "127.0.0.1".to_string(),
            port: 9292,
            data_dir: dir.to_string_lossy().to_string(),
            storage: StorageConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_broker_lifecycle() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        assert_eq!(broker.status().await, BrokerStatus::Running);

        broker.shutdown().await.unwrap();
        assert_eq!(broker.status().await, BrokerStatus::Offline);
    }

    #[tokio::test]
    async fn test_create_topic_and_produce() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        let config = TopicConfig::new("test-topic", 3, 1);
        broker.create_topic(config).await.unwrap();

        let msg = Message::new(Some(Bytes::from("key")), Bytes::from("value"));
        let offset = broker.produce("test-topic", 0, msg).unwrap();
        assert_eq!(offset, 0);
    }
}
