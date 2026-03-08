//! # Broker
//!
//! A broker is a single node in the IntelStream cluster. It hosts partitions,
//! participates in consensus, handles produce/consume requests, and coordinates
//! replication.

use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::consensus::{ConsensusConfig, RaftNode};
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
    /// Raft consensus node for cluster coordination.
    raft_node: Arc<RwLock<RaftNode>>,
}

impl Broker {
    /// Create a new broker instance.
    ///
    /// Peers are discovered from the `INTELSTREAM_CLUSTER_PEERS` environment variable,
    /// which should be a comma-separated list of broker IDs (e.g., "2,3,4").
    pub fn new(config: BrokerConfig) -> Result<Self> {
        let data_dir = PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)?;

        // Parse cluster peers from environment
        let peers: Vec<BrokerId> = std::env::var("INTELSTREAM_CLUSTER_PEERS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|s| s.trim().parse::<BrokerId>().ok())
            .filter(|&id| id != config.id)
            .collect();

        let raft_node = RaftNode::new(config.id, peers.clone(), ConsensusConfig::default());

        info!(
            "Broker {} starting at {}:{} with {} peers",
            config.id,
            config.host,
            config.port,
            peers.len()
        );

        Ok(Self {
            config,
            status: Arc::new(RwLock::new(BrokerStatus::Starting)),
            topic_registry: Arc::new(RwLock::new(TopicRegistry::new())),
            partitions: DashMap::new(),
            data_dir,
            raft_node: Arc::new(RwLock::new(raft_node)),
        })
    }

    /// Start the broker, loading existing topics and partitions from disk.
    ///
    /// If running standalone (no peers), the broker auto-elects as leader.
    pub async fn start(&self) -> Result<()> {
        *self.status.write().await = BrokerStatus::Running;

        // Auto-elect as leader when running as a single node
        let mut raft = self.raft_node.write().await;
        if raft.peers().is_empty() {
            raft.start_election();
            raft.become_leader();
            info!(
                "Broker {} auto-elected as leader (standalone mode)",
                self.config.id
            );
        }
        drop(raft);

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
    pub fn produce(&self, topic: &str, partition_id: u32, message: Message) -> Result<u64> {
        let key = format!("{}-{}", topic, partition_id);
        let mut partition =
            self.partitions
                .get_mut(&key)
                .ok_or_else(|| IntelStreamError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition: partition_id,
                })?;

        partition.append(&message)
    }

    /// Consume messages from a topic-partition.
    pub fn consume(
        &self,
        topic: &str,
        partition_id: u32,
        offset: Option<u64>,
        max_messages: u32,
        group_id: Option<&str>,
    ) -> Result<Vec<(crate::message::MessageHeader, Message)>> {
        let key = format!("{}-{}", topic, partition_id);
        let mut partition =
            self.partitions
                .get_mut(&key)
                .ok_or_else(|| IntelStreamError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition: partition_id,
                })?;

        partition.read(offset, max_messages, group_id)
    }

    /// List all topic names.
    pub async fn list_topics(&self) -> Vec<String> {
        let registry = self.topic_registry.read().await;
        registry
            .list_topics()
            .into_iter()
            .map(String::from)
            .collect()
    }

    /// Get topic metadata by name.
    pub async fn get_topic(&self, name: &str) -> Result<TopicMetadata> {
        let registry = self.topic_registry.read().await;
        registry.get_topic(name).cloned()
    }

    /// Delete a topic and remove its partitions.
    pub async fn delete_topic(&self, name: &str) -> Result<TopicMetadata> {
        let mut registry = self.topic_registry.write().await;
        let metadata = registry.delete_topic(name)?;

        // Remove partitions for this topic
        let partition_keys: Vec<String> = self
            .partitions
            .iter()
            .filter(|entry| entry.key().starts_with(&format!("{}-", name)))
            .map(|entry| entry.key().clone())
            .collect();

        for key in &partition_keys {
            self.partitions.remove(key);
        }

        info!(
            "Deleted topic '{}' ({} partitions removed)",
            name,
            partition_keys.len()
        );
        Ok(metadata)
    }

    /// Commit a consumer group offset for a topic-partition.
    pub fn commit_offset(
        &self,
        topic: &str,
        partition_id: u32,
        group_id: &str,
        offset: u64,
    ) -> Result<()> {
        let key = format!("{}-{}", topic, partition_id);
        let mut partition =
            self.partitions
                .get_mut(&key)
                .ok_or_else(|| IntelStreamError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition: partition_id,
                })?;

        partition.commit_offset(group_id, offset);
        Ok(())
    }

    /// Get broker status.
    pub async fn status(&self) -> BrokerStatus {
        *self.status.read().await
    }

    /// Get broker ID.
    pub fn id(&self) -> BrokerId {
        self.config.id
    }

    /// Get broker config.
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    /// Get a reference to the Raft consensus node.
    pub fn raft_node(&self) -> &Arc<RwLock<RaftNode>> {
        &self.raft_node
    }

    /// Check if this broker is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.raft_node.read().await.is_leader()
    }

    /// List all hosted partition keys.
    pub fn hosted_partitions(&self) -> Vec<String> {
        self.partitions
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the total number of partitions hosted.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
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
    use std::path::Path;

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

    #[tokio::test]
    async fn test_produce_and_consume() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        let config = TopicConfig::new("orders", 1, 1);
        broker.create_topic(config).await.unwrap();

        // Produce 5 messages
        for i in 0..5 {
            let msg = Message::new(None, Bytes::from(format!("order-{}", i)));
            broker.produce("orders", 0, msg).unwrap();
        }

        // Consume all
        let records = broker.consume("orders", 0, Some(0), 10, None).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].0.offset, 0);
        assert_eq!(records[4].0.offset, 4);
    }

    #[tokio::test]
    async fn test_list_topics() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        assert!(broker.list_topics().await.is_empty());

        broker
            .create_topic(TopicConfig::new("alpha", 1, 1))
            .await
            .unwrap();
        broker
            .create_topic(TopicConfig::new("beta", 1, 1))
            .await
            .unwrap();

        let mut topics = broker.list_topics().await;
        topics.sort();
        assert_eq!(topics, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_get_topic() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        broker
            .create_topic(TopicConfig::new("events", 4, 1))
            .await
            .unwrap();

        let meta = broker.get_topic("events").await.unwrap();
        assert_eq!(meta.config.partition_count, 4);

        assert!(broker.get_topic("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        broker
            .create_topic(TopicConfig::new("temp", 2, 1))
            .await
            .unwrap();
        assert_eq!(broker.partition_count(), 2);

        broker.delete_topic("temp").await.unwrap();
        assert!(broker.list_topics().await.is_empty());
        assert_eq!(broker.partition_count(), 0);
    }

    #[tokio::test]
    async fn test_commit_offset() {
        let tmp = TempDir::new().unwrap();
        let broker = Broker::new(test_broker_config(tmp.path())).unwrap();
        broker.start().await.unwrap();

        broker
            .create_topic(TopicConfig::new("logs", 1, 1))
            .await
            .unwrap();

        // Produce some messages
        for i in 0..3 {
            let msg = Message::new(None, Bytes::from(format!("log-{}", i)));
            broker.produce("logs", 0, msg).unwrap();
        }

        // Commit offset
        broker.commit_offset("logs", 0, "my-group", 2).unwrap();

        // Consume from committed offset (no explicit offset)
        let records = broker
            .consume("logs", 0, None, 10, Some("my-group"))
            .unwrap();
        assert_eq!(records.len(), 1); // only offset 2
        assert_eq!(records[0].0.offset, 2);
    }
}
