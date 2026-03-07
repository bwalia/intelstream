//! # Consumer Client
//!
//! Asynchronous consumer for subscribing to IntelStream topics.
//! Supports consumer groups, offset management, and rebalancing.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::error::{ClientError, Result};
use crate::ClientConfig;

/// Strategy for determining the initial offset when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest offset (skip historical messages).
    Latest,
    /// Fail if no committed offset is found.
    None,
}

/// Configuration for the consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Base client config.
    pub client: ClientConfig,
    /// Consumer group ID.
    pub group_id: String,
    /// Auto-commit interval (ms). Set to 0 to disable auto-commit.
    pub auto_commit_interval_ms: u64,
    /// Maximum number of messages to fetch per poll.
    pub max_poll_records: u32,
    /// Maximum bytes to fetch per partition per poll.
    pub max_partition_fetch_bytes: u64,
    /// Strategy for initial offset.
    pub auto_offset_reset: AutoOffsetReset,
    /// Session timeout for consumer group membership (ms).
    pub session_timeout_ms: u64,
    /// Heartbeat interval for consumer group protocol (ms).
    pub heartbeat_interval_ms: u64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            client: ClientConfig::new("localhost:9292"),
            group_id: String::new(),
            auto_commit_interval_ms: 5_000,
            max_poll_records: 500,
            max_partition_fetch_bytes: 1_048_576,
            auto_offset_reset: AutoOffsetReset::Latest,
            session_timeout_ms: 30_000,
            heartbeat_interval_ms: 3_000,
        }
    }
}

/// A consumed record.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: std::collections::HashMap<String, String>,
    pub timestamp: i64,
}

/// The IntelStream consumer client.
pub struct Consumer {
    config: ConsumerConfig,
    subscriptions: Arc<RwLock<HashSet<String>>>,
    // TODO: partition assignment, fetch sessions, heartbeat task
}

impl Consumer {
    /// Create a new consumer and join the specified consumer group.
    pub async fn new(config: ClientConfig, group_id: &str) -> Result<Self> {
        let consumer_config = ConsumerConfig {
            client: config,
            group_id: group_id.to_string(),
            ..Default::default()
        };

        info!(
            client_id = %consumer_config.client.client_id,
            group_id = %consumer_config.group_id,
            "Initializing IntelStream consumer"
        );

        // TODO: connect to cluster
        // TODO: join consumer group
        // TODO: start heartbeat task

        Ok(Self {
            config: consumer_config,
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Create a consumer with full configuration.
    pub async fn with_config(config: ConsumerConfig) -> Result<Self> {
        info!(
            client_id = %config.client.client_id,
            group_id = %config.group_id,
            "Initializing IntelStream consumer"
        );

        Ok(Self {
            config,
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Subscribe to one or more topics.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        for topic in topics {
            subs.insert(topic.to_string());
            info!(topic, group_id = %self.config.group_id, "Subscribed to topic");
        }

        // TODO: trigger rebalance with consumer group coordinator

        Ok(())
    }

    /// Unsubscribe from all topics.
    pub async fn unsubscribe(&self) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        subs.clear();
        info!(group_id = %self.config.group_id, "Unsubscribed from all topics");
        Ok(())
    }

    /// Poll for new messages.
    pub async fn poll(&self, timeout_ms: u64) -> Result<Vec<ConsumerRecord>> {
        debug!(
            timeout_ms,
            group_id = %self.config.group_id,
            "Polling for messages"
        );

        // TODO: fetch messages from assigned partitions
        // TODO: respect max_poll_records and max_partition_fetch_bytes
        // TODO: auto-commit offsets if enabled

        Ok(vec![])
    }

    /// Manually commit offsets for consumed records.
    pub async fn commit(&self) -> Result<()> {
        debug!(group_id = %self.config.group_id, "Committing offsets");
        // TODO: commit current offsets to the cluster
        Ok(())
    }

    /// Commit a specific offset for a topic-partition.
    pub async fn commit_offset(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        debug!(
            topic,
            partition,
            offset,
            group_id = %self.config.group_id,
            "Committing offset"
        );
        // TODO: commit specific offset
        Ok(())
    }

    /// Get the current subscription set.
    pub async fn subscriptions(&self) -> HashSet<String> {
        self.subscriptions.read().await.clone()
    }

    /// Close the consumer, leaving the consumer group.
    pub async fn close(self) -> Result<()> {
        info!(group_id = %self.config.group_id, "Consumer closing");
        // TODO: leave consumer group
        // TODO: commit final offsets
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_consumer_creation() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "test-group").await.unwrap();
        assert!(consumer.subscriptions().await.is_empty());
    }

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "test-group").await.unwrap();

        consumer.subscribe(&["topic-a", "topic-b"]).await.unwrap();
        assert_eq!(consumer.subscriptions().await.len(), 2);

        consumer.unsubscribe().await.unwrap();
        assert!(consumer.subscriptions().await.is_empty());
    }

    #[tokio::test]
    async fn test_subscribe_deduplicates_topics() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "test-group").await.unwrap();

        consumer.subscribe(&["topic-a", "topic-a", "topic-b"]).await.unwrap();
        // HashSet deduplicates, so only 2 unique topics
        assert_eq!(consumer.subscriptions().await.len(), 2);
    }

    #[tokio::test]
    async fn test_poll_returns_empty_initially() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "test-group").await.unwrap();

        consumer.subscribe(&["topic-a"]).await.unwrap();
        let records = consumer.poll(100).await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_commit_succeeds() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "test-group").await.unwrap();

        consumer.commit().await.unwrap();
        consumer.commit_offset("topic-a", 0, 42).await.unwrap();
    }

    #[tokio::test]
    async fn test_consumer_with_config() {
        let config = ConsumerConfig {
            client: ClientConfig::new("localhost:9292"),
            group_id: "custom-group".to_string(),
            auto_commit_interval_ms: 10_000,
            max_poll_records: 1000,
            max_partition_fetch_bytes: 2_097_152,
            auto_offset_reset: AutoOffsetReset::Earliest,
            session_timeout_ms: 60_000,
            heartbeat_interval_ms: 5_000,
        };
        let consumer = Consumer::with_config(config).await.unwrap();
        assert!(consumer.subscriptions().await.is_empty());
    }

    #[tokio::test]
    async fn test_consumer_close() {
        let config = ClientConfig::new("localhost:9292");
        let consumer = Consumer::new(config, "close-group").await.unwrap();
        consumer.subscribe(&["topic-a"]).await.unwrap();
        consumer.close().await.unwrap();
    }

    #[test]
    fn test_auto_offset_reset_variants() {
        assert_eq!(AutoOffsetReset::Earliest, AutoOffsetReset::Earliest);
        assert_eq!(AutoOffsetReset::Latest, AutoOffsetReset::Latest);
        assert_eq!(AutoOffsetReset::None, AutoOffsetReset::None);
        assert_ne!(AutoOffsetReset::Earliest, AutoOffsetReset::Latest);
    }

    #[test]
    fn test_consumer_config_defaults() {
        let config = ConsumerConfig::default();
        assert_eq!(config.auto_commit_interval_ms, 5_000);
        assert_eq!(config.max_poll_records, 500);
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert_eq!(config.session_timeout_ms, 30_000);
        assert_eq!(config.heartbeat_interval_ms, 3_000);
    }
}
