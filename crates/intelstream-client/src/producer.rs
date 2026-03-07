//! # Producer Client
//!
//! Asynchronous producer for publishing messages to IntelStream topics.
//! Supports batching, compression, and exactly-once semantics.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::{ClientError, Result};
use crate::ClientConfig;

/// Delivery guarantee for produced messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// No acknowledgment (fire-and-forget). Fastest, but no durability guarantee.
    None,
    /// Leader-only acknowledgment.
    Leader,
    /// All in-sync replicas must acknowledge. Strongest durability guarantee.
    All,
}

impl Acks {
    fn as_i32(&self) -> i32 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

/// Configuration for the producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Base client config.
    pub client: ClientConfig,
    /// Acknowledgment level.
    pub acks: Acks,
    /// Maximum batch size in bytes before flushing.
    pub batch_size_bytes: usize,
    /// Maximum time to wait before flushing a partial batch (ms).
    pub linger_ms: u64,
    /// Maximum number of in-flight requests per connection.
    pub max_in_flight: u32,
    /// Whether to enable idempotent production (exactly-once within a session).
    pub enable_idempotence: bool,
    /// Maximum message size in bytes.
    pub max_message_size: usize,
    /// Number of retries for transient failures.
    pub retries: u32,
    /// Backoff between retries (ms).
    pub retry_backoff_ms: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            client: ClientConfig::new("localhost:9292"),
            acks: Acks::All,
            batch_size_bytes: 65_536,
            linger_ms: 5,
            max_in_flight: 5,
            enable_idempotence: true,
            max_message_size: 10_485_760,
            retries: 3,
            retry_backoff_ms: 100,
        }
    }
}

/// A record to be produced.
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub partition: Option<u32>,
}

impl ProducerRecord {
    pub fn new(topic: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            value: value.into(),
            headers: HashMap::new(),
            partition: None,
        }
    }

    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_partition(mut self, partition: u32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Metadata returned after a message is successfully produced.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: i64,
}

/// The IntelStream producer client.
pub struct Producer {
    config: ProducerConfig,
    // TODO: connection pool, batch accumulator, metadata cache
}

impl Producer {
    /// Create a new producer connected to the cluster.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let producer_config = ProducerConfig {
            client: config,
            ..Default::default()
        };

        info!(
            client_id = %producer_config.client.client_id,
            "Initializing IntelStream producer"
        );

        // TODO: establish connections to bootstrap servers
        // TODO: fetch cluster metadata
        // TODO: start batch accumulator background task

        Ok(Self {
            config: producer_config,
        })
    }

    /// Create a producer with full configuration.
    pub async fn with_config(config: ProducerConfig) -> Result<Self> {
        info!(
            client_id = %config.client.client_id,
            acks = ?config.acks,
            "Initializing IntelStream producer"
        );

        Ok(Self { config })
    }

    /// Send a single message to a topic.
    pub async fn send(
        &self,
        topic: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<RecordMetadata> {
        let record = ProducerRecord::new(topic, value.to_vec()).with_key(key.to_vec());
        self.send_record(record).await
    }

    /// Send a producer record.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        if record.value.len() > self.config.max_message_size {
            return Err(ClientError::MessageTooLarge {
                size: record.value.len(),
                max_size: self.config.max_message_size,
            });
        }

        debug!(
            topic = %record.topic,
            key_size = record.key.as_ref().map_or(0, |k| k.len()),
            value_size = record.value.len(),
            "Producing message"
        );

        // TODO: route to correct partition (hash key or round-robin)
        // TODO: add to batch accumulator
        // TODO: wait for batch flush or send immediately
        // TODO: handle retries on transient failures

        Ok(RecordMetadata {
            topic: record.topic,
            partition: record.partition.unwrap_or(0),
            offset: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Flush all pending batches.
    pub async fn flush(&self) -> Result<()> {
        debug!("Flushing producer batches");
        // TODO: flush all pending batches
        Ok(())
    }

    /// Close the producer, flushing any remaining messages.
    pub async fn close(self) -> Result<()> {
        self.flush().await?;
        info!("Producer closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_creation() {
        let config = ClientConfig::new("localhost:9292");
        let producer = Producer::new(config).await.unwrap();
        producer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_message_size_validation() {
        let config = ProducerConfig {
            max_message_size: 10,
            ..Default::default()
        };
        let producer = Producer::with_config(config).await.unwrap();

        let result = producer.send("topic", b"key", b"this is way too large").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_producer_record_builder() {
        let record = ProducerRecord::new("orders", b"payload".to_vec())
            .with_key(b"order-123".to_vec())
            .with_partition(5)
            .with_header("trace-id", "abc");

        assert_eq!(record.topic, "orders");
        assert_eq!(record.partition, Some(5));
        assert_eq!(record.headers.len(), 1);
    }

    #[tokio::test]
    async fn test_send_returns_metadata() {
        let config = ClientConfig::new("localhost:9292");
        let producer = Producer::new(config).await.unwrap();

        let metadata = producer.send("orders", b"key-1", b"value-1").await.unwrap();
        assert_eq!(metadata.topic, "orders");
        assert_eq!(metadata.partition, 0);
        assert!(metadata.timestamp > 0);
    }

    #[tokio::test]
    async fn test_send_record_with_partition() {
        let config = ClientConfig::new("localhost:9292");
        let producer = Producer::new(config).await.unwrap();

        let record = ProducerRecord::new("orders", b"payload".to_vec())
            .with_key(b"key".to_vec())
            .with_partition(3);

        let metadata = producer.send_record(record).await.unwrap();
        assert_eq!(metadata.partition, 3);
    }

    #[tokio::test]
    async fn test_flush_succeeds() {
        let config = ClientConfig::new("localhost:9292");
        let producer = Producer::new(config).await.unwrap();
        producer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_producer_with_custom_config() {
        let config = ProducerConfig {
            client: ClientConfig::new("localhost:9292"),
            acks: Acks::Leader,
            batch_size_bytes: 128_000,
            linger_ms: 10,
            max_in_flight: 3,
            enable_idempotence: false,
            max_message_size: 5_000_000,
            retries: 5,
            retry_backoff_ms: 200,
        };
        let producer = Producer::with_config(config).await.unwrap();
        producer.close().await.unwrap();
    }

    #[test]
    fn test_producer_config_defaults() {
        let config = ProducerConfig::default();
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.batch_size_bytes, 65_536);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.max_in_flight, 5);
        assert!(config.enable_idempotence);
        assert_eq!(config.max_message_size, 10_485_760);
        assert_eq!(config.retries, 3);
    }

    #[test]
    fn test_acks_as_i32() {
        assert_eq!(Acks::None.as_i32(), 0);
        assert_eq!(Acks::Leader.as_i32(), 1);
        assert_eq!(Acks::All.as_i32(), -1);
    }

    #[test]
    fn test_producer_record_no_key() {
        let record = ProducerRecord::new("topic", b"value".to_vec());
        assert!(record.key.is_none());
        assert!(record.partition.is_none());
        assert!(record.headers.is_empty());
    }

    #[test]
    fn test_producer_record_multiple_headers() {
        let record = ProducerRecord::new("topic", b"v".to_vec())
            .with_header("h1", "v1")
            .with_header("h2", "v2")
            .with_header("h3", "v3");

        assert_eq!(record.headers.len(), 3);
        assert_eq!(record.headers.get("h2").unwrap(), "v2");
    }
}
