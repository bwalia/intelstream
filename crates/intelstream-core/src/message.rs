use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A single message in the IntelStream log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier (UUIDv7 for time-ordered keys).
    pub id: Uuid,
    /// Optional key for partitioning and log compaction.
    pub key: Option<Bytes>,
    /// Message payload.
    pub value: Bytes,
    /// Message headers for metadata propagation.
    pub headers: HashMap<String, String>,
    /// Timestamp when the message was produced.
    pub timestamp: DateTime<Utc>,
    /// Schema ID if the message is schema-encoded.
    pub schema_id: Option<u32>,
}

/// Metadata header attached to each persisted message in the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    /// Offset within the partition log.
    pub offset: u64,
    /// Size of the serialized message in bytes.
    pub size: u32,
    /// CRC32 checksum for integrity verification.
    pub crc: u32,
    /// Compression codec used for this message.
    pub compression: Compression,
    /// Timestamp of when the broker appended this message.
    pub append_timestamp: DateTime<Utc>,
    /// Producer epoch for exactly-once semantics.
    pub producer_epoch: u64,
    /// Sequence number within the producer session.
    pub sequence_number: u64,
}

/// A batch of messages for efficient network and disk I/O.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBatch {
    /// First offset in this batch.
    pub base_offset: u64,
    /// Number of messages in the batch.
    pub count: u32,
    /// The batch-level CRC covering all messages.
    pub batch_crc: u32,
    /// Compression applied to the entire batch.
    pub compression: Compression,
    /// Producer ID for idempotency tracking.
    pub producer_id: u64,
    /// Producer epoch for exactly-once semantics.
    pub producer_epoch: u64,
    /// First sequence number in the batch.
    pub base_sequence: u64,
    /// Individual messages.
    pub messages: Vec<Message>,
}

/// Supported compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Compression {
    #[default]
    None,
    Lz4,
    Zstd,
}

impl Message {
    /// Create a new message with a generated UUIDv7 ID and current timestamp.
    pub fn new(key: Option<Bytes>, value: Bytes) -> Self {
        Self {
            id: Uuid::now_v7(),
            key,
            value,
            headers: HashMap::new(),
            timestamp: Utc::now(),
            schema_id: None,
        }
    }

    /// Add a header to this message.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the schema ID for this message.
    pub fn with_schema_id(mut self, schema_id: u32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Compute the serialized size of this message in bytes.
    pub fn size_bytes(&self) -> usize {
        let key_size = self.key.as_ref().map_or(0, |k| k.len());
        let header_size: usize = self.headers.iter().map(|(k, v)| k.len() + v.len()).sum();
        // id(16) + key + value + headers + timestamp(8) + schema_id(4)
        16 + key_size + self.value.len() + header_size + 8 + 4
    }
}

impl MessageBatch {
    /// Create a new empty batch starting at the given offset.
    pub fn new(base_offset: u64, producer_id: u64, producer_epoch: u64) -> Self {
        Self {
            base_offset,
            count: 0,
            batch_crc: 0,
            compression: Compression::default(),
            producer_id,
            producer_epoch,
            base_sequence: 0,
            messages: Vec::new(),
        }
    }

    /// Append a message to the batch.
    pub fn push(&mut self, message: Message) {
        self.messages.push(message);
        self.count = self.messages.len() as u32;
    }

    /// Total serialized size of all messages in the batch.
    pub fn total_size_bytes(&self) -> usize {
        self.messages.iter().map(|m| m.size_bytes()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let message = Message::new(
            Some(Bytes::from("user-123")),
            Bytes::from(r#"{"event":"login"}"#),
        );

        assert!(message.key.is_some());
        assert!(!message.value.is_empty());
        assert!(message.headers.is_empty());
        assert!(message.schema_id.is_none());
    }

    #[test]
    fn test_message_with_headers() {
        let message = Message::new(None, Bytes::from("payload"))
            .with_header("trace-id", "abc-123")
            .with_header("content-type", "application/json");

        assert_eq!(message.headers.len(), 2);
        assert_eq!(message.headers["trace-id"], "abc-123");
    }

    #[test]
    fn test_batch_push() {
        let mut batch = MessageBatch::new(0, 1, 0);
        batch.push(Message::new(None, Bytes::from("msg1")));
        batch.push(Message::new(None, Bytes::from("msg2")));

        assert_eq!(batch.count, 2);
        assert_eq!(batch.messages.len(), 2);
    }
}
