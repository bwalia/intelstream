//! # Partition Management
//!
//! Each topic is divided into partitions for parallelism and ordering.
//! A partition owns a commit log and tracks consumer group offsets.

use std::collections::HashMap;

use crate::error::{IntelStreamError, Result};
use crate::message::Message;
use crate::storage::{CommitLog, RetentionPolicy, StorageConfig};
use std::path::Path;

/// The role a broker plays for a given partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionRole {
    Leader,
    Follower,
    Observer,
}

/// A single partition within a topic.
pub struct Partition {
    /// Topic this partition belongs to.
    topic: String,
    /// Partition identifier.
    id: u32,
    /// This broker's role for the partition.
    role: PartitionRole,
    /// The durable commit log backing this partition.
    log: CommitLog,
    /// High watermark: the offset up to which all replicas have acknowledged.
    high_watermark: u64,
    /// Consumer group committed offsets: group_id -> committed offset.
    committed_offsets: HashMap<String, u64>,
}

impl Partition {
    /// Create or open a partition at the given data directory.
    pub fn open(
        topic: &str,
        partition_id: u32,
        data_dir: &Path,
        storage_config: StorageConfig,
        retention: RetentionPolicy,
        role: PartitionRole,
    ) -> Result<Self> {
        let partition_dir = data_dir.join(format!("{}-{}", topic, partition_id));
        let log = CommitLog::open(&partition_dir, storage_config, retention)?;

        Ok(Self {
            topic: topic.to_string(),
            id: partition_id,
            role,
            log,
            high_watermark: 0,
            committed_offsets: HashMap::new(),
        })
    }

    /// Append a message. Only the leader should accept produce requests.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        if self.role != PartitionRole::Leader {
            return Err(IntelStreamError::NotLeader(self.id));
        }
        self.log.append(message)
    }

    /// Advance the high watermark when replicas acknowledge.
    pub fn advance_high_watermark(&mut self, offset: u64) {
        if offset > self.high_watermark {
            self.high_watermark = offset;
        }
    }

    /// Commit a consumer group's offset.
    pub fn commit_offset(&mut self, group_id: &str, offset: u64) {
        self.committed_offsets.insert(group_id.to_string(), offset);
    }

    /// Get the committed offset for a consumer group.
    pub fn committed_offset(&self, group_id: &str) -> Option<u64> {
        self.committed_offsets.get(group_id).copied()
    }

    /// Calculate consumer lag for a group.
    pub fn consumer_lag(&self, group_id: &str) -> u64 {
        let committed = self.committed_offset(group_id).unwrap_or(0);
        self.high_watermark.saturating_sub(committed)
    }

    /// Read messages from this partition starting at the given offset.
    /// If `group_id` is provided and no offset is specified, reads from the committed offset.
    pub fn read(
        &mut self,
        offset: Option<u64>,
        max_messages: u32,
        group_id: Option<&str>,
    ) -> Result<Vec<(crate::message::MessageHeader, Message)>> {
        let start_offset = match offset {
            Some(o) => o,
            None => {
                // If a group_id is provided, start from its committed offset
                group_id
                    .and_then(|gid| self.committed_offset(gid))
                    .unwrap_or(self.log.start_offset())
            }
        };
        self.log.read(start_offset, max_messages)
    }

    /// Flush the underlying commit log to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()
    }

    // --- Accessors ---

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn role(&self) -> PartitionRole {
        self.role
    }

    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    pub fn start_offset(&self) -> u64 {
        self.log.start_offset()
    }

    pub fn end_offset(&self) -> u64 {
        self.log.end_offset()
    }

    pub fn log_size_bytes(&self) -> u64 {
        self.log.total_size_bytes()
    }
}

/// Assigns partitions to brokers using a round-robin strategy.
pub fn assign_partitions_round_robin(
    partition_count: u32,
    replication_factor: u32,
    broker_ids: &[u32],
) -> Result<HashMap<u32, Vec<u32>>> {
    if broker_ids.is_empty() {
        return Err(IntelStreamError::ClusterNotReady(
            "No brokers available".to_string(),
        ));
    }
    if replication_factor as usize > broker_ids.len() {
        return Err(IntelStreamError::InsufficientReplicas {
            required: replication_factor,
            available: broker_ids.len() as u32,
        });
    }

    let mut assignments = HashMap::new();
    let broker_count = broker_ids.len();

    for partition_id in 0..partition_count {
        let mut replicas = Vec::with_capacity(replication_factor as usize);
        for replica_index in 0..replication_factor {
            let broker_index = ((partition_id + replica_index) as usize) % broker_count;
            replicas.push(broker_ids[broker_index]);
        }
        assignments.insert(partition_id, replicas);
    }

    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_assignment() {
        let brokers = vec![1, 2, 3];
        let assignments = assign_partitions_round_robin(6, 2, &brokers).unwrap();

        assert_eq!(assignments.len(), 6);
        for replicas in assignments.values() {
            assert_eq!(replicas.len(), 2);
            // No duplicate brokers in a single partition's replica set
            assert_ne!(replicas[0], replicas[1]);
        }
    }

    #[test]
    fn test_insufficient_brokers() {
        let brokers = vec![1];
        let result = assign_partitions_round_robin(3, 3, &brokers);
        assert!(result.is_err());
    }
}
