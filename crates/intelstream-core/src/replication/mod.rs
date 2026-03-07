//! # Replication Engine
//!
//! Handles leader-follower replication for partition durability.
//! Followers fetch log entries from the leader and append them locally.
//! The replication protocol tracks ISR (In-Sync Replicas) and manages
//! high-watermark advancement.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::broker::BrokerId;
use crate::error::Result;

/// Configuration for replication behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Default replication factor for new topics.
    pub default_replication_factor: u32,
    /// Minimum number of in-sync replicas required for a write to be acknowledged.
    pub min_in_sync_replicas: u32,
    /// Timeout for a replica to be considered out of sync (milliseconds).
    pub replica_lag_timeout_ms: u64,
    /// Maximum number of bytes to fetch per replication request.
    pub max_fetch_bytes: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            default_replication_factor: 3,
            min_in_sync_replicas: 2,
            replica_lag_timeout_ms: 30_000,
            max_fetch_bytes: 1_048_576, // 1 MiB
        }
    }
}

/// Tracks the replication state for a single partition.
#[derive(Debug, Clone)]
pub struct ReplicaState {
    /// The topic-partition identifier.
    pub topic: String,
    pub partition_id: u32,
    /// Current leader broker ID.
    pub leader_id: BrokerId,
    /// Set of all assigned replicas.
    pub replicas: HashSet<BrokerId>,
    /// In-Sync Replica set (includes leader).
    pub isr: HashSet<BrokerId>,
    /// The leader's log end offset.
    pub leader_end_offset: u64,
    /// Each follower's last fetched offset.
    pub follower_offsets: HashMap<BrokerId, u64>,
    /// High watermark: offset up to which all ISR members have replicated.
    pub high_watermark: u64,
}

impl ReplicaState {
    /// Create a new replication state for a partition.
    pub fn new(
        topic: String,
        partition_id: u32,
        leader_id: BrokerId,
        replicas: HashSet<BrokerId>,
    ) -> Self {
        let isr = replicas.clone();
        let follower_offsets = replicas
            .iter()
            .filter(|&&id| id != leader_id)
            .map(|&id| (id, 0))
            .collect();

        Self {
            topic,
            partition_id,
            leader_id,
            replicas,
            isr,
            leader_end_offset: 0,
            follower_offsets,
            high_watermark: 0,
        }
    }

    /// Update a follower's fetch offset and recalculate the high watermark.
    pub fn update_follower_offset(&mut self, broker_id: BrokerId, offset: u64) {
        if let Some(current) = self.follower_offsets.get_mut(&broker_id) {
            *current = offset;
        }
        self.recalculate_high_watermark();
    }

    /// Remove a broker from the ISR (it has fallen too far behind).
    pub fn shrink_isr(&mut self, broker_id: BrokerId) {
        if self.isr.remove(&broker_id) {
            warn!(
                "Broker {} removed from ISR for {}-{}",
                broker_id, self.topic, self.partition_id
            );
        }
    }

    /// Add a broker back to the ISR (it has caught up).
    pub fn expand_isr(&mut self, broker_id: BrokerId) {
        if self.replicas.contains(&broker_id) {
            self.isr.insert(broker_id);
            info!(
                "Broker {} added to ISR for {}-{}",
                broker_id, self.topic, self.partition_id
            );
        }
    }

    /// Check if a write can be acknowledged (enough ISR members).
    pub fn can_acknowledge(&self, min_isr: u32) -> bool {
        self.isr.len() as u32 >= min_isr
    }

    /// Recalculate the high watermark based on ISR offsets.
    fn recalculate_high_watermark(&mut self) {
        let min_isr_offset = self
            .isr
            .iter()
            .filter(|&&id| id != self.leader_id)
            .filter_map(|id| self.follower_offsets.get(id))
            .min()
            .copied()
            .unwrap_or(self.leader_end_offset);

        // High watermark is the minimum of all ISR member offsets
        let new_hwm = min_isr_offset.min(self.leader_end_offset);
        if new_hwm > self.high_watermark {
            debug!(
                "High watermark for {}-{} advanced: {} -> {}",
                self.topic, self.partition_id, self.high_watermark, new_hwm
            );
            self.high_watermark = new_hwm;
        }
    }
}

/// A fetch request sent by a follower to the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub follower_id: BrokerId,
    pub topic: String,
    pub partition_id: u32,
    pub fetch_offset: u64,
    pub max_bytes: u64,
}

/// A fetch response from the leader containing log entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub topic: String,
    pub partition_id: u32,
    pub leader_end_offset: u64,
    pub high_watermark: u64,
    /// Serialized log entries from fetch_offset to leader_end_offset.
    pub records: Vec<u8>,
}

/// Trait for the replication transport layer.
#[async_trait]
pub trait ReplicationTransport: Send + Sync {
    /// Fetch log entries from the leader.
    async fn fetch(&self, request: FetchRequest) -> Result<FetchResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isr_management() {
        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let mut state = ReplicaState::new("test".to_string(), 0, 1, replicas);

        assert_eq!(state.isr.len(), 3);

        state.shrink_isr(3);
        assert_eq!(state.isr.len(), 2);
        assert!(!state.isr.contains(&3));

        state.expand_isr(3);
        assert_eq!(state.isr.len(), 3);
    }

    #[test]
    fn test_high_watermark_advancement() {
        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let mut state = ReplicaState::new("test".to_string(), 0, 1, replicas);
        state.leader_end_offset = 10;

        state.update_follower_offset(2, 5);
        state.update_follower_offset(3, 8);

        // HWM should be min(5, 8, 10) = 5
        assert_eq!(state.high_watermark, 5);

        state.update_follower_offset(2, 8);
        // HWM should now be min(8, 8, 10) = 8
        assert_eq!(state.high_watermark, 8);
    }

    #[test]
    fn test_can_acknowledge() {
        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let mut state = ReplicaState::new("test".to_string(), 0, 1, replicas);

        assert!(state.can_acknowledge(2));
        assert!(state.can_acknowledge(3));

        state.shrink_isr(3);
        assert!(state.can_acknowledge(2));
        assert!(!state.can_acknowledge(3));
    }
}
