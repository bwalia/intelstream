//! Replication Manager
//!
//! Coordinates replication for all partitions on a broker.
//! Leaders track follower progress; followers periodically fetch from leaders.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::broker::BrokerId;
use crate::error::Result;

use super::{FetchRequest, ReplicaState, ReplicationConfig, ReplicationTransport};

/// Manages replication state for all partitions on this broker.
pub struct ReplicationManager {
    /// This broker's ID.
    broker_id: BrokerId,
    /// Configuration for replication behavior.
    config: ReplicationConfig,
    /// Per-partition replication state, keyed by "topic-partition_id".
    partition_states: Arc<RwLock<HashMap<String, ReplicaState>>>,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(broker_id: BrokerId, config: ReplicationConfig) -> Self {
        info!(
            "Replication manager initialized for broker {} (min_isr={})",
            broker_id, config.min_in_sync_replicas
        );
        Self {
            broker_id,
            config,
            partition_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a partition for replication tracking.
    pub async fn register_partition(&self, state: ReplicaState) {
        let key = format!("{}-{}", state.topic, state.partition_id);
        info!(
            "Registering replication for partition {} (leader={})",
            key, state.leader_id
        );
        self.partition_states.write().await.insert(key, state);
    }

    /// Remove a partition from replication tracking.
    pub async fn unregister_partition(&self, topic: &str, partition_id: u32) {
        let key = format!("{}-{}", topic, partition_id);
        self.partition_states.write().await.remove(&key);
        debug!("Unregistered replication for partition {}", key);
    }

    /// Update a follower's fetch offset and recalculate the high watermark.
    pub async fn update_follower_progress(
        &self,
        topic: &str,
        partition_id: u32,
        follower_id: BrokerId,
        offset: u64,
    ) {
        let key = format!("{}-{}", topic, partition_id);
        let mut states = self.partition_states.write().await;
        if let Some(state) = states.get_mut(&key) {
            state.update_follower_offset(follower_id, offset);
        }
    }

    /// Run a single fetch cycle for all follower partitions.
    ///
    /// For partitions where this broker is a follower, issue a fetch request
    /// to the leader via the provided transport.
    pub async fn run_fetch_cycle(&self, transport: &dyn ReplicationTransport) -> Result<u32> {
        let states = self.partition_states.read().await;
        let mut fetched = 0u32;

        for (key, state) in states.iter() {
            // Only fetch if we're a follower for this partition
            if state.leader_id == self.broker_id {
                continue;
            }

            let current_offset = state
                .follower_offsets
                .get(&self.broker_id)
                .copied()
                .unwrap_or(0);

            let request = FetchRequest {
                follower_id: self.broker_id,
                topic: state.topic.clone(),
                partition_id: state.partition_id,
                fetch_offset: current_offset,
                max_bytes: self.config.max_fetch_bytes,
            };

            match transport.fetch(request).await {
                Ok(response) => {
                    if !response.records.is_empty() {
                        debug!(
                            "Fetched {} bytes for {} from leader {}",
                            response.records.len(),
                            key,
                            state.leader_id
                        );
                        fetched += 1;
                    }
                }
                Err(e) => {
                    warn!(
                        "Fetch failed for {} from leader {}: {}",
                        key, state.leader_id, e
                    );
                }
            }
        }

        Ok(fetched)
    }

    /// Get the number of tracked partitions.
    pub async fn partition_count(&self) -> usize {
        self.partition_states.read().await.len()
    }

    /// Get the high watermark for a specific partition.
    pub async fn high_watermark(&self, topic: &str, partition_id: u32) -> Option<u64> {
        let key = format!("{}-{}", topic, partition_id);
        let states = self.partition_states.read().await;
        states.get(&key).map(|s| s.high_watermark)
    }

    /// Check if a partition can acknowledge writes (enough ISR members).
    pub async fn can_acknowledge(&self, topic: &str, partition_id: u32) -> bool {
        let key = format!("{}-{}", topic, partition_id);
        let states = self.partition_states.read().await;
        states
            .get(&key)
            .map(|s| s.can_acknowledge(self.config.min_in_sync_replicas))
            .unwrap_or(false)
    }

    /// Get the replication config.
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[tokio::test]
    async fn test_register_and_unregister() {
        let mgr = ReplicationManager::new(1, ReplicationConfig::default());
        assert_eq!(mgr.partition_count().await, 0);

        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let state = ReplicaState::new("test".to_string(), 0, 1, replicas);
        mgr.register_partition(state).await;
        assert_eq!(mgr.partition_count().await, 1);

        mgr.unregister_partition("test", 0).await;
        assert_eq!(mgr.partition_count().await, 0);
    }

    #[tokio::test]
    async fn test_follower_progress_updates_hwm() {
        let mgr = ReplicationManager::new(1, ReplicationConfig::default());

        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let mut state = ReplicaState::new("events".to_string(), 0, 1, replicas);
        state.leader_end_offset = 10;
        mgr.register_partition(state).await;

        mgr.update_follower_progress("events", 0, 2, 5).await;
        mgr.update_follower_progress("events", 0, 3, 8).await;

        assert_eq!(mgr.high_watermark("events", 0).await, Some(5));
    }

    #[tokio::test]
    async fn test_can_acknowledge() {
        let mgr = ReplicationManager::new(1, ReplicationConfig::default());

        let replicas: HashSet<BrokerId> = [1, 2, 3].into_iter().collect();
        let state = ReplicaState::new("logs".to_string(), 0, 1, replicas);
        mgr.register_partition(state).await;

        assert!(mgr.can_acknowledge("logs", 0).await);
        assert!(!mgr.can_acknowledge("nonexistent", 0).await);
    }
}
