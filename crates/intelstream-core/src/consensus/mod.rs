//! # Consensus Module
//!
//! Built-in Raft-based consensus for leader election and metadata management.
//! Eliminates the need for external coordination services (no Zookeeper dependency).
//!
//! The consensus module handles:
//! - Leader election for partitions
//! - Cluster membership changes
//! - Metadata replication (topic configs, partition assignments)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::broker::BrokerId;
use crate::error::{IntelStreamError, Result};

/// Configuration for the consensus protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Election timeout range (milliseconds).
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Heartbeat interval (milliseconds).
    pub heartbeat_interval_ms: u64,
    /// Snapshot threshold: take a snapshot after this many log entries.
    pub snapshot_interval: u64,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 500,
            election_timeout_max_ms: 1500,
            heartbeat_interval_ms: 200,
            snapshot_interval: 10_000,
        }
    }
}

/// The current state of a Raft node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// A log entry in the Raft consensus log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    /// Term when this entry was created.
    pub term: u64,
    /// Index in the Raft log.
    pub index: u64,
    /// The command to be applied to the state machine.
    pub command: ClusterCommand,
}

/// Commands replicated through Raft for cluster-wide coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Create a new topic with the given configuration.
    CreateTopic {
        name: String,
        partition_count: u32,
        replication_factor: u32,
    },
    /// Delete a topic.
    DeleteTopic { name: String },
    /// Update partition assignments.
    ReassignPartitions {
        topic: String,
        assignments: HashMap<u32, Vec<BrokerId>>,
    },
    /// Register a new broker in the cluster.
    RegisterBroker {
        id: BrokerId,
        host: String,
        port: u16,
    },
    /// Deregister a broker from the cluster.
    DeregisterBroker { id: BrokerId },
    /// Update cluster-wide configuration.
    UpdateConfig { key: String, value: String },
    /// No-op used for leader confirmation.
    Noop,
}

/// Persistent state for a Raft node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPersistentState {
    /// Current term.
    pub current_term: u64,
    /// Candidate voted for in the current term (if any).
    pub voted_for: Option<BrokerId>,
    /// The Raft log entries.
    pub log: Vec<RaftLogEntry>,
}

impl Default for RaftPersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

/// Volatile state for the Raft leader.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each follower: the next log index to send.
    pub next_index: HashMap<BrokerId, u64>,
    /// For each follower: the highest log index known to be replicated.
    pub match_index: HashMap<BrokerId, u64>,
}

/// The Raft consensus node.
pub struct RaftNode {
    /// This node's broker ID.
    id: BrokerId,
    /// Current node state.
    state: NodeState,
    /// Persistent state (term, vote, log).
    persistent: RaftPersistentState,
    /// Volatile leader state (only valid when state == Leader).
    leader_state: Option<LeaderState>,
    /// Known cluster members.
    peers: Vec<BrokerId>,
    /// Commit index: highest log entry known to be committed.
    commit_index: u64,
    /// Last applied: highest log entry applied to the state machine.
    _last_applied: u64,
    /// Configuration.
    _config: ConsensusConfig,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(id: BrokerId, peers: Vec<BrokerId>, config: ConsensusConfig) -> Self {
        info!("Initializing Raft node {} with {} peers", id, peers.len());
        Self {
            id,
            state: NodeState::Follower,
            persistent: RaftPersistentState::default(),
            leader_state: None,
            peers,
            commit_index: 0,
            _last_applied: 0,
            _config: config,
        }
    }

    /// Propose a command to be replicated. Only the leader can accept proposals.
    pub fn propose(&mut self, command: ClusterCommand) -> Result<u64> {
        if self.state != NodeState::Leader {
            return Err(IntelStreamError::Consensus(
                "Not the leader; cannot propose commands".to_string(),
            ));
        }

        let entry = RaftLogEntry {
            term: self.persistent.current_term,
            index: self.persistent.log.len() as u64 + 1,
            command,
        };

        let index = entry.index;
        self.persistent.log.push(entry);

        debug!("Proposed command at index {}", index);
        Ok(index)
    }

    /// Transition to candidate state and start an election.
    pub fn start_election(&mut self) {
        self.persistent.current_term += 1;
        self.state = NodeState::Candidate;
        self.persistent.voted_for = Some(self.id);
        info!(
            "Node {} starting election for term {}",
            self.id, self.persistent.current_term
        );
    }

    /// Transition to leader state after winning an election.
    pub fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        let next_index = self.persistent.log.len() as u64 + 1;
        self.leader_state = Some(LeaderState {
            next_index: self.peers.iter().map(|&id| (id, next_index)).collect(),
            match_index: self.peers.iter().map(|&id| (id, 0)).collect(),
        });
        info!(
            "Node {} became leader for term {}",
            self.id, self.persistent.current_term
        );
    }

    /// Step down to follower state (e.g. on discovering a higher term).
    pub fn become_follower(&mut self, term: u64) {
        if term > self.persistent.current_term {
            self.persistent.current_term = term;
            self.persistent.voted_for = None;
        }
        self.state = NodeState::Follower;
        self.leader_state = None;
        debug!("Node {} became follower for term {}", self.id, term);
    }

    // --- Accessors ---

    pub fn id(&self) -> BrokerId {
        self.id
    }

    pub fn state(&self) -> NodeState {
        self.state
    }

    pub fn current_term(&self) -> u64 {
        self.persistent.current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn log_length(&self) -> usize {
        self.persistent.log.len()
    }

    pub fn is_leader(&self) -> bool {
        self.state == NodeState::Leader
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::new(1, vec![2, 3], ConsensusConfig::default());
        assert_eq!(node.state(), NodeState::Follower);
        assert_eq!(node.current_term(), 0);
        assert!(!node.is_leader());
    }

    #[test]
    fn test_election_and_leadership() {
        let mut node = RaftNode::new(1, vec![2, 3], ConsensusConfig::default());

        node.start_election();
        assert_eq!(node.state(), NodeState::Candidate);
        assert_eq!(node.current_term(), 1);

        node.become_leader();
        assert_eq!(node.state(), NodeState::Leader);
        assert!(node.is_leader());
    }

    #[test]
    fn test_propose_command() {
        let mut node = RaftNode::new(1, vec![2, 3], ConsensusConfig::default());

        // Cannot propose as follower
        let result = node.propose(ClusterCommand::Noop);
        assert!(result.is_err());

        // Become leader and propose
        node.start_election();
        node.become_leader();

        let index = node
            .propose(ClusterCommand::CreateTopic {
                name: "test".to_string(),
                partition_count: 3,
                replication_factor: 2,
            })
            .unwrap();
        assert_eq!(index, 1);
        assert_eq!(node.log_length(), 1);
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut node = RaftNode::new(1, vec![2, 3], ConsensusConfig::default());
        node.start_election();
        node.become_leader();

        node.become_follower(5);
        assert_eq!(node.state(), NodeState::Follower);
        assert_eq!(node.current_term(), 5);
    }
}
