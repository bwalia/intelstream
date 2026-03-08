//! Raft consensus transport layer.
//!
//! Defines the RPC message types and transport trait for Raft communication
//! between cluster nodes.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::broker::BrokerId;
use crate::error::Result;

use super::RaftLogEntry;

/// A vote request (RequestVote RPC) sent by a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    /// Term of the candidate.
    pub term: u64,
    /// Candidate requesting the vote.
    pub candidate_id: BrokerId,
    /// Index of the candidate's last log entry.
    pub last_log_index: u64,
    /// Term of the candidate's last log entry.
    pub last_log_term: u64,
}

/// Response to a vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    /// Current term of the responder (for candidate to update itself).
    pub term: u64,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// An AppendEntries RPC sent by the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader's ID (so followers can redirect clients).
    pub leader_id: BrokerId,
    /// Index of the log entry immediately preceding the new entries.
    pub prev_log_index: u64,
    /// Term of the prev_log_index entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<RaftLogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

/// Response to an AppendEntries RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term of the responder.
    pub term: u64,
    /// True if the follower accepted the entries.
    pub success: bool,
    /// The follower's last log index after processing (for fast catch-up).
    pub match_index: u64,
}

/// Trait defining the transport layer for Raft RPCs.
///
/// Implementations can use TCP, gRPC, or in-memory channels.
#[async_trait]
pub trait RaftTransport: Send + Sync {
    /// Send a RequestVote RPC to a peer.
    async fn request_vote(&self, target: BrokerId, request: VoteRequest) -> Result<VoteResponse>;

    /// Send an AppendEntries RPC to a peer.
    async fn append_entries(
        &self,
        target: BrokerId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_request_serialization() {
        let req = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 1,
        };
        let serialized = serde_json::to_string(&req).unwrap();
        let deserialized: VoteRequest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.term, 1);
        assert_eq!(deserialized.candidate_id, 2);
    }

    #[test]
    fn test_append_entries_serialization() {
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 1,
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 4,
        };
        let serialized = serde_json::to_string(&req).unwrap();
        let deserialized: AppendEntriesRequest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.term, 3);
        assert_eq!(deserialized.leader_id, 1);
        assert!(deserialized.entries.is_empty());
    }

    #[test]
    fn test_vote_response_serialization() {
        let resp = VoteResponse {
            term: 2,
            vote_granted: true,
        };
        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: VoteResponse = serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.vote_granted);
    }
}
