//! Request and response models for the REST API.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request to create a new topic.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TopicCreateRequest {
    /// Topic name (unique within the cluster).
    pub name: String,
    /// Number of partitions.
    pub partition_count: u32,
    /// Replication factor.
    pub replication_factor: u32,
    /// Retention in hours (default: 168 / 7 days).
    #[serde(default = "default_retention")]
    pub retention_hours: u64,
    /// Enable log compaction.
    #[serde(default)]
    pub compact: bool,
}

fn default_retention() -> u64 {
    168
}

/// Topic metadata response.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TopicResponse {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
    pub retention_hours: u64,
    pub compact: bool,
    pub created_at: String,
}

/// Request to produce a message.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ProduceRequest {
    /// Optional message key for partitioning.
    pub key: Option<String>,
    /// Message payload (base64-encoded for binary data).
    pub value: String,
    /// Optional message headers.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

/// Response after producing a message.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ProduceResponse {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: String,
}

/// Request parameters for consuming messages.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ConsumeRequest {
    /// Offset to start consuming from.
    pub offset: Option<u64>,
    /// Maximum number of messages to return.
    #[serde(default = "default_max_messages")]
    pub max_messages: u32,
    /// Consumer group ID for offset tracking.
    pub group_id: Option<String>,
}

fn default_max_messages() -> u32 {
    100
}

/// Response containing consumed messages.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ConsumeResponse {
    pub topic: String,
    pub partition: u32,
    pub messages: Vec<MessageResponse>,
    pub next_offset: u64,
}

/// A single consumed message.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MessageResponse {
    pub offset: u64,
    pub key: Option<String>,
    pub value: String,
    pub headers: std::collections::HashMap<String, String>,
    pub timestamp: String,
}

/// Cluster status overview.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ClusterStatusResponse {
    pub cluster_name: String,
    pub brokers: Vec<BrokerInfo>,
    pub total_topics: u32,
    pub total_partitions: u32,
    pub controller_id: u32,
}

/// Individual broker information.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BrokerInfo {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub status: String,
    pub partitions_led: u32,
    pub partitions_followed: u32,
}

/// Health check response.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub broker_id: u32,
    pub uptime_secs: u64,
}
