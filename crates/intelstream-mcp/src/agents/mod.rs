//! # AI Agents
//!
//! Pluggable AI agents that monitor cluster health and take automated actions.
//! Each agent runs independently, consuming metrics and emitting recommendations
//! or executing actions directly.

pub mod anomaly_detector;
pub mod auto_scaler;
pub mod failover_manager;
pub mod load_balancer;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Severity level for agent-generated alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// An action recommended or executed by an AI agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAction {
    /// Name of the agent that produced this action.
    pub agent: String,
    /// Human-readable description of what the action does.
    pub description: String,
    /// Whether this action was auto-executed or requires approval.
    pub auto_executed: bool,
    /// Severity/urgency level.
    pub severity: AlertSeverity,
    /// Timestamp of the action.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Trait that all MCP AI agents must implement.
#[async_trait]
pub trait McpAgent: Send + Sync {
    /// Unique name of this agent.
    fn name(&self) -> &str;

    /// Evaluate the current cluster state and return any recommended actions.
    async fn evaluate(&self, context: &AgentContext) -> Vec<AgentAction>;

    /// Execute a specific action (if auto-execution is enabled).
    async fn execute(&self, action: &AgentAction) -> anyhow::Result<()>;
}

/// Context provided to agents for decision-making.
#[derive(Debug, Clone)]
pub struct AgentContext {
    /// Current cluster-wide metrics snapshot.
    pub metrics: MetricsSnapshot,
    /// Number of active brokers.
    pub broker_count: u32,
    /// Total partition count across all topics.
    pub total_partitions: u32,
    /// Aggregate consumer lag across all groups.
    pub total_consumer_lag: u64,
}

/// A snapshot of cluster metrics at a point in time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Average produce latency in microseconds.
    pub avg_produce_latency_us: f64,
    /// P99 produce latency in microseconds.
    pub p99_produce_latency_us: f64,
    /// Messages per second (cluster-wide).
    pub messages_per_second: f64,
    /// Bytes per second (cluster-wide).
    pub bytes_per_second: f64,
    /// CPU utilization per broker (0.0 - 1.0).
    pub broker_cpu_utilization: Vec<f64>,
    /// Disk utilization per broker (0.0 - 1.0).
    pub broker_disk_utilization: Vec<f64>,
    /// Network utilization per broker (0.0 - 1.0).
    pub broker_network_utilization: Vec<f64>,
}
