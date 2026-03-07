//! # MCP Dashboard
//!
//! Web dashboard for monitoring cluster health, viewing AI agent recommendations,
//! and configuring operational automation.

use serde::{Deserialize, Serialize};

/// Dashboard overview data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardOverview {
    pub cluster_name: String,
    pub broker_count: u32,
    pub topic_count: u32,
    pub total_partitions: u32,
    pub messages_per_second: f64,
    pub bytes_per_second: f64,
    pub active_consumer_groups: u32,
    pub total_consumer_lag: u64,
    pub recent_alerts: Vec<AlertEntry>,
}

/// An alert entry for the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEntry {
    pub timestamp: String,
    pub agent: String,
    pub severity: String,
    pub description: String,
    pub resolved: bool,
}

// TODO: Implement axum routes for the dashboard API
// TODO: Serve a static web UI (or integrate with a frontend framework)
// TODO: WebSocket endpoint for real-time metrics streaming
