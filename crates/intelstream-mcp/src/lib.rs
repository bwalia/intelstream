//! # IntelStream MCP (Management Control Plane)
//!
//! AI-driven operational automation for the IntelStream platform.
//! Provides intelligent agents for auto-scaling, anomaly detection,
//! predictive load balancing, and automated failover.

pub mod agents;
pub mod dashboard;
pub mod metrics_collector;

use serde::{Deserialize, Serialize};

/// Configuration for the MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpConfig {
    /// Port for the MCP dashboard and API.
    pub port: u16,
    /// AI model backend to use for agent decisions.
    pub ai_model: AiModelBackend,
    /// Whether auto-scaling is enabled.
    pub auto_scaling_enabled: bool,
    /// Whether anomaly detection is enabled.
    pub anomaly_detection_enabled: bool,
    /// Metrics retention period in hours.
    pub metrics_retention_hours: u64,
}

/// Supported AI model backends for MCP agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AiModelBackend {
    /// Local rule-based engine (no external dependencies).
    Local,
    /// OpenAI API.
    OpenAi { api_key: String, model: String },
    /// Anthropic API.
    Anthropic { api_key: String, model: String },
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            port: 8090,
            ai_model: AiModelBackend::Local,
            auto_scaling_enabled: false,
            anomaly_detection_enabled: true,
            metrics_retention_hours: 720,
        }
    }
}
