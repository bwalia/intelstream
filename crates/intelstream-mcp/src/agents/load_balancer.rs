//! Predictive load balancer agent that recommends partition reassignment.

use async_trait::async_trait;
use tracing::info;

use super::{AgentAction, AgentContext, AlertSeverity, McpAgent};

/// Load balancer that detects skewed partition distribution.
pub struct LoadBalancer {
    /// Maximum allowed CPU skew between brokers (0.0 - 1.0).
    max_cpu_skew: f64,
}

impl LoadBalancer {
    pub fn new(max_cpu_skew: f64) -> Self {
        Self { max_cpu_skew }
    }
}

impl Default for LoadBalancer {
    fn default() -> Self {
        Self {
            max_cpu_skew: 0.30,
        }
    }
}

#[async_trait]
impl McpAgent for LoadBalancer {
    fn name(&self) -> &str {
        "load-balancer"
    }

    async fn evaluate(&self, context: &AgentContext) -> Vec<AgentAction> {
        let mut actions = Vec::new();

        if context.metrics.broker_cpu_utilization.len() < 2 {
            return actions;
        }

        let max_cpu = context
            .metrics
            .broker_cpu_utilization
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let min_cpu = context
            .metrics
            .broker_cpu_utilization
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);

        let skew = max_cpu - min_cpu;
        if skew > self.max_cpu_skew {
            actions.push(AgentAction {
                agent: self.name().to_string(),
                description: format!(
                    "Load imbalance detected: CPU skew {:.1}% (max allowed: {:.1}%). Consider partition reassignment.",
                    skew * 100.0,
                    self.max_cpu_skew * 100.0
                ),
                auto_executed: false,
                severity: AlertSeverity::Warning,
                timestamp: chrono::Utc::now(),
            });
        }

        actions
    }

    async fn execute(&self, action: &AgentAction) -> anyhow::Result<()> {
        info!(action = %action.description, "Load balancer recommends partition reassignment");
        // TODO: compute optimal partition assignment and propose via consensus
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::{AgentContext, MetricsSnapshot};

    #[tokio::test]
    async fn test_balanced_cluster_no_action() {
        let lb = LoadBalancer::default();
        let context = AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: vec![0.5, 0.55, 0.52],
                ..Default::default()
            },
            broker_count: 3,
            total_partitions: 12,
            total_consumer_lag: 0,
        };
        let actions = lb.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_imbalanced_cluster_triggers_alert() {
        let lb = LoadBalancer::new(0.30);
        let context = AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: vec![0.2, 0.9, 0.3],
                ..Default::default()
            },
            broker_count: 3,
            total_partitions: 12,
            total_consumer_lag: 0,
        };
        let actions = lb.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions[0].description.contains("imbalance"));
    }

    #[tokio::test]
    async fn test_single_broker_no_action() {
        let lb = LoadBalancer::default();
        let context = AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: vec![0.8],
                ..Default::default()
            },
            broker_count: 1,
            total_partitions: 3,
            total_consumer_lag: 0,
        };
        let actions = lb.evaluate(&context).await;
        assert!(actions.is_empty());
    }
}
