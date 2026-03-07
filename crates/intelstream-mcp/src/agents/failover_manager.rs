//! Automated failover manager that detects broker failures and initiates recovery.

use async_trait::async_trait;
use tracing::{info, warn};

use super::{AgentAction, AgentContext, AlertSeverity, McpAgent};

/// Manages automated failover when brokers become unresponsive.
pub struct FailoverManager {
    /// Number of consecutive health check failures before triggering failover.
    _failure_threshold: u32,
    /// Current failure counts per broker.
    _failure_counts: std::collections::HashMap<u32, u32>,
}

impl FailoverManager {
    pub fn new(failure_threshold: u32) -> Self {
        Self {
            _failure_threshold: failure_threshold,
            _failure_counts: std::collections::HashMap::new(),
        }
    }
}

impl Default for FailoverManager {
    fn default() -> Self {
        Self::new(3)
    }
}

#[async_trait]
impl McpAgent for FailoverManager {
    fn name(&self) -> &str {
        "failover-manager"
    }

    async fn evaluate(&self, context: &AgentContext) -> Vec<AgentAction> {
        let mut actions = Vec::new();

        // TODO: check broker heartbeat status from consensus module
        // For now, detect brokers with extremely high CPU as potentially failing
        for (i, &cpu) in context.metrics.broker_cpu_utilization.iter().enumerate() {
            if cpu >= 0.99 {
                warn!(broker_id = i, cpu = %format!("{:.1}%", cpu * 100.0), "Broker may be unresponsive");
                actions.push(AgentAction {
                    agent: self.name().to_string(),
                    description: format!(
                        "Broker {} appears unresponsive (CPU {:.1}%). Initiating leader election for affected partitions.",
                        i,
                        cpu * 100.0
                    ),
                    auto_executed: true,
                    severity: AlertSeverity::Critical,
                    timestamp: chrono::Utc::now(),
                });
            }
        }

        actions
    }

    async fn execute(&self, action: &AgentAction) -> anyhow::Result<()> {
        info!(action = %action.description, "Failover action initiated");
        // TODO: trigger leader election via consensus module
        // TODO: update ISR sets
        // TODO: notify cluster of topology change
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::{AgentContext, AlertSeverity, MetricsSnapshot};

    #[tokio::test]
    async fn test_healthy_brokers_no_failover() {
        let fm = FailoverManager::default();
        let context = AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: vec![0.5, 0.6, 0.55],
                ..Default::default()
            },
            broker_count: 3,
            total_partitions: 12,
            total_consumer_lag: 0,
        };
        let actions = fm.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_unresponsive_broker_triggers_failover() {
        let fm = FailoverManager::default();
        let context = AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: vec![0.5, 0.99, 0.55],
                ..Default::default()
            },
            broker_count: 3,
            total_partitions: 12,
            total_consumer_lag: 0,
        };
        let actions = fm.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions[0].auto_executed);
        assert_eq!(actions[0].severity, AlertSeverity::Critical);
    }
}
