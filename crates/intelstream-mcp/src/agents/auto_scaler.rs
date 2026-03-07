//! Auto-scaling agent that recommends or executes broker/partition scaling.

use async_trait::async_trait;
use tracing::info;

use super::{AgentAction, AgentContext, AlertSeverity, McpAgent};

/// Configuration for auto-scaling behavior.
pub struct AutoScalerConfig {
    /// Scale up when average CPU exceeds this threshold.
    pub scale_up_cpu_threshold: f64,
    /// Scale down when average CPU is below this threshold.
    pub scale_down_cpu_threshold: f64,
    /// Minimum cooldown between scaling actions (seconds).
    pub cooldown_secs: u64,
    /// Minimum broker count (never scale below this).
    pub min_brokers: u32,
    /// Maximum broker count.
    pub max_brokers: u32,
}

impl Default for AutoScalerConfig {
    fn default() -> Self {
        Self {
            scale_up_cpu_threshold: 0.80,
            scale_down_cpu_threshold: 0.20,
            cooldown_secs: 300,
            min_brokers: 3,
            max_brokers: 50,
        }
    }
}

/// Auto-scaling agent for broker and partition management.
pub struct AutoScaler {
    config: AutoScalerConfig,
}

impl AutoScaler {
    pub fn new(config: AutoScalerConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl McpAgent for AutoScaler {
    fn name(&self) -> &str {
        "auto-scaler"
    }

    async fn evaluate(&self, context: &AgentContext) -> Vec<AgentAction> {
        let mut actions = Vec::new();
        let now = chrono::Utc::now();

        if context.metrics.broker_cpu_utilization.is_empty() {
            return actions;
        }

        let avg_cpu: f64 = context.metrics.broker_cpu_utilization.iter().sum::<f64>()
            / context.metrics.broker_cpu_utilization.len() as f64;

        if avg_cpu > self.config.scale_up_cpu_threshold
            && context.broker_count < self.config.max_brokers
        {
            actions.push(AgentAction {
                agent: self.name().to_string(),
                description: format!(
                    "Recommend scaling up: avg CPU {:.1}% exceeds {:.1}% threshold ({} brokers -> {})",
                    avg_cpu * 100.0,
                    self.config.scale_up_cpu_threshold * 100.0,
                    context.broker_count,
                    context.broker_count + 1
                ),
                auto_executed: false,
                severity: AlertSeverity::Warning,
                timestamp: now,
            });
        } else if avg_cpu < self.config.scale_down_cpu_threshold
            && context.broker_count > self.config.min_brokers
        {
            actions.push(AgentAction {
                agent: self.name().to_string(),
                description: format!(
                    "Recommend scaling down: avg CPU {:.1}% below {:.1}% threshold ({} brokers -> {})",
                    avg_cpu * 100.0,
                    self.config.scale_down_cpu_threshold * 100.0,
                    context.broker_count,
                    context.broker_count - 1
                ),
                auto_executed: false,
                severity: AlertSeverity::Info,
                timestamp: now,
            });
        }

        actions
    }

    async fn execute(&self, action: &AgentAction) -> anyhow::Result<()> {
        info!(action = %action.description, "Auto-scaler action (requires cluster orchestration)");
        // TODO: integrate with Kubernetes API or cluster provisioner
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::{AgentContext, MetricsSnapshot};

    fn context_with_cpu(cpus: Vec<f64>, broker_count: u32) -> AgentContext {
        AgentContext {
            metrics: MetricsSnapshot {
                broker_cpu_utilization: cpus,
                ..Default::default()
            },
            broker_count,
            total_partitions: 12,
            total_consumer_lag: 0,
        }
    }

    #[tokio::test]
    async fn test_no_scaling_in_normal_range() {
        let scaler = AutoScaler::new(AutoScalerConfig::default());
        let context = context_with_cpu(vec![0.5, 0.5, 0.5], 3);
        let actions = scaler.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_scale_up_recommended() {
        let scaler = AutoScaler::new(AutoScalerConfig::default());
        let context = context_with_cpu(vec![0.9, 0.85, 0.95], 3);
        let actions = scaler.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions[0].description.contains("scaling up"));
    }

    #[tokio::test]
    async fn test_scale_down_recommended() {
        let scaler = AutoScaler::new(AutoScalerConfig::default());
        let context = context_with_cpu(vec![0.1, 0.1, 0.1], 5);
        let actions = scaler.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions[0].description.contains("scaling down"));
    }

    #[tokio::test]
    async fn test_no_scale_up_at_max_brokers() {
        let config = AutoScalerConfig {
            max_brokers: 3,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config);
        let context = context_with_cpu(vec![0.9, 0.9, 0.9], 3);
        let actions = scaler.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_no_scale_down_at_min_brokers() {
        let config = AutoScalerConfig {
            min_brokers: 3,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config);
        let context = context_with_cpu(vec![0.1, 0.1, 0.1], 3);
        let actions = scaler.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_empty_metrics_no_action() {
        let scaler = AutoScaler::new(AutoScalerConfig::default());
        let context = context_with_cpu(vec![], 3);
        let actions = scaler.evaluate(&context).await;
        assert!(actions.is_empty());
    }
}
