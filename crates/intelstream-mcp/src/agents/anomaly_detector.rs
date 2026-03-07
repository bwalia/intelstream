//! Anomaly detection agent that monitors for unusual patterns in cluster metrics.

use async_trait::async_trait;
use tracing::{info, warn};

use super::{AgentAction, AgentContext, AlertSeverity, McpAgent};

/// Thresholds for anomaly detection.
pub struct AnomalyThresholds {
    /// Latency spike: if P99 exceeds this multiplier of the average.
    pub latency_spike_multiplier: f64,
    /// CPU utilization threshold (0.0 - 1.0).
    pub cpu_threshold: f64,
    /// Disk utilization threshold (0.0 - 1.0).
    pub disk_threshold: f64,
    /// Consumer lag threshold (total messages behind).
    pub consumer_lag_threshold: u64,
}

impl Default for AnomalyThresholds {
    fn default() -> Self {
        Self {
            latency_spike_multiplier: 5.0,
            cpu_threshold: 0.85,
            disk_threshold: 0.90,
            consumer_lag_threshold: 100_000,
        }
    }
}

/// Detects anomalies in cluster behavior using threshold-based rules.
/// Can be extended with ML-based detection when using external AI backends.
pub struct AnomalyDetector {
    thresholds: AnomalyThresholds,
}

impl AnomalyDetector {
    pub fn new(thresholds: AnomalyThresholds) -> Self {
        Self { thresholds }
    }
}

#[async_trait]
impl McpAgent for AnomalyDetector {
    fn name(&self) -> &str {
        "anomaly-detector"
    }

    async fn evaluate(&self, context: &AgentContext) -> Vec<AgentAction> {
        let mut actions = Vec::new();
        let now = chrono::Utc::now();

        // Check for latency spikes
        if context.metrics.avg_produce_latency_us > 0.0 {
            let ratio =
                context.metrics.p99_produce_latency_us / context.metrics.avg_produce_latency_us;
            if ratio > self.thresholds.latency_spike_multiplier {
                warn!(
                    p99 = context.metrics.p99_produce_latency_us,
                    avg = context.metrics.avg_produce_latency_us,
                    "Latency spike detected"
                );
                actions.push(AgentAction {
                    agent: self.name().to_string(),
                    description: format!(
                        "Latency spike detected: P99 ({:.0}us) is {:.1}x the average ({:.0}us)",
                        context.metrics.p99_produce_latency_us,
                        ratio,
                        context.metrics.avg_produce_latency_us
                    ),
                    auto_executed: false,
                    severity: AlertSeverity::Warning,
                    timestamp: now,
                });
            }
        }

        // Check CPU utilization across brokers
        for (i, &cpu) in context.metrics.broker_cpu_utilization.iter().enumerate() {
            if cpu > self.thresholds.cpu_threshold {
                actions.push(AgentAction {
                    agent: self.name().to_string(),
                    description: format!(
                        "Broker {} CPU utilization at {:.1}% (threshold: {:.1}%)",
                        i,
                        cpu * 100.0,
                        self.thresholds.cpu_threshold * 100.0
                    ),
                    auto_executed: false,
                    severity: AlertSeverity::Warning,
                    timestamp: now,
                });
            }
        }

        // Check disk utilization
        for (i, &disk) in context.metrics.broker_disk_utilization.iter().enumerate() {
            if disk > self.thresholds.disk_threshold {
                actions.push(AgentAction {
                    agent: self.name().to_string(),
                    description: format!(
                        "Broker {} disk utilization at {:.1}% — risk of data loss",
                        i,
                        disk * 100.0
                    ),
                    auto_executed: false,
                    severity: AlertSeverity::Critical,
                    timestamp: now,
                });
            }
        }

        // Check consumer lag
        if context.total_consumer_lag > self.thresholds.consumer_lag_threshold {
            actions.push(AgentAction {
                agent: self.name().to_string(),
                description: format!(
                    "Total consumer lag is {} messages (threshold: {})",
                    context.total_consumer_lag, self.thresholds.consumer_lag_threshold
                ),
                auto_executed: false,
                severity: AlertSeverity::Warning,
                timestamp: now,
            });
        }

        actions
    }

    async fn execute(&self, action: &AgentAction) -> anyhow::Result<()> {
        info!(action = %action.description, "Anomaly alert triggered (manual review required)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::{AgentContext, MetricsSnapshot};

    fn default_context() -> AgentContext {
        AgentContext {
            metrics: MetricsSnapshot::default(),
            broker_count: 3,
            total_partitions: 12,
            total_consumer_lag: 0,
        }
    }

    #[tokio::test]
    async fn test_no_anomalies_on_healthy_cluster() {
        let detector = AnomalyDetector::new(AnomalyThresholds::default());
        let context = default_context();
        let actions = detector.evaluate(&context).await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn test_latency_spike_detected() {
        let detector = AnomalyDetector::new(AnomalyThresholds::default());
        let mut context = default_context();
        context.metrics.avg_produce_latency_us = 100.0;
        context.metrics.p99_produce_latency_us = 1000.0; // 10x spike
        let actions = detector.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions[0].description.contains("Latency spike"));
    }

    #[tokio::test]
    async fn test_cpu_threshold_alert() {
        let detector = AnomalyDetector::new(AnomalyThresholds::default());
        let mut context = default_context();
        context.metrics.broker_cpu_utilization = vec![0.5, 0.9, 0.6];
        let actions = detector.evaluate(&context).await;
        assert!(!actions.is_empty());
        assert!(actions.iter().any(|a| a.description.contains("CPU utilization")));
    }

    #[tokio::test]
    async fn test_disk_threshold_critical_alert() {
        let detector = AnomalyDetector::new(AnomalyThresholds::default());
        let mut context = default_context();
        context.metrics.broker_disk_utilization = vec![0.5, 0.95];
        let actions = detector.evaluate(&context).await;
        assert!(actions.iter().any(|a| a.severity == AlertSeverity::Critical));
    }

    #[tokio::test]
    async fn test_consumer_lag_alert() {
        let detector = AnomalyDetector::new(AnomalyThresholds::default());
        let mut context = default_context();
        context.total_consumer_lag = 500_000;
        let actions = detector.evaluate(&context).await;
        assert!(actions.iter().any(|a| a.description.contains("consumer lag")));
    }

    #[tokio::test]
    async fn test_custom_thresholds() {
        let thresholds = AnomalyThresholds {
            cpu_threshold: 0.50,
            ..Default::default()
        };
        let detector = AnomalyDetector::new(thresholds);
        let mut context = default_context();
        context.metrics.broker_cpu_utilization = vec![0.55];
        let actions = detector.evaluate(&context).await;
        assert!(!actions.is_empty());
    }
}
