//! Metrics collector that aggregates cluster-wide metrics for AI agent consumption.

use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

use crate::agents::MetricsSnapshot;

/// A timestamped metrics sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSample {
    pub timestamp: DateTime<Utc>,
    pub snapshot: MetricsSnapshot,
}

/// Collects and stores time-series metrics for AI agents to analyze.
pub struct MetricsCollector {
    /// Rolling window of metrics samples.
    samples: Arc<RwLock<VecDeque<MetricsSample>>>,
    /// Maximum number of samples to retain.
    max_samples: usize,
    /// Per-broker latest metrics.
    _broker_metrics: DashMap<u32, MetricsSnapshot>,
}

impl MetricsCollector {
    /// Create a new metrics collector.
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Arc::new(RwLock::new(VecDeque::with_capacity(max_samples))),
            max_samples,
            _broker_metrics: DashMap::new(),
        }
    }

    /// Record a new metrics snapshot.
    pub async fn record(&self, snapshot: MetricsSnapshot) {
        let sample = MetricsSample {
            timestamp: Utc::now(),
            snapshot,
        };

        let mut samples = self.samples.write().await;
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(sample);
        debug!("Recorded metrics sample ({} total)", samples.len());
    }

    /// Get the latest metrics snapshot.
    pub async fn latest(&self) -> Option<MetricsSnapshot> {
        let samples = self.samples.read().await;
        samples.back().map(|s| s.snapshot.clone())
    }

    /// Get all samples within a time range.
    pub async fn range(&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<MetricsSample> {
        let samples = self.samples.read().await;
        samples
            .iter()
            .filter(|s| s.timestamp >= from && s.timestamp <= to)
            .cloned()
            .collect()
    }

    /// Get the total number of stored samples.
    pub async fn sample_count(&self) -> usize {
        self.samples.read().await.len()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::MetricsSnapshot;

    #[tokio::test]
    async fn test_record_and_retrieve() {
        let collector = MetricsCollector::new(100);
        let snapshot = MetricsSnapshot {
            messages_per_second: 1000.0,
            ..Default::default()
        };
        collector.record(snapshot.clone()).await;

        let latest = collector.latest().await.unwrap();
        assert_eq!(latest.messages_per_second, 1000.0);
        assert_eq!(collector.sample_count().await, 1);
    }

    #[tokio::test]
    async fn test_rolling_window_eviction() {
        let collector = MetricsCollector::new(3);

        for i in 0..5 {
            let snapshot = MetricsSnapshot {
                messages_per_second: i as f64,
                ..Default::default()
            };
            collector.record(snapshot).await;
        }

        assert_eq!(collector.sample_count().await, 3);
        let latest = collector.latest().await.unwrap();
        assert_eq!(latest.messages_per_second, 4.0);
    }

    #[tokio::test]
    async fn test_empty_collector_returns_none() {
        let collector = MetricsCollector::new(10);
        assert!(collector.latest().await.is_none());
        assert_eq!(collector.sample_count().await, 0);
    }

    #[tokio::test]
    async fn test_range_query() {
        let collector = MetricsCollector::new(100);
        let now = chrono::Utc::now();

        for i in 0..5 {
            collector
                .record(MetricsSnapshot {
                    messages_per_second: i as f64,
                    ..Default::default()
                })
                .await;
        }

        let from = now - chrono::Duration::seconds(10);
        let to = now + chrono::Duration::seconds(10);
        let results = collector.range(from, to).await;
        assert_eq!(results.len(), 5);
    }
}
