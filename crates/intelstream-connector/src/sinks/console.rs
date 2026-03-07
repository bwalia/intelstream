//! Console sink connector — writes records to stdout (for debugging/development).

use async_trait::async_trait;
use tracing::info;

use crate::{ConnectorConfig, ConnectorRecord, SinkConnector};

/// Sink connector that prints records to the console.
pub struct ConsoleSink {
    name: String,
    records_written: u64,
}

impl ConsoleSink {
    pub fn new() -> Self {
        Self {
            name: "console-sink".to_string(),
            records_written: 0,
        }
    }
}

impl Default for ConsoleSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for ConsoleSink {
    async fn start(&mut self, config: &ConnectorConfig) -> anyhow::Result<()> {
        self.name = config.name.clone();
        info!(connector = %self.name, "Console sink started");
        Ok(())
    }

    async fn put(&mut self, records: Vec<ConnectorRecord>) -> anyhow::Result<()> {
        for record in &records {
            let key = record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_else(|| "null".to_string());
            let value = String::from_utf8_lossy(&record.value);
            println!("[{}] key={} value={}", record.topic, key, value);
            self.records_written += 1;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        info!(
            connector = %self.name,
            records_written = self.records_written,
            "Console sink stopped"
        );
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectorConfig, ConnectorRecord, ConnectorType};
    use std::collections::HashMap;

    fn test_config() -> ConnectorConfig {
        ConnectorConfig {
            name: "test-console".to_string(),
            connector_type: ConnectorType::Sink,
            connector_class: "console".to_string(),
            tasks_max: 1,
            topics: vec!["test".to_string()],
            properties: HashMap::new(),
        }
    }

    fn test_record(key: &str, value: &str) -> ConnectorRecord {
        ConnectorRecord {
            key: Some(key.as_bytes().to_vec()),
            value: value.as_bytes().to_vec(),
            topic: "test-topic".to_string(),
            partition: Some(0),
            timestamp: None,
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_console_sink_lifecycle() {
        let mut sink = ConsoleSink::new();
        sink.start(&test_config()).await.unwrap();
        assert_eq!(sink.name(), "test-console");

        sink.put(vec![test_record("k1", "v1"), test_record("k2", "v2")])
            .await
            .unwrap();

        sink.flush().await.unwrap();
        sink.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_console_sink_empty_put() {
        let mut sink = ConsoleSink::new();
        sink.start(&test_config()).await.unwrap();
        sink.put(vec![]).await.unwrap();
        sink.stop().await.unwrap();
    }
}
