//! # IntelStream Connector Framework
//!
//! Pluggable connector system for integrating IntelStream with external systems.
//! Supports both source connectors (ingesting data into IntelStream) and
//! sink connectors (exporting data from IntelStream).

pub mod sinks;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Connector status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorStatus {
    Created,
    Running,
    Paused,
    Failed,
    Stopped,
}

/// Connector type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorType {
    Source,
    Sink,
}

/// Configuration for a connector instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Unique connector name.
    pub name: String,
    /// Connector type (source or sink).
    pub connector_type: ConnectorType,
    /// The connector class/plugin name.
    pub connector_class: String,
    /// Number of parallel tasks.
    pub tasks_max: u32,
    /// Topics to read from (sink) or write to (source).
    pub topics: Vec<String>,
    /// Connector-specific properties.
    pub properties: HashMap<String, String>,
}

/// A record flowing through a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub topic: String,
    pub partition: Option<u32>,
    pub timestamp: Option<i64>,
    pub headers: HashMap<String, String>,
}

/// Trait for source connectors that produce data into IntelStream.
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// Initialize the connector with its configuration.
    async fn start(&mut self, config: &ConnectorConfig) -> anyhow::Result<()>;

    /// Poll for new records from the external system.
    async fn poll(&mut self) -> anyhow::Result<Vec<ConnectorRecord>>;

    /// Stop the connector.
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Connector name.
    fn name(&self) -> &str;
}

/// Trait for sink connectors that export data from IntelStream.
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Initialize the connector with its configuration.
    async fn start(&mut self, config: &ConnectorConfig) -> anyhow::Result<()>;

    /// Write a batch of records to the external system.
    async fn put(&mut self, records: Vec<ConnectorRecord>) -> anyhow::Result<()>;

    /// Flush any buffered records.
    async fn flush(&mut self) -> anyhow::Result<()>;

    /// Stop the connector.
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Connector name.
    fn name(&self) -> &str;
}

/// Connector registry for managing available connector plugins.
pub struct ConnectorRegistry {
    source_factories: HashMap<String, Box<dyn Fn() -> Box<dyn SourceConnector> + Send + Sync>>,
    sink_factories: HashMap<String, Box<dyn Fn() -> Box<dyn SinkConnector> + Send + Sync>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            source_factories: HashMap::new(),
            sink_factories: HashMap::new(),
        }
    }

    /// Register a source connector factory.
    pub fn register_source<F>(&mut self, class_name: &str, factory: F)
    where
        F: Fn() -> Box<dyn SourceConnector> + Send + Sync + 'static,
    {
        self.source_factories
            .insert(class_name.to_string(), Box::new(factory));
    }

    /// Register a sink connector factory.
    pub fn register_sink<F>(&mut self, class_name: &str, factory: F)
    where
        F: Fn() -> Box<dyn SinkConnector> + Send + Sync + 'static,
    {
        self.sink_factories
            .insert(class_name.to_string(), Box::new(factory));
    }

    /// Create a source connector instance by class name.
    pub fn create_source(&self, class_name: &str) -> Option<Box<dyn SourceConnector>> {
        self.source_factories.get(class_name).map(|f| f())
    }

    /// Create a sink connector instance by class name.
    pub fn create_sink(&self, class_name: &str) -> Option<Box<dyn SinkConnector>> {
        self.sink_factories.get(class_name).map(|f| f())
    }

    /// List available source connector classes.
    pub fn list_sources(&self) -> Vec<&str> {
        self.source_factories.keys().map(|s| s.as_str()).collect()
    }

    /// List available sink connector classes.
    pub fn list_sinks(&self) -> Vec<&str> {
        self.sink_factories.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockSink {
        name: String,
        records: Vec<ConnectorRecord>,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                name: "mock-sink".to_string(),
                records: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl SinkConnector for MockSink {
        async fn start(&mut self, config: &ConnectorConfig) -> anyhow::Result<()> {
            self.name = config.name.clone();
            Ok(())
        }
        async fn put(&mut self, records: Vec<ConnectorRecord>) -> anyhow::Result<()> {
            self.records.extend(records);
            Ok(())
        }
        async fn flush(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn stop(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn test_connector_registry_register_and_create_sink() {
        let mut registry = ConnectorRegistry::new();
        registry.register_sink("mock-sink", || Box::new(MockSink::new()));

        let sinks = registry.list_sinks();
        assert!(sinks.contains(&"mock-sink"));

        let sink = registry.create_sink("mock-sink");
        assert!(sink.is_some());
    }

    #[test]
    fn test_connector_registry_missing_returns_none() {
        let registry = ConnectorRegistry::new();
        assert!(registry.create_sink("nonexistent").is_none());
        assert!(registry.create_source("nonexistent").is_none());
    }

    #[test]
    fn test_connector_config_creation() {
        let config = ConnectorConfig {
            name: "my-connector".to_string(),
            connector_type: ConnectorType::Sink,
            connector_class: "console".to_string(),
            tasks_max: 2,
            topics: vec!["topic-a".to_string()],
            properties: HashMap::new(),
        };

        assert_eq!(config.name, "my-connector");
        assert_eq!(config.connector_type, ConnectorType::Sink);
        assert_eq!(config.tasks_max, 2);
    }

    #[test]
    fn test_connector_record_creation() {
        let record = ConnectorRecord {
            key: Some(b"key-1".to_vec()),
            value: b"value-1".to_vec(),
            topic: "test-topic".to_string(),
            partition: Some(0),
            timestamp: Some(1234567890),
            headers: HashMap::new(),
        };

        assert_eq!(record.key.as_ref().unwrap(), &b"key-1".to_vec());
        assert_eq!(record.topic, "test-topic");
    }
}
