//! File sink connector — writes records to files on disk.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use async_trait::async_trait;
use tracing::info;

use crate::{ConnectorConfig, ConnectorRecord, SinkConnector};

/// Sink connector that appends records to a file.
pub struct FileSink {
    name: String,
    writer: Option<BufWriter<File>>,
    path: PathBuf,
    records_written: u64,
}

impl FileSink {
    pub fn new() -> Self {
        Self {
            name: "file-sink".to_string(),
            writer: None,
            path: PathBuf::from("/tmp/intelstream-sink.log"),
            records_written: 0,
        }
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for FileSink {
    async fn start(&mut self, config: &ConnectorConfig) -> anyhow::Result<()> {
        self.name = config.name.clone();

        if let Some(path) = config.properties.get("file.path") {
            self.path = PathBuf::from(path);
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = Some(BufWriter::new(file));

        info!(
            connector = %self.name,
            path = %self.path.display(),
            "File sink started"
        );
        Ok(())
    }

    async fn put(&mut self, records: Vec<ConnectorRecord>) -> anyhow::Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("File sink not started"))?;

        for record in &records {
            let line = serde_json::to_string(record)?;
            writeln!(writer, "{}", line)?;
            self.records_written += 1;
        }

        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        self.flush().await?;
        self.writer = None;
        info!(
            connector = %self.name,
            records_written = self.records_written,
            "File sink stopped"
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

    fn test_config(path: &str) -> ConnectorConfig {
        let mut properties = HashMap::new();
        properties.insert("file.path".to_string(), path.to_string());
        ConnectorConfig {
            name: "test-file-sink".to_string(),
            connector_type: ConnectorType::Sink,
            connector_class: "file".to_string(),
            tasks_max: 1,
            topics: vec!["test".to_string()],
            properties,
        }
    }

    fn test_record(value: &str) -> ConnectorRecord {
        ConnectorRecord {
            key: Some(b"key".to_vec()),
            value: value.as_bytes().to_vec(),
            topic: "test".to_string(),
            partition: Some(0),
            timestamp: None,
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_file_sink_write_and_flush() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let mut sink = FileSink::new();
        sink.start(&test_config(&path)).await.unwrap();

        sink.put(vec![test_record("message-1"), test_record("message-2")])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        sink.stop().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.trim().lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line is a JSON-serialized ConnectorRecord; verify deserialization
        let record1: ConnectorRecord = serde_json::from_str(lines[0]).unwrap();
        let record2: ConnectorRecord = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(record1.value, b"message-1");
        assert_eq!(record2.value, b"message-2");
        assert_eq!(record1.topic, "test");
    }

    #[tokio::test]
    async fn test_file_sink_name() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let mut sink = FileSink::new();
        sink.start(&test_config(&path)).await.unwrap();
        assert_eq!(sink.name(), "test-file-sink");
        sink.stop().await.unwrap();
    }
}
