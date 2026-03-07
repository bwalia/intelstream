//! Stream processor runtime that executes a topology.

use crate::topology::StreamTopology;
use crate::{StreamConfig, StreamRecord};
use tracing::info;

/// The stream processor runtime.
pub struct StreamProcessor {
    config: StreamConfig,
    topology: StreamTopology,
}

impl StreamProcessor {
    /// Create a new stream processor from a topology and configuration.
    pub fn new(config: StreamConfig, topology: StreamTopology) -> Self {
        Self { config, topology }
    }

    /// Start the stream processor.
    pub async fn start(&self) -> anyhow::Result<()> {
        info!(
            app_id = %self.config.application_id,
            sources = ?self.topology.source_topics,
            sinks = ?self.topology.sink_topics,
            operators = self.topology.operators.len(),
            "Starting stream processor"
        );

        // TODO: subscribe to source topics via consumer
        // TODO: process records through operator chain
        // TODO: produce results to sink topics
        // TODO: manage offsets and state stores

        Ok(())
    }

    /// Process a single record through the operator chain.
    pub fn process_record(&self, mut records: Vec<StreamRecord>) -> Vec<StreamRecord> {
        for operator in &self.topology.operators {
            let mut next_records = Vec::new();
            for record in records {
                next_records.extend(operator.process(record));
            }
            records = next_records;
        }
        records
    }

    /// Stop the stream processor.
    pub async fn stop(&self) -> anyhow::Result<()> {
        info!(
            app_id = %self.config.application_id,
            "Stopping stream processor"
        );
        // TODO: commit final offsets, close state stores
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::StreamBuilder;
    use crate::{StreamConfig, StreamRecord};

    fn test_record(value: &str) -> StreamRecord {
        StreamRecord {
            key: None,
            value: value.as_bytes().to_vec(),
            timestamp: 0,
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            headers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_process_record_through_chain() {
        let topology = StreamBuilder::new("test")
            .source("in")
            .filter(|r: &StreamRecord| r.value.len() > 2)
            .map(|mut r: StreamRecord| {
                r.value = r.value.iter().map(|b| b.to_ascii_uppercase()).collect();
                r
            })
            .sink("out")
            .build();

        let processor = StreamProcessor::new(StreamConfig::default(), topology);

        // Record passes filter
        let results = processor.process_record(vec![test_record("hello")]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, b"HELLO");

        // Record filtered out
        let results = processor.process_record(vec![test_record("hi")]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_empty_operator_chain() {
        let topology = StreamBuilder::new("test").source("in").sink("out").build();
        let processor = StreamProcessor::new(StreamConfig::default(), topology);

        let results = processor.process_record(vec![test_record("pass-through")]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, b"pass-through");
    }

    #[test]
    fn test_process_multiple_records() {
        let topology = StreamBuilder::new("test")
            .source("in")
            .filter(|r: &StreamRecord| r.value != b"drop")
            .sink("out")
            .build();
        let processor = StreamProcessor::new(StreamConfig::default(), topology);

        let records = vec![
            test_record("keep"),
            test_record("drop"),
            test_record("also-keep"),
        ];
        let results = processor.process_record(records);
        assert_eq!(results.len(), 2);
    }
}
