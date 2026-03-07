//! Stream topology builder for defining processing pipelines.

use crate::operators::Operator;
use crate::StreamRecord;

/// A complete stream processing topology.
#[derive(Debug)]
pub struct StreamTopology {
    pub application_id: String,
    pub source_topics: Vec<String>,
    pub sink_topics: Vec<String>,
    pub operators: Vec<Box<dyn Operator>>,
}

/// Builder for constructing stream topologies.
pub struct StreamBuilder {
    application_id: String,
    source_topics: Vec<String>,
    sink_topics: Vec<String>,
    operators: Vec<Box<dyn Operator>>,
}

impl StreamBuilder {
    /// Create a new stream builder with the given application ID.
    pub fn new(application_id: impl Into<String>) -> Self {
        Self {
            application_id: application_id.into(),
            source_topics: Vec::new(),
            sink_topics: Vec::new(),
            operators: Vec::new(),
        }
    }

    /// Add a source topic to read from.
    pub fn source(mut self, topic: impl Into<String>) -> Self {
        self.source_topics.push(topic.into());
        self
    }

    /// Add a filter operator.
    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&StreamRecord) -> bool + Send + Sync + 'static,
    {
        self.operators
            .push(Box::new(crate::operators::FilterOperator::new(predicate)));
        self
    }

    /// Add a map operator.
    pub fn map<F>(mut self, transform: F) -> Self
    where
        F: Fn(StreamRecord) -> StreamRecord + Send + Sync + 'static,
    {
        self.operators
            .push(Box::new(crate::operators::MapOperator::new(transform)));
        self
    }

    /// Add a sink topic to write results to.
    pub fn sink(mut self, topic: impl Into<String>) -> Self {
        self.sink_topics.push(topic.into());
        self
    }

    /// Build the stream topology.
    pub fn build(self) -> StreamTopology {
        StreamTopology {
            application_id: self.application_id,
            source_topics: self.source_topics,
            sink_topics: self.sink_topics,
            operators: self.operators,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamRecord;

    #[test]
    fn test_builder_creates_topology() {
        let topology = StreamBuilder::new("test-app")
            .source("input-topic")
            .filter(|r: &StreamRecord| !r.value.is_empty())
            .map(|r: StreamRecord| r)
            .sink("output-topic")
            .build();

        assert_eq!(topology.application_id, "test-app");
        assert_eq!(topology.source_topics, vec!["input-topic"]);
        assert_eq!(topology.sink_topics, vec!["output-topic"]);
        assert_eq!(topology.operators.len(), 2);
    }

    #[test]
    fn test_multiple_sources_and_sinks() {
        let topology = StreamBuilder::new("multi-app")
            .source("topic-a")
            .source("topic-b")
            .sink("output-1")
            .sink("output-2")
            .build();

        assert_eq!(topology.source_topics.len(), 2);
        assert_eq!(topology.sink_topics.len(), 2);
    }

    #[test]
    fn test_empty_topology() {
        let topology = StreamBuilder::new("empty-app").build();
        assert!(topology.source_topics.is_empty());
        assert!(topology.sink_topics.is_empty());
        assert!(topology.operators.is_empty());
    }
}
