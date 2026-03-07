//! # IntelStream Stream Processing
//!
//! A Rust-native stream processing API inspired by Kafka Streams.
//! Provides stateless and stateful transformations over IntelStream topics
//! with exactly-once processing guarantees.
//!
//! # Example
//!
//! ```rust,no_run
//! use intelstream_stream::{StreamBuilder, StreamTopology};
//!
//! let topology = StreamBuilder::new("my-app")
//!     .source("input-topic")
//!     .filter(|record| record.value.len() > 10)
//!     .map(|record| record)
//!     .sink("output-topic")
//!     .build();
//! ```

pub mod operators;
pub mod processor;
pub mod state;
pub mod topology;

pub use processor::StreamProcessor;
pub use topology::{StreamBuilder, StreamTopology};

use serde::{Deserialize, Serialize};

/// A record flowing through the stream processing pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub headers: std::collections::HashMap<String, String>,
}

/// Configuration for a stream processing application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Application ID (used for consumer group and state store naming).
    pub application_id: String,
    /// Bootstrap servers.
    pub bootstrap_servers: Vec<String>,
    /// Number of stream processing threads.
    pub num_threads: u32,
    /// Processing guarantee: at_least_once or exactly_once.
    pub processing_guarantee: ProcessingGuarantee,
    /// State store directory.
    pub state_dir: String,
    /// Commit interval for offset management (ms).
    pub commit_interval_ms: u64,
}

/// Processing guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingGuarantee {
    AtLeastOnce,
    ExactlyOnce,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            application_id: "intelstream-app".to_string(),
            bootstrap_servers: vec!["localhost:9292".to_string()],
            num_threads: 1,
            processing_guarantee: ProcessingGuarantee::ExactlyOnce,
            state_dir: "/tmp/intelstream-state".to_string(),
            commit_interval_ms: 30_000,
        }
    }
}
