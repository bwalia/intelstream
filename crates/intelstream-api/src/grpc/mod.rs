//! # gRPC API
//!
//! High-performance gRPC interface for produce/consume and cluster management.
//! The protobuf definitions are compiled at build time by tonic-build.

pub mod service;

// Include the generated protobuf code (auto-generated, skip formatting).
#[rustfmt::skip]
#[path = "intelstream.rs"]
pub mod proto;

// Re-export generated server types for convenience.
pub use proto::cluster_service_server::{ClusterService, ClusterServiceServer};
pub use proto::intel_stream_server::{IntelStream, IntelStreamServer};
pub use proto::topic_service_server::{TopicService, TopicServiceServer};
