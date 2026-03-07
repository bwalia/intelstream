//! # IntelStream Core
//!
//! The core messaging engine for the IntelStream distributed streaming platform.
//! This crate provides the fundamental building blocks: topics, partitions,
//! durable log storage, replication, and consensus.

pub mod broker;
pub mod consensus;
pub mod error;
pub mod message;
pub mod partition;
pub mod replication;
pub mod storage;
pub mod topic;

pub use error::{IntelStreamError, Result};
pub use message::{Message, MessageBatch, MessageHeader};
