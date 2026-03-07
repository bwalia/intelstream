//! # Log Storage Engine
//!
//! Durable, append-only log storage backed by segmented files and an index.
//! Each partition owns a `CommitLog` that manages multiple `LogSegment` instances.
//! Segments are rolled based on size or age, and old segments are reclaimed
//! according to the configured retention policy.

pub mod commit_log;
pub mod index;
pub mod log_segment;

pub use commit_log::CommitLog;
pub use index::OffsetIndex;
pub use log_segment::LogSegment;

use crate::message::Compression;
use serde::{Deserialize, Serialize};

/// Retention policy controlling when old segments are eligible for deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Maximum age of a segment before it is eligible for deletion.
    /// `None` means time-based retention is disabled.
    pub max_age_ms: Option<u64>,
    /// Maximum total log size in bytes. Oldest segments are deleted first.
    /// `None` means size-based retention is disabled.
    pub max_size_bytes: Option<u64>,
    /// Whether log compaction is enabled (retain only latest value per key).
    pub compaction_enabled: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_ms: Some(7 * 24 * 3600 * 1000), // 7 days
            max_size_bytes: None,
            compaction_enabled: false,
        }
    }
}

/// Configuration for the storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Root directory for all log data.
    pub data_dir: String,
    /// Maximum size of a single log segment file in bytes.
    pub segment_size_bytes: u64,
    /// Maximum size of a single message in bytes.
    pub max_message_size_bytes: usize,
    /// Default compression for new segments.
    pub default_compression: Compression,
    /// Whether to fsync after every write (durability vs performance).
    pub fsync_on_write: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            segment_size_bytes: 1_073_741_824, // 1 GiB
            max_message_size_bytes: 10_485_760, // 10 MiB
            default_compression: Compression::None,
            fsync_on_write: false,
        }
    }
}
