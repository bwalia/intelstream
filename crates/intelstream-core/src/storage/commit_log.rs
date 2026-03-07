use std::path::{Path, PathBuf};

use crate::error::{IntelStreamError, Result};
use crate::message::Message;
use crate::storage::{LogSegment, RetentionPolicy, StorageConfig};
use tracing::{debug, info, warn};

/// The commit log manages an ordered sequence of log segments for a single partition.
///
/// Messages are always appended to the active (latest) segment. When the active
/// segment fills up, it is sealed and a new segment is opened. Background tasks
/// enforce retention policies by deleting or compacting old segments.
pub struct CommitLog {
    /// Directory containing all segment files for this partition.
    dir: PathBuf,
    /// Ordered list of segments (oldest first).
    segments: Vec<LogSegment>,
    /// Storage configuration.
    config: StorageConfig,
    /// Retention policy for this log.
    retention: RetentionPolicy,
}

impl CommitLog {
    /// Open or create a commit log in the given directory.
    pub fn open(dir: &Path, config: StorageConfig, retention: RetentionPolicy) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        let mut segments = Vec::new();

        // Scan for existing segment files
        let mut segment_offsets: Vec<u64> = std::fs::read_dir(dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "log"))
            .filter_map(|entry| {
                entry
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .collect();

        segment_offsets.sort();

        for base_offset in segment_offsets {
            let segment = LogSegment::open(dir, base_offset, config.segment_size_bytes)?;
            segments.push(segment);
        }

        // If no segments exist, create the first one
        if segments.is_empty() {
            let segment = LogSegment::open(dir, 0, config.segment_size_bytes)?;
            segments.push(segment);
            info!("Created initial log segment at {}", dir.display());
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
            config,
            retention,
        })
    }

    /// Append a message to the log, returning the assigned offset.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        let message_size = message.size_bytes();
        if message_size > self.config.max_message_size_bytes {
            return Err(IntelStreamError::MessageTooLarge {
                size: message_size,
                max_size: self.config.max_message_size_bytes,
            });
        }

        // Roll to a new segment if the active one is full
        if self.active_segment().is_full() {
            self.roll_segment()?;
        }

        let offset = self.active_segment_mut().append(message)?;
        debug!("Appended message at offset {offset}");

        Ok(offset)
    }

    /// Force-flush all buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.active_segment_mut().flush()
    }

    /// Seal the active segment and create a new one.
    fn roll_segment(&mut self) -> Result<()> {
        let next_offset = self.active_segment().next_offset();

        self.active_segment_mut().seal()?;
        info!(
            "Sealed segment at base offset {}, rolling to {}",
            self.active_segment().base_offset(),
            next_offset
        );

        let new_segment = LogSegment::open(&self.dir, next_offset, self.config.segment_size_bytes)?;
        self.segments.push(new_segment);

        Ok(())
    }

    /// Apply retention policy, deleting eligible old segments.
    pub fn enforce_retention(&mut self) -> Result<usize> {
        let mut deleted = 0;

        // Size-based retention
        if let Some(max_bytes) = self.retention.max_size_bytes {
            let mut total_size: u64 = self.segments.iter().map(|s| s.size_bytes()).sum();
            while total_size > max_bytes && self.segments.len() > 1 {
                let segment = self.segments.remove(0);
                total_size -= segment.size_bytes();
                std::fs::remove_file(segment.path())?;
                deleted += 1;
                warn!(
                    "Deleted segment (size retention): {}",
                    segment.path().display()
                );
            }
        }

        // TODO: Time-based retention
        // TODO: Log compaction

        Ok(deleted)
    }

    /// The earliest available offset in the log.
    pub fn start_offset(&self) -> u64 {
        self.segments.first().map_or(0, |s| s.base_offset())
    }

    /// The next offset that will be assigned (end of log).
    pub fn end_offset(&self) -> u64 {
        self.active_segment().next_offset()
    }

    /// Number of segments in the log.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Total size of all segments in bytes.
    pub fn total_size_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes()).sum()
    }

    /// Reference to the active (latest) segment.
    fn active_segment(&self) -> &LogSegment {
        self.segments
            .last()
            .expect("commit log must have at least one segment")
    }

    /// Mutable reference to the active (latest) segment.
    fn active_segment_mut(&mut self) -> &mut LogSegment {
        self.segments
            .last_mut()
            .expect("commit log must have at least one segment")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn test_config() -> StorageConfig {
        StorageConfig {
            data_dir: "/tmp".to_string(),
            segment_size_bytes: 4096, // small segments for testing
            max_message_size_bytes: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn test_commit_log_append() {
        let tmp = TempDir::new().unwrap();
        let mut log =
            CommitLog::open(tmp.path(), test_config(), RetentionPolicy::default()).unwrap();

        let msg = Message::new(None, Bytes::from("test"));
        let offset = log.append(&msg).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(log.end_offset(), 1);
    }

    #[test]
    fn test_commit_log_rejects_oversized_messages() {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig {
            max_message_size_bytes: 10,
            ..test_config()
        };
        let mut log = CommitLog::open(tmp.path(), config, RetentionPolicy::default()).unwrap();

        let msg = Message::new(
            None,
            Bytes::from("this message is way too large for the limit"),
        );
        assert!(log.append(&msg).is_err());
    }
}
