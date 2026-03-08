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
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
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

    /// Read up to `max_messages` starting from the given offset across segments.
    pub fn read(
        &mut self,
        offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(crate::message::MessageHeader, Message)>> {
        // Validate offset range
        if offset >= self.end_offset() {
            return Ok(vec![]);
        }
        if offset < self.start_offset() {
            return Err(IntelStreamError::OffsetOutOfRange {
                requested: offset,
                range_start: self.start_offset(),
                range_end: self.end_offset(),
            });
        }

        // Flush the active segment so reads see all data
        self.active_segment_mut().flush()?;

        let mut results = Vec::new();
        let mut remaining = max_messages;
        let mut current_offset = offset;

        for segment in &self.segments {
            if remaining == 0 {
                break;
            }
            // Skip segments that don't contain the current offset
            if segment.next_offset() <= current_offset {
                continue;
            }
            if segment.base_offset() > current_offset + remaining as u64 {
                break;
            }

            let records = segment.read_range(current_offset, remaining)?;
            for record in records {
                current_offset = record.0.offset + 1;
                results.push(record);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }

        Ok(results)
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

    #[test]
    fn test_commit_log_read() {
        let tmp = TempDir::new().unwrap();
        let mut log =
            CommitLog::open(tmp.path(), test_config(), RetentionPolicy::default()).unwrap();

        for i in 0..5 {
            let msg = Message::new(None, Bytes::from(format!("msg-{}", i)));
            log.append(&msg).unwrap();
        }

        let records = log.read(0, 10).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].0.offset, 0);
        assert_eq!(records[4].0.offset, 4);
    }

    #[test]
    fn test_commit_log_read_partial() {
        let tmp = TempDir::new().unwrap();
        let mut log =
            CommitLog::open(tmp.path(), test_config(), RetentionPolicy::default()).unwrap();

        for i in 0..10 {
            let msg = Message::new(None, Bytes::from(format!("msg-{}", i)));
            log.append(&msg).unwrap();
        }

        // Read only 3 messages starting at offset 5
        let records = log.read(5, 3).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0.offset, 5);
        assert_eq!(records[2].0.offset, 7);
    }

    #[test]
    fn test_commit_log_read_beyond_end() {
        let tmp = TempDir::new().unwrap();
        let mut log =
            CommitLog::open(tmp.path(), test_config(), RetentionPolicy::default()).unwrap();

        let msg = Message::new(None, Bytes::from("only-one"));
        log.append(&msg).unwrap();

        // Reading beyond the end returns empty
        let records = log.read(100, 10);
        assert!(records.unwrap().is_empty());
    }
}
