use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::message::{Message, MessageHeader};
use chrono::Utc;

/// A single segment file in the commit log.
///
/// Each segment stores a contiguous range of messages starting at `base_offset`.
/// When the segment exceeds `max_size_bytes`, a new segment is created.
pub struct LogSegment {
    /// The base offset (first message offset in this segment).
    base_offset: u64,
    /// Current write position / total bytes written.
    position: u64,
    /// Maximum size before the segment is sealed.
    max_size_bytes: u64,
    /// Next offset to assign.
    next_offset: u64,
    /// Path to the segment file on disk.
    path: PathBuf,
    /// Buffered writer for appending messages.
    writer: BufWriter<File>,
    /// Whether this segment is sealed (read-only).
    sealed: bool,
}

impl LogSegment {
    /// Open or create a new log segment at the given directory.
    pub fn open(dir: &Path, base_offset: u64, max_size_bytes: u64) -> Result<Self> {
        let filename = format!("{:020}.log", base_offset);
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let position = file.metadata()?.len();

        Ok(Self {
            base_offset,
            position,
            max_size_bytes,
            next_offset: base_offset,
            path,
            writer: BufWriter::new(file),
            sealed: false,
        })
    }

    /// Append a message to this segment, returning the assigned offset.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        if self.sealed {
            return Err(crate::error::IntelStreamError::StorageIo(
                std::io::Error::new(std::io::ErrorKind::Other, "segment is sealed"),
            ));
        }

        // Serialize the message (simplified — production would use a binary format)
        let data = bincode::serialize(message)
            .map_err(|e| crate::error::IntelStreamError::Serialization(e.to_string()))?;

        let _header = MessageHeader {
            offset: self.next_offset,
            size: data.len() as u32,
            crc: crc32fast::hash(&data),
            compression: crate::message::Compression::None,
            append_timestamp: Utc::now(),
            producer_epoch: 0,
            sequence_number: self.next_offset,
        };

        // Write length-prefixed record: [header_len(4)][header][data_len(4)][data]
        let header_bytes = bincode::serialize(&_header)
            .map_err(|e| crate::error::IntelStreamError::Serialization(e.to_string()))?;

        self.writer
            .write_all(&(header_bytes.len() as u32).to_le_bytes())?;
        self.writer.write_all(&header_bytes)?;
        self.writer
            .write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&data)?;

        let offset = self.next_offset;
        self.position += 4 + header_bytes.len() as u64 + 4 + data.len() as u64;
        self.next_offset += 1;

        Ok(offset)
    }

    /// Flush buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Seal this segment, preventing further writes.
    pub fn seal(&mut self) -> Result<()> {
        self.flush()?;
        self.sealed = true;
        Ok(())
    }

    /// Whether this segment has reached its maximum size.
    pub fn is_full(&self) -> bool {
        self.position >= self.max_size_bytes
    }

    /// Whether this segment is sealed (read-only).
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// The base offset of this segment.
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// The next offset that will be assigned.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Current size of the segment file in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.position
    }

    /// Path to the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[test]
    fn test_segment_append_and_offset() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        let msg = Message::new(None, Bytes::from("hello"));
        let offset = segment.append(&msg).unwrap();
        assert_eq!(offset, 0);

        let msg2 = Message::new(None, Bytes::from("world"));
        let offset2 = segment.append(&msg2).unwrap();
        assert_eq!(offset2, 1);

        assert_eq!(segment.next_offset(), 2);
        assert!(segment.size_bytes() > 0);
    }

    #[test]
    fn test_sealed_segment_rejects_writes() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();
        segment.seal().unwrap();

        let msg = Message::new(None, Bytes::from("fail"));
        assert!(segment.append(&msg).is_err());
    }
}
