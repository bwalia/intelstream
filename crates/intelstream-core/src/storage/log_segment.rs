use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read as IoRead, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{IntelStreamError, Result};
use crate::message::{Message, MessageHeader};
use crate::storage::OffsetIndex;
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
    /// Sparse offset index for fast lookups.
    index: OffsetIndex,
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

        // Load or create a sparse offset index (entry every 16 messages)
        let index = OffsetIndex::load(dir, base_offset, 16)
            .unwrap_or_else(|_| OffsetIndex::new(dir, base_offset, 16));

        Ok(Self {
            base_offset,
            position,
            max_size_bytes,
            next_offset: base_offset,
            path,
            writer: BufWriter::new(file),
            sealed: false,
            index,
        })
    }

    /// Append a message to this segment, returning the assigned offset.
    pub fn append(&mut self, message: &Message) -> Result<u64> {
        if self.sealed {
            return Err(IntelStreamError::StorageIo(std::io::Error::other(
                "segment is sealed",
            )));
        }

        // Record position before writing for the offset index
        let file_position = self.position;

        // Serialize the message
        let data = bincode::serialize(message)
            .map_err(|e| IntelStreamError::Serialization(e.to_string()))?;

        let header = MessageHeader {
            offset: self.next_offset,
            size: data.len() as u32,
            crc: crc32fast::hash(&data),
            compression: crate::message::Compression::None,
            append_timestamp: Utc::now(),
            producer_epoch: 0,
            sequence_number: self.next_offset,
        };

        // Write length-prefixed record: [header_len(4)][header][data_len(4)][data]
        let header_bytes = bincode::serialize(&header)
            .map_err(|e| IntelStreamError::Serialization(e.to_string()))?;

        self.writer
            .write_all(&(header_bytes.len() as u32).to_le_bytes())?;
        self.writer.write_all(&header_bytes)?;
        self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&data)?;

        let offset = self.next_offset;
        self.position += 4 + header_bytes.len() as u64 + 4 + data.len() as u64;
        self.next_offset += 1;

        // Record in the sparse index
        self.index.record_append(offset, file_position);

        Ok(offset)
    }

    /// Flush buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Seal this segment, preventing further writes. Also persists the index.
    pub fn seal(&mut self) -> Result<()> {
        self.flush()?;
        self.index.flush()?;
        self.sealed = true;
        Ok(())
    }

    /// Read a single record at the given file position.
    /// Returns `(header, message)` and the number of bytes consumed.
    pub fn read_at(&self, file_position: u64) -> Result<(MessageHeader, Message, u64)> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(file_position))?;

        // Read header length
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let header_len = u32::from_le_bytes(len_buf) as usize;

        // Read header
        let mut header_buf = vec![0u8; header_len];
        file.read_exact(&mut header_buf)?;
        let header: MessageHeader = bincode::deserialize(&header_buf)
            .map_err(|e| IntelStreamError::Serialization(e.to_string()))?;

        // Read data length
        file.read_exact(&mut len_buf)?;
        let data_len = u32::from_le_bytes(len_buf) as usize;

        // Read data
        let mut data_buf = vec![0u8; data_len];
        file.read_exact(&mut data_buf)?;

        // Verify CRC
        let actual_crc = crc32fast::hash(&data_buf);
        if actual_crc != header.crc {
            return Err(IntelStreamError::CrcMismatch {
                expected: header.crc,
                actual: actual_crc,
            });
        }

        let message: Message = bincode::deserialize(&data_buf)
            .map_err(|e| IntelStreamError::Serialization(e.to_string()))?;

        let bytes_consumed = 4 + header_len as u64 + 4 + data_len as u64;
        Ok((header, message, bytes_consumed))
    }

    /// Read up to `max_messages` starting from the given offset.
    /// Uses the sparse index for a fast seek, then scans forward.
    pub fn read_range(
        &self,
        start_offset: u64,
        max_messages: u32,
    ) -> Result<Vec<(MessageHeader, Message)>> {
        // Ensure the writer is flushed so the read handle sees all data
        // (caller is responsible for flushing beforehand)

        if start_offset >= self.next_offset {
            return Ok(vec![]);
        }

        // Use the index to find the nearest lower position, or start from 0
        let scan_start = self
            .index
            .lookup(start_offset)
            .map(|(_off, pos)| pos)
            .unwrap_or(0);

        let mut results = Vec::new();
        let mut pos = scan_start;

        loop {
            if pos >= self.position || results.len() >= max_messages as usize {
                break;
            }

            match self.read_at(pos) {
                Ok((header, message, bytes_consumed)) => {
                    if header.offset >= start_offset {
                        results.push((header, message));
                    }
                    pos += bytes_consumed;
                }
                Err(IntelStreamError::StorageIo(ref e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(results)
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

    /// Whether this segment contains messages with the given offset.
    pub fn contains_offset(&self, offset: u64) -> bool {
        offset >= self.base_offset && offset < self.next_offset
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

    #[test]
    fn test_read_at_single_message() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        let msg = Message::new(Some(Bytes::from("key1")), Bytes::from("value1"));
        segment.append(&msg).unwrap();
        segment.flush().unwrap();

        let (header, read_msg, _bytes) = segment.read_at(0).unwrap();
        assert_eq!(header.offset, 0);
        assert_eq!(read_msg.value, Bytes::from("value1"));
        assert_eq!(read_msg.key.unwrap(), Bytes::from("key1"));
    }

    #[test]
    fn test_read_range_multiple_messages() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        for i in 0..5 {
            let msg = Message::new(None, Bytes::from(format!("msg-{}", i)));
            segment.append(&msg).unwrap();
        }
        segment.flush().unwrap();

        let records = segment.read_range(0, 10).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].0.offset, 0);
        assert_eq!(records[4].0.offset, 4);
    }

    #[test]
    fn test_read_range_with_offset_skip() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        for i in 0..10 {
            let msg = Message::new(None, Bytes::from(format!("msg-{}", i)));
            segment.append(&msg).unwrap();
        }
        segment.flush().unwrap();

        // Read starting from offset 5
        let records = segment.read_range(5, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].0.offset, 5);
        assert_eq!(records[4].0.offset, 9);
    }

    #[test]
    fn test_read_range_respects_max_messages() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        for i in 0..10 {
            let msg = Message::new(None, Bytes::from(format!("msg-{}", i)));
            segment.append(&msg).unwrap();
        }
        segment.flush().unwrap();

        let records = segment.read_range(0, 3).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_contains_offset() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 10, 1_048_576).unwrap();

        let msg = Message::new(None, Bytes::from("data"));
        segment.append(&msg).unwrap();

        assert!(segment.contains_offset(10));
        assert!(!segment.contains_offset(9));
        assert!(!segment.contains_offset(11));
    }

    #[test]
    fn test_crc_integrity_on_read() {
        let tmp = TempDir::new().unwrap();
        let mut segment = LogSegment::open(tmp.path(), 0, 1_048_576).unwrap();

        let msg = Message::new(None, Bytes::from("integrity-test"));
        segment.append(&msg).unwrap();
        segment.flush().unwrap();

        // A valid read should pass CRC check
        let (header, read_msg, _) = segment.read_at(0).unwrap();
        assert_eq!(header.offset, 0);
        assert_eq!(read_msg.value, Bytes::from("integrity-test"));
    }
}
