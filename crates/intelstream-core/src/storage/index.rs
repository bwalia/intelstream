use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Sparse offset index mapping message offsets to file positions within a segment.
///
/// The index is sparse: it stores an entry every `interval` messages.
/// Lookups find the nearest lower entry and scan forward from there.
#[derive(Debug, Serialize, Deserialize)]
pub struct OffsetIndex {
    /// Segment base offset this index belongs to.
    base_offset: u64,
    /// Sparse index entries: offset -> file position.
    entries: BTreeMap<u64, u64>,
    /// Number of messages between index entries.
    interval: u64,
    /// Counter for determining when to add the next entry.
    messages_since_last_entry: u64,
    /// Path to the persisted index file.
    path: PathBuf,
}

impl OffsetIndex {
    /// Create a new offset index for a segment.
    pub fn new(dir: &Path, base_offset: u64, interval: u64) -> Self {
        let filename = format!("{:020}.index", base_offset);
        let path = dir.join(filename);

        Self {
            base_offset,
            entries: BTreeMap::new(),
            interval,
            messages_since_last_entry: 0,
            path,
        }
    }

    /// Record that a message was appended at the given offset and file position.
    /// An index entry is created only every `interval` messages.
    pub fn record_append(&mut self, offset: u64, file_position: u64) {
        self.messages_since_last_entry += 1;
        if self.messages_since_last_entry >= self.interval {
            self.entries.insert(offset, file_position);
            self.messages_since_last_entry = 0;
        }
    }

    /// Look up the file position for the given offset.
    /// Returns the nearest lower-or-equal indexed position.
    pub fn lookup(&self, offset: u64) -> Option<(u64, u64)> {
        self.entries
            .range(..=offset)
            .next_back()
            .map(|(&k, &v)| (k, v))
    }

    /// Persist the index to disk.
    pub fn flush(&self) -> Result<()> {
        let data = bincode::serialize(&self.entries)
            .map_err(|e| crate::error::IntelStreamError::Serialization(e.to_string()))?;
        std::fs::write(&self.path, data)?;
        Ok(())
    }

    /// Load a previously persisted index from disk.
    pub fn load(dir: &Path, base_offset: u64, interval: u64) -> Result<Self> {
        let filename = format!("{:020}.index", base_offset);
        let path = dir.join(filename);

        let entries = if path.exists() {
            let data = std::fs::read(&path)?;
            bincode::deserialize(&data)
                .map_err(|e| crate::error::IntelStreamError::Serialization(e.to_string()))?
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            base_offset,
            entries,
            interval,
            messages_since_last_entry: 0,
            path,
        })
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sparse_index() {
        let tmp = TempDir::new().unwrap();
        let mut index = OffsetIndex::new(tmp.path(), 0, 4);

        // Only every 4th append creates an entry
        for i in 0..12 {
            index.record_append(i, i * 100);
        }

        // Entries at offsets 3, 7, 11 (every 4 messages)
        assert_eq!(index.len(), 3);

        // Lookup offset 5 should find the entry at offset 3
        let (nearest_offset, _position) = index.lookup(5).unwrap();
        assert!(nearest_offset <= 5);
    }

    #[test]
    fn test_index_persistence() {
        let tmp = TempDir::new().unwrap();
        let mut index = OffsetIndex::new(tmp.path(), 0, 2);

        index.record_append(0, 0);
        index.record_append(1, 100);
        index.record_append(2, 200);
        index.record_append(3, 300);

        index.flush().unwrap();

        let loaded = OffsetIndex::load(tmp.path(), 0, 2).unwrap();
        assert_eq!(loaded.len(), index.len());
    }
}
