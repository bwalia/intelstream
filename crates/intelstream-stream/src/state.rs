//! State store abstraction for stateful stream processing operations.

use async_trait::async_trait;
use std::collections::HashMap;

/// Trait for key-value state stores used in stateful stream processing.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Get a value by key.
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;

    /// Put a key-value pair.
    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>;

    /// Delete a key.
    async fn delete(&self, key: &[u8]) -> anyhow::Result<()>;

    /// Flush pending writes to durable storage.
    async fn flush(&self) -> anyhow::Result<()>;
}

/// In-memory state store (for testing and lightweight use cases).
pub struct InMemoryStateStore {
    data: tokio::sync::RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self {
            data: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let mut data = self.data.write().await;
        data.insert(key, value);
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // In-memory store has nothing to flush
        Ok(())
    }
}

// TODO: RocksDB-backed state store for production use
// pub struct RocksDbStateStore { ... }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_put_and_get() {
        let store = InMemoryStateStore::new();
        store
            .put(b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        let result = store.get(b"key1").await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_get_nonexistent() {
        let store = InMemoryStateStore::new();
        let result = store.get(b"missing").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_delete() {
        let store = InMemoryStateStore::new();
        store.put(b"key".to_vec(), b"val".to_vec()).await.unwrap();
        store.delete(b"key").await.unwrap();

        let result = store.get(b"key").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_overwrite() {
        let store = InMemoryStateStore::new();
        store.put(b"key".to_vec(), b"old".to_vec()).await.unwrap();
        store.put(b"key".to_vec(), b"new".to_vec()).await.unwrap();

        let result = store.get(b"key").await.unwrap();
        assert_eq!(result, Some(b"new".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_flush_succeeds() {
        let store = InMemoryStateStore::new();
        store.flush().await.unwrap();
    }
}
