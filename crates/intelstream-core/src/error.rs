use thiserror::Error;

/// Unified error type for IntelStream core operations.
#[derive(Error, Debug)]
pub enum IntelStreamError {
    // --- Storage errors ---
    #[error("Storage I/O error: {0}")]
    StorageIo(#[from] std::io::Error),

    #[error("Log segment corrupted at offset {offset}: {reason}")]
    CorruptedSegment { offset: u64, reason: String },

    #[error("Offset {requested} out of range [{}..{}]", .range_start, .range_end)]
    OffsetOutOfRange {
        requested: u64,
        range_start: u64,
        range_end: u64,
    },

    // --- Topic & Partition errors ---
    #[error("Topic '{0}' not found")]
    TopicNotFound(String),

    #[error("Topic '{0}' already exists")]
    TopicAlreadyExists(String),

    #[error("Partition {partition} not found in topic '{topic}'")]
    PartitionNotFound { topic: String, partition: u32 },

    #[error("Invalid partition count: {0} (must be > 0)")]
    InvalidPartitionCount(u32),

    // --- Replication errors ---
    #[error("Replication failed for partition {partition}: {reason}")]
    ReplicationFailed { partition: u32, reason: String },

    #[error("Insufficient replicas: need {required}, have {available}")]
    InsufficientReplicas { required: u32, available: u32 },

    #[error("Not the leader for partition {0}")]
    NotLeader(u32),

    // --- Consensus errors ---
    #[error("Consensus error: {0}")]
    Consensus(String),

    #[error("Leader election in progress")]
    ElectionInProgress,

    // --- Message errors ---
    #[error("Message too large: {size} bytes (max: {max_size})")]
    MessageTooLarge { size: usize, max_size: usize },

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("CRC mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    // --- Serialization errors ---
    #[error("Serialization error: {0}")]
    Serialization(String),

    // --- Configuration errors ---
    #[error("Configuration error: {0}")]
    Config(String),

    // --- Broker errors ---
    #[error("Broker {0} is unavailable")]
    BrokerUnavailable(u32),

    #[error("Cluster not ready: {0}")]
    ClusterNotReady(String),
}

/// Convenience Result type for IntelStream operations.
pub type Result<T> = std::result::Result<T, IntelStreamError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_messages() {
        let err = IntelStreamError::TopicNotFound("orders".to_string());
        assert_eq!(err.to_string(), "Topic 'orders' not found");

        let err = IntelStreamError::TopicAlreadyExists("orders".to_string());
        assert_eq!(err.to_string(), "Topic 'orders' already exists");

        let err = IntelStreamError::PartitionNotFound {
            topic: "orders".to_string(),
            partition: 5,
        };
        assert_eq!(err.to_string(), "Partition 5 not found in topic 'orders'");

        let err = IntelStreamError::InvalidPartitionCount(0);
        assert_eq!(err.to_string(), "Invalid partition count: 0 (must be > 0)");
    }

    #[test]
    fn test_replication_error_messages() {
        let err = IntelStreamError::InsufficientReplicas {
            required: 3,
            available: 1,
        };
        assert_eq!(err.to_string(), "Insufficient replicas: need 3, have 1");

        let err = IntelStreamError::NotLeader(2);
        assert_eq!(err.to_string(), "Not the leader for partition 2");

        let err = IntelStreamError::ReplicationFailed {
            partition: 1,
            reason: "timeout".to_string(),
        };
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_message_error_messages() {
        let err = IntelStreamError::MessageTooLarge {
            size: 20_000_000,
            max_size: 10_485_760,
        };
        assert!(err.to_string().contains("20000000"));
        assert!(err.to_string().contains("10485760"));

        let err = IntelStreamError::CrcMismatch {
            expected: 0xDEADBEEF,
            actual: 0xCAFEBABE,
        };
        assert!(err.to_string().contains("0xdeadbeef"));
        assert!(err.to_string().contains("0xcafebabe"));
    }

    #[test]
    fn test_offset_out_of_range() {
        let err = IntelStreamError::OffsetOutOfRange {
            requested: 100,
            range_start: 0,
            range_end: 50,
        };
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("0"));
        assert!(err.to_string().contains("50"));
    }

    #[test]
    fn test_consensus_errors() {
        let err = IntelStreamError::Consensus("split brain detected".to_string());
        assert!(err.to_string().contains("split brain"));

        let err = IntelStreamError::ElectionInProgress;
        assert_eq!(err.to_string(), "Leader election in progress");
    }

    #[test]
    fn test_broker_errors() {
        let err = IntelStreamError::BrokerUnavailable(3);
        assert_eq!(err.to_string(), "Broker 3 is unavailable");

        let err = IntelStreamError::ClusterNotReady("bootstrapping".to_string());
        assert!(err.to_string().contains("bootstrapping"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: IntelStreamError = io_err.into();
        assert!(matches!(err, IntelStreamError::StorageIo(_)));
        assert!(err.to_string().contains("file missing"));
    }
}
