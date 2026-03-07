use thiserror::Error;

/// Errors that can occur in the IntelStream client SDK.
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Topic '{0}' not found")]
    TopicNotFound(String),

    #[error("Partition {partition} not available for topic '{topic}'")]
    PartitionUnavailable { topic: String, partition: u32 },

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Authorization denied: {0}")]
    AuthorizationDenied(String),

    #[error("Message too large: {size} bytes (max: {max_size})")]
    MessageTooLarge { size: usize, max_size: usize },

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ClientError>;
