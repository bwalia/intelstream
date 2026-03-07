//! # IntelStream Client SDK (Rust)
//!
//! High-level producer and consumer clients for the IntelStream platform.
//!
//! # Example
//!
//! ```rust,no_run
//! use intelstream_client::{ClientConfig, Producer, Consumer};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ClientConfig::new("localhost:9292");
//!
//!     // Produce
//!     let producer = Producer::new(config.clone()).await.unwrap();
//!     producer.send("my-topic", b"key", b"value").await.unwrap();
//!
//!     // Consume
//!     let consumer = Consumer::new(config, "my-group").await.unwrap();
//!     consumer.subscribe(&["my-topic"]).await.unwrap();
//! }
//! ```

pub mod consumer;
pub mod error;
pub mod producer;

pub use consumer::Consumer;
pub use error::ClientError;
pub use producer::Producer;

use serde::{Deserialize, Serialize};

/// Client configuration for connecting to an IntelStream cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Bootstrap server addresses (comma-separated).
    pub bootstrap_servers: Vec<String>,
    /// Client ID for tracking.
    pub client_id: String,
    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Whether to use TLS.
    pub use_tls: bool,
    /// Authentication token (if auth is enabled).
    pub auth_token: Option<String>,
}

impl ClientConfig {
    /// Create a new client config with a single bootstrap server.
    pub fn new(bootstrap_server: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: vec![bootstrap_server.into()],
            client_id: format!("intelstream-client-{}", uuid::Uuid::now_v7()),
            request_timeout_ms: 30_000,
            use_tls: false,
            auth_token: None,
        }
    }

    /// Add multiple bootstrap servers.
    pub fn with_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = servers;
        self
    }

    /// Set the client ID.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// Enable TLS.
    pub fn with_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    /// Set authentication token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_new() {
        let config = ClientConfig::new("localhost:9292");
        assert_eq!(config.bootstrap_servers, vec!["localhost:9292"]);
        assert!(!config.use_tls);
        assert!(config.auth_token.is_none());
        assert_eq!(config.request_timeout_ms, 30_000);
        assert!(config.client_id.starts_with("intelstream-client-"));
    }

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfig::new("node-1:9292")
            .with_servers(vec![
                "node-1:9292".to_string(),
                "node-2:9292".to_string(),
                "node-3:9292".to_string(),
            ])
            .with_client_id("my-producer")
            .with_tls()
            .with_auth_token("secret-token");

        assert_eq!(config.bootstrap_servers.len(), 3);
        assert_eq!(config.client_id, "my-producer");
        assert!(config.use_tls);
        assert_eq!(config.auth_token.as_deref(), Some("secret-token"));
    }

    #[test]
    fn test_client_config_serialization() {
        let config = ClientConfig::new("localhost:9292")
            .with_client_id("test-id");

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("localhost:9292"));
        assert!(json.contains("test-id"));

        let deserialized: ClientConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.bootstrap_servers, config.bootstrap_servers);
        assert_eq!(deserialized.client_id, "test-id");
    }
}
