//! Server configuration model, loaded from TOML files.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub broker: BrokerSection,
    pub api: ApiSection,
    pub auth: AuthSection,
    pub tls: TlsSection,
    pub schema_registry: SchemaRegistrySection,
    pub mcp: McpSection,
    pub metrics: MetricsSection,
    pub logging: LoggingSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerSection {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub log_segment_size_bytes: u64,
    pub max_message_size_bytes: u64,
    #[serde(default)]
    pub replication: ReplicationSection,
    #[serde(default)]
    pub retention: RetentionSection,
    #[serde(default)]
    pub performance: PerformanceSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSection {
    #[serde(default = "default_replication_factor")]
    pub default_replication_factor: u32,
    #[serde(default = "default_min_isr")]
    pub min_in_sync_replicas: u32,
    #[serde(default = "default_replication_timeout")]
    pub replication_timeout_ms: u64,
}

impl Default for ReplicationSection {
    fn default() -> Self {
        Self {
            default_replication_factor: default_replication_factor(),
            min_in_sync_replicas: default_min_isr(),
            replication_timeout_ms: default_replication_timeout(),
        }
    }
}

fn default_replication_factor() -> u32 {
    3
}
fn default_min_isr() -> u32 {
    2
}
fn default_replication_timeout() -> u64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionSection {
    #[serde(default = "default_retention_hours")]
    pub default_retention_hours: u64,
    #[serde(default = "default_retention_bytes")]
    pub default_retention_bytes: i64,
    #[serde(default)]
    pub log_compaction_enabled: bool,
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_ms: u64,
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self {
            default_retention_hours: default_retention_hours(),
            default_retention_bytes: default_retention_bytes(),
            log_compaction_enabled: false,
            compaction_interval_ms: default_compaction_interval(),
        }
    }
}

fn default_retention_hours() -> u64 {
    168
}
fn default_retention_bytes() -> i64 {
    -1
}
fn default_compaction_interval() -> u64 {
    300_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSection {
    #[serde(default = "default_io_threads")]
    pub num_io_threads: u32,
    #[serde(default = "default_network_threads")]
    pub num_network_threads: u32,
    #[serde(default = "default_batch_size")]
    pub batch_size_bytes: u64,
    #[serde(default = "default_linger")]
    pub linger_ms: u64,
    #[serde(default = "default_compression")]
    pub compression: String,
}

impl Default for PerformanceSection {
    fn default() -> Self {
        Self {
            num_io_threads: default_io_threads(),
            num_network_threads: default_network_threads(),
            batch_size_bytes: default_batch_size(),
            linger_ms: default_linger(),
            compression: default_compression(),
        }
    }
}

fn default_io_threads() -> u32 {
    4
}
fn default_network_threads() -> u32 {
    4
}
fn default_batch_size() -> u64 {
    65_536
}
fn default_linger() -> u64 {
    5
}
fn default_compression() -> String {
    "zstd".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSection {
    pub rest_port: u16,
    pub grpc_port: u16,
    #[serde(default = "default_true")]
    pub enable_swagger: bool,
    #[serde(default = "default_max_request_size")]
    pub max_request_size_bytes: u64,
    #[serde(default)]
    pub cors: CorsSection,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CorsSection {
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    #[serde(default = "default_cors_max_age")]
    pub max_age_secs: u64,
}

fn default_cors_max_age() -> u64 {
    3600
}
fn default_max_request_size() -> u64 {
    52_428_800
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSection {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_auth_provider")]
    pub provider: String,
    #[serde(default = "default_token_expiry")]
    pub token_expiry_secs: u64,
    #[serde(default)]
    pub rbac: RbacSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RbacSection {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_role")]
    pub default_role: String,
}

impl Default for RbacSection {
    fn default() -> Self {
        Self {
            enabled: false,
            default_role: default_role(),
        }
    }
}

fn default_auth_provider() -> String {
    "jwt".to_string()
}
fn default_token_expiry() -> u64 {
    3600
}
fn default_role() -> String {
    "reader".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSection {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub cert_file: String,
    #[serde(default)]
    pub key_file: String,
    #[serde(default)]
    pub ca_file: String,
    #[serde(default)]
    pub require_client_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistrySection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_schema_port")]
    pub port: u16,
    #[serde(default = "default_compatibility")]
    pub compatibility: String,
}

fn default_schema_port() -> u16 {
    8081
}
fn default_compatibility() -> String {
    "backward".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_mcp_port")]
    pub port: u16,
    #[serde(default = "default_ai_model")]
    pub ai_model: String,
    #[serde(default)]
    pub auto_scaling_enabled: bool,
    #[serde(default = "default_true")]
    pub anomaly_detection_enabled: bool,
}

fn default_mcp_port() -> u16 {
    8090
}
fn default_ai_model() -> String {
    "local".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSection {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
    #[serde(default = "default_export_interval")]
    pub export_interval_secs: u64,
}

fn default_prometheus_port() -> u16 {
    9191
}
fn default_export_interval() -> u64 {
    15
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingSection {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default)]
    pub file: String,
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "json".to_string()
}
fn default_true() -> bool {
    true
}

impl ServerConfig {
    /// Load configuration from a TOML file.
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: ServerConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;
        Ok(config)
    }

    /// Parse configuration from a TOML string (useful for testing).
    #[cfg(test)]
    pub fn from_str(content: &str) -> Result<Self> {
        let config: ServerConfig =
            toml::from_str(content).with_context(|| "Failed to parse config string".to_string())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_CONFIG: &str = r#"
[broker]
id = 1
host = "0.0.0.0"
port = 9292
data_dir = "/data"
log_segment_size_bytes = 1073741824
max_message_size_bytes = 10485760

[api]
rest_port = 8080
grpc_port = 9090

[auth]
enabled = false

[tls]
enabled = false

[schema_registry]
enabled = true

[mcp]
enabled = true

[metrics]
enabled = true

[logging]
level = "info"
"#;

    #[test]
    fn test_parse_minimal_config() {
        let config = ServerConfig::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(config.broker.id, 1);
        assert_eq!(config.broker.host, "0.0.0.0");
        assert_eq!(config.broker.port, 9292);
        assert_eq!(config.api.rest_port, 8080);
        assert_eq!(config.api.grpc_port, 9090);
        assert!(!config.auth.enabled);
        assert!(!config.tls.enabled);
        assert!(config.schema_registry.enabled);
        assert!(config.mcp.enabled);
        assert!(config.metrics.enabled);
    }

    #[test]
    fn test_defaults_applied() {
        let config = ServerConfig::from_str(MINIMAL_CONFIG).unwrap();

        // Replication defaults
        assert_eq!(config.broker.replication.default_replication_factor, 3);
        assert_eq!(config.broker.replication.min_in_sync_replicas, 2);

        // Retention defaults
        assert_eq!(config.broker.retention.default_retention_hours, 168);

        // Performance defaults
        assert_eq!(config.broker.performance.num_io_threads, 4);
        assert_eq!(config.broker.performance.compression, "zstd");

        // Auth defaults
        assert_eq!(config.auth.provider, "jwt");
        assert_eq!(config.auth.token_expiry_secs, 3600);
        assert_eq!(config.auth.rbac.default_role, "reader");

        // Schema registry defaults
        assert_eq!(config.schema_registry.port, 8081);
        assert_eq!(config.schema_registry.compatibility, "backward");

        // MCP defaults
        assert_eq!(config.mcp.port, 8090);
        assert_eq!(config.mcp.ai_model, "local");
        assert!(config.mcp.anomaly_detection_enabled);

        // Metrics defaults
        assert_eq!(config.metrics.prometheus_port, 9191);
        assert_eq!(config.metrics.export_interval_secs, 15);

        // Logging defaults
        assert_eq!(config.logging.format, "json");
    }

    #[test]
    fn test_custom_overrides() {
        let config_str = r#"
[broker]
id = 5
host = "192.168.1.10"
port = 19292
data_dir = "/mnt/data"
log_segment_size_bytes = 536870912
max_message_size_bytes = 5242880

[broker.replication]
default_replication_factor = 5
min_in_sync_replicas = 3

[broker.performance]
num_io_threads = 8
compression = "lz4"

[api]
rest_port = 18080
grpc_port = 19090

[auth]
enabled = true
provider = "oauth2"
token_expiry_secs = 7200

[tls]
enabled = true
cert_file = "/certs/server.crt"
key_file = "/certs/server.key"

[schema_registry]
enabled = false

[mcp]
enabled = false

[metrics]
enabled = true
prometheus_port = 19191

[logging]
level = "debug"
format = "pretty"
"#;
        let config = ServerConfig::from_str(config_str).unwrap();
        assert_eq!(config.broker.id, 5);
        assert_eq!(config.broker.port, 19292);
        assert_eq!(config.broker.replication.default_replication_factor, 5);
        assert_eq!(config.broker.performance.num_io_threads, 8);
        assert_eq!(config.broker.performance.compression, "lz4");
        assert!(config.auth.enabled);
        assert_eq!(config.auth.provider, "oauth2");
        assert!(config.tls.enabled);
        assert!(!config.schema_registry.enabled);
        assert!(!config.mcp.enabled);
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.format, "pretty");
    }

    #[test]
    fn test_load_actual_default_config() {
        // Test loading the actual default.toml from the config directory
        let config = ServerConfig::load("config/default.toml");
        // This test only works when run from the workspace root
        if let Ok(config) = config {
            assert_eq!(config.broker.id, 0);
            assert_eq!(config.broker.port, 9292);
            assert_eq!(config.api.rest_port, 8080);
        }
    }

    #[test]
    fn test_load_missing_file_errors() {
        let result = ServerConfig::load("/nonexistent/path.toml");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_toml_errors() {
        let result = ServerConfig::from_str("NOT VALID TOML {{{}}}");
        assert!(result.is_err());
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = ServerConfig::from_str(MINIMAL_CONFIG).unwrap();
        let serialized = toml::to_string(&config).unwrap();
        let deserialized = ServerConfig::from_str(&serialized).unwrap();
        assert_eq!(config.broker.id, deserialized.broker.id);
        assert_eq!(config.broker.port, deserialized.broker.port);
        assert_eq!(config.api.rest_port, deserialized.api.rest_port);
    }
}
