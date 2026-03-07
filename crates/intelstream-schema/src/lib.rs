//! # IntelStream Schema Registry
//!
//! Manages message schema versions and compatibility checking.
//! Supports Avro, JSON Schema, and Protobuf schema formats.
//! Ensures backward/forward compatibility between schema versions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{info, warn};

/// Errors from the schema registry.
#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Schema not found: subject '{subject}', version {version}")]
    NotFound { subject: String, version: u32 },

    #[error("Subject '{0}' not found")]
    SubjectNotFound(String),

    #[error("Schema incompatible with previous version: {0}")]
    Incompatible(String),

    #[error("Invalid schema: {0}")]
    Invalid(String),

    #[error("Schema format not supported: {0}")]
    UnsupportedFormat(String),
}

pub type Result<T> = std::result::Result<T, SchemaError>;

/// Supported schema formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaFormat {
    Avro,
    Json,
    Protobuf,
}

/// Compatibility modes for schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityMode {
    /// No compatibility checking.
    None,
    /// New schema can read data written with the previous schema.
    Backward,
    /// Previous schema can read data written with the new schema.
    Forward,
    /// Both backward and forward compatible.
    Full,
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        Self::Backward
    }
}

/// A registered schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Global unique schema ID.
    pub id: u32,
    /// Subject (typically "topic-name-value" or "topic-name-key").
    pub subject: String,
    /// Version within the subject.
    pub version: u32,
    /// Schema format.
    pub format: SchemaFormat,
    /// The schema definition string.
    pub definition: String,
    /// Registration timestamp.
    pub registered_at: DateTime<Utc>,
}

/// Schema registry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Port for the schema registry HTTP API.
    pub port: u16,
    /// Default compatibility mode.
    pub default_compatibility: CompatibilityMode,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            port: 8081,
            default_compatibility: CompatibilityMode::Backward,
        }
    }
}

/// The in-memory schema registry.
pub struct SchemaRegistry {
    /// Global schema ID counter.
    next_id: AtomicU32,
    /// Schemas by global ID.
    schemas_by_id: DashMap<u32, Schema>,
    /// Subject -> version -> schema ID.
    subjects: DashMap<String, Vec<u32>>,
    /// Per-subject compatibility mode overrides.
    compatibility: DashMap<String, CompatibilityMode>,
    /// Default compatibility mode.
    default_compatibility: CompatibilityMode,
}

impl SchemaRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            next_id: AtomicU32::new(1),
            schemas_by_id: DashMap::new(),
            subjects: DashMap::new(),
            compatibility: DashMap::new(),
            default_compatibility: config.default_compatibility,
        }
    }

    /// Register a new schema under a subject.
    pub fn register(
        &self,
        subject: &str,
        format: SchemaFormat,
        definition: String,
    ) -> Result<Schema> {
        // TODO: validate schema syntax
        // TODO: check compatibility with previous version

        let schema_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut versions = self.subjects.entry(subject.to_string()).or_insert_with(Vec::new);
        let version = versions.len() as u32 + 1;

        let schema = Schema {
            id: schema_id,
            subject: subject.to_string(),
            version,
            format,
            definition,
            registered_at: Utc::now(),
        };

        versions.push(schema_id);
        self.schemas_by_id.insert(schema_id, schema.clone());

        info!(
            subject,
            version,
            schema_id,
            "Registered new schema"
        );

        Ok(schema)
    }

    /// Get a schema by its global ID.
    pub fn get_by_id(&self, id: u32) -> Result<Schema> {
        self.schemas_by_id
            .get(&id)
            .map(|s| s.clone())
            .ok_or(SchemaError::NotFound {
                subject: "unknown".to_string(),
                version: 0,
            })
    }

    /// Get the latest schema for a subject.
    pub fn get_latest(&self, subject: &str) -> Result<Schema> {
        let versions = self
            .subjects
            .get(subject)
            .ok_or_else(|| SchemaError::SubjectNotFound(subject.to_string()))?;

        let latest_id = versions
            .last()
            .ok_or_else(|| SchemaError::SubjectNotFound(subject.to_string()))?;

        self.schemas_by_id
            .get(latest_id)
            .map(|s| s.clone())
            .ok_or(SchemaError::NotFound {
                subject: subject.to_string(),
                version: versions.len() as u32,
            })
    }

    /// Get a specific version of a schema.
    pub fn get_version(&self, subject: &str, version: u32) -> Result<Schema> {
        let versions = self
            .subjects
            .get(subject)
            .ok_or_else(|| SchemaError::SubjectNotFound(subject.to_string()))?;

        let schema_id = versions
            .get((version - 1) as usize)
            .ok_or(SchemaError::NotFound {
                subject: subject.to_string(),
                version,
            })?;

        self.schemas_by_id
            .get(schema_id)
            .map(|s| s.clone())
            .ok_or(SchemaError::NotFound {
                subject: subject.to_string(),
                version,
            })
    }

    /// List all subjects.
    pub fn list_subjects(&self) -> Vec<String> {
        self.subjects.iter().map(|e| e.key().clone()).collect()
    }

    /// Get the compatibility mode for a subject.
    pub fn get_compatibility(&self, subject: &str) -> CompatibilityMode {
        self.compatibility
            .get(subject)
            .map(|c| *c)
            .unwrap_or(self.default_compatibility)
    }

    /// Set the compatibility mode for a subject.
    pub fn set_compatibility(&self, subject: &str, mode: CompatibilityMode) {
        self.compatibility.insert(subject.to_string(), mode);
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new(RegistryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_retrieve_schema() {
        let registry = SchemaRegistry::default();

        let schema = registry
            .register(
                "orders-value",
                SchemaFormat::Avro,
                r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#
                    .to_string(),
            )
            .unwrap();

        assert_eq!(schema.version, 1);
        assert_eq!(schema.subject, "orders-value");

        let retrieved = registry.get_by_id(schema.id).unwrap();
        assert_eq!(retrieved.definition, schema.definition);
    }

    #[test]
    fn test_schema_versioning() {
        let registry = SchemaRegistry::default();

        registry
            .register("test-value", SchemaFormat::Json, "v1".to_string())
            .unwrap();
        registry
            .register("test-value", SchemaFormat::Json, "v2".to_string())
            .unwrap();

        let latest = registry.get_latest("test-value").unwrap();
        assert_eq!(latest.version, 2);
        assert_eq!(latest.definition, "v2");

        let v1 = registry.get_version("test-value", 1).unwrap();
        assert_eq!(v1.definition, "v1");
    }

    #[test]
    fn test_subject_not_found() {
        let registry = SchemaRegistry::default();
        assert!(registry.get_latest("nonexistent").is_err());
    }

    #[test]
    fn test_list_subjects() {
        let registry = SchemaRegistry::default();

        registry.register("orders-value", SchemaFormat::Avro, "v1".to_string()).unwrap();
        registry.register("payments-value", SchemaFormat::Json, "v1".to_string()).unwrap();
        registry.register("users-key", SchemaFormat::Protobuf, "v1".to_string()).unwrap();

        let subjects = registry.list_subjects();
        assert_eq!(subjects.len(), 3);
        assert!(subjects.contains(&"orders-value".to_string()));
        assert!(subjects.contains(&"payments-value".to_string()));
        assert!(subjects.contains(&"users-key".to_string()));
    }

    #[test]
    fn test_get_version_specific() {
        let registry = SchemaRegistry::default();

        registry.register("test", SchemaFormat::Json, "schema-v1".to_string()).unwrap();
        registry.register("test", SchemaFormat::Json, "schema-v2".to_string()).unwrap();
        registry.register("test", SchemaFormat::Json, "schema-v3".to_string()).unwrap();

        let v1 = registry.get_version("test", 1).unwrap();
        assert_eq!(v1.definition, "schema-v1");

        let v2 = registry.get_version("test", 2).unwrap();
        assert_eq!(v2.definition, "schema-v2");

        let v3 = registry.get_version("test", 3).unwrap();
        assert_eq!(v3.definition, "schema-v3");

        // Version 4 does not exist
        assert!(registry.get_version("test", 4).is_err());
    }

    #[test]
    fn test_schema_ids_are_unique() {
        let registry = SchemaRegistry::default();

        let s1 = registry.register("a", SchemaFormat::Avro, "a-v1".to_string()).unwrap();
        let s2 = registry.register("b", SchemaFormat::Json, "b-v1".to_string()).unwrap();
        let s3 = registry.register("a", SchemaFormat::Avro, "a-v2".to_string()).unwrap();

        assert_ne!(s1.id, s2.id);
        assert_ne!(s2.id, s3.id);
        assert_ne!(s1.id, s3.id);
    }

    #[test]
    fn test_get_by_id() {
        let registry = SchemaRegistry::default();

        let schema = registry.register("test", SchemaFormat::Json, "def".to_string()).unwrap();
        let retrieved = registry.get_by_id(schema.id).unwrap();
        assert_eq!(retrieved.subject, "test");
        assert_eq!(retrieved.definition, "def");

        // Non-existent ID
        assert!(registry.get_by_id(99999).is_err());
    }

    #[test]
    fn test_compatibility_mode_management() {
        let registry = SchemaRegistry::default();

        // Default compatibility
        assert_eq!(registry.get_compatibility("any-subject"), CompatibilityMode::Backward);

        // Override for a specific subject
        registry.set_compatibility("orders-value", CompatibilityMode::Full);
        assert_eq!(registry.get_compatibility("orders-value"), CompatibilityMode::Full);

        // Other subjects still use default
        assert_eq!(registry.get_compatibility("other"), CompatibilityMode::Backward);
    }

    #[test]
    fn test_compatibility_mode_default() {
        assert_eq!(CompatibilityMode::default(), CompatibilityMode::Backward);
    }

    #[test]
    fn test_registry_config_defaults() {
        let config = RegistryConfig::default();
        assert_eq!(config.port, 8081);
        assert_eq!(config.default_compatibility, CompatibilityMode::Backward);
    }

    #[test]
    fn test_schema_error_messages() {
        let err = SchemaError::SubjectNotFound("test".to_string());
        assert_eq!(err.to_string(), "Subject 'test' not found");

        let err = SchemaError::NotFound {
            subject: "orders".to_string(),
            version: 5,
        };
        assert!(err.to_string().contains("orders"));
        assert!(err.to_string().contains("5"));

        let err = SchemaError::Incompatible("field removed".to_string());
        assert!(err.to_string().contains("field removed"));
    }

    #[test]
    fn test_custom_compatibility_config() {
        let config = RegistryConfig {
            port: 9090,
            default_compatibility: CompatibilityMode::None,
        };
        let registry = SchemaRegistry::new(config);
        assert_eq!(registry.get_compatibility("anything"), CompatibilityMode::None);
    }
}
