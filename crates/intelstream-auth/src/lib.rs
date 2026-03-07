//! # IntelStream Auth
//!
//! Authentication and authorization module supporting JWT tokens,
//! OAuth2 flows, and role-based access control (RBAC).

pub mod jwt;
pub mod rbac;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Authentication and authorization errors.
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Authentication required")]
    Unauthenticated,

    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Token expired")]
    TokenExpired,

    #[error("Insufficient permissions: requires '{required}' on '{resource}'")]
    Forbidden { required: String, resource: String },

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Internal auth error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, AuthError>;

/// Authenticated identity of a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Unique user or service account ID.
    pub subject: String,
    /// Display name.
    pub name: String,
    /// Assigned roles.
    pub roles: Vec<String>,
    /// Token expiration time.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Configuration for the auth module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// Auth provider: "jwt" or "oauth2".
    pub provider: String,
    /// JWT secret key (for HS256) or public key path (for RS256).
    pub jwt_secret: String,
    /// Token expiry in seconds.
    pub token_expiry_secs: u64,
    /// Refresh token expiry in seconds.
    pub refresh_token_expiry_secs: u64,
    /// Whether RBAC is enabled.
    pub rbac_enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "jwt".to_string(),
            jwt_secret: String::new(),
            token_expiry_secs: 3600,
            refresh_token_expiry_secs: 86400,
            rbac_enabled: false,
        }
    }
}
