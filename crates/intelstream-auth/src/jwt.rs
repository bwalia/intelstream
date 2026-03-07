//! JWT token generation and validation.

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{AuthError, Identity, Result};

/// JWT claims structure.
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (user ID).
    pub sub: String,
    /// Display name.
    pub name: String,
    /// Roles.
    pub roles: Vec<String>,
    /// Issued at (Unix timestamp).
    pub iat: i64,
    /// Expiration (Unix timestamp).
    pub exp: i64,
    /// Issuer.
    pub iss: String,
}

/// JWT token manager for generating and validating tokens.
pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    token_expiry_secs: i64,
    issuer: String,
}

impl JwtManager {
    /// Create a new JWT manager with the given secret.
    pub fn new(secret: &str, token_expiry_secs: u64) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            token_expiry_secs: token_expiry_secs as i64,
            issuer: "intelstream".to_string(),
        }
    }

    /// Generate a JWT token for the given identity.
    pub fn generate_token(&self, identity: &Identity) -> Result<String> {
        let now = Utc::now();
        let claims = Claims {
            sub: identity.subject.clone(),
            name: identity.name.clone(),
            roles: identity.roles.clone(),
            iat: now.timestamp(),
            exp: (now + Duration::seconds(self.token_expiry_secs)).timestamp(),
            iss: self.issuer.clone(),
        };

        encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| AuthError::Internal(format!("Failed to generate token: {}", e)))
    }

    /// Validate a JWT token and return the identity.
    pub fn validate_token(&self, token: &str) -> Result<Identity> {
        let mut validation = Validation::default();
        validation.set_issuer(&[&self.issuer]);

        let token_data = decode::<Claims>(token, &self.decoding_key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken(e.to_string()),
            })?;

        let claims = token_data.claims;
        debug!(subject = %claims.sub, "Token validated");

        Ok(Identity {
            subject: claims.sub,
            name: claims.name,
            roles: claims.roles,
            expires_at: Some(
                chrono::DateTime::from_timestamp(claims.exp, 0)
                    .unwrap_or_else(Utc::now),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_validate_token() {
        let manager = JwtManager::new("test-secret-key-at-least-32-bytes!", 3600);

        let identity = Identity {
            subject: "user-123".to_string(),
            name: "Test User".to_string(),
            roles: vec!["admin".to_string()],
            expires_at: None,
        };

        let token = manager.generate_token(&identity).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        assert_eq!(validated.subject, "user-123");
        assert_eq!(validated.name, "Test User");
        assert_eq!(validated.roles, vec!["admin"]);
    }

    #[test]
    fn test_invalid_token_rejected() {
        let manager = JwtManager::new("test-secret-key-at-least-32-bytes!", 3600);
        assert!(manager.validate_token("invalid.token.here").is_err());
    }

    #[test]
    fn test_token_preserves_multiple_roles() {
        let manager = JwtManager::new("test-secret-key-at-least-32-bytes!", 3600);

        let identity = Identity {
            subject: "multi-role-user".to_string(),
            name: "Multi Role".to_string(),
            roles: vec!["admin".to_string(), "writer".to_string(), "reader".to_string()],
            expires_at: None,
        };

        let token = manager.generate_token(&identity).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        assert_eq!(validated.roles.len(), 3);
        assert!(validated.roles.contains(&"admin".to_string()));
        assert!(validated.roles.contains(&"writer".to_string()));
        assert!(validated.roles.contains(&"reader".to_string()));
    }

    #[test]
    fn test_wrong_secret_rejects_token() {
        let manager1 = JwtManager::new("secret-one-at-least-32-bytes!!!!", 3600);
        let manager2 = JwtManager::new("secret-two-at-least-32-bytes!!!!", 3600);

        let identity = Identity {
            subject: "user".to_string(),
            name: "User".to_string(),
            roles: vec!["reader".to_string()],
            expires_at: None,
        };

        let token = manager1.generate_token(&identity).unwrap();
        assert!(manager2.validate_token(&token).is_err());
    }

    #[test]
    fn test_expired_token_rejected() {
        // Create a manager with 0-second expiry
        let manager = JwtManager::new("test-secret-key-at-least-32-bytes!", 0);

        let identity = Identity {
            subject: "user".to_string(),
            name: "User".to_string(),
            roles: vec!["reader".to_string()],
            expires_at: None,
        };

        let token = manager.generate_token(&identity).unwrap();
        // Token with 0s expiry should be expired immediately
        let result = manager.validate_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_has_correct_expiry() {
        let manager = JwtManager::new("test-secret-key-at-least-32-bytes!", 7200);

        let identity = Identity {
            subject: "user".to_string(),
            name: "User".to_string(),
            roles: vec![],
            expires_at: None,
        };

        let token = manager.generate_token(&identity).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        // The validated identity should have an expires_at roughly 2 hours from now
        assert!(validated.expires_at.is_some());
        let expires = validated.expires_at.unwrap();
        let now = chrono::Utc::now();
        let diff = (expires - now).num_seconds();
        // Allow 10 seconds of tolerance
        assert!(diff > 7100 && diff <= 7200, "Expected ~7200s, got {}s", diff);
    }
}
