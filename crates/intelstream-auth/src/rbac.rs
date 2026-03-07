//! Role-Based Access Control (RBAC) for IntelStream resources.

use std::collections::HashSet;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{AuthError, Identity, Result};

/// Permissions that can be granted on resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Read/consume messages.
    Read,
    /// Write/produce messages.
    Write,
    /// Create topics.
    Create,
    /// Delete topics.
    Delete,
    /// Manage cluster settings.
    Admin,
    /// Describe topic/cluster metadata.
    Describe,
    /// Alter topic configuration.
    Alter,
}

/// A resource that can have permissions applied to it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Resource {
    /// A specific topic.
    Topic(String),
    /// All topics matching a prefix.
    TopicPrefix(String),
    /// Consumer group.
    Group(String),
    /// Cluster-level resource.
    Cluster,
    /// Schema registry subject.
    Schema(String),
}

/// A role definition with a set of permissions on resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub description: String,
    pub grants: Vec<Grant>,
}

/// A single permission grant on a resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub resource: Resource,
    pub permissions: HashSet<Permission>,
}

/// RBAC policy engine.
pub struct RbacEngine {
    /// Defined roles.
    roles: DashMap<String, Role>,
    /// Default role assigned to new users.
    _default_role: String,
}

impl RbacEngine {
    /// Create a new RBAC engine with built-in default roles.
    pub fn new(default_role: &str) -> Self {
        let engine = Self {
            roles: DashMap::new(),
            _default_role: default_role.to_string(),
        };

        // Register built-in roles
        engine.register_role(Role {
            name: "admin".to_string(),
            description: "Full access to all resources".to_string(),
            grants: vec![Grant {
                resource: Resource::Cluster,
                permissions: [
                    Permission::Read,
                    Permission::Write,
                    Permission::Create,
                    Permission::Delete,
                    Permission::Admin,
                    Permission::Describe,
                    Permission::Alter,
                ]
                .into_iter()
                .collect(),
            }],
        });

        engine.register_role(Role {
            name: "writer".to_string(),
            description: "Read and write access to topics".to_string(),
            grants: vec![Grant {
                resource: Resource::Cluster,
                permissions: [Permission::Read, Permission::Write, Permission::Describe]
                    .into_iter()
                    .collect(),
            }],
        });

        engine.register_role(Role {
            name: "reader".to_string(),
            description: "Read-only access to topics".to_string(),
            grants: vec![Grant {
                resource: Resource::Cluster,
                permissions: [Permission::Read, Permission::Describe]
                    .into_iter()
                    .collect(),
            }],
        });

        engine
    }

    /// Register a custom role.
    pub fn register_role(&self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    /// Check if an identity has a specific permission on a resource.
    pub fn authorize(
        &self,
        identity: &Identity,
        resource: &Resource,
        permission: Permission,
    ) -> Result<()> {
        for role_name in &identity.roles {
            if let Some(role) = self.roles.get(role_name) {
                for grant in &role.grants {
                    if self.resource_matches(&grant.resource, resource)
                        && grant.permissions.contains(&permission)
                    {
                        debug!(
                            subject = %identity.subject,
                            role = %role_name,
                            ?permission,
                            "Authorization granted"
                        );
                        return Ok(());
                    }
                }
            }
        }

        warn!(
            subject = %identity.subject,
            ?permission,
            ?resource,
            "Authorization denied"
        );
        Err(AuthError::Forbidden {
            required: format!("{:?}", permission),
            resource: format!("{:?}", resource),
        })
    }

    /// Check if a grant's resource pattern matches the target resource.
    fn resource_matches(&self, pattern: &Resource, target: &Resource) -> bool {
        match (pattern, target) {
            (Resource::Cluster, _) => true, // Cluster grants apply everywhere
            (Resource::Topic(p), Resource::Topic(t)) => p == t,
            (Resource::TopicPrefix(prefix), Resource::Topic(topic)) => topic.starts_with(prefix),
            (Resource::Group(p), Resource::Group(t)) => p == t,
            (Resource::Schema(p), Resource::Schema(t)) => p == t,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_identity() -> Identity {
        Identity {
            subject: "admin-user".to_string(),
            name: "Admin".to_string(),
            roles: vec!["admin".to_string()],
            expires_at: None,
        }
    }

    fn reader_identity() -> Identity {
        Identity {
            subject: "reader-user".to_string(),
            name: "Reader".to_string(),
            roles: vec!["reader".to_string()],
            expires_at: None,
        }
    }

    #[test]
    fn test_admin_has_full_access() {
        let engine = RbacEngine::new("reader");
        let identity = admin_identity();

        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Write
            )
            .is_ok());
        assert!(engine
            .authorize(&identity, &Resource::Cluster, Permission::Admin)
            .is_ok());
    }

    #[test]
    fn test_reader_cannot_write() {
        let engine = RbacEngine::new("reader");
        let identity = reader_identity();

        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Read
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Write
            )
            .is_err());
    }

    #[test]
    fn test_no_roles_denied() {
        let engine = RbacEngine::new("reader");
        let identity = Identity {
            subject: "nobody".to_string(),
            name: "Nobody".to_string(),
            roles: vec![],
            expires_at: None,
        };

        assert!(engine
            .authorize(&identity, &Resource::Cluster, Permission::Read)
            .is_err());
    }

    #[test]
    fn test_writer_can_read_and_write() {
        let engine = RbacEngine::new("reader");
        let identity = Identity {
            subject: "writer-user".to_string(),
            name: "Writer".to_string(),
            roles: vec!["writer".to_string()],
            expires_at: None,
        };

        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Read
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Write
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Describe
            )
            .is_ok());
        // Writer cannot create or delete
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Create
            )
            .is_err());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders".to_string()),
                Permission::Delete
            )
            .is_err());
    }

    #[test]
    fn test_custom_role_with_topic_prefix() {
        let engine = RbacEngine::new("reader");

        engine.register_role(Role {
            name: "team-orders".to_string(),
            description: "Read/write for orders-* topics".to_string(),
            grants: vec![Grant {
                resource: Resource::TopicPrefix("orders-".to_string()),
                permissions: [Permission::Read, Permission::Write].into_iter().collect(),
            }],
        });

        let identity = Identity {
            subject: "team-member".to_string(),
            name: "Team Member".to_string(),
            roles: vec!["team-orders".to_string()],
            expires_at: None,
        };

        // Matches prefix
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders-events".to_string()),
                Permission::Read
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("orders-events".to_string()),
                Permission::Write
            )
            .is_ok());

        // Does not match prefix
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("payments".to_string()),
                Permission::Read
            )
            .is_err());
    }

    #[test]
    fn test_multiple_roles_combined() {
        let engine = RbacEngine::new("reader");
        let identity = Identity {
            subject: "dual-role".to_string(),
            name: "Dual Role".to_string(),
            roles: vec!["reader".to_string(), "writer".to_string()],
            expires_at: None,
        };

        // Reader role allows Read, writer role allows Write
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("t".to_string()),
                Permission::Read
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("t".to_string()),
                Permission::Write
            )
            .is_ok());
    }

    #[test]
    fn test_resource_matches_exact_topic() {
        let engine = RbacEngine::new("reader");

        engine.register_role(Role {
            name: "specific".to_string(),
            description: "Access to a single topic".to_string(),
            grants: vec![Grant {
                resource: Resource::Topic("my-topic".to_string()),
                permissions: [Permission::Read].into_iter().collect(),
            }],
        });

        let identity = Identity {
            subject: "u".to_string(),
            name: "U".to_string(),
            roles: vec!["specific".to_string()],
            expires_at: None,
        };

        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("my-topic".to_string()),
                Permission::Read
            )
            .is_ok());
        assert!(engine
            .authorize(
                &identity,
                &Resource::Topic("other-topic".to_string()),
                Permission::Read
            )
            .is_err());
    }

    #[test]
    fn test_forbidden_error_details() {
        let engine = RbacEngine::new("reader");
        let identity = Identity {
            subject: "nobody".to_string(),
            name: "Nobody".to_string(),
            roles: vec![],
            expires_at: None,
        };

        let err = engine
            .authorize(&identity, &Resource::Cluster, Permission::Admin)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Admin"),
            "Error should mention the permission: {}",
            msg
        );
    }
}
