use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::password::{HashAlgorithm, PasswordError, hash_password, verify_password};
use crate::permissions::Permission;

/// User role tags (matching RabbitMQ).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserTag {
    Administrator,
    Monitoring,
    Policymaker,
    Management,
}

/// A user in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub hashing_algorithm: HashAlgorithm,
    pub tags: Vec<UserTag>,
    /// Per-vhost permissions. Key is vhost name.
    pub permissions: HashMap<String, Permission>,
}

impl User {
    pub fn new(
        username: impl Into<String>,
        password: &str,
        algorithm: HashAlgorithm,
        tags: Vec<UserTag>,
    ) -> Result<Self, PasswordError> {
        let password_hash = hash_password(password, &algorithm)?;
        Ok(Self {
            username: username.into(),
            password_hash,
            hashing_algorithm: algorithm,
            tags,
            permissions: HashMap::new(),
        })
    }

    pub fn verify_password(&self, password: &str) -> Result<bool, PasswordError> {
        verify_password(password, &self.password_hash, &self.hashing_algorithm)
    }

    pub fn set_password(
        &mut self,
        password: &str,
        algorithm: HashAlgorithm,
    ) -> Result<(), PasswordError> {
        self.password_hash = hash_password(password, &algorithm)?;
        self.hashing_algorithm = algorithm;
        Ok(())
    }

    pub fn has_tag(&self, tag: &UserTag) -> bool {
        self.tags.contains(tag)
    }

    pub fn is_admin(&self) -> bool {
        self.has_tag(&UserTag::Administrator)
    }

    /// Get permissions for a vhost. Returns None if no permissions set.
    pub fn get_permissions(&self, vhost: &str) -> Option<&Permission> {
        self.permissions.get(vhost)
    }

    /// Set permissions for a vhost.
    pub fn set_permissions(&mut self, vhost: impl Into<String>, permission: Permission) {
        self.permissions.insert(vhost.into(), permission);
    }

    /// Remove permissions for a vhost.
    pub fn remove_permissions(&mut self, vhost: &str) {
        self.permissions.remove(vhost);
    }

    /// Check if user can configure a resource in the given vhost.
    pub fn can_configure(&self, vhost: &str, resource: &str) -> bool {
        self.get_permissions(vhost)
            .map_or(false, |p| p.can_configure(resource))
    }

    /// Check if user can write to a resource in the given vhost.
    pub fn can_write(&self, vhost: &str, resource: &str) -> bool {
        self.get_permissions(vhost)
            .map_or(false, |p| p.can_write(resource))
    }

    /// Check if user can read from a resource in the given vhost.
    pub fn can_read(&self, vhost: &str, resource: &str) -> bool {
        self.get_permissions(vhost)
            .map_or(false, |p| p.can_read(resource))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_user_and_verify() {
        let user = User::new(
            "testuser",
            "password123",
            HashAlgorithm::Sha256,
            vec![UserTag::Administrator],
        )
        .unwrap();

        assert_eq!(user.username, "testuser");
        assert!(user.verify_password("password123").unwrap());
        assert!(!user.verify_password("wrong").unwrap());
        assert!(user.is_admin());
    }

    #[test]
    fn test_user_permissions() {
        let mut user = User::new(
            "testuser",
            "pass",
            HashAlgorithm::Plaintext,
            vec![],
        )
        .unwrap();

        // No permissions initially
        assert!(!user.can_write("/", "my-exchange"));

        // Grant permissions
        user.set_permissions("/", Permission::full());
        assert!(user.can_configure("/", "anything"));
        assert!(user.can_write("/", "my-exchange"));
        assert!(user.can_read("/", "my-queue"));

        // Different vhost — no access
        assert!(!user.can_write("other-vhost", "my-exchange"));

        // Remove permissions
        user.remove_permissions("/");
        assert!(!user.can_write("/", "my-exchange"));
    }

    #[test]
    fn test_change_password() {
        let mut user = User::new(
            "testuser",
            "old-pass",
            HashAlgorithm::Plaintext,
            vec![],
        )
        .unwrap();

        assert!(user.verify_password("old-pass").unwrap());

        user.set_password("new-pass", HashAlgorithm::Sha256).unwrap();
        assert!(!user.verify_password("old-pass").unwrap());
        assert!(user.verify_password("new-pass").unwrap());
    }

    #[test]
    fn test_user_tags() {
        let user = User::new(
            "admin",
            "pass",
            HashAlgorithm::Plaintext,
            vec![UserTag::Administrator, UserTag::Monitoring],
        )
        .unwrap();

        assert!(user.has_tag(&UserTag::Administrator));
        assert!(user.has_tag(&UserTag::Monitoring));
        assert!(!user.has_tag(&UserTag::Policymaker));
        assert!(user.is_admin());
    }
}
