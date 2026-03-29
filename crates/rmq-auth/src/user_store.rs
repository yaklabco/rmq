use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use thiserror::Error;

use crate::password::{HashAlgorithm, PasswordError};
use crate::permissions::Permission;
use crate::user::{User, UserTag};

#[derive(Debug, Error)]
pub enum UserStoreError {
    #[error("user '{0}' not found")]
    NotFound(String),
    #[error("user '{0}' already exists")]
    AlreadyExists(String),
    #[error("password error: {0}")]
    Password(#[from] PasswordError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Persistent user store backed by a JSON file.
pub struct UserStore {
    path: PathBuf,
    users: RwLock<HashMap<String, User>>,
}

impl UserStore {
    /// Open or create a user store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, UserStoreError> {
        let path = path.as_ref().to_path_buf();
        let users = if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let users: HashMap<String, User> = serde_json::from_str(&data)?;
            users
        } else {
            HashMap::new()
        };

        Ok(Self {
            path,
            users: RwLock::new(users),
        })
    }

    /// Open or create with a default admin user.
    pub fn open_with_defaults(
        path: impl AsRef<Path>,
        default_user: &str,
        default_pass: &str,
    ) -> Result<Self, UserStoreError> {
        let store = Self::open(path)?;

        // Create default user if store is empty
        if store.users.read().is_empty() {
            let mut user = User::new(
                default_user,
                default_pass,
                HashAlgorithm::Bcrypt,
                vec![UserTag::Administrator],
            )?;
            user.set_permissions("/", Permission::full());
            store.users.write().insert(default_user.to_string(), user);
            store.save()?;
        }

        Ok(store)
    }

    /// Authenticate a user. Returns the user if credentials are valid.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<User, UserStoreError> {
        let users = self.users.read();
        let user = users
            .get(username)
            .ok_or_else(|| UserStoreError::NotFound(username.to_string()))?;

        if user.verify_password(password)? {
            Ok(user.clone())
        } else {
            Err(UserStoreError::NotFound(username.to_string()))
        }
    }

    /// Get a user by name.
    pub fn get(&self, username: &str) -> Option<User> {
        self.users.read().get(username).cloned()
    }

    /// List all usernames.
    pub fn list(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    /// Create a new user.
    pub fn create(
        &self,
        username: &str,
        password: &str,
        algorithm: HashAlgorithm,
        tags: Vec<UserTag>,
    ) -> Result<(), UserStoreError> {
        let mut users = self.users.write();
        if users.contains_key(username) {
            return Err(UserStoreError::AlreadyExists(username.to_string()));
        }
        let user = User::new(username, password, algorithm, tags)?;
        users.insert(username.to_string(), user);
        drop(users);
        self.save()?;
        Ok(())
    }

    /// Delete a user.
    pub fn delete(&self, username: &str) -> Result<(), UserStoreError> {
        let mut users = self.users.write();
        if users.remove(username).is_none() {
            return Err(UserStoreError::NotFound(username.to_string()));
        }
        drop(users);
        self.save()?;
        Ok(())
    }

    /// Set permissions for a user on a vhost.
    pub fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        permission: Permission,
    ) -> Result<(), UserStoreError> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| UserStoreError::NotFound(username.to_string()))?;
        user.set_permissions(vhost, permission);
        drop(users);
        self.save()?;
        Ok(())
    }

    /// Change a user's password.
    pub fn set_password(
        &self,
        username: &str,
        password: &str,
        algorithm: HashAlgorithm,
    ) -> Result<(), UserStoreError> {
        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| UserStoreError::NotFound(username.to_string()))?;
        user.set_password(password, algorithm)?;
        drop(users);
        self.save()?;
        Ok(())
    }

    /// Persist users to disk.
    fn save(&self) -> Result<(), UserStoreError> {
        let users = self.users.read();
        let data = serde_json::to_string_pretty(&*users)?;
        // Write atomically via temp file
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_authenticate() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store
            .create("alice", "password", HashAlgorithm::Sha256, vec![UserTag::Administrator])
            .unwrap();

        let user = store.authenticate("alice", "password").unwrap();
        assert_eq!(user.username, "alice");
        assert!(user.is_admin());

        // Wrong password
        assert!(store.authenticate("alice", "wrong").is_err());

        // Unknown user
        assert!(store.authenticate("bob", "pass").is_err());
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");

        {
            let store = UserStore::open(&path).unwrap();
            store
                .create("bob", "secret", HashAlgorithm::Sha256, vec![])
                .unwrap();
        }

        // Reopen
        let store = UserStore::open(&path).unwrap();
        let user = store.authenticate("bob", "secret").unwrap();
        assert_eq!(user.username, "bob");
    }

    #[test]
    fn test_default_user() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");

        let store = UserStore::open_with_defaults(&path, "guest", "guest").unwrap();
        let user = store.authenticate("guest", "guest").unwrap();
        assert!(user.is_admin());
        assert!(user.can_write("/", "any-exchange"));
    }

    #[test]
    fn test_delete_user() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store.create("temp", "pass", HashAlgorithm::Plaintext, vec![]).unwrap();
        assert!(store.get("temp").is_some());

        store.delete("temp").unwrap();
        assert!(store.get("temp").is_none());
    }

    #[test]
    fn test_set_permissions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store.create("alice", "pass", HashAlgorithm::Plaintext, vec![]).unwrap();

        store
            .set_permissions("alice", "/", Permission {
                configure: "my-.*".to_string(),
                write: "my-.*".to_string(),
                read: ".*".to_string(),
            })
            .unwrap();

        let user = store.get("alice").unwrap();
        assert!(user.can_configure("/", "my-queue"));
        assert!(!user.can_configure("/", "other-queue"));
        assert!(user.can_read("/", "anything"));
    }

    #[test]
    fn test_change_password() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store.create("alice", "old", HashAlgorithm::Plaintext, vec![]).unwrap();
        assert!(store.authenticate("alice", "old").is_ok());

        store.set_password("alice", "new", HashAlgorithm::Sha256).unwrap();
        assert!(store.authenticate("alice", "old").is_err());
        assert!(store.authenticate("alice", "new").is_ok());
    }

    #[test]
    fn test_duplicate_user() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store.create("alice", "pass", HashAlgorithm::Plaintext, vec![]).unwrap();
        let err = store.create("alice", "pass2", HashAlgorithm::Plaintext, vec![]).unwrap_err();
        assert!(matches!(err, UserStoreError::AlreadyExists(_)));
    }

    #[test]
    fn test_list_users() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let store = UserStore::open(&path).unwrap();

        store.create("alice", "p", HashAlgorithm::Plaintext, vec![]).unwrap();
        store.create("bob", "p", HashAlgorithm::Plaintext, vec![]).unwrap();

        let mut names = store.list();
        names.sort();
        assert_eq!(names, vec!["alice", "bob"]);
    }
}
