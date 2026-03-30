use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Per-vhost permissions for a user.
/// Each field is a regex pattern that is matched against resource names.
/// Compiled regexes are cached lazily on first use via `OnceLock`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    /// Regex for resources the user can configure (declare/delete).
    pub configure: String,
    /// Regex for resources the user can write to (publish).
    pub write: String,
    /// Regex for resources the user can read from (consume/get).
    pub read: String,

    #[serde(skip)]
    configure_re: OnceLock<Option<Regex>>,
    #[serde(skip)]
    write_re: OnceLock<Option<Regex>>,
    #[serde(skip)]
    read_re: OnceLock<Option<Regex>>,
}

impl Permission {
    /// Create a new permission with the given regex patterns.
    pub fn new(
        configure: impl Into<String>,
        write: impl Into<String>,
        read: impl Into<String>,
    ) -> Self {
        Self {
            configure: configure.into(),
            write: write.into(),
            read: read.into(),
            configure_re: OnceLock::new(),
            write_re: OnceLock::new(),
            read_re: OnceLock::new(),
        }
    }

    /// Full access to everything.
    pub fn full() -> Self {
        Self::new(".*", ".*", ".*")
    }

    /// No access.
    pub fn none() -> Self {
        Self::new("", "", "")
    }

    pub fn can_configure(&self, resource: &str) -> bool {
        self.matches_cached(&self.configure, &self.configure_re, resource)
    }

    pub fn can_write(&self, resource: &str) -> bool {
        self.matches_cached(&self.write, &self.write_re, resource)
    }

    pub fn can_read(&self, resource: &str) -> bool {
        self.matches_cached(&self.read, &self.read_re, resource)
    }

    /// Match against a cached compiled regex. Compiles on first call.
    fn matches_cached(
        &self,
        pattern: &str,
        cache: &OnceLock<Option<Regex>>,
        resource: &str,
    ) -> bool {
        if pattern.is_empty() {
            return false;
        }
        let re = cache.get_or_init(|| {
            let anchored = format!("^(?:{pattern})$");
            Regex::new(&anchored).ok()
        });
        re.as_ref().is_some_and(|r| r.is_match(resource))
    }
}

impl Default for Permission {
    fn default() -> Self {
        Self::full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_permission() {
        let perm = Permission::full();
        assert!(perm.can_configure("anything"));
        assert!(perm.can_write("anything"));
        assert!(perm.can_read("anything"));
    }

    #[test]
    fn test_no_permission() {
        let perm = Permission::none();
        assert!(!perm.can_configure("anything"));
        assert!(!perm.can_write("anything"));
        assert!(!perm.can_read("anything"));
    }

    #[test]
    fn test_regex_pattern() {
        let perm = Permission::new("amq\\.gen.*", "orders\\..*", ".*");

        assert!(perm.can_configure("amq.gen-abc123"));
        assert!(!perm.can_configure("my-queue"));
        assert!(perm.can_write("orders.new"));
        assert!(!perm.can_write("payments.new"));
        assert!(perm.can_read("anything"));
    }

    #[test]
    fn test_exact_match() {
        let perm = Permission::new("my-queue", "my-queue", "my-queue");

        assert!(perm.can_configure("my-queue"));
        assert!(!perm.can_configure("my-queue-2"));
    }

    #[test]
    fn test_regex_is_cached() {
        let perm = Permission::new("test-.*", "", "");

        // First call compiles and caches
        assert!(perm.can_configure("test-queue"));
        // Second call uses cache (OnceLock guarantees single init)
        assert!(perm.can_configure("test-exchange"));
        assert!(!perm.can_configure("other"));

        // Verify the OnceLock was initialized
        assert!(perm.configure_re.get().is_some());
    }

    #[test]
    fn test_deserialized_permission_caches_regex() {
        // Simulate deserialization (OnceLock fields will be default/empty)
        let json = r#"{"configure":"foo-.*","write":"bar","read":".*"}"#;
        let perm: Permission = serde_json::from_str(json).unwrap();

        // OnceLock should be uninitialized after deserialization
        assert!(perm.configure_re.get().is_none());

        // First use should compile and cache
        assert!(perm.can_configure("foo-123"));
        assert!(perm.configure_re.get().is_some());

        // Subsequent uses still work
        assert!(!perm.can_configure("baz"));
    }
}
