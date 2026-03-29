use regex::Regex;
use serde::{Deserialize, Serialize};

/// Per-vhost permissions for a user.
/// Each field is a regex pattern that is matched against resource names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    /// Regex for resources the user can configure (declare/delete).
    pub configure: String,
    /// Regex for resources the user can write to (publish).
    pub write: String,
    /// Regex for resources the user can read from (consume/get).
    pub read: String,
}

impl Permission {
    /// Full access to everything.
    pub fn full() -> Self {
        Self {
            configure: ".*".to_string(),
            write: ".*".to_string(),
            read: ".*".to_string(),
        }
    }

    /// No access.
    pub fn none() -> Self {
        Self {
            configure: String::new(),
            write: String::new(),
            read: String::new(),
        }
    }

    pub fn can_configure(&self, resource: &str) -> bool {
        matches_pattern(&self.configure, resource)
    }

    pub fn can_write(&self, resource: &str) -> bool {
        matches_pattern(&self.write, resource)
    }

    pub fn can_read(&self, resource: &str) -> bool {
        matches_pattern(&self.read, resource)
    }
}

impl Default for Permission {
    fn default() -> Self {
        Self::full()
    }
}

fn matches_pattern(pattern: &str, resource: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    // Anchor the pattern to match the full string
    let anchored = format!("^(?:{pattern})$");
    Regex::new(&anchored)
        .map(|re| re.is_match(resource))
        .unwrap_or(false)
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
        let perm = Permission {
            configure: "amq\\.gen.*".to_string(),
            write: "orders\\..*".to_string(),
            read: ".*".to_string(),
        };

        assert!(perm.can_configure("amq.gen-abc123"));
        assert!(!perm.can_configure("my-queue"));
        assert!(perm.can_write("orders.new"));
        assert!(!perm.can_write("payments.new"));
        assert!(perm.can_read("anything"));
    }

    #[test]
    fn test_exact_match() {
        let perm = Permission {
            configure: "my-queue".to_string(),
            write: "my-queue".to_string(),
            read: "my-queue".to_string(),
        };

        assert!(perm.can_configure("my-queue"));
        assert!(!perm.can_configure("my-queue-2"));
    }
}
