use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Apply-to target for a policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplyTo {
    All,
    Queues,
    Exchanges,
}

impl Default for ApplyTo {
    fn default() -> Self {
        Self::All
    }
}

/// A policy definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    pub pattern: String,
    pub apply_to: ApplyTo,
    pub priority: i32,
    pub definition: HashMap<String, PolicyValue>,
}

/// Policy value — can be a string, integer, or boolean.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PolicyValue {
    String(String),
    Integer(i64),
    Bool(bool),
}

/// The merged effective policy for a resource.
#[derive(Debug, Clone, Default)]
pub struct EffectivePolicy {
    pub values: HashMap<String, PolicyValue>,
}

impl EffectivePolicy {
    pub fn get_string(&self, key: &str) -> Option<&str> {
        match self.values.get(key)? {
            PolicyValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.values.get(key)? {
            PolicyValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.values.get(key)? {
            PolicyValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

/// Policy store that manages policies and computes effective policies.
pub struct PolicyStore {
    policies: Vec<Policy>,
    operator_policies: Vec<Policy>,
}

impl PolicyStore {
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
            operator_policies: Vec::new(),
        }
    }

    /// Add or update a user policy.
    pub fn set_policy(&mut self, policy: Policy) {
        self.policies.retain(|p| p.name != policy.name);
        self.policies.push(policy);
    }

    /// Remove a user policy.
    pub fn remove_policy(&mut self, name: &str) -> bool {
        let len = self.policies.len();
        self.policies.retain(|p| p.name != name);
        self.policies.len() < len
    }

    /// Add or update an operator policy.
    pub fn set_operator_policy(&mut self, policy: Policy) {
        self.operator_policies.retain(|p| p.name != policy.name);
        self.operator_policies.push(policy);
    }

    /// Remove an operator policy.
    pub fn remove_operator_policy(&mut self, name: &str) -> bool {
        let len = self.operator_policies.len();
        self.operator_policies.retain(|p| p.name != name);
        self.operator_policies.len() < len
    }

    /// List all user policies.
    pub fn policies(&self) -> &[Policy] {
        &self.policies
    }

    /// List all operator policies.
    pub fn operator_policies(&self) -> &[Policy] {
        &self.operator_policies
    }

    /// Compute the effective policy for a queue.
    pub fn effective_policy_for_queue(&self, queue_name: &str) -> EffectivePolicy {
        self.compute_effective(queue_name, ApplyTo::Queues)
    }

    /// Compute the effective policy for an exchange.
    pub fn effective_policy_for_exchange(&self, exchange_name: &str) -> EffectivePolicy {
        self.compute_effective(exchange_name, ApplyTo::Exchanges)
    }

    fn compute_effective(&self, name: &str, resource_type: ApplyTo) -> EffectivePolicy {
        let mut result = HashMap::new();

        // Find the highest-priority matching user policy
        if let Some(policy) = self.find_matching_policy(&self.policies, name, &resource_type) {
            result.extend(policy.definition.clone());
        }

        // Operator policies override specific keys
        if let Some(op_policy) =
            self.find_matching_policy(&self.operator_policies, name, &resource_type)
        {
            for (key, value) in &op_policy.definition {
                // Only allow operator-safe keys
                if is_operator_key(key) {
                    result.insert(key.clone(), value.clone());
                }
            }
        }

        EffectivePolicy { values: result }
    }

    fn find_matching_policy<'a>(
        &self,
        policies: &'a [Policy],
        name: &str,
        resource_type: &ApplyTo,
    ) -> Option<&'a Policy> {
        policies
            .iter()
            .filter(|p| {
                // Check apply-to matches
                match (&p.apply_to, resource_type) {
                    (ApplyTo::All, _) => true,
                    (ApplyTo::Queues, ApplyTo::Queues) => true,
                    (ApplyTo::Exchanges, ApplyTo::Exchanges) => true,
                    _ => false,
                }
            })
            .filter(|p| {
                // Check regex pattern matches
                let anchored = format!("^(?:{})$", p.pattern);
                Regex::new(&anchored)
                    .map(|re| re.is_match(name))
                    .unwrap_or(false)
            })
            .max_by_key(|p| p.priority)
    }
}

impl Default for PolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Keys allowed in operator policies.
fn is_operator_key(key: &str) -> bool {
    matches!(
        key,
        "expires" | "message-ttl" | "max-length" | "max-length-bytes"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_matching_policy() {
        let store = PolicyStore::new();
        let effective = store.effective_policy_for_queue("my-queue");
        assert!(effective.values.is_empty());
    }

    #[test]
    fn test_simple_policy_match() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "ttl-policy".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::Queues,
            priority: 0,
            definition: HashMap::from([
                ("message-ttl".into(), PolicyValue::Integer(60000)),
                ("max-length".into(), PolicyValue::Integer(1000)),
            ]),
        });

        let effective = store.effective_policy_for_queue("any-queue");
        assert_eq!(effective.get_i64("message-ttl"), Some(60000));
        assert_eq!(effective.get_i64("max-length"), Some(1000));
    }

    #[test]
    fn test_pattern_matching() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "orders-policy".into(),
            pattern: "orders\\..*".into(),
            apply_to: ApplyTo::Queues,
            priority: 0,
            definition: HashMap::from([("message-ttl".into(), PolicyValue::Integer(30000))]),
        });

        let effective = store.effective_policy_for_queue("orders.new");
        assert_eq!(effective.get_i64("message-ttl"), Some(30000));

        let effective = store.effective_policy_for_queue("payments.new");
        assert!(effective.values.is_empty());
    }

    #[test]
    fn test_highest_priority_wins() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "low".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([("message-ttl".into(), PolicyValue::Integer(60000))]),
        });
        store.set_policy(Policy {
            name: "high".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 10,
            definition: HashMap::from([("message-ttl".into(), PolicyValue::Integer(30000))]),
        });

        let effective = store.effective_policy_for_queue("q");
        assert_eq!(effective.get_i64("message-ttl"), Some(30000));
    }

    #[test]
    fn test_apply_to_filter() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "queues-only".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::Queues,
            priority: 0,
            definition: HashMap::from([("max-length".into(), PolicyValue::Integer(100))]),
        });

        // Should apply to queues
        let effective = store.effective_policy_for_queue("q");
        assert_eq!(effective.get_i64("max-length"), Some(100));

        // Should NOT apply to exchanges
        let effective = store.effective_policy_for_exchange("ex");
        assert!(effective.values.is_empty());
    }

    #[test]
    fn test_operator_policy_overrides() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "user-policy".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([
                ("message-ttl".into(), PolicyValue::Integer(60000)),
                ("max-length".into(), PolicyValue::Integer(1000)),
                (
                    "dead-letter-exchange".into(),
                    PolicyValue::String("dlx".into()),
                ),
            ]),
        });
        store.set_operator_policy(Policy {
            name: "op-policy".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([
                ("max-length".into(), PolicyValue::Integer(500)), // override
                (
                    "dead-letter-exchange".into(),
                    PolicyValue::String("ignored".into()),
                ), // NOT an operator key, ignored
            ]),
        });

        let effective = store.effective_policy_for_queue("q");
        assert_eq!(effective.get_i64("message-ttl"), Some(60000)); // from user
        assert_eq!(effective.get_i64("max-length"), Some(500)); // overridden by operator
        assert_eq!(
            effective.get_string("dead-letter-exchange"),
            Some("dlx") // operator can't override non-operator keys
        );
    }

    #[test]
    fn test_remove_policy() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "p1".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([("max-length".into(), PolicyValue::Integer(100))]),
        });

        assert!(store.remove_policy("p1"));
        assert!(!store.remove_policy("p1")); // already removed

        let effective = store.effective_policy_for_queue("q");
        assert!(effective.values.is_empty());
    }

    #[test]
    fn test_update_policy() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "p1".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([("max-length".into(), PolicyValue::Integer(100))]),
        });

        // Update same-named policy
        store.set_policy(Policy {
            name: "p1".into(),
            pattern: ".*".into(),
            apply_to: ApplyTo::All,
            priority: 0,
            definition: HashMap::from([("max-length".into(), PolicyValue::Integer(200))]),
        });

        let effective = store.effective_policy_for_queue("q");
        assert_eq!(effective.get_i64("max-length"), Some(200));
        assert_eq!(store.policies().len(), 1); // no duplicates
    }

    #[test]
    fn test_exchange_policy() {
        let mut store = PolicyStore::new();
        store.set_policy(Policy {
            name: "alt-ex".into(),
            pattern: "my\\..*".into(),
            apply_to: ApplyTo::Exchanges,
            priority: 0,
            definition: HashMap::from([(
                "alternate-exchange".into(),
                PolicyValue::String("unrouted".into()),
            )]),
        });

        let effective = store.effective_policy_for_exchange("my.exchange");
        assert_eq!(effective.get_string("alternate-exchange"), Some("unrouted"));
    }
}
