use rmq_protocol::field_table::FieldTable;

/// Represents a binding between a source exchange and a destination (queue or exchange).
#[derive(Debug, Clone)]
pub struct Binding {
    pub source: String,
    pub destination: String,
    pub routing_key: String,
    pub arguments: FieldTable,
}

/// A binding key used for matching in exchanges.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BindingKey {
    pub routing_key: String,
    pub arguments_hash: Option<String>,
}

impl BindingKey {
    pub fn new(routing_key: String) -> Self {
        Self {
            routing_key,
            arguments_hash: None,
        }
    }

    pub fn with_arguments(routing_key: String, arguments: &FieldTable) -> Self {
        let hash = if arguments.is_empty() {
            None
        } else {
            // Create a deterministic hash of arguments for headers exchange
            Some(format!("{:?}", arguments))
        };
        Self {
            routing_key,
            arguments_hash: hash,
        }
    }
}
