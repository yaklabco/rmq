use std::collections::{HashMap, HashSet};

use rmq_protocol::field_table::FieldTable;

/// Destination for a routed message.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Destination {
    Queue(String),
    Exchange(String),
}

/// Result of routing a message through an exchange.
#[derive(Debug)]
pub struct RouteResult {
    pub queues: Vec<String>,
}

/// Exchange trait — implemented by each exchange type.
pub trait Exchange: Send + Sync {
    fn name(&self) -> &str;
    fn exchange_type(&self) -> &str;
    fn is_durable(&self) -> bool;
    fn is_auto_delete(&self) -> bool;
    fn is_internal(&self) -> bool;
    fn arguments(&self) -> &FieldTable;

    /// Add a binding.
    fn bind(&mut self, destination: Destination, routing_key: &str, arguments: &FieldTable);

    /// Remove a binding.
    fn unbind(&mut self, destination: &Destination, routing_key: &str, arguments: &FieldTable);

    /// Route a message. Returns the set of queue names that should receive it.
    fn route(&self, routing_key: &str, headers: Option<&FieldTable>) -> Vec<Destination>;

    /// Number of bindings.
    fn binding_count(&self) -> usize;
}

/// Common exchange state.
#[derive(Debug, Clone)]
pub struct ExchangeConfig {
    pub name: String,
    pub exchange_type: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: FieldTable,
}

/// Direct exchange: routes by exact routing key match.
pub struct DirectExchange {
    config: ExchangeConfig,
    bindings: HashMap<String, HashSet<Destination>>,
}

impl DirectExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            bindings: HashMap::new(),
        }
    }
}

impl Exchange for DirectExchange {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn exchange_type(&self) -> &str {
        "direct"
    }
    fn is_durable(&self) -> bool {
        self.config.durable
    }
    fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }
    fn is_internal(&self) -> bool {
        self.config.internal
    }
    fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }

    fn bind(&mut self, destination: Destination, routing_key: &str, _arguments: &FieldTable) {
        self.bindings
            .entry(routing_key.to_string())
            .or_default()
            .insert(destination);
    }

    fn unbind(&mut self, destination: &Destination, routing_key: &str, _arguments: &FieldTable) {
        if let Some(set) = self.bindings.get_mut(routing_key) {
            set.remove(destination);
            if set.is_empty() {
                self.bindings.remove(routing_key);
            }
        }
    }

    fn route(&self, routing_key: &str, _headers: Option<&FieldTable>) -> Vec<Destination> {
        self.bindings
            .get(routing_key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn binding_count(&self) -> usize {
        self.bindings.values().map(|s| s.len()).sum()
    }
}

/// Fanout exchange: routes to all bound destinations regardless of routing key.
pub struct FanoutExchange {
    config: ExchangeConfig,
    bindings: HashSet<Destination>,
}

impl FanoutExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            bindings: HashSet::new(),
        }
    }
}

impl Exchange for FanoutExchange {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn exchange_type(&self) -> &str {
        "fanout"
    }
    fn is_durable(&self) -> bool {
        self.config.durable
    }
    fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }
    fn is_internal(&self) -> bool {
        self.config.internal
    }
    fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }

    fn bind(&mut self, destination: Destination, _routing_key: &str, _arguments: &FieldTable) {
        self.bindings.insert(destination);
    }

    fn unbind(&mut self, destination: &Destination, _routing_key: &str, _arguments: &FieldTable) {
        self.bindings.remove(destination);
    }

    fn route(&self, _routing_key: &str, _headers: Option<&FieldTable>) -> Vec<Destination> {
        self.bindings.iter().cloned().collect()
    }

    fn binding_count(&self) -> usize {
        self.bindings.len()
    }
}

/// Default exchange: routes to queue with name matching the routing key.
pub struct DefaultExchange {
    config: ExchangeConfig,
}

impl DefaultExchange {
    pub fn new() -> Self {
        Self {
            config: ExchangeConfig {
                name: String::new(),
                exchange_type: "direct".into(),
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            },
        }
    }
}

impl Exchange for DefaultExchange {
    fn name(&self) -> &str {
        ""
    }
    fn exchange_type(&self) -> &str {
        "direct"
    }
    fn is_durable(&self) -> bool {
        true
    }
    fn is_auto_delete(&self) -> bool {
        false
    }
    fn is_internal(&self) -> bool {
        false
    }
    fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }

    fn bind(&mut self, _destination: Destination, _routing_key: &str, _arguments: &FieldTable) {
        // Default exchange doesn't accept explicit bindings
    }

    fn unbind(&mut self, _destination: &Destination, _routing_key: &str, _arguments: &FieldTable) {
        // Default exchange doesn't accept explicit bindings
    }

    fn route(&self, routing_key: &str, _headers: Option<&FieldTable>) -> Vec<Destination> {
        // Route to queue with same name as routing key
        vec![Destination::Queue(routing_key.to_string())]
    }

    fn binding_count(&self) -> usize {
        0
    }
}

/// Topic exchange: routes using wildcard patterns on dot-separated routing keys.
/// `*` matches exactly one word, `#` matches zero or more words.
pub struct TopicExchange {
    config: ExchangeConfig,
    bindings: Vec<(TopicPattern, Destination)>,
}

/// A compiled topic binding pattern.
#[derive(Debug, Clone)]
struct TopicPattern {
    original: String,
    segments: Vec<TopicSegment>,
}

#[derive(Debug, Clone, PartialEq)]
enum TopicSegment {
    Exact(String),
    Star,  // * — matches exactly one word
    Hash,  // # — matches zero or more words
}

impl TopicPattern {
    fn compile(pattern: &str) -> Self {
        let segments = pattern
            .split('.')
            .map(|seg| match seg {
                "*" => TopicSegment::Star,
                "#" => TopicSegment::Hash,
                _ => TopicSegment::Exact(seg.to_string()),
            })
            .collect();
        TopicPattern {
            original: pattern.to_string(),
            segments,
        }
    }

    fn matches(&self, routing_key: &str) -> bool {
        let words: Vec<&str> = routing_key.split('.').collect();
        Self::match_segments(&self.segments, &words)
    }

    fn match_segments(segments: &[TopicSegment], words: &[&str]) -> bool {
        match (segments.first(), words.first()) {
            (None, None) => true,
            (Some(TopicSegment::Hash), _) => {
                // # can match zero or more words
                let rest_segs = &segments[1..];
                if rest_segs.is_empty() {
                    return true; // # at end matches everything
                }
                // Try matching # as 0, 1, 2, ... words
                for skip in 0..=words.len() {
                    if Self::match_segments(rest_segs, &words[skip..]) {
                        return true;
                    }
                }
                false
            }
            (Some(TopicSegment::Star), Some(_)) => {
                // * matches exactly one word
                Self::match_segments(&segments[1..], &words[1..])
            }
            (Some(TopicSegment::Exact(expected)), Some(word)) => {
                if expected == word {
                    Self::match_segments(&segments[1..], &words[1..])
                } else {
                    false
                }
            }
            (None, Some(_)) => false, // more words than segments
            (Some(_), None) => {
                // Remaining segments — only valid if all are #
                segments.iter().all(|s| matches!(s, TopicSegment::Hash))
            }
        }
    }
}

impl TopicExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            bindings: Vec::new(),
        }
    }
}

impl Exchange for TopicExchange {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn exchange_type(&self) -> &str {
        "topic"
    }
    fn is_durable(&self) -> bool {
        self.config.durable
    }
    fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }
    fn is_internal(&self) -> bool {
        self.config.internal
    }
    fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }

    fn bind(&mut self, destination: Destination, routing_key: &str, _arguments: &FieldTable) {
        let pattern = TopicPattern::compile(routing_key);
        // Avoid duplicate bindings
        if !self.bindings.iter().any(|(p, d)| p.original == routing_key && d == &destination) {
            self.bindings.push((pattern, destination));
        }
    }

    fn unbind(&mut self, destination: &Destination, routing_key: &str, _arguments: &FieldTable) {
        self.bindings.retain(|(p, d)| !(p.original == routing_key && d == destination));
    }

    fn route(&self, routing_key: &str, _headers: Option<&FieldTable>) -> Vec<Destination> {
        let mut result = HashSet::new();
        for (pattern, dest) in &self.bindings {
            if pattern.matches(routing_key) {
                result.insert(dest.clone());
            }
        }
        result.into_iter().collect()
    }

    fn binding_count(&self) -> usize {
        self.bindings.len()
    }
}

/// Headers exchange: routes based on message header matching.
/// Bindings specify header key-value pairs and an x-match mode:
/// - `all` (default): all specified headers must match
/// - `any`: at least one specified header must match
pub struct HeadersExchange {
    config: ExchangeConfig,
    bindings: Vec<(FieldTable, Destination, HeadersMatchMode)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum HeadersMatchMode {
    All,
    Any,
}

impl HeadersExchange {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            bindings: Vec::new(),
        }
    }
}

impl Exchange for HeadersExchange {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn exchange_type(&self) -> &str {
        "headers"
    }
    fn is_durable(&self) -> bool {
        self.config.durable
    }
    fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }
    fn is_internal(&self) -> bool {
        self.config.internal
    }
    fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }

    fn bind(&mut self, destination: Destination, _routing_key: &str, arguments: &FieldTable) {
        let mode = match arguments.get("x-match") {
            Some(rmq_protocol::field_table::FieldValue::ShortString(s)) if s == "any" => {
                HeadersMatchMode::Any
            }
            _ => HeadersMatchMode::All,
        };

        // Strip x-match from the binding arguments for matching purposes
        let mut match_args = arguments.clone();
        match_args.0.remove("x-match");

        self.bindings.push((match_args, destination, mode));
    }

    fn unbind(&mut self, destination: &Destination, _routing_key: &str, arguments: &FieldTable) {
        let mut match_args = arguments.clone();
        match_args.0.remove("x-match");
        self.bindings.retain(|(args, d, _)| !(args == &match_args && d == destination));
    }

    fn route(&self, _routing_key: &str, headers: Option<&FieldTable>) -> Vec<Destination> {
        let headers = match headers {
            Some(h) => h,
            None => return vec![],
        };

        let mut result = HashSet::new();
        for (bind_args, dest, mode) in &self.bindings {
            if bind_args.is_empty() {
                continue; // empty binding args never match
            }

            let matched = match mode {
                HeadersMatchMode::All => {
                    bind_args.0.iter().all(|(key, value)| {
                        headers.get(key).map_or(false, |v| v == value)
                    })
                }
                HeadersMatchMode::Any => {
                    bind_args.0.iter().any(|(key, value)| {
                        headers.get(key).map_or(false, |v| v == value)
                    })
                }
            };

            if matched {
                result.insert(dest.clone());
            }
        }
        result.into_iter().collect()
    }

    fn binding_count(&self) -> usize {
        self.bindings.len()
    }
}

/// Create an exchange from its type string.
pub fn create_exchange(config: ExchangeConfig) -> Box<dyn Exchange> {
    match config.exchange_type.as_str() {
        "direct" => Box::new(DirectExchange::new(config)),
        "fanout" => Box::new(FanoutExchange::new(config)),
        "topic" => Box::new(TopicExchange::new(config)),
        "headers" => Box::new(HeadersExchange::new(config)),
        _ => Box::new(DirectExchange::new(config)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direct_exchange_routing() {
        let config = ExchangeConfig {
            name: "test".into(),
            exchange_type: "direct".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        };
        let mut exchange = DirectExchange::new(config);

        exchange.bind(
            Destination::Queue("q1".into()),
            "key1",
            &FieldTable::new(),
        );
        exchange.bind(
            Destination::Queue("q2".into()),
            "key2",
            &FieldTable::new(),
        );

        let result = exchange.route("key1", None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Destination::Queue("q1".into())));

        let result = exchange.route("key2", None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Destination::Queue("q2".into())));

        let result = exchange.route("nonexistent", None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_direct_exchange_multiple_bindings_same_key() {
        let config = ExchangeConfig {
            name: "test".into(),
            exchange_type: "direct".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        };
        let mut exchange = DirectExchange::new(config);

        exchange.bind(
            Destination::Queue("q1".into()),
            "key",
            &FieldTable::new(),
        );
        exchange.bind(
            Destination::Queue("q2".into()),
            "key",
            &FieldTable::new(),
        );

        let result = exchange.route("key", None);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_fanout_exchange_routing() {
        let config = ExchangeConfig {
            name: "test".into(),
            exchange_type: "fanout".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        };
        let mut exchange = FanoutExchange::new(config);

        exchange.bind(Destination::Queue("q1".into()), "", &FieldTable::new());
        exchange.bind(Destination::Queue("q2".into()), "", &FieldTable::new());
        exchange.bind(Destination::Queue("q3".into()), "", &FieldTable::new());

        let result = exchange.route("any-key", None);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_default_exchange_routing() {
        let exchange = DefaultExchange::new();
        let result = exchange.route("my-queue", None);
        assert_eq!(result, vec![Destination::Queue("my-queue".into())]);
    }

    #[test]
    fn test_unbind() {
        let config = ExchangeConfig {
            name: "test".into(),
            exchange_type: "direct".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        };
        let mut exchange = DirectExchange::new(config);

        let dest = Destination::Queue("q1".into());
        exchange.bind(dest.clone(), "key", &FieldTable::new());
        assert_eq!(exchange.binding_count(), 1);

        exchange.unbind(&dest, "key", &FieldTable::new());
        assert_eq!(exchange.binding_count(), 0);
        assert!(exchange.route("key", None).is_empty());
    }

    // --- Topic Exchange Tests ---

    fn topic_exchange() -> TopicExchange {
        TopicExchange::new(ExchangeConfig {
            name: "test-topic".into(),
            exchange_type: "topic".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        })
    }

    #[test]
    fn test_topic_exact_match() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.usd.nyse", &FieldTable::new());

        assert_eq!(ex.route("stock.usd.nyse", None).len(), 1);
        assert!(ex.route("stock.usd.nasdaq", None).is_empty());
        assert!(ex.route("stock.usd", None).is_empty());
    }

    #[test]
    fn test_topic_star_wildcard() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.*.nyse", &FieldTable::new());

        assert_eq!(ex.route("stock.usd.nyse", None).len(), 1);
        assert_eq!(ex.route("stock.eur.nyse", None).len(), 1);
        assert!(ex.route("stock.usd.nasdaq", None).is_empty());
        assert!(ex.route("stock.nyse", None).is_empty()); // * must match exactly one word
        assert!(ex.route("stock.usd.gbp.nyse", None).is_empty());
    }

    #[test]
    fn test_topic_hash_wildcard() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.#", &FieldTable::new());

        assert_eq!(ex.route("stock.usd.nyse", None).len(), 1);
        assert_eq!(ex.route("stock.eur", None).len(), 1);
        assert_eq!(ex.route("stock", None).len(), 1); // # matches zero words
        assert!(ex.route("bond.usd", None).is_empty());
    }

    #[test]
    fn test_topic_hash_in_middle() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.#.nyse", &FieldTable::new());

        assert_eq!(ex.route("stock.nyse", None).len(), 1); // # matches zero
        assert_eq!(ex.route("stock.usd.nyse", None).len(), 1); // # matches one
        assert_eq!(ex.route("stock.usd.eur.nyse", None).len(), 1); // # matches two
        assert!(ex.route("stock.usd.nasdaq", None).is_empty());
    }

    #[test]
    fn test_topic_hash_only() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "#", &FieldTable::new());

        assert_eq!(ex.route("anything", None).len(), 1);
        assert_eq!(ex.route("a.b.c.d", None).len(), 1);
        assert_eq!(ex.route("", None).len(), 1);
    }

    #[test]
    fn test_topic_multiple_bindings() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.*", &FieldTable::new());
        ex.bind(Destination::Queue("q2".into()), "stock.usd", &FieldTable::new());

        // Both should match
        let result = ex.route("stock.usd", None);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_topic_no_duplicate_bindings() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.*", &FieldTable::new());
        ex.bind(Destination::Queue("q1".into()), "stock.*", &FieldTable::new());
        assert_eq!(ex.binding_count(), 1);
    }

    #[test]
    fn test_topic_unbind() {
        let mut ex = topic_exchange();
        ex.bind(Destination::Queue("q1".into()), "stock.*", &FieldTable::new());
        assert_eq!(ex.binding_count(), 1);

        ex.unbind(&Destination::Queue("q1".into()), "stock.*", &FieldTable::new());
        assert_eq!(ex.binding_count(), 0);
    }

    // --- Headers Exchange Tests ---

    fn headers_exchange() -> HeadersExchange {
        HeadersExchange::new(ExchangeConfig {
            name: "test-headers".into(),
            exchange_type: "headers".into(),
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        })
    }

    use rmq_protocol::field_table::FieldValue;

    #[test]
    fn test_headers_match_all() {
        let mut ex = headers_exchange();
        let mut bind_args = FieldTable::new();
        bind_args.insert("x-match", FieldValue::ShortString("all".into()));
        bind_args.insert("format", FieldValue::ShortString("pdf".into()));
        bind_args.insert("type", FieldValue::ShortString("report".into()));
        ex.bind(Destination::Queue("q1".into()), "", &bind_args);

        // Both headers match
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        headers.insert("type", FieldValue::ShortString("report".into()));
        assert_eq!(ex.route("", Some(&headers)).len(), 1);

        // Only one header matches — should NOT route
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        headers.insert("type", FieldValue::ShortString("log".into()));
        assert!(ex.route("", Some(&headers)).is_empty());

        // Missing header
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        assert!(ex.route("", Some(&headers)).is_empty());
    }

    #[test]
    fn test_headers_match_any() {
        let mut ex = headers_exchange();
        let mut bind_args = FieldTable::new();
        bind_args.insert("x-match", FieldValue::ShortString("any".into()));
        bind_args.insert("format", FieldValue::ShortString("pdf".into()));
        bind_args.insert("type", FieldValue::ShortString("report".into()));
        ex.bind(Destination::Queue("q1".into()), "", &bind_args);

        // One header matches — should route
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        headers.insert("type", FieldValue::ShortString("log".into()));
        assert_eq!(ex.route("", Some(&headers)).len(), 1);

        // No headers match
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("csv".into()));
        headers.insert("type", FieldValue::ShortString("log".into()));
        assert!(ex.route("", Some(&headers)).is_empty());
    }

    #[test]
    fn test_headers_no_message_headers() {
        let mut ex = headers_exchange();
        let mut bind_args = FieldTable::new();
        bind_args.insert("x-match", FieldValue::ShortString("all".into()));
        bind_args.insert("format", FieldValue::ShortString("pdf".into()));
        ex.bind(Destination::Queue("q1".into()), "", &bind_args);

        // No headers on message
        assert!(ex.route("", None).is_empty());
    }

    #[test]
    fn test_headers_default_match_mode_is_all() {
        let mut ex = headers_exchange();
        let mut bind_args = FieldTable::new();
        // No x-match specified — defaults to "all"
        bind_args.insert("format", FieldValue::ShortString("pdf".into()));
        bind_args.insert("type", FieldValue::ShortString("report".into()));
        ex.bind(Destination::Queue("q1".into()), "", &bind_args);

        // Only one matches — all mode requires both
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        assert!(ex.route("", Some(&headers)).is_empty());
    }

    #[test]
    fn test_headers_extra_message_headers_ok() {
        let mut ex = headers_exchange();
        let mut bind_args = FieldTable::new();
        bind_args.insert("x-match", FieldValue::ShortString("all".into()));
        bind_args.insert("format", FieldValue::ShortString("pdf".into()));
        ex.bind(Destination::Queue("q1".into()), "", &bind_args);

        // Extra headers on message should not prevent match
        let mut headers = FieldTable::new();
        headers.insert("format", FieldValue::ShortString("pdf".into()));
        headers.insert("extra", FieldValue::ShortString("ignored".into()));
        assert_eq!(ex.route("", Some(&headers)).len(), 1);
    }
}
