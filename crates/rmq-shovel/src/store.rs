use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::federation::FederationUpstream;
use crate::shovel::ShovelConfig;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Persistent store for shovel and federation configurations.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ShovelStore {
    pub shovels: HashMap<String, ShovelConfig>,
    pub upstreams: HashMap<String, FederationUpstream>,
    #[serde(skip)]
    path: PathBuf,
}

impl ShovelStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let data = std::fs::read_to_string(&path)?;
            let mut store: ShovelStore = serde_json::from_str(&data)?;
            store.path = path;
            Ok(store)
        } else {
            Ok(Self {
                shovels: HashMap::new(),
                upstreams: HashMap::new(),
                path,
            })
        }
    }

    pub fn add_shovel(&mut self, config: ShovelConfig) -> Result<(), StoreError> {
        self.shovels.insert(config.name.clone(), config);
        self.save()
    }

    pub fn remove_shovel(&mut self, name: &str) -> Result<bool, StoreError> {
        let removed = self.shovels.remove(name).is_some();
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    pub fn add_upstream(&mut self, upstream: FederationUpstream) -> Result<(), StoreError> {
        self.upstreams.insert(upstream.name.clone(), upstream);
        self.save()
    }

    pub fn remove_upstream(&mut self, name: &str) -> Result<bool, StoreError> {
        let removed = self.upstreams.remove(name).is_some();
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    fn save(&self) -> Result<(), StoreError> {
        let data = serde_json::to_string_pretty(self)?;
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shovel::AckMode;
    use tempfile::TempDir;

    #[test]
    fn test_store_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shovels.json");

        {
            let mut store = ShovelStore::open(&path).unwrap();
            store
                .add_shovel(ShovelConfig {
                    name: "s1".into(),
                    src_queue: "q1".into(),
                    dest_exchange: "ex1".into(),
                    dest_routing_key: "k1".into(),
                    prefetch: 10,
                    ack_mode: AckMode::OnConfirm,
                    reconnect_delay_max: 30,
                })
                .unwrap();
            store
                .add_upstream(FederationUpstream {
                    name: "u1".into(),
                    uri: "amqp://remote".into(),
                    exchange: "ex1".into(),
                    max_hops: 5,
                    prefetch: 10,
                    reconnect_delay: 5,
                })
                .unwrap();
        }

        let store = ShovelStore::open(&path).unwrap();
        assert_eq!(store.shovels.len(), 1);
        assert_eq!(store.upstreams.len(), 1);
        assert_eq!(store.shovels["s1"].src_queue, "q1");
        assert_eq!(store.upstreams["u1"].exchange, "ex1");
    }

    #[test]
    fn test_store_remove() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shovels.json");

        let mut store = ShovelStore::open(&path).unwrap();
        store
            .add_shovel(ShovelConfig {
                name: "s1".into(),
                src_queue: "q1".into(),
                dest_exchange: "ex1".into(),
                dest_routing_key: "k1".into(),
                prefetch: 10,
                ack_mode: AckMode::OnConfirm,
                reconnect_delay_max: 30,
            })
            .unwrap();

        assert!(store.remove_shovel("s1").unwrap());
        assert!(!store.remove_shovel("s1").unwrap()); // already removed
        assert!(store.shovels.is_empty());
    }
}
