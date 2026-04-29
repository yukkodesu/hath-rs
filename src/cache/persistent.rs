use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentCacheState {
    pub cache_count: u32,
    pub cache_size: u64,
    pub lru_clear_pointer: usize,
    pub static_range_ages: HashMap<String, u64>,
    pub lru_cache_table: Vec<u16>,
}

impl PersistentCacheState {
    pub fn new() -> Self {
        Self {
            cache_count: 0,
            cache_size: 0,
            lru_clear_pointer: 0,
            static_range_ages: HashMap::new(),
            lru_cache_table: vec![0u16; 1_048_576],
        }
    }
}

impl Default for PersistentCacheState {
    fn default() -> Self {
        Self::new()
    }
}
