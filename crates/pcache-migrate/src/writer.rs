use sha1::{Digest, Sha1};

use crate::protocol::LegacyCacheState;
use crate::transaction::ImportError;

pub struct EncodedSnapshot {
    pub ages: Vec<u8>,
    pub lru: Vec<u8>,
    pub info: Vec<u8>,
    pub ages_hash: String,
    pub lru_hash: String,
}

pub fn encode(state: &LegacyCacheState) -> Result<EncodedSnapshot, ImportError> {
    let ages = bincode::serialize(&state.static_range_ages)?;
    let lru = bincode::serialize(&state.lru_cache_table)?;
    let ages_hash = sha1_hex(&ages);
    let lru_hash = sha1_hex(&lru);
    let info = format!(
        "formatVersion=1\ncacheCount={}\ncacheSize={}\nlruClearPointer={}\nagesHash={}\nlruHash={}",
        state.cache_count, state.cache_size, state.lru_clear_pointer, ages_hash, lru_hash,
    )
    .into_bytes();

    Ok(EncodedSnapshot {
        ages,
        lru,
        info,
        ages_hash,
        lru_hash,
    })
}

pub fn sha1_hex(bytes: &[u8]) -> String {
    Sha1::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
