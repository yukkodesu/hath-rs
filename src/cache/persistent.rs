use crate::config::Config;
use crate::error::{HathError, Result};
use crate::utils;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

const PERSISTENT_CACHE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PersistentCacheState {
    pub(super) cache_count: u32,
    pub(super) cache_size: u64,
    pub(super) lru_clear_pointer: usize,
    pub(super) static_range_ages: HashMap<String, u64>,
    pub(super) lru_cache_table: Vec<u16>,
}

impl PersistentCacheState {
    pub(super) fn new() -> Self {
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

/// Java: `CacheHandler.savePersistentData()`
pub(super) fn save(config: &Config, state: &PersistentCacheState) -> Result<()> {
    let ages_path = config.data_dir.join("pcache_ages");
    let lru_path = config.data_dir.join("pcache_lru");
    let info_path = config.data_dir.join("pcache_info");

    // `pcache_info` is the publish marker. Remove any old marker before
    // writing so an interrupted save can only force a rescan, never validate
    // stale companion files.
    remove_if_exists(&info_path)?;

    let ages_data = bincode::serialize(&state.static_range_ages)
        .map_err(|e| HathError::Cache(format!("Failed to serialize ages: {}", e)))?;
    fs::write(&ages_path, &ages_data)
        .map_err(|e| HathError::Cache(format!("Failed to write ages: {}", e)))?;
    let ages_hash = utils::sha1_bytes(&ages_data);

    let lru_data = bincode::serialize(&state.lru_cache_table)
        .map_err(|e| HathError::Cache(format!("Failed to serialize lru: {}", e)))?;
    fs::write(&lru_path, &lru_data)
        .map_err(|e| HathError::Cache(format!("Failed to write lru: {}", e)))?;
    let lru_hash = utils::sha1_bytes(&lru_data);

    let info = format!(
        "formatVersion={}\ncacheCount={}\ncacheSize={}\nlruClearPointer={}\nagesHash={}\nlruHash={}",
        PERSISTENT_CACHE_FORMAT_VERSION,
        state.cache_count,
        state.cache_size,
        state.lru_clear_pointer,
        ages_hash,
        lru_hash
    );
    fs::write(&info_path, info.as_bytes())
        .map_err(|e| HathError::Cache(format!("Failed to write info: {}", e)))?;

    Ok(())
}

/// Remove every persistent cache artifact.
///
/// Java deletes this set after deciding whether startup can reuse it. A later
/// clean shutdown is the only point at which a new set becomes authoritative.
pub(super) fn clear(config: &Config) -> Result<()> {
    for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
        remove_if_exists(&config.data_dir.join(name))?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(HathError::Cache(format!(
            "Failed to remove persistent cache file {}: {}",
            path.display(),
            e
        ))),
    }
}

/// Java: `CacheHandler.loadPersistentData()`
pub(super) fn try_load(config: &Config) -> Option<PersistentCacheState> {
    let info_path = config.data_dir.join("pcache_info");
    let lru_path = config.data_dir.join("pcache_lru");
    let ages_path = config.data_dir.join("pcache_ages");

    if !info_path.exists() {
        tracing::debug!("CacheHandler: Missing pcache_info, forcing rescan");
        return None;
    }

    // Read and parse pcache_info (plain text key=value)
    let info_content = utils::read_string_file(&info_path).ok()?;
    let mut info_checksum: u32 = 0;
    let mut ages_hash: Option<String> = None;
    let mut lru_hash: Option<String> = None;
    let mut cache_count: u32 = 0;
    let mut cache_size: u64 = 0;
    let mut lru_clear_pointer: usize = 0;
    let mut format_version: Option<u32> = None;

    for line in info_content.lines() {
        if let Some((key, value)) = line.split_once('=') {
            match key {
                "formatVersion" => format_version = Some(value.parse().ok()?),
                "cacheCount" => {
                    cache_count = value.parse().ok()?;
                    tracing::debug!("CacheHandler: Loaded persistent cacheCount={}", cache_count);
                    info_checksum |= 1;
                }
                "cacheSize" => {
                    cache_size = value.parse().ok()?;
                    tracing::debug!("CacheHandler: Loaded persistent cacheSize={}", cache_size);
                    info_checksum |= 2;
                }
                "lruClearPointer" => {
                    lru_clear_pointer = value.parse().ok()?;
                    tracing::debug!(
                        "CacheHandler: Loaded persistent lruClearPointer={}",
                        lru_clear_pointer
                    );
                    info_checksum |= 4;
                }
                "agesHash" => {
                    ages_hash = Some(value.to_string());
                    tracing::debug!("CacheHandler: Found agesHash={}", value);
                    info_checksum |= 8;
                }
                "lruHash" => {
                    lru_hash = Some(value.to_string());
                    tracing::debug!("CacheHandler: Found lruHash={}", value);
                    info_checksum |= 16;
                }
                _ => {}
            }
        }
    }

    if info_checksum != 31 {
        tracing::info!("CacheHandler: Persistent fields were missing, forcing rescan");
        return None;
    }

    if format_version.is_some_and(|version| version != PERSISTENT_CACHE_FORMAT_VERSION) {
        tracing::info!(
            "CacheHandler: Unsupported persistent cache format {}, forcing rescan",
            format_version.unwrap()
        );
        return None;
    }

    let ages_hash = ages_hash?;
    let lru_hash = lru_hash?;

    tracing::info!("CacheHandler: All persistent fields found, loading remaining objects");

    // Verify SHA-1 and deserialize pcache_ages
    let static_range_ages: HashMap<String, u64> = match read_object(&ages_path, &ages_hash) {
        Ok(ages) => ages,
        Err(e) => {
            tracing::warn!(
                "CacheHandler: Failed to load pcache_ages: {}, forcing rescan",
                e
            );
            return None;
        }
    };
    tracing::info!("CacheHandler: Loaded static range ages");

    // Verify SHA-1 and deserialize pcache_lru
    let lru_cache_table: Vec<u16> = match read_object(&lru_path, &lru_hash) {
        Ok(lru) => lru,
        Err(e) => {
            tracing::warn!(
                "CacheHandler: Failed to load pcache_lru: {}, forcing rescan",
                e
            );
            return None;
        }
    };
    tracing::info!("CacheHandler: Loaded LRU cache");

    Some(PersistentCacheState {
        cache_count,
        cache_size,
        lru_clear_pointer,
        static_range_ages,
        lru_cache_table,
    })
}

/// Verify SHA-1 hash of a file and deserialize its contents with bincode.
/// Java: `CacheHandler.readCacheObject()`
fn read_object<T: serde::de::DeserializeOwned>(file: &Path, expected_hash: &str) -> Result<T> {
    if !file.exists() {
        return Err(HathError::Cache(format!(
            "Missing file: {}",
            file.display()
        )));
    }

    let data = fs::read(file)
        .map_err(|e| HathError::Cache(format!("Failed to read file {}: {}", file.display(), e)))?;

    let actual_hash = utils::sha1_bytes(&data);
    if actual_hash != expected_hash {
        return Err(HathError::Cache(format!(
            "Incorrect file hash while reading {} (expected {}, got {})",
            file.display(),
            expected_hash,
            actual_hash
        )));
    }

    bincode::deserialize(&data)
        .map_err(|e| HathError::Cache(format!("Failed to deserialize {}: {}", file.display(), e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CliArgs, Config};
    use clap::Parser;

    fn config_for_data_dir(data_dir: &Path) -> Config {
        let args = CliArgs::try_parse_from([
            "hath-rs",
            "--client-id",
            "12345",
            "--client-key",
            "abcde12345abcde12345",
        ])
        .unwrap();
        let mut config = Config::load(args).unwrap();
        config.data_dir = data_dir.to_path_buf();
        config
    }

    #[test]
    fn save_then_try_load_roundtrips_without_deleting_info_file() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());

        let mut ages: HashMap<String, u64> = HashMap::new();
        ages.insert("abcd".to_string(), 1234);
        let state = PersistentCacheState {
            cache_count: 7,
            cache_size: 99,
            lru_clear_pointer: 17,
            static_range_ages: ages,
            lru_cache_table: vec![1u16, 2, 3],
        };

        save(&config, &state).unwrap();

        let loaded = try_load(&config).unwrap();

        assert_eq!(loaded.cache_count, state.cache_count);
        assert_eq!(loaded.cache_size, state.cache_size);
        assert_eq!(loaded.lru_clear_pointer, state.lru_clear_pointer);
        assert_eq!(loaded.static_range_ages.get("abcd"), Some(&1234));
        assert_eq!(loaded.lru_cache_table, state.lru_cache_table);
        assert!(temp.path().join("pcache_info").exists());
    }

    #[test]
    fn clear_removes_every_persistent_cache_file() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());
        let state = PersistentCacheState::default();

        save(&config, &state).unwrap();
        clear(&config).unwrap();

        for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
            assert!(!temp.path().join(name).exists(), "{name} should be removed");
        }
    }

    #[test]
    fn save_marks_new_snapshots_as_format_v1() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());

        save(&config, &PersistentCacheState::default()).unwrap();

        assert!(
            fs::read_to_string(temp.path().join("pcache_info"))
                .unwrap()
                .contains("formatVersion=1")
        );
    }

    #[test]
    fn unversioned_rust_snapshot_remains_format_v1() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());

        save(&config, &PersistentCacheState::default()).unwrap();
        let path = temp.path().join("pcache_info");
        let info = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .filter(|line| !line.starts_with("formatVersion="))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, info).unwrap();

        assert!(try_load(&config).is_some());
    }

    #[test]
    fn unknown_persistent_format_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());

        save(&config, &PersistentCacheState::default()).unwrap();
        let path = temp.path().join("pcache_info");
        let info = fs::read_to_string(&path)
            .unwrap()
            .replace("formatVersion=1", "formatVersion=2");
        fs::write(path, info).unwrap();

        assert!(try_load(&config).is_none());
    }

    #[test]
    fn java_importer_output_loads_through_client_persistence_path() {
        use hath_rs_pcache_migrate::protocol::{LRU_CACHE_SIZE, LegacyCacheState};
        use std::io::Cursor;

        let temp = tempfile::tempdir().unwrap();
        let config = config_for_data_dir(temp.path());
        let mut lru_cache_table = vec![0u16; LRU_CACHE_SIZE];
        lru_cache_table[123] = 0x8000;
        lru_cache_table[456] = 0xffff;
        let source = LegacyCacheState {
            cache_count: 7,
            cache_size: 99,
            lru_clear_pointer: 17,
            static_range_ages: HashMap::from([("a3f0".to_string(), 1_700_000_000_000)]),
            lru_cache_table,
        };

        hath_rs_pcache_migrate::import_from_reader(
            Cursor::new(hath_rs_pcache_migrate::test_support::encode_hpcache_v1(
                &source,
            )),
            temp.path(),
            false,
        )
        .unwrap();

        let loaded = try_load(&config).unwrap();
        assert_eq!(loaded.cache_count, 7);
        assert_eq!(loaded.cache_size, 99);
        assert_eq!(loaded.lru_clear_pointer, 17);
        assert_eq!(
            loaded.static_range_ages.get("a3f0"),
            Some(&1_700_000_000_000)
        );
        assert_eq!(loaded.lru_cache_table[123], 0x8000);
        assert_eq!(loaded.lru_cache_table[456], 0xffff);
    }
}
