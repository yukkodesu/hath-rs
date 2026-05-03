use crate::cache::lru::LruState;
use crate::config::Config;
use crate::error::Result;
use crate::hvfile::HVFile;
use crate::utils;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs;
use std::io::Read;

pub(crate) struct StartupCacheState {
    pub(crate) lru: LruState,
    pub(crate) cache_count: u32,
    pub(crate) cache_size: u64,
    pub(crate) static_range_oldest: HashMap<String, u64>,
}

/// Java: `CacheHandler.startupCacheCleanup()` — move orphan L1 files to
/// correct L2 dirs and remove invalid or unassigned files.
pub(crate) fn cleanup(config: &Config) -> Result<()> {
    tracing::info!("Cache cleanup pass...");
    let l1_dirs = utils::list_sorted_files(&config.cache_dir);

    for l1_dir in &l1_dirs {
        if !l1_dir.is_dir() {
            utils::remove_file(l1_dir);
            continue;
        }

        let l2_entries = utils::list_sorted_files(l1_dir);
        if l2_entries.is_empty() {
            utils::remove_dir(l1_dir);
            continue;
        }

        for entry in &l2_entries {
            if entry.is_dir() {
                continue;
            }

            let filename = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let hv = match HVFile::from_fileid(filename) {
                Some(h) => h,
                None => {
                    utils::remove_file(entry);
                    continue;
                }
            };

            if !config.is_static_range(&hv.static_range()) {
                utils::remove_file(entry);
                continue;
            }

            let target = hv.cache_path(&config.cache_dir);
            if let Err(e) = utils::ensure_dir(target.parent().unwrap()) {
                tracing::warn!("Cannot create cache dir: {}", e);
                continue;
            }
            if let Err(e) = fs::rename(entry, &target) {
                tracing::warn!(
                    "Failed to move {} to {}: {}",
                    entry.display(),
                    target.display(),
                    e
                );
            }
        }
    }

    tracing::info!("Cache cleanup complete");
    Ok(())
}

/// Java: `CacheHandler.startupInitCache()` — scan cache directories, validate
/// files, build static range age state, and initialize LRU for recent files.
pub(crate) fn full_rescan(config: &Config, verify_cache: bool) -> Result<StartupCacheState> {
    let mut hasher = Sha1::new();
    let mut read_buf = vec![0u8; 65536];
    let print_freq: u32 = if verify_cache { 1000 } else { 10000 };

    if verify_cache {
        tracing::info!(
            "CacheHandler: Loading cache with full file verification. Depending on the size of your cache, this can take a long time."
        );
    } else {
        tracing::info!("Loading cache...");
    }

    let mut cache_count = 0u32;
    let mut cache_size = 0u64;
    let mut static_range_oldest = HashMap::new();
    let mut lru = LruState::new();
    // Java: recentlyAccessedCutoff = System.currentTimeMillis() - 604800000
    let recently_accessed_cutoff = utils::millis_now().saturating_sub(604_800_000);

    for l1_dir in &utils::list_sorted_files(&config.cache_dir) {
        if !l1_dir.is_dir() {
            continue;
        }
        let l1_name = l1_dir.file_name().and_then(|n| n.to_str()).unwrap_or("");

        for l2_dir in &utils::list_sorted_files(l1_dir) {
            if !l2_dir.is_dir() {
                continue;
            }
            let l2_name = l2_dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let static_range = format!("{}{}", l1_name, l2_name);

            let files = utils::list_sorted_files(l2_dir);
            if files.is_empty() {
                utils::remove_dir(l2_dir);
                continue;
            }

            let mut oldest_modified = u64::MAX;
            let mut found_valid = false;

            for file in &files {
                if !file.is_file() {
                    continue;
                }

                let filename = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let hv = match HVFile::from_fileid(filename) {
                    Some(h) => h,
                    None => {
                        utils::remove_file(file);
                        continue;
                    }
                };

                // Size validation (Java: HVFile.getHVFileFromFile checks this first)
                if file.metadata().map(|m| m.len()).unwrap_or(0) != hv.size as u64 {
                    utils::remove_file(file);
                    continue;
                }

                // SHA-1 verification (Java: FileValidator.validateFile)
                if verify_cache {
                    let expected = hv.hash.as_str();
                    let mut sha1_ok = false;
                    hasher.reset();

                    if let Ok(mut f) = fs::File::open(file) {
                        let mut read_ok = true;
                        loop {
                            match f.read(&mut read_buf) {
                                Ok(0) => break,
                                Ok(n) => hasher.update(&read_buf[..n]),
                                Err(_) => {
                                    read_ok = false;
                                    break;
                                }
                            }
                        }
                        if read_ok {
                            let actual = utils::hex_encode(&hasher.finalize_reset());
                            sha1_ok = actual == expected;
                        } else {
                            hasher.reset();
                        }
                    }

                    if !sha1_ok {
                        utils::remove_file(file);
                        continue;
                    }
                }

                if !config.is_static_range(&hv.static_range()) {
                    utils::remove_file(file);
                    continue;
                }

                found_valid = true;
                cache_count += 1;
                cache_size += hv.size as u64;

                let modified = utils::modified_millis(file);
                oldest_modified = oldest_modified.min(modified);

                // Java: if fileLastModified > recentlyAccessedCutoff,
                // markRecentlyAccessed(hvFile, true) — sets LRU bit without
                // updating file mtime.
                if modified > recently_accessed_cutoff {
                    lru.mark_recently_accessed(hv.fileid().as_str());
                }

                if cache_count.is_multiple_of(print_freq) {
                    tracing::info!("CacheHandler: Loaded {} files so far...", cache_count);
                }
            }

            if found_valid {
                static_range_oldest.insert(static_range, oldest_modified);
            } else {
                utils::remove_dir(l2_dir);
            }
        }
    }

    tracing::info!(
        "Cache init complete: {} files, {} apparent bytes, {} estimated on disk",
        cache_count,
        cache_size,
        cache_size_with_overhead(cache_size, cache_count, config)
    );

    Ok(StartupCacheState {
        lru,
        cache_count,
        cache_size,
        static_range_oldest,
    })
}

fn cache_size_with_overhead(actual: u64, count: u32, config: &Config) -> u64 {
    actual + count as u64 * config.filesystem_blocksize / 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FixtureDirs;

    const VALID_FILEID: &str = "aabbccddeeff00112233445566778899aabbccdd-4-jpg";
    const WRONG_SIZE_FILEID: &str = "aabbccddeeff00112233445566778899aabbccde-5-jpg";
    const UNASSIGNED_FILEID: &str = "ccddeeff00112233445566778899aabbccddaabb-4-jpg";

    fn config_with_static_range() -> (FixtureDirs, Config) {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        config.static_ranges.insert("aabb".to_string(), 0);
        (fixture, config)
    }

    #[test]
    fn cleanup_moves_valid_l1_orphan_to_l2_cache_dir() {
        let (_fixture, config) = config_with_static_range();
        let orphan = config.cache_dir.join("aa").join(VALID_FILEID);
        utils::ensure_dir(orphan.parent().unwrap()).unwrap();
        fs::write(&orphan, b"data").unwrap();

        cleanup(&config).unwrap();

        let hv = HVFile::from_fileid(VALID_FILEID).unwrap();
        assert!(!orphan.exists());
        assert_eq!(fs::read(hv.cache_path(&config.cache_dir)).unwrap(), b"data");
    }

    #[test]
    fn full_rescan_counts_valid_static_range_files_and_removes_invalid_entries() {
        let (_fixture, config) = config_with_static_range();
        let valid = HVFile::from_fileid(VALID_FILEID).unwrap();
        let wrong_size = HVFile::from_fileid(WRONG_SIZE_FILEID).unwrap();
        let unassigned = HVFile::from_fileid(UNASSIGNED_FILEID).unwrap();

        utils::ensure_dir(valid.cache_path(&config.cache_dir).parent().unwrap()).unwrap();
        utils::ensure_dir(unassigned.cache_path(&config.cache_dir).parent().unwrap()).unwrap();
        fs::write(valid.cache_path(&config.cache_dir), b"data").unwrap();
        fs::write(wrong_size.cache_path(&config.cache_dir), b"data").unwrap();
        fs::write(unassigned.cache_path(&config.cache_dir), b"data").unwrap();
        fs::write(
            valid
                .cache_path(&config.cache_dir)
                .with_file_name("not-a-fileid"),
            b"junk",
        )
        .unwrap();

        let mut state = full_rescan(&config, false).unwrap();

        assert_eq!(state.cache_count, 1);
        assert_eq!(state.cache_size, 4);
        assert!(state.static_range_oldest.contains_key("aabb"));
        assert!(!state.lru.mark_recently_accessed(VALID_FILEID));
        assert!(valid.cache_path(&config.cache_dir).exists());
        assert!(!wrong_size.cache_path(&config.cache_dir).exists());
        assert!(!unassigned.cache_path(&config.cache_dir).exists());
    }
}
