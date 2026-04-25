pub mod persistent;
pub mod pruner;

use crate::config::Config;
use crate::error::Result;
use crate::hvfile::HVFile;
use crate::stats::Stats;
use crate::utils;
use crate::cache::persistent::PersistentCacheState;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Time constants for pruning age cutoffs.
const SIX_MONTHS: Duration = Duration::from_secs(15_552_000);
const THREE_MONTHS: Duration = Duration::from_secs(7_776_000);
const ONE_MONTH: Duration = Duration::from_secs(2_592_000);
const THIRTY_DAYS: Duration = Duration::from_secs(2_592_000);
const SEVEN_DAYS: Duration = Duration::from_secs(604_800);
const THREE_DAYS: Duration = Duration::from_secs(259_200);
const ONE_DAY: Duration = Duration::from_secs(86_400);

/// Information needed to execute a prune pass without holding the cache lock.
pub struct PrunePlan {
    pub static_range: String,
    pub range_dir: PathBuf,
    /// Files with `last_modified < cutoff` should be deleted.
    pub cutoff: u64,
    pub fast_delete: bool,
}

/// Result of executing a prune pass.
pub struct PruneResult {
    pub static_range: String,
    pub range_dir: PathBuf,
    /// Number of files remaining in the directory after pruning.
    pub file_count: usize,
    /// Oldest last-modified timestamp among remaining files.
    pub oldest_last_modified: u64,
    pub files_deleted: usize,
    pub bytes_deleted: u64,
}

/// The recommended action after checking cache state.
pub enum PruneAction {
    /// Cache is over limit — prune this range.
    Prune(PrunePlan),
    /// Cache is within limit — adjust check frequency.
    NoPrune { frequency: u32 },
}

/// Compute the recommended pruner check frequency based on free space.
fn prune_frequency(cache_limit: u64, cache_size_with_overhead: u64, want_free: u64) -> u32 {
    let free = cache_limit.saturating_sub(cache_size_with_overhead);
    if free > want_free * 10 {
        600
    } else if free > want_free {
        60
    } else {
        10
    }
}

pub const LRU_CACHE_SIZE: usize = 1_048_576;

/// (lru_array, total_files, unique_files, total_size, res_counts)
type RescanResult = (Box<[u16; LRU_CACHE_SIZE]>, usize, u32, u64, HashMap<String, u64>);

#[derive(Debug)]
pub struct CacheHandler {
    pub config: Arc<ArcSwap<Config>>,
    pub stats: Arc<Stats>,
    pub lru_cache_table: Box<[u16; LRU_CACHE_SIZE]>,
    pub lru_clear_pointer: usize,
    pub cache_count: u32,
    pub cache_size: u64,
    pub static_range_oldest: HashMap<String, u64>,
    pub cache_loaded: bool,
    last_file_verification_cooldown: std::time::Instant,
}

impl CacheHandler {
    pub fn new(config: Arc<ArcSwap<Config>>, stats: Arc<Stats>) -> Result<Self> {
        let cfg = config.load();

        // Clean up orphaned temp files (matching Java)
        for entry in utils::list_sorted_files(&cfg.temp_dir) {
            if entry.is_file() {
                let name = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !name.starts_with("log_") && !name.starts_with("pcache_") && name != "client_login" {
                    utils::remove_file(&entry);
                }
            }
        }

        // Try persistent load first
        let mut cache_loaded = false;
        let (lru_cache_table, lru_clear_pointer, cache_count, cache_size, static_range_oldest) =
            if !cfg.rescan_cache {
                if let Some(state) = Self::try_load_persistent(&cfg) {
                    tracing::info!("Successfully loaded persistent cache data");
                    cache_loaded = true;
                    let mut arr = Box::new([0u16; LRU_CACHE_SIZE]);
                    let len = state.lru_cache_table.len().min(LRU_CACHE_SIZE);
                    arr[..len].copy_from_slice(&state.lru_cache_table[..len]);
                    (arr, state.lru_clear_pointer, state.cache_count, state.cache_size, state.static_range_ages)
                } else {
                    Self::startup_cache_cleanup(&cfg)?;
                    Self::full_rescan(&cfg, &stats)?
                }
            } else {
                Self::startup_cache_cleanup(&cfg)?;
                Self::full_rescan(&cfg, &stats)?
            };

        Self::delete_persistent_data(&cfg);

        stats.set_cache_count(cache_count);
        stats.set_cache_size(Self::cache_size_with_overhead(cache_size, cache_count, &cfg));

        Ok(Self {
            config,
            stats,
            lru_cache_table,
            lru_clear_pointer,
            cache_count,
            cache_size,
            static_range_oldest,
            cache_loaded,
            last_file_verification_cooldown: std::time::Instant::now(),
        })
    }

    /// Java: startupCacheCleanup — move orphan L1 files to correct L2 dirs
    fn startup_cache_cleanup(config: &Config) -> Result<()> {
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
                if entry.is_dir() { continue; }

                let filename = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let hv = match HVFile::from_fileid(filename) {
                    Some(h) => h,
                    None => { utils::remove_file(entry); continue; }
                };

                if !config.is_static_range(&hv.static_range()) {
                    utils::remove_file(entry);
                    continue;
                }

                // Move file to correct L2 dir
                let target = hv.cache_path(&config.cache_dir);
                if let Err(e) = utils::ensure_dir(target.parent().unwrap()) {
                    tracing::warn!("Cannot create cache dir: {}", e);
                    continue;
                }
                if let Err(e) = fs::rename(entry, &target) {
                    tracing::warn!("Failed to move {} to {}: {}", entry.display(), target.display(), e);
                }
            }
        }

        tracing::info!("Cache cleanup complete");
        Ok(())
    }

    fn try_load_persistent(_config: &Config) -> Option<PersistentCacheState> {
        // Stub: Phase 1 always does rescan (avoids Java serialization compat issue)
        // Full impl would read pcache_info, verify SHA-1 of pcache_lru/pcache_ages
        None
    }

    fn delete_persistent_data(config: &Config) {
        for name in &["pcache_info", "pcache_lru", "pcache_ages"] {
            utils::remove_file(&config.data_dir.join(name));
        }
    }

    /// Full rescan: iterates all cache directories, validates files, builds LRU state.
    fn full_rescan(config: &Config, stats: &Stats) -> Result<RescanResult> {
        tracing::info!("Loading cache...");
        let lru = Box::new([0u16; LRU_CACHE_SIZE]);
        let mut count = 0u32;
        let mut size = 0u64;
        let mut range_ages = HashMap::new();

        for l1_dir in &utils::list_sorted_files(&config.cache_dir) {
            if !l1_dir.is_dir() { continue; }
            let l1_name = l1_dir.file_name().and_then(|n| n.to_str()).unwrap_or("");

            for l2_dir in &utils::list_sorted_files(l1_dir) {
                if !l2_dir.is_dir() { continue; }
                let l2_name = l2_dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let static_range = format!("{}{}", l1_name, l2_name);

                let files = utils::list_sorted_files(l2_dir);
                if files.is_empty() { utils::remove_dir(l2_dir); continue; }

                let mut oldest_modified = u64::MAX;

                for file in &files {
                    if !file.is_file() { continue; }

                    let filename = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    let hv = match HVFile::from_fileid(filename) {
                        Some(h) => h,
                        None => { utils::remove_file(file); continue; }
                    };

                    // Size validation
                    if file.metadata().map(|m| m.len()).unwrap_or(0) != hv.size as u64 {
                        utils::remove_file(file);
                        continue;
                    }

                    // Static range check
                    if !config.is_static_range(&hv.static_range()) {
                        utils::remove_file(file);
                        continue;
                    }

                    count += 1;
                    size += hv.size as u64;

                    let modified = utils::modified_millis(file);
                    oldest_modified = oldest_modified.min(modified);

                    if count.is_multiple_of(10000) {
                        tracing::info!("Loaded {} files so far...", count);
                    }
                }

                range_ages.insert(static_range, oldest_modified);
            }
        }

        stats.set_cache_count(count);
        stats.set_cache_size(Self::cache_size_with_overhead(size, count, config));

        tracing::info!("Cache init complete: {} files, {} apparent bytes, {} estimated on disk",
            count, size, Self::cache_size_with_overhead(size, count, config));

        Ok((lru, 0, count, size, range_ages))
    }

    pub fn cache_size_with_overhead(actual: u64, count: u32, config: &Config) -> u64 {
        actual + count as u64 * config.filesystem_blocksize / 2
    }

    pub fn get_cache_size_with_overhead(&self) -> u64 {
        let cfg = self.config.load();
        Self::cache_size_with_overhead(self.cache_size, self.cache_count, &cfg)
    }

    pub fn is_file_verification_on_cooldown(&mut self) -> bool {
        let elapsed = self.last_file_verification_cooldown.elapsed();
        if elapsed.as_millis() < 2000 {
            return true;
        }
        self.last_file_verification_cooldown = std::time::Instant::now();
        false
    }

    pub fn mark_recently_accessed(&mut self, fileid: &str, skip_meta_update: bool) -> bool {
        if fileid.len() < 10 { return false; }
        let array_index = usize::from_str_radix(&fileid[4..9], 16).unwrap_or(0);
        let bit_mask: u16 = 1u16 << u16::from_str_radix(&fileid[9..10], 16).unwrap_or(0);
        if (self.lru_cache_table[array_index] & bit_mask) != 0 { return false; }
        self.lru_cache_table[array_index] |= bit_mask;
        !skip_meta_update
    }

    pub fn cycle_lru_cache_table(&mut self) {
        let clear_until = (self.lru_clear_pointer + 17).min(LRU_CACHE_SIZE);
        self.lru_cache_table[self.lru_clear_pointer..clear_until].fill(0);
        self.lru_clear_pointer = if clear_until >= LRU_CACHE_SIZE { 0 } else { clear_until };
    }

    pub fn delete_file_from_cache(&mut self, fileid: &str) -> Result<()> {
        if let Some(hv) = HVFile::from_fileid(fileid) {
            let cfg = self.config.load();
            let path = hv.cache_path(&cfg.cache_dir);
            if path.exists() {
                fs::remove_file(&path)?;
                self.cache_count = self.cache_count.saturating_sub(1);
                self.cache_size = self.cache_size.saturating_sub(hv.size as u64);
                self.stats.set_cache_count(self.cache_count);
                self.stats.set_cache_size(self.get_cache_size_with_overhead());
            }
        }
        Ok(())
    }

    /// Java: checkAndPruneCache() — phase 1 (lock held, read-only).
    ///
    /// Checks if the cache is over the disk limit and returns a [`PruneAction`].
    /// The actual file deletion (I/O with sleeps) happens outside the lock.
    pub fn check_prune_action(&self, config: &Config) -> PruneAction {
        let want_free = 104_857_600u64;
        let cache_limit = config.disklimit_bytes;
        let cache_size_with_overhead = self.get_cache_size_with_overhead();

        tracing::debug!(
            "CacheHandler: cacheSize={}, cacheSizeWithOverhead={}, cacheLimit={}, cacheFree={}",
            self.cache_size,
            cache_size_with_overhead,
            cache_limit,
            cache_limit.saturating_sub(cache_size_with_overhead)
        );

        if cache_size_with_overhead <= cache_limit
            || self.cache_count == 0
            || self.static_range_oldest.is_empty()
        {
            return PruneAction::NoPrune {
                frequency: prune_frequency(cache_limit, cache_size_with_overhead, want_free),
            };
        }

        let bytes_to_free = cache_size_with_overhead - cache_limit + 100_000_000;
        let fast_delete = bytes_to_free > config.disklimit_bytes / 4;

        // Find the oldest static range and its age in one HashMap traversal.
        let (prune_static_range, oldest_range_age) = match self
            .static_range_oldest
            .iter()
            .min_by_key(|(_, age)| **age)
        {
            Some((r, age)) => (r.clone(), *age),
            None => return PruneAction::NoPrune {
                frequency: prune_frequency(cache_limit, cache_size_with_overhead, want_free),
            },
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Determine the last-modified cutoff based on file age.
        let cutoff = if oldest_range_age < now.saturating_sub(SIX_MONTHS.as_millis() as u64) {
            oldest_range_age + THIRTY_DAYS.as_millis() as u64
        } else if oldest_range_age < now.saturating_sub(THREE_MONTHS.as_millis() as u64) {
            oldest_range_age + SEVEN_DAYS.as_millis() as u64
        } else if oldest_range_age < now.saturating_sub(ONE_MONTH.as_millis() as u64) {
            oldest_range_age + THREE_DAYS.as_millis() as u64
        } else {
            oldest_range_age + ONE_DAY.as_millis() as u64
        };

        let range_dir = config
            .cache_dir
            .join(&prune_static_range[0..2])
            .join(&prune_static_range[2..4]);

        tracing::debug!(
            "CacheHandler: Trying to free {} bytes from range {}",
            bytes_to_free,
            prune_static_range
        );

        PruneAction::Prune(PrunePlan {
            static_range: prune_static_range,
            range_dir,
            cutoff,
            fast_delete,
        })
    }

    /// Java: checkAndPruneCache() — phase 3 (lock held, write).
    ///
    /// Applies the results of a prune pass that was executed outside the lock.
    /// Updates [`static_range_oldest`], cache counters, and stats.
    pub fn apply_prune_result(&mut self, result: PruneResult) {
        if result.file_count > 0 {
            self.static_range_oldest
                .insert(result.static_range.clone(), result.oldest_last_modified);
            tracing::debug!(
                "CacheHandler: Updated age cache for range {}, oldest={}",
                result.static_range,
                result.oldest_last_modified
            );
        } else {
            let _ = fs::remove_dir(&result.range_dir);
            self.static_range_oldest.remove(&result.static_range);
            tracing::debug!(
                "CacheHandler: Removed empty static range dir {}",
                result.static_range
            );
        }

        self.cache_count = self.cache_count.saturating_sub(result.files_deleted as u32);
        self.cache_size = self.cache_size.saturating_sub(result.bytes_deleted);
        self.stats.set_cache_count(self.cache_count);
        self.stats.set_cache_size(self.get_cache_size_with_overhead());
    }

}
