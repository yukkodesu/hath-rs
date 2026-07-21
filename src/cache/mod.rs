mod blacklist;
mod lru;
mod persistent;
mod prune;
mod pruner;
mod startup;

pub(crate) use self::blacklist::{
    fetch_initial as fetch_initial_blacklist, spawn_fetcher as spawn_blacklist_fetcher,
};
use self::lru::{LRU_CACHE_SIZE, LruState};
use self::prune::{
    ONE_DAY, ONE_MONTH, SEVEN_DAYS, SIX_MONTHS, THIRTY_DAYS, THREE_DAYS, THREE_MONTHS,
    prune_frequency,
};
use self::prune::{PruneAction, PrunePlan, PruneResult};
use crate::cache::persistent::PersistentCacheState;
use crate::cache::pruner::CachePruner;
use crate::cache::startup::StartupCacheState;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::stats::Stats;
use crate::utils;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::task::JoinHandle;

/// Cache metadata and operations.
///
/// Locking strategy (matches Java's thread-ownership model):
/// - [`LruState`] → own `std::sync::Mutex` (frequent HTTP-request access)
/// - `cache_count` / `cache_size` → atomics (read by many, written by pruner + init)
/// - `static_range_oldest` → `std::sync::Mutex` (pruner-only, no contention)
/// - `last_file_verification_cooldown` → `std::sync::Mutex` (rarely accessed)
///
/// All methods take `&self` — no outer `Mutex<CacheHandler>` needed.
#[derive(Debug)]
pub struct CacheHandler {
    config: Arc<ArcSwap<Config>>,
    stats: Arc<Stats>,
    lru: std::sync::Mutex<LruState>,
    /// Serializes count/size mutations so the pair cannot underflow or be
    /// persisted as a partially applied file operation.
    accounting: std::sync::Mutex<()>,
    cache_count: AtomicU32,
    cache_size: AtomicU64,
    static_range_oldest: std::sync::Mutex<HashMap<String, u64>>,
    cache_loaded: bool,
    /// Cleared when accounting detects an invariant violation. A later clean
    /// restart must rescan rather than trust the resulting counters.
    persistent_state_valid: AtomicBool,
    last_file_verification_cooldown: std::sync::Mutex<std::time::Instant>,
}

impl CacheHandler {
    pub fn new(
        config: Arc<ArcSwap<Config>>,
        stats: Arc<Stats>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<Self> {
        let cfg = config.load();

        // Java: dieWithError if cache root directory exists but is unreadable
        if cfg.cache_dir.exists() && std::fs::read_dir(&cfg.cache_dir).is_err() {
            tracing::error!(
                "CacheHandler: Unable to access {}; check permissions and I/O errors.",
                cfg.cache_dir.display()
            );
            shutdown.cancel();
            return Err(HathError::Fatal("cannot read cache directory".into()));
        }

        // Clean up orphaned temp files (matching Java)
        for entry in utils::list_sorted_files(&cfg.temp_dir) {
            if entry.is_file() {
                let name = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !name.starts_with("log_")
                    && !name.starts_with("pcache_")
                    && name != "client_login"
                {
                    utils::remove_file(&entry);
                }
            }
        }

        // Try persistent load first
        let persistent_state = if !cfg.rescan_cache {
            persistent::try_load(&cfg)
        } else {
            None
        };

        // Java: deletePersistentData() after the load decision. Any state we
        // retained above is now in-memory only until a clean shutdown writes a
        // new snapshot.
        persistent::clear(&cfg)?;

        let cache_loaded;
        let startup_state = if let Some(state) = persistent_state {
            tracing::info!("Successfully loaded persistent cache data");
            let mut lru = LruState::new();
            let len = state.lru_cache_table.len().min(LRU_CACHE_SIZE);
            lru.lru_cache_table[..len].copy_from_slice(&state.lru_cache_table[..len]);
            lru.lru_clear_pointer = state.lru_clear_pointer;
            cache_loaded = true;
            StartupCacheState {
                lru,
                cache_count: state.cache_count,
                cache_size: state.cache_size,
                static_range_oldest: state.static_range_ages,
            }
        } else {
            startup::cleanup(&cfg)?;
            let state = startup::full_rescan(&cfg, cfg.verify_cache)?;
            cache_loaded = true;
            state
        };

        stats.set_cache_count(startup_state.cache_count);
        stats.set_cache_size(Self::cache_size_with_overhead(
            startup_state.cache_size,
            startup_state.cache_count,
            &cfg,
        ));

        // Java: startup safety checks (CacheHandler constructor lines 111-127)
        // Java: Settings.getStaticRangeCount() — server-assigned ranges, not cached ranges
        // Java: Settings.getStaticRangeCount() — server-assigned count
        let static_range_count = cfg.static_range_count;
        if !cfg.skip_free_space_check
            && let Ok(free) = fs2::free_space(&cfg.cache_dir)
        {
            let needed = cfg
                .disklimit_bytes
                .saturating_sub(Self::cache_size_with_overhead(
                    startup_state.cache_size,
                    startup_state.cache_count,
                    &cfg,
                ));
            if free < needed {
                tracing::error!(
                    "The storage device does not have enough space available to \
                     hold the set cache size. Free up space for H@H, or reduce \
                     the cache size from the H@H settings page."
                );
                shutdown.cancel();
                return Err(HathError::Fatal("insufficient disk space for cache".into()));
            }
        }
        if startup_state.cache_count < 1 && static_range_count > 20 {
            tracing::error!(
                "This client has static ranges assigned to it, but the cache is empty. \
                 Check file permissions and file system integrity. If the cache has been \
                 deleted or is otherwise lost, you have to manually reset your static \
                 ranges from the H@H settings page."
            );
            shutdown.cancel();
            return Err(HathError::Fatal(
                "empty cache with static ranges assigned".into(),
            ));
        }

        Ok(Self {
            config,
            stats,
            lru: std::sync::Mutex::new(startup_state.lru),
            accounting: std::sync::Mutex::new(()),
            cache_count: AtomicU32::new(startup_state.cache_count),
            cache_size: AtomicU64::new(startup_state.cache_size),
            static_range_oldest: std::sync::Mutex::new(startup_state.static_range_oldest),
            cache_loaded,
            persistent_state_valid: AtomicBool::new(true),
            last_file_verification_cooldown: std::sync::Mutex::new(std::time::Instant::now()),
        })
    }

    /// Save cache state to persistent files for fast restart.
    /// Java: `CacheHandler.savePersistentData()`
    ///
    /// Writes three files:
    /// - `pcache_ages`: bincode-serialized static range oldest timestamps
    /// - `pcache_lru`: bincode-serialized LRU cache table
    /// - `pcache_info`: plain-text metadata with SHA-1 hashes of the two files
    pub fn save_persistent_data(&self) {
        if !self.cache_loaded {
            return;
        }

        let cfg = self.config.load();

        let result: std::result::Result<(), String> = (|| {
            if !self.persistent_state_valid.load(Ordering::Acquire) {
                return Err(
                    "cache accounting state is invalid; forcing rescan on next startup".to_string(),
                );
            }
            let static_range_ages = {
                let ages = self.static_range_oldest.lock().unwrap();
                ages.clone()
            };
            let (lru_cache_table, lru_clear_pointer) = {
                let lru = self.lru.lock().unwrap();
                (lru.lru_cache_table.to_vec(), lru.lru_clear_pointer)
            };
            let state = {
                let _accounting = self.accounting.lock().unwrap();
                PersistentCacheState {
                    cache_count: self.cache_count.load(Ordering::Relaxed),
                    cache_size: self.cache_size.load(Ordering::Relaxed),
                    lru_clear_pointer,
                    static_range_ages,
                    lru_cache_table,
                }
            };
            persistent::save(&cfg, &state).map_err(|e| e.to_string())?;

            Ok(())
        })();

        if let Err(e) = result {
            tracing::warn!("Failed to save persistent cache data: {}", e);
            self.discard_persistent_data();
        }
    }

    /// Ensure the next startup rebuilds cache state from the filesystem.
    pub fn discard_persistent_data(&self) {
        self.persistent_state_valid.store(false, Ordering::Release);
        let cfg = self.config.load();
        if let Err(e) = persistent::clear(&cfg) {
            tracing::warn!("Failed to discard persistent cache data: {}", e);
        }
    }

    pub fn cache_size_with_overhead(actual: u64, count: u32, config: &Config) -> u64 {
        actual + count as u64 * config.filesystem_blocksize / 2
    }

    pub fn get_cache_size_with_overhead(&self) -> u64 {
        let cfg = self.config.load();
        Self::cache_size_with_overhead(
            self.cache_size.load(Ordering::Relaxed),
            self.cache_count.load(Ordering::Relaxed),
            &cfg,
        )
    }

    pub fn is_file_verification_on_cooldown(&self) -> bool {
        let mut guard = self.last_file_verification_cooldown.lock().unwrap();
        let elapsed = guard.elapsed();
        if elapsed.as_millis() < 2000 {
            return true;
        }
        *guard = std::time::Instant::now();
        false
    }

    pub fn delete_file_from_cache(&self, fileid: &str) -> Result<()> {
        if let Some(hv) = HVFile::from_fileid(fileid) {
            let cfg = self.config.load();
            let path = hv.cache_path(&cfg.cache_dir);
            if path.exists() {
                fs::remove_file(&path)?;
                self.account_file_removed(&hv);
            }
        }
        Ok(())
    }

    /// Java: CacheHandler.importFileToCache() — add a verified file to active cache.
    /// Increments cacheCount, cacheSize, updates LRU and staticRangeOldest.
    /// Java: `CacheHandler.markRecentlyAccessed(hvFile, skipMetaUpdate)`
    ///
    /// Marks the LRU bit for the file. Returns `true` if the bit was NOT
    /// previously set (i.e., the file hasn't been accessed recently).
    /// If `skip_meta_update` is false and the file's last-modified time
    /// is older than 7 days, updates it to now.
    pub fn mark_recently_accessed(&self, hv: &HVFile, skip_meta_update: bool) -> bool {
        let fileid_str = hv.fileid();
        let mut lru = self.lru.lock().unwrap();
        let mark_file = lru.mark_recently_accessed(fileid_str.as_str());
        drop(lru);
        if mark_file && !skip_meta_update {
            let cache_path = hv.cache_path(&self.config.load().cache_dir);
            let week = std::time::Duration::from_secs(7 * 24 * 3600);
            if let Ok(meta) = std::fs::metadata(&cache_path)
                && let Ok(mtime) = meta.modified()
                && mtime < std::time::SystemTime::now() - week
                && let Ok(file) = std::fs::File::open(&cache_path)
            {
                let _ = file.set_modified(std::time::SystemTime::now());
            }
        }
        mark_file
    }

    pub fn register_proxy_file(&self, hv_file: &HVFile) {
        self.account_file_added(hv_file);

        // markRecentlyAccessed with skipMetaUpdate=true
        if let Ok(mut lru) = self.lru.try_lock() {
            lru.mark_recently_accessed(hv_file.fileid().as_str());
        }

        // Create staticRangeOldest entry if missing
        let static_range = hv_file.static_range();
        let mut range_ages = self.static_range_oldest.lock().unwrap();
        if !range_ages.contains_key(&static_range) {
            tracing::debug!(
                "CacheHandler: Created staticRangeOldest entry for {}",
                static_range
            );
            range_ages.insert(static_range.to_string(), utils::millis_now());
        }
    }

    /// Record a cache file removed by the pruner after its filesystem unlink
    /// has succeeded. This is deliberately separate from `delete_file...`,
    /// whose path is reconstructed from a file id.
    pub(super) fn record_pruned_file(&self, hv_file: &HVFile) {
        self.account_file_removed(hv_file);
    }

    fn account_file_added(&self, hv_file: &HVFile) {
        let _accounting = self.accounting.lock().unwrap();
        let count = self.cache_count.load(Ordering::Relaxed);
        let size = self.cache_size.load(Ordering::Relaxed);
        let Some(next_count) = count.checked_add(1) else {
            self.invalidate_accounting("cache count overflow while registering a file");
            return;
        };
        let Some(next_size) = size.checked_add(hv_file.size as u64) else {
            self.invalidate_accounting("cache size overflow while registering a file");
            return;
        };
        self.cache_count.store(next_count, Ordering::Relaxed);
        self.cache_size.store(next_size, Ordering::Relaxed);
        self.update_cache_stats(next_count, next_size);
    }

    fn account_file_removed(&self, hv_file: &HVFile) {
        let _accounting = self.accounting.lock().unwrap();
        let count = self.cache_count.load(Ordering::Relaxed);
        let size = self.cache_size.load(Ordering::Relaxed);
        let Some(next_count) = count.checked_sub(1) else {
            self.invalidate_accounting("cache count underflow while removing a file");
            return;
        };
        let Some(next_size) = size.checked_sub(hv_file.size as u64) else {
            self.invalidate_accounting("cache size underflow while removing a file");
            return;
        };
        self.cache_count.store(next_count, Ordering::Relaxed);
        self.cache_size.store(next_size, Ordering::Relaxed);
        self.update_cache_stats(next_count, next_size);
    }

    fn update_cache_stats(&self, count: u32, size: u64) {
        let cfg = self.config.load();
        self.stats.set_cache_count(count);
        self.stats
            .set_cache_size(Self::cache_size_with_overhead(size, count, &cfg));
    }

    fn invalidate_accounting(&self, message: &str) {
        self.persistent_state_valid.store(false, Ordering::Release);
        tracing::error!(
            "CacheHandler: {}; forcing a cache rescan on next startup",
            message
        );
    }

    /// Java: checkAndPruneCache() — phase 1 (lock held, read-only).
    ///
    /// Checks if the cache is over the disk limit and returns a [`PruneAction`].
    /// The actual file deletion (I/O with sleeps) happens outside the lock.
    fn check_prune_action(&self, config: &Config) -> PruneAction {
        let want_free = 104_857_600u64;
        let cache_limit = config.disklimit_bytes;
        let count = self.cache_count.load(Ordering::Relaxed);
        let raw_size = self.cache_size.load(Ordering::Relaxed);
        let cache_size_with_overhead = self.get_cache_size_with_overhead();

        tracing::debug!(
            "CacheHandler: cacheSize={}, cacheSizeWithOverhead={}, cacheLimit={}, cacheFree={}",
            raw_size,
            cache_size_with_overhead,
            cache_limit,
            cache_limit.saturating_sub(cache_size_with_overhead)
        );

        let range_ages = self.static_range_oldest.lock().unwrap();
        // Java: prune when over limit OR within 100 MB of limit
        let mut bytes_to_free: u64 = 0;
        let mut fast_delete = false;

        if cache_size_with_overhead > cache_limit {
            // the cache (with overhead) is larger than the limit
            bytes_to_free = want_free + cache_size_with_overhead - cache_limit;
            fast_delete = true;
        } else if count > 0
            && !range_ages.is_empty()
            && cache_limit.saturating_sub(cache_size_with_overhead) < want_free
        {
            // there is less than 100 MiB available cache space
            bytes_to_free = want_free - (cache_limit - cache_size_with_overhead);
        }

        // Java only prunes while the server currently assigns static ranges.
        // `static_range_oldest` can contain persisted ranges from an older
        // assignment and must not independently authorize deletion.
        if bytes_to_free == 0
            || count == 0
            || config.static_range_count == 0
            || range_ages.is_empty()
        {
            return PruneAction::NoPrune {
                frequency: prune_frequency(cache_limit, cache_size_with_overhead, want_free),
            };
        }

        // Find the oldest static range and its age in one HashMap traversal.
        let (prune_static_range, oldest_range_age) = match range_ages
            .iter()
            .min_by_key(|(_, age)| **age)
        {
            Some((r, age)) => (r.clone(), *age),
            None => {
                return PruneAction::NoPrune {
                    frequency: prune_frequency(cache_limit, cache_size_with_overhead, want_free),
                };
            }
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
    /// Applies range-age metadata from a fully completed prune pass. Successful
    /// file deletions have already updated counters at unlink time.
    fn apply_prune_result(&self, result: PruneResult) {
        {
            let mut range_ages = self.static_range_oldest.lock().unwrap();
            if result.file_count > 0 {
                range_ages.insert(result.static_range.clone(), result.oldest_last_modified);
                tracing::debug!(
                    "CacheHandler: Updated age cache for range {}, oldest={}",
                    result.static_range,
                    result.oldest_last_modified
                );
            } else {
                let _ = fs::remove_dir(&result.range_dir);
                range_ages.remove(&result.static_range);
                tracing::debug!(
                    "CacheHandler: Removed empty static range dir {}",
                    result.static_range
                );
            }
        } // release range_ages lock
    }
}

/// Spawn the cache pruner background task.
pub fn spawn_pruner(
    cache: Arc<CacheHandler>,
    config: Arc<ArcSwap<Config>>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    let pruner = CachePruner::new(cache, config, shutdown);
    tokio::spawn(async move { pruner.run().await })
}

/// Spawn periodic LRU cycle + stats shift (10s interval).
pub fn spawn_periodic_stats(
    cache: Arc<CacheHandler>,
    stats: Arc<Stats>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(10),
        move || {
            let cache = cache.clone();
            let stats = stats.clone();
            async move {
                if let Ok(mut lru) = cache.lru.try_lock() {
                    lru.cycle();
                }
                stats.shift_bytes_sent_history();
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FixtureDirs;

    #[test]
    fn startup_consumes_persistent_files_after_loading_them() {
        let fixture = FixtureDirs::new();
        let config = fixture.config();
        let state = PersistentCacheState {
            cache_count: 7,
            cache_size: 99,
            lru_clear_pointer: 0,
            static_range_ages: HashMap::new(),
            lru_cache_table: vec![0; LRU_CACHE_SIZE],
        };
        persistent::save(&config, &state).unwrap();

        let config = Arc::new(ArcSwap::from_pointee(config));
        let stats = Arc::new(Stats::new());
        let cache = CacheHandler::new(
            config.clone(),
            stats,
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();

        assert_eq!(cache.cache_count.load(Ordering::Relaxed), state.cache_count);
        assert_eq!(cache.cache_size.load(Ordering::Relaxed), state.cache_size);
        for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
            assert!(
                !config.load().data_dir.join(name).exists(),
                "{name} should only exist after a clean shutdown save"
            );
        }
    }

    #[test]
    fn forced_rescan_also_discards_persistent_files() {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        let state = PersistentCacheState::default();
        persistent::save(&config, &state).unwrap();
        config.rescan_cache = true;

        let config = Arc::new(ArcSwap::from_pointee(config));
        CacheHandler::new(
            config.clone(),
            Arc::new(Stats::new()),
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();

        for name in ["pcache_info", "pcache_ages", "pcache_lru"] {
            assert!(
                !config.load().data_dir.join(name).exists(),
                "{name} should be discarded before a forced rescan"
            );
        }
    }

    #[test]
    fn deleting_a_file_updates_accounting_once_after_successful_unlink() {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        config.static_ranges.insert("aabb".to_string(), 0);
        let hv = HVFile::from_fileid("aabbccddeeff00112233445566778899aabbccdd-4-jpg").unwrap();
        let path = hv.cache_path(&config.cache_dir);
        utils::ensure_dir(path.parent().unwrap()).unwrap();
        fs::write(&path, b"data").unwrap();

        let config = Arc::new(ArcSwap::from_pointee(config));
        let stats = Arc::new(Stats::new());
        let cache =
            CacheHandler::new(config, stats, tokio_util::sync::CancellationToken::new()).unwrap();

        cache.delete_file_from_cache(hv.fileid().as_str()).unwrap();
        cache.delete_file_from_cache(hv.fileid().as_str()).unwrap();

        assert_eq!(cache.cache_count.load(Ordering::Relaxed), 0);
        assert_eq!(cache.cache_size.load(Ordering::Relaxed), 0);
    }

    fn cache_with_prunable_range(static_range_count: u32) -> (Arc<ArcSwap<Config>>, CacheHandler) {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        config.disklimit_bytes = 1;
        config.static_range_count = static_range_count;
        let config = Arc::new(ArcSwap::from_pointee(config));
        let cache = CacheHandler::new(
            config.clone(),
            Arc::new(Stats::new()),
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
        cache.cache_count.store(1, Ordering::Relaxed);
        cache.cache_size.store(4, Ordering::Relaxed);
        cache
            .static_range_oldest
            .lock()
            .unwrap()
            .insert("aabb".to_string(), 0);
        (config, cache)
    }

    #[test]
    fn prune_does_not_use_historical_range_state_without_current_assignment() {
        let (config, cache) = cache_with_prunable_range(0);

        assert!(matches!(
            cache.check_prune_action(&config.load()),
            PruneAction::NoPrune { .. }
        ));
    }

    #[test]
    fn prune_uses_historical_range_state_when_current_assignment_exists() {
        let (config, cache) = cache_with_prunable_range(1);

        match cache.check_prune_action(&config.load()) {
            PruneAction::Prune(plan) => assert_eq!(plan.static_range, "aabb"),
            PruneAction::NoPrune { .. } => panic!("assigned static range should be prunable"),
        }
    }
}
