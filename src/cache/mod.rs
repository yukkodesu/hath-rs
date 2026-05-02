pub mod persistent;
pub mod pruner;

use crate::cache::persistent::PersistentCacheState;
use crate::cache::pruner::CachePruner;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::rpc_client::RpcClient;
use crate::stats::Stats;
use crate::utils;
use arc_swap::ArcSwap;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Time constants for pruning age cutoffs.
const ONE_DAY: Duration = Duration::from_secs(86400);
const THREE_DAYS: Duration = Duration::from_secs(3 * 86400);
const SEVEN_DAYS: Duration = Duration::from_secs(7 * 86400);
const THIRTY_DAYS: Duration = Duration::from_secs(30 * 86400);
const ONE_MONTH: Duration = Duration::from_secs(30 * 86400);
const THREE_MONTHS: Duration = Duration::from_secs(90 * 86400);
const SIX_MONTHS: Duration = Duration::from_secs(180 * 86400);

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

/// Per-file LRU tracking. Protected by its own [`std::sync::Mutex`] because it is
/// accessed on every HTTP request (via [`LruState::mark_recently_accessed`]) and
/// periodically cycled (via [`LruState::cycle`]).
#[derive(Debug)]
pub struct LruState {
    pub lru_cache_table: Box<[u16; LRU_CACHE_SIZE]>,
    pub lru_clear_pointer: usize,
}

impl Default for LruState {
    fn default() -> Self {
        Self::new()
    }
}

impl LruState {
    pub fn new() -> Self {
        Self {
            lru_cache_table: Box::new([0u16; LRU_CACHE_SIZE]),
            lru_clear_pointer: 0,
        }
    }

    /// Low-level LRU bit check/set. Returns `true` if the file's LRU bit was
    /// NOT previously set (i.e., the file hasn't been accessed recently).
    pub fn mark_recently_accessed(&mut self, fileid: &str) -> bool {
        if fileid.len() < 10 {
            return false;
        }
        let array_index = usize::from_str_radix(&fileid[4..9], 16).unwrap_or(0);
        let bit_mask: u16 = 1u16 << u16::from_str_radix(&fileid[9..10], 16).unwrap_or(0);
        if (self.lru_cache_table[array_index] & bit_mask) != 0 {
            return false;
        }
        self.lru_cache_table[array_index] |= bit_mask;
        true
    }

    /// Java: `CacheHandler.cycleLRUCacheTable()`
    pub fn cycle(&mut self) {
        let clear_until = (self.lru_clear_pointer + 17).min(LRU_CACHE_SIZE);
        self.lru_cache_table[self.lru_clear_pointer..clear_until].fill(0);
        self.lru_clear_pointer = if clear_until >= LRU_CACHE_SIZE {
            0
        } else {
            clear_until
        };
    }
}

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
    pub config: Arc<ArcSwap<Config>>,
    pub stats: Arc<Stats>,
    pub lru: std::sync::Mutex<LruState>,
    pub cache_count: AtomicU32,
    pub cache_size: AtomicU64,
    pub static_range_oldest: std::sync::Mutex<HashMap<String, u64>>,
    pub cache_loaded: bool,
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
        let cache_loaded;
        let (lru, cache_count, cache_size, static_range_oldest) = if !cfg.rescan_cache {
            if let Some(state) = Self::try_load_persistent(&cfg) {
                tracing::info!("Successfully loaded persistent cache data");
                let mut lru = LruState::new();
                let len = state.lru_cache_table.len().min(LRU_CACHE_SIZE);
                lru.lru_cache_table[..len].copy_from_slice(&state.lru_cache_table[..len]);
                lru.lru_clear_pointer = state.lru_clear_pointer;
                cache_loaded = true;
                (
                    lru,
                    state.cache_count,
                    state.cache_size,
                    state.static_range_ages,
                )
            } else {
                Self::startup_cache_cleanup(&cfg)?;
                let (lru, count, size, ages) = Self::full_rescan(&cfg, &stats, cfg.verify_cache)?;
                cache_loaded = true;
                (lru, count, size, ages)
            }
        } else {
            Self::startup_cache_cleanup(&cfg)?;
            let (lru, count, size, ages) = Self::full_rescan(&cfg, &stats, cfg.verify_cache)?;
            cache_loaded = true;
            (lru, count, size, ages)
        };

        stats.set_cache_count(cache_count);
        stats.set_cache_size(Self::cache_size_with_overhead(
            cache_size,
            cache_count,
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
                    cache_size,
                    cache_count,
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
        if cache_count < 1 && static_range_count > 20 {
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
            lru: std::sync::Mutex::new(lru),
            cache_count: AtomicU32::new(cache_count),
            cache_size: AtomicU64::new(cache_size),
            static_range_oldest: std::sync::Mutex::new(static_range_oldest),
            cache_loaded,
            last_file_verification_cooldown: std::sync::Mutex::new(std::time::Instant::now()),
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

                // Move file to correct L2 dir
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

    fn try_load_persistent(config: &Config) -> Option<PersistentCacheState> {
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

        for line in info_content.lines() {
            if let Some((key, value)) = line.split_once('=') {
                match key {
                    "cacheCount" => {
                        cache_count = value.parse().ok()?;
                        tracing::debug!(
                            "CacheHandler: Loaded persistent cacheCount={}",
                            cache_count
                        );
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

        let ages_hash = ages_hash?;
        let lru_hash = lru_hash?;

        tracing::info!("CacheHandler: All persistent fields found, loading remaining objects");

        // Verify SHA-1 and deserialize pcache_ages
        let static_range_ages: HashMap<String, u64> =
            match Self::read_persistent_object(&ages_path, &ages_hash) {
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
        let lru_cache_table_vec: Vec<u16> = match Self::read_persistent_object(&lru_path, &lru_hash)
        {
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
            lru_cache_table: lru_cache_table_vec,
        })
    }

    /// Verify SHA-1 hash of a file and deserialize its contents with bincode.
    /// Java: `CacheHandler.readCacheObject()`
    fn read_persistent_object<T: serde::de::DeserializeOwned>(
        file: &std::path::Path,
        expected_hash: &str,
    ) -> Result<T> {
        if !file.exists() {
            return Err(HathError::Cache(format!(
                "Missing file: {}",
                file.display()
            )));
        }

        let data = fs::read(file).map_err(|e| {
            HathError::Cache(format!("Failed to read file {}: {}", file.display(), e))
        })?;

        let actual_hash = utils::sha1_bytes(&data);
        if actual_hash != expected_hash {
            return Err(HathError::Cache(format!(
                "Incorrect file hash while reading {} (expected {}, got {})",
                file.display(),
                expected_hash,
                actual_hash
            )));
        }

        bincode::deserialize(&data).map_err(|e| {
            HathError::Cache(format!("Failed to deserialize {}: {}", file.display(), e))
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
        let ages_path = cfg.data_dir.join("pcache_ages");
        let lru_path = cfg.data_dir.join("pcache_lru");
        let info_path = cfg.data_dir.join("pcache_info");

        let result: std::result::Result<(), String> = (|| {
            // 1. Serialize under lock, drop lock, write to disk, hash from buffer.
            let ages_data = {
                let ages = self.static_range_oldest.lock().unwrap();
                bincode::serialize(&*ages)
                    .map_err(|e| format!("Failed to serialize ages: {}", e))?
            };
            fs::write(&ages_path, &ages_data)
                .map_err(|e| format!("Failed to write ages: {}", e))?;
            let ages_hash = utils::sha1_bytes(&ages_data);

            // 2. Capture lru_clear_pointer in the same lock scope (avoids a
            //    third redundant acquisition), serialize, then write and hash.
            let (lru_data, lru_clear_pointer) = {
                let lru = self.lru.lock().unwrap();
                let data = bincode::serialize(&lru.lru_cache_table[..])
                    .map_err(|e| format!("Failed to serialize lru: {}", e))?;
                (data, lru.lru_clear_pointer)
            };
            fs::write(&lru_path, &lru_data).map_err(|e| format!("Failed to write lru: {}", e))?;
            let lru_hash = utils::sha1_bytes(&lru_data);

            // 3. Write pcache_info (plain text key=value)
            let cache_count = self.cache_count.load(Ordering::Relaxed);
            let cache_size = self.cache_size.load(Ordering::Relaxed);

            let info = format!(
                "cacheCount={}\ncacheSize={}\nlruClearPointer={}\nagesHash={}\nlruHash={}",
                cache_count, cache_size, lru_clear_pointer, ages_hash, lru_hash
            );
            fs::write(&info_path, info.as_bytes())
                .map_err(|e| format!("Failed to write info: {}", e))?;

            Ok(())
        })();

        if let Err(e) = result {
            tracing::warn!("Failed to save persistent cache data: {}", e);
        }
    }

    fn delete_persistent_data(config: &Config) {
        for name in &["pcache_info", "pcache_lru", "pcache_ages"] {
            utils::remove_file(&config.data_dir.join(name));
        }
    }

    /// Full rescan: iterates all cache directories, validates files, builds initial state.
    /// Java: startupInitCache — returns LRU initialized with recently-accessed files
    /// (modified within 7 days) so that the first request for a recent file won't
    /// update its mtime unnecessarily.
    fn full_rescan(
        config: &Config,
        stats: &Stats,
        verify_cache: bool,
    ) -> Result<(LruState, u32, u64, HashMap<String, u64>)> {
        // Java: create a single MessageDigest + ByteBuffer and reuse across all files
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

        let mut count = 0u32;
        let mut size = 0u64;
        let mut range_ages = HashMap::new();
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
                                // IO error during read — clean up partial hasher state
                                hasher.reset();
                            }
                        }

                        if !sha1_ok {
                            utils::remove_file(file);
                            continue;
                        }
                    }

                    // Static range check
                    if !config.is_static_range(&hv.static_range()) {
                        utils::remove_file(file);
                        continue;
                    }

                    found_valid = true;
                    count += 1;
                    size += hv.size as u64;

                    let modified = utils::modified_millis(file);
                    oldest_modified = oldest_modified.min(modified);

                    // Java: if fileLastModified > recentlyAccessedCutoff,
                    // markRecentlyAccessed(hvFile, true) — sets LRU bit without
                    // updating file mtime.
                    if modified > recently_accessed_cutoff {
                        lru.mark_recently_accessed(hv.fileid().as_str());
                    }

                    if count.is_multiple_of(print_freq) {
                        tracing::info!("CacheHandler: Loaded {} files so far...", count);
                    }
                }

                if found_valid {
                    range_ages.insert(static_range, oldest_modified);
                } else {
                    utils::remove_dir(l2_dir);
                }
            }
        }

        stats.set_cache_count(count);
        stats.set_cache_size(Self::cache_size_with_overhead(size, count, config));

        tracing::info!(
            "Cache init complete: {} files, {} apparent bytes, {} estimated on disk",
            count,
            size,
            Self::cache_size_with_overhead(size, count, config)
        );

        Ok((lru, count, size, range_ages))
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
                self.cache_count.fetch_sub(1, Ordering::Relaxed);
                self.cache_size.fetch_sub(hv.size as u64, Ordering::Relaxed);
                let count = self.cache_count.load(Ordering::Relaxed);
                self.stats.set_cache_count(count);
                self.stats
                    .set_cache_size(self.get_cache_size_with_overhead());
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
        // addFileToActiveCache
        self.cache_count.fetch_add(1, Ordering::Relaxed);
        self.cache_size
            .fetch_add(hv_file.size as u64, Ordering::Relaxed);
        let count = self.cache_count.load(Ordering::Relaxed);
        self.stats.set_cache_count(count);
        self.stats
            .set_cache_size(self.get_cache_size_with_overhead());

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

    /// Java: checkAndPruneCache() — phase 1 (lock held, read-only).
    ///
    /// Checks if the cache is over the disk limit and returns a [`PruneAction`].
    /// The actual file deletion (I/O with sleeps) happens outside the lock.
    pub fn check_prune_action(&self, config: &Config) -> PruneAction {
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

        if bytes_to_free == 0 || count == 0 || range_ages.is_empty() {
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
    /// Applies the results of a prune pass that was executed outside the lock.
    /// Updates [`static_range_oldest`], cache counters, and stats.
    pub fn apply_prune_result(&self, result: PruneResult) {
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

        self.cache_count
            .fetch_sub(result.files_deleted as u32, Ordering::Relaxed);
        self.cache_size
            .fetch_sub(result.bytes_deleted, Ordering::Relaxed);
        let count = self.cache_count.load(Ordering::Relaxed);
        self.stats.set_cache_count(count);
        self.stats
            .set_cache_size(self.get_cache_size_with_overhead());
    }
}

/// Spawn the cache pruner background task.
pub fn spawn_pruner(
    cache: Arc<CacheHandler>,
    config: Arc<ArcSwap<Config>>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let pruner = CachePruner::new(cache, config, shutdown);
    tokio::spawn(async move { pruner.run().await });
}

/// Spawn periodic LRU cycle + stats shift (10s interval).
pub fn spawn_periodic_stats(
    cache: Arc<CacheHandler>,
    stats: Arc<Stats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
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
    ));
}

/// Synchronous initial blacklist fetch at startup.
/// Fetches blacklist with 3-day delta and deletes matching files from cache.
pub async fn fetch_initial_blacklist(rpc_client: &RpcClient, cache: &CacheHandler) {
    match rpc_client.get_blacklist(259200).await {
        Ok(resp) if resp.status == crate::rpc::ResponseStatus::Ok => {
            for fileid in &resp.lines {
                let _ = cache.delete_file_from_cache(fileid);
            }
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Initial blacklist fetch failed: {}", e);
        }
    }
}

/// Spawn periodic blacklist fetch (6h interval).
pub fn spawn_blacklist_fetcher(
    rpc_client: Arc<RpcClient>,
    cache: Arc<CacheHandler>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(21600),
        move || {
            let rpc_client = rpc_client.clone();
            let cache = cache.clone();
            async move {
                match rpc_client.get_blacklist(43200).await {
                    Ok(resp) if resp.status == crate::rpc::ResponseStatus::Ok => {
                        for fileid in &resp.lines {
                            let _ = cache.delete_file_from_cache(fileid);
                        }
                    }
                    _ => {
                        tracing::warn!(
                            "CacheHandler: Failed to retrieve file blacklist, will try again later."
                        );
                    }
                }
            }
        },
    ));
}
