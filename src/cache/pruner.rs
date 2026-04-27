use crate::cache::{CacheHandler, PruneAction, PrunePlan, PruneResult};
use crate::config::Config;
use crate::hvfile::HVFile;
use crate::utils;
use arc_swap::ArcSwap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Java: CachePruner — runs in a background task, periodically checking
/// whether the cache has exceeded the disk limit and pruning old files.
///
/// Takes `Arc<CacheHandler>` directly — no outer Mutex needed because
/// CacheHandler manages its own fine-grained locks internally.
pub struct CachePruner {
    cache: Arc<CacheHandler>,
    config: Arc<ArcSwap<Config>>,
    /// Seconds between cache checks when not over limit.
    /// Adjusted dynamically based on free space.
    check_frequency: u32,
    shutdown: CancellationToken,
}

impl CachePruner {
    pub fn new(cache: Arc<CacheHandler>, config: Arc<ArcSwap<Config>>, shutdown: CancellationToken) -> Self {
        Self { cache, config, check_frequency: 60, shutdown }
    }

    pub async fn run(mut self) {
        let mut cache_check_ticks = 0u32;
        let mut disk_check_ticks = 0u32;

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }

            // Phase 1: check cache state (brief internal locks on static_range_oldest).
            let cfg = self.config.load();
            let action = {
                let cache_size = self.cache.get_cache_size_with_overhead();
                if cache_size > cfg.disklimit_bytes {
                    let pct = 100.0 * (cache_size as f64 / cfg.disklimit_bytes as f64) - 100.0;
                    tracing::info!("Cache is {:.3}% over limit, aggressive pruning", pct);
                    self.cache.check_prune_action(&cfg)
                } else if cache_check_ticks < self.check_frequency {
                    cache_check_ticks += 1;
                    PruneAction::NoPrune { frequency: self.check_frequency }
                } else {
                    cache_check_ticks = 0;
                    self.cache.check_prune_action(&cfg)
                }
            };

            match action {
                PruneAction::Prune(plan) => {
                    // Phase 2: execute I/O without holding any locks.
                    let result = Self::execute_prune(&plan).await;
                    // Phase 3: apply result (brief internal lock on static_range_oldest).
                    self.cache.apply_prune_result(result);
                    self.check_frequency = 0; // Re-check immediately after pruning.
                }
                PruneAction::NoPrune { frequency } => {
                    self.check_frequency = frequency;
                }
            }

            // Periodic disk free space check (every 300 ticks ≈ 5 min).
            // Java: CachePruner calls cacheHandler.hasFreeDiskSpace() →
            // dieWithError if disk is full. Respect skipFreeSpaceCheck.
            disk_check_ticks += 1;
            if disk_check_ticks >= 300 {
                if !cfg.skip_free_space_check
                    && let Ok(free) = fs2::free_space(&cfg.cache_dir)
                {
                    let min_remaining = cfg.diskremaining_bytes.max(104_857_600);
                    if free < min_remaining {
                        tracing::error!(
                            "The free disk space has dropped below the minimum \
                             allowed threshold. H@H cannot safely continue. \
                             Free up space, or reduce the cache size from the \
                             H@H settings page."
                        );
                        self.shutdown.cancel();
                        break;
                    }
                }
                disk_check_ticks = 0;
            }
        }

        tracing::debug!("CacheHandler: Pruner task exited due to client shutdown");
    }

    /// Delete files matching the prune plan. Called without any cache locks
    /// so file I/O and sleep delays don't block other cache operations.
    async fn execute_prune(plan: &PrunePlan) -> PruneResult {
        let mut files_deleted = 0usize;
        let mut bytes_deleted = 0u64;
        let mut oldest_last_modified = u64::MAX;
        let mut file_count = 0usize;

        let files = utils::list_sorted_files(&plan.range_dir);

        for file in &files {
            if !file.is_file() {
                continue;
            }

            let last_modified = utils::modified_millis(file);
            oldest_last_modified = oldest_last_modified.min(last_modified);
            file_count += 1;

            if last_modified < plan.cutoff {
                let filename = file
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                if let Some(hv) = HVFile::from_fileid(filename) {
                    if fs::remove_file(file).is_ok() {
                        files_deleted += 1;
                        bytes_deleted += hv.size as u64;
                        tracing::debug!(
                            "CacheHandler: Pruned file {} lastModified={} size={}",
                            hv.fileid(),
                            last_modified,
                            hv.size
                        );
                    }
                } else {
                    // Invalid filename — remove it.
                    let _ = fs::remove_file(file);
                }

                // Delay between deletions to reduce disk activity bursts.
                tokio::time::sleep(Duration::from_millis(
                    if plan.fast_delete { 100 } else { 1000 },
                )).await;
            }
        }

        // If no valid files tracked age, use current time.
        if oldest_last_modified == u64::MAX {
            oldest_last_modified = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
        }

        file_count -= files_deleted;

        PruneResult {
            static_range: plan.static_range.clone(),
            range_dir: plan.range_dir.clone(),
            file_count,
            oldest_last_modified,
            files_deleted,
            bytes_deleted,
        }
    }
}
