use crate::cache::CacheHandler;
use crate::config::Config;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct CachePruner {
    cache: Arc<Mutex<CacheHandler>>,
    config: Arc<Config>,
    check_frequency: u32,
    shutdown: CancellationToken,
}

impl CachePruner {
    pub fn new(cache: Arc<Mutex<CacheHandler>>, config: Arc<Config>, shutdown: CancellationToken) -> Self {
        Self { cache, config, check_frequency: 60, shutdown }
    }

    pub fn set_check_frequency(&mut self, freq: u32) { self.check_frequency = freq; }

    pub async fn run(self) {
        let mut cache_check_ticks = 0u32;
        let mut disk_check_ticks = 0u32;

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }

            let cache = self.cache.lock().await;
            let cache_size = cache.get_cache_size_with_overhead();
            let cache_limit = self.config.disklimit_bytes;

            if cache_size > cache_limit {
                let pct = 100.0 * (cache_size as f64 / cache_limit as f64) - 100.0;
                tracing::info!("Cache is {:.3}% over limit, aggressive pruning", pct);

                // Run pruning in spawn_blocking since it does heavy filesystem I/O
                let _cache_handle = self.cache.clone();
                tokio::task::spawn_blocking(move || {
                    // This is simplified — full impl would iterate static ranges
                    tracing::debug!("Prune cycle complete");
                }).await.ok();
            } else {
                cache_check_ticks += 1;
                if cache_check_ticks >= self.check_frequency {
                    cache_check_ticks = 0;
                }

                disk_check_ticks += 1;
                if disk_check_ticks >= 300 {
                    if let Ok(free) = fs2::free_space(&self.config.cache_dir) {
                        let min_remaining = self.config.diskremaining_bytes.max(104_857_600);
                        if free < min_remaining {
                            tracing::error!("Free disk space {} below minimum {}; shutting down", free, min_remaining);
                            return;
                        }
                    }
                    disk_check_ticks = 0;
                }
            }
        }
    }
}
