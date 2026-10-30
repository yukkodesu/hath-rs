//! Bounded on-demand downloads, streamed through an append-only temporary file.
#[cfg(test)]
mod tests;
mod transfer;

use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::stats::Stats;
use crate::utils;
use reqwest::{Client, Url};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use transfer::Upstream;

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadFailReason {
    PrematureEof,
    NetworkError,
    Timeout,
    DiskWriteError,
    Sha1Mismatch,
    FinalizeFailed,
}

#[derive(Clone, Debug)]
pub(crate) enum DownloadState {
    /// Bytes flushed to the temporary file; never decreases during recovery.
    InProgress(u64),
    /// SHA1 verified and moved into cache after downstream completion.
    Done,
    Failed(DownloadFailReason),
}

/// Owns the shared HTTP transport for on-demand downloads.
#[derive(Clone)]
pub(crate) struct ProxyDownloader {
    client: Client,
    timeout: Duration,
}

/// A download that has passed response validation and is now streaming.
pub(crate) struct ProxyDownload {
    content_length: usize,
    content_type: String,
    body: ProxyBodySource,
}

pub(crate) struct ProxyBodySource {
    temp_file: PathBuf,
    watch_rx: watch::Receiver<DownloadState>,
    proxy_done_tx: oneshot::Sender<()>,
}

impl ProxyDownload {
    pub(crate) fn content_length(&self) -> usize {
        self.content_length
    }

    pub(crate) fn content_type(&self) -> &str {
        &self.content_type
    }

    pub(crate) fn open_temp_file(&self) -> io::Result<std::fs::File> {
        std::fs::File::open(&self.body.temp_file)
    }

    pub(crate) fn abandon(self) {
        drop(self.body.proxy_done_tx);
    }

    pub(crate) fn into_body_source(self) -> ProxyBodySource {
        self.body
    }
}

impl ProxyBodySource {
    pub(crate) fn into_components(
        self,
    ) -> (PathBuf, watch::Receiver<DownloadState>, oneshot::Sender<()>) {
        (self.temp_file, self.watch_rx, self.proxy_done_tx)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        temp_file: PathBuf,
        watch_rx: watch::Receiver<DownloadState>,
        proxy_done_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            temp_file,
            watch_rx,
            proxy_done_tx,
        }
    }
}

impl ProxyDownloader {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        // Every GET must count against the attempt budget, so a redirect is a
        // failed attempt rather than extra requests inside one attempt.
        let client = crate::downloader::build_image_proxy_client(
            config,
            CONNECT_TIMEOUT,
            READ_TIMEOUT,
            reqwest::redirect::Policy::none(),
        )?;
        Ok(Self {
            client,
            timeout: DOWNLOAD_TIMEOUT,
        })
    }

    pub(crate) async fn start(
        &self,
        fileid: &str,
        sources: &[Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        stats: Arc<Stats>,
        tasks: &crate::tasks::TransferTasks,
    ) -> Result<ProxyDownload> {
        let hv_file = HVFile::from_fileid(fileid)
            .ok_or_else(|| HathError::Parse(format!("invalid fileid: {fileid}")))?;
        let mut upstream = Upstream::new(
            self.client.clone(),
            sources,
            hv_file.clone(),
            config,
            self.timeout,
        )?;
        let response = upstream.connect().await?;
        let temp_file = TempFile::create(&hv_file, config)?;
        let temp_path = temp_file.0.clone();
        let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0));
        let (proxy_done_tx, proxy_done_rx) = oneshot::channel();
        let content_length = hv_file.size as usize;
        let content_type = hv_file.mime_type().to_owned();
        if !tasks.spawn(
            ProxyDownloadTask {
                upstream,
                response,
                watch_tx,
                proxy_done_rx,
                temp_file,
                hv_file,
                cache_dir: config.cache_dir.clone(),
                cache_handler,
                stats,
            }
            .run(),
        ) {
            return Err(HathError::Shutdown);
        }
        Ok(ProxyDownload {
            content_length,
            content_type,
            body: ProxyBodySource {
                temp_file: temp_path,
                watch_rx,
                proxy_done_tx,
            },
        })
    }
}

struct ProxyDownloadTask {
    upstream: Upstream,
    response: reqwest::Response,
    watch_tx: watch::Sender<DownloadState>,
    proxy_done_rx: oneshot::Receiver<()>,
    temp_file: TempFile,
    hv_file: HVFile,
    cache_dir: PathBuf,
    cache_handler: Option<Arc<CacheHandler>>,
    stats: Arc<Stats>,
}

impl ProxyDownloadTask {
    async fn run(self) {
        let Self {
            mut upstream,
            response,
            watch_tx,
            proxy_done_rx,
            temp_file,
            hv_file,
            cache_dir,
            cache_handler,
            stats,
        } = self;
        if let Err(error) = upstream
            .download(response, &temp_file.0, &watch_tx, &stats)
            .await
        {
            upstream.log_failed(&error);
            watch_tx.send_replace(DownloadState::Failed(error.reason()));
            let _ = proxy_done_rx.await;
            return;
        }
        stats.record_file_rcvd();
        upstream.log_complete();

        // Keep the path available until downstream opens and finishes reading it.
        let _ = proxy_done_rx.await;
        let cache_path = hv_file.cache_path(&cache_dir);
        let finalized = async {
            utils::ensure_dir(cache_path.parent().unwrap())?;
            move_temp_to_cache(&temp_file.0, &cache_path).await
        }
        .await;
        match finalized {
            Ok(()) => {
                if let Some(cache) = cache_handler {
                    cache.register_proxy_file(&hv_file);
                }
                tracing::info!(
                    "Proxy download: cached {} ({} bytes)",
                    hv_file.fileid(),
                    hv_file.size
                );
                watch_tx.send_replace(DownloadState::Done);
            }
            Err(error) => {
                tracing::warn!(fileid = %hv_file.fileid(), %error, "Proxy download: cache finalization failed");
                watch_tx.send_replace(DownloadState::Failed(DownloadFailReason::FinalizeFailed));
            }
        }
    }
}

/// Removes the temporary file however the transfer ends, including task abort.
/// After a move into cache nothing is left to remove.
struct TempFile(PathBuf);

impl TempFile {
    fn create(hv_file: &HVFile, config: &Config) -> Result<Self> {
        let path = config.temp_dir.join(format!(
            "proxyfile_{}_{}",
            hv_file.fileid(),
            uuid::Uuid::now_v7()
        ));
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        utils::remove_file(&self.0);
    }
}

async fn move_temp_to_cache(temp_file: &Path, cache_path: &Path) -> io::Result<()> {
    match tokio::fs::rename(temp_file, cache_path).await {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::debug!(%error, "Proxy download: rename failed; falling back to copy");
            copy_temp_to_cache_and_delete(temp_file, cache_path).await
        }
    }
}

async fn copy_temp_to_cache_and_delete(temp_file: &Path, cache_path: &Path) -> io::Result<()> {
    tokio::fs::copy(temp_file, cache_path).await?;
    if let Err(error) = tokio::fs::remove_file(temp_file).await {
        tracing::warn!(%error, "Proxy download: copied to cache but could not delete temporary file");
    }
    Ok(())
}
