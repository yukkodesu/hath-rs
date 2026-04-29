use crate::bandwidth::BandwidthMonitor;
use crate::error::{HathError, Result};
use bytes::BytesMut;
use reqwest::{Client, Url};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub enum DownloadMode {
    Memory,
    File(PathBuf),
    Discard,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FileDownloader {
    source: Url,
    timeout_ms: u64,
    max_dl_time_ms: u64,
    retries: AtomicU32,
    mode: DownloadMode,
    allow_proxy: bool,
    download_limiter: Option<Arc<BandwidthMonitor>>,
    pub content_length: AtomicI32,
    pub download_time_millis: AtomicU64,
}

impl FileDownloader {
    pub fn new(
        source: Url,
        timeout_ms: u64,
        max_dl_time_ms: u64,
        mode: DownloadMode,
        allow_proxy: bool,
    ) -> Self {
        Self {
            source,
            timeout_ms,
            max_dl_time_ms,
            retries: AtomicU32::new(3),
            mode,
            allow_proxy,
            download_limiter: None,
            content_length: AtomicI32::new(0),
            download_time_millis: AtomicU64::new(0),
        }
    }

    pub fn set_download_limiter(&mut self, limiter: Arc<BandwidthMonitor>) {
        self.download_limiter = Some(limiter);
    }

    pub async fn download(&self) -> Result<Option<BytesMut>> {
        let mut builder = Client::builder()
            .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
            // Java: setConnectTimeout(5000) — 5s connect timeout
            .connect_timeout(std::time::Duration::from_secs(5));

        // Java: SOCKS/HTTP proxy support via Settings.getImageProxy()
        if self.allow_proxy
            && let Ok(proxy_url) = std::env::var("HATH_PROXY")
            && let Ok(proxy) = reqwest::Proxy::all(&proxy_url)
        {
            builder = builder.proxy(proxy);
        }

        let client = builder
            .build()
            .map_err(|e| HathError::Network(e.to_string()))?;

        loop {
            let remaining = self.retries.load(Ordering::Relaxed);
            if remaining == 0 {
                return Err(HathError::Network(format!(
                    "exhausted retries for {}",
                    self.source
                )));
            }
            self.retries.store(remaining - 1, Ordering::Relaxed);

            match self.attempt_download(&client).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    tracing::warn!("Download failed: {} (retrying, {} left)", e, remaining - 1)
                }
            }
        }
    }

    async fn attempt_download(&self, client: &Client) -> Result<Option<BytesMut>> {
        let mut resp = client
            .get(self.source.clone())
            // Java: setRequestProperty("Connection", "Close")
            .header("Connection", "Close")
            .timeout(std::time::Duration::from_millis(self.timeout_ms))
            .send()
            .await
            .map_err(|e| HathError::Network(e.to_string()))?;

        let content_length = resp
            .content_length()
            .ok_or_else(|| HathError::Network("missing Content-Length header".into()))?
            as i32;

        if content_length < 0 {
            return Err(HathError::Network("invalid Content-Length".into()));
        }

        // Check size limits
        if content_length > 10_485_760 && matches!(self.mode, DownloadMode::Memory) {
            return Err(HathError::Network(
                "content too large for memory buffer".into(),
            ));
        }

        self.content_length.store(content_length, Ordering::Relaxed);

        let mut buffer = match &self.mode {
            DownloadMode::Memory => Some(BytesMut::with_capacity(content_length as usize)),
            _ => None,
        };

        let mut file = match &self.mode {
            DownloadMode::File(path) => Some(fs::File::create(path).await?),
            _ => None,
        };

        let download_start = std::time::Instant::now();
        let mut total_bytes: u64 = 0;

        loop {
            let chunk = match resp.chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => break,
                Err(e) => return Err(HathError::Network(e.to_string())),
            };
            total_bytes += chunk.len() as u64;

            match &self.mode {
                DownloadMode::Memory => {
                    if let Some(ref mut buf) = buffer {
                        buf.extend_from_slice(&chunk);
                    }
                }
                DownloadMode::File(_) => {
                    if let Some(ref mut f) = file {
                        f.write_all(&chunk).await?;
                    }
                }
                DownloadMode::Discard => {}
            }

            if let Some(ref limiter) = self.download_limiter {
                limiter.wait_for_quota(chunk.len()).await;
            }
        }

        if total_bytes != content_length as u64 {
            return Err(HathError::Network(format!(
                "incomplete: got {} of {}",
                total_bytes, content_length
            )));
        }

        self.download_time_millis.store(
            download_start.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        Ok(buffer)
    }
}
