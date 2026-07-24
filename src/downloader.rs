use crate::bandwidth::BandwidthMonitor;
use crate::config::Config;
use crate::error::{HathError, Result};
use bytes::BytesMut;
use reqwest::{Client, Url};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub enum DownloadMode {
    Memory,
    File(PathBuf),
    Discard,
}

#[derive(Debug)]
pub struct FileDownloader {
    source: Url,
    connect_timeout: Duration,
    read_timeout: Duration,
    max_dl_time: Duration,
    retries: AtomicU32,
    mode: DownloadMode,
    client: Option<Client>,
    download_limiter: Option<Arc<BandwidthMonitor>>,
    max_content_length: Option<u64>,
    pub content_length: AtomicI32,
    pub download_time_millis: AtomicU64,
}

enum DownloadAttemptError {
    Retryable(HathError),
    NotFound(HathError),
}

impl From<HathError> for DownloadAttemptError {
    fn from(error: HathError) -> Self {
        Self::Retryable(error)
    }
}

impl From<std::io::Error> for DownloadAttemptError {
    fn from(error: std::io::Error) -> Self {
        Self::Retryable(HathError::Io(error))
    }
}

impl FileDownloader {
    pub fn new(source: Url, read_timeout_ms: u64, max_dl_time_ms: u64, mode: DownloadMode) -> Self {
        Self {
            source,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_millis(read_timeout_ms),
            // Java FileDownloader stores maxDLTime but does not enforce it.
            max_dl_time: Duration::from_millis(max_dl_time_ms),
            retries: AtomicU32::new(3),
            mode,
            client: None,
            download_limiter: None,
            max_content_length: None,
            content_length: AtomicI32::new(0),
            download_time_millis: AtomicU64::new(0),
        }
    }

    pub fn set_connect_timeout(&mut self, timeout: Duration) {
        self.connect_timeout = timeout;
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = timeout;
    }

    pub fn max_dl_time(&self) -> Duration {
        self.max_dl_time
    }

    pub fn set_download_limiter(&mut self, limiter: Arc<BandwidthMonitor>) {
        self.download_limiter = Some(limiter);
    }

    /// Reuse a caller-owned HTTP client. Gallery downloads use this to select
    /// the direct or configured image-proxy transport without rebuilding a
    /// client for every file.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Reject responses larger than this value before opening the output file.
    pub fn set_max_content_length(&mut self, max_content_length: u64) {
        self.max_content_length = Some(max_content_length);
    }

    pub async fn download(&self) -> Result<Option<BytesMut>> {
        let client = match &self.client {
            Some(client) => client.clone(),
            None => build_direct_client(self.connect_timeout, self.read_timeout)?,
        };

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
                Err(DownloadAttemptError::NotFound(e)) => return Err(e),
                Err(DownloadAttemptError::Retryable(e)) => {
                    tracing::warn!("Download failed: {} (retrying, {} left)", e, remaining - 1)
                }
            }
        }
    }

    async fn attempt_download(
        &self,
        client: &Client,
    ) -> std::result::Result<Option<BytesMut>, DownloadAttemptError> {
        let mut resp = client
            .get(self.source.clone())
            .header("Connection", "Close")
            .send()
            .await
            .map_err(|e| HathError::Network(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(DownloadAttemptError::NotFound(HathError::Network(format!(
                "server returned 404 for {}",
                self.source
            ))));
        }
        if !resp.status().is_success() {
            return Err(HathError::Network(format!(
                "server returned {} for {}",
                resp.status(),
                self.source
            ))
            .into());
        }

        let content_length = resp
            .content_length()
            .ok_or_else(|| HathError::Network("missing Content-Length header".into()))?
            as i32;

        if content_length < 0 {
            return Err(HathError::Network("invalid Content-Length".into()).into());
        }

        if let Some(max) = self.max_content_length
            && content_length as u64 > max
        {
            return Err(HathError::Network(format!(
                "content too large: {} exceeds {}",
                content_length, max
            ))
            .into());
        }

        // Check size limits
        if content_length > 10_485_760 && matches!(self.mode, DownloadMode::Memory) {
            return Err(HathError::Network("content too large for memory buffer".into()).into());
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
                Err(e) => return Err(HathError::Network(e.to_string()).into()),
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
            ))
            .into());
        }

        self.download_time_millis.store(
            download_start.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        Ok(buffer)
    }
}

/// Build a direct client with the transport limits shared by one logical
/// download. The client is cheap to clone and should normally be reused.
pub(crate) fn build_direct_client(
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<Client> {
    Client::builder()
        .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .build()
        .map_err(|e| HathError::Network(e.to_string()))
}

/// Build an image-proxied client using the documented H@H configuration.
/// This is shared by gallery and streaming proxy downloads; it intentionally
/// does not use the old ad-hoc HATH_PROXY environment variable.
pub(crate) fn build_image_proxy_client(
    config: &Config,
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<Client> {
    let mut builder = Client::builder()
        .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout);

    if let Some(proxy_host) = &config.image_proxy_host {
        let proxy_type = config.image_proxy_type.as_deref().unwrap_or("socks");
        let default_port = if proxy_type == "http" { 8080 } else { 1080 };
        let proxy_port = config.image_proxy_port.unwrap_or(default_port);
        let proxy_url = build_proxy_url(proxy_type, proxy_host, proxy_port)?;
        let proxy = reqwest::Proxy::all(proxy_url.as_str())
            .map_err(|e| HathError::Config(format!("invalid image proxy: {}", e)))?;
        builder = builder.proxy(proxy);
    }

    builder
        .build()
        .map_err(|e| HathError::Network(e.to_string()))
}

pub(crate) fn build_proxy_url(proxy_type: &str, proxy_host: &str, proxy_port: u16) -> Result<Url> {
    let mut url = match proxy_type {
        "socks" => Url::parse("socks://hath.invalid/"),
        "http" => Url::parse("http://hath.invalid/"),
        _ => {
            return Err(HathError::Config(format!(
                "invalid proxy type: {}",
                proxy_type
            )));
        }
    }
    .map_err(|e| HathError::Config(format!("invalid proxy URL base: {}", e)))?;
    if let Ok(ip) = proxy_host.parse::<IpAddr>() {
        url.set_ip_host(ip)
            .map_err(|_| HathError::Config("invalid proxy host".into()))?;
    } else {
        url.set_host(Some(proxy_host))
            .map_err(|e| HathError::Config(format!("invalid proxy host: {}", e)))?;
    }
    url.set_port(Some(proxy_port))
        .map_err(|_| HathError::Config(format!("invalid proxy port: {}", proxy_port)))?;
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeHttpServer, FakeResponse};

    #[tokio::test]
    async fn does_not_retry_a_not_found_response() {
        let server = FakeHttpServer::start(vec![
            FakeResponse::status(404, "Not Found", "missing"),
            FakeResponse::ok("should not be requested"),
        ])
        .await;
        let downloader =
            FileDownloader::new(server.url("/missing"), 1_000, 1_000, DownloadMode::Discard);

        assert!(downloader.download().await.is_err());
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn rejects_a_response_above_the_configured_limit_before_writing() {
        let server = FakeHttpServer::start(vec![FakeResponse::ok(vec![0u8; 16])]).await;
        let mut downloader =
            FileDownloader::new(server.url("/large"), 1_000, 1_000, DownloadMode::Memory);
        downloader.set_max_content_length(8);

        assert!(downloader.download().await.is_err());
    }
}
