use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::utils;
use reqwest::{Client, Url};
use sha1::Digest;
use std::io::{Read, Seek, SeekFrom};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;

/// Streaming proxy download: downloads from an upstream image server
/// while simultaneously serving data to the requesting HTTPSession.
///
/// After construction, the download runs in a background tokio task.
/// The server response body streams data to the client as it arrives.
#[allow(dead_code)]
pub struct ProxyFileDownloader {
    pub content_length: usize,
    pub content_type: String,
    /// Path to the temp file where downloaded data is written.
    pub temp_file: PathBuf,
    /// Atomic counter: bytes written so far by the download task.
    pub write_offset: Arc<std::sync::atomic::AtomicU64>,
    /// Expected total file size in bytes.
    pub total_size: u64,
    /// Notified each time new data is written to the temp file.
    pub notify: Arc<Notify>,
    /// Notified when the body finishes reading.
    pub body_done_notify: Arc<Notify>,
    /// True when the download task has finished (success or failure).
    /// The body reader checks this to avoid waiting for data that will never arrive.
    pub download_done: Arc<AtomicBool>,
}

enum DownloadAttemptResult {
    Success(String),
    Retry,
    Fatal,
}

impl ProxyFileDownloader {
    /// Initialize with a list of upstream source URLs.
    /// Returns a handle that can be used to stream data to the client.
    pub async fn new(
        fileid: &str,
        sources: &[Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        client: &Arc<Client>,
    ) -> Result<Self> {
        let hv_file = HVFile::from_fileid(fileid)
            .ok_or_else(|| HathError::Parse(format!("invalid fileid: {}", fileid)))?;
        let temp_file = Self::create_temp_file(&hv_file, config)?;

        let client = Arc::clone(client);

        let mut last_err = None;

        for source in sources {
            // Java: ProxyFileDownloader has inner retry loop (3 attempts per source)
            for attempt in 0..3u32 {
                match Self::try_source(
                    &client,
                    source,
                    &hv_file,
                    config,
                    cache_handler.clone(),
                    temp_file.clone(),
                )
                .await
                {
                    Ok(this) => return Ok(this),
                    Err(e) => {
                        if attempt < 2 {
                            tracing::debug!(
                                "Proxy download attempt {} failed for {}: {}, retrying...",
                                attempt + 1,
                                source,
                                e
                            );
                        }
                        last_err = Some(e);
                        if attempt < 2 {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }

        // Java: returns 500 when no source works, 502 when source has bad content.
        utils::remove_file(&temp_file);
        Err(last_err.unwrap_or_else(|| HathError::ProxyDownloader {
            status: 500,
            message: "all sources exhausted".into(),
        }))
    }

    async fn try_source(
        client: &Client,
        source: &Url,
        hv_file: &HVFile,
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        temp_file: PathBuf,
    ) -> Result<Self> {
        // Hath-Request header: "{cid}-{SHA1(clientKey + fileid)}"
        let hath_request = format!(
            "{}-{}",
            config.client_id.0,
            utils::sha1_string(&format!(
                "{}{}",
                config.client_key.as_str(),
                hv_file.fileid().as_str()
            ))
        );

        let resp = client
            .get(source.clone())
            .header("Hath-Request", &hath_request)
            .header(
                "User-Agent",
                format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
            )
            .send()
            .await
            .map_err(|e| HathError::Network(e.to_string()))?;

        let content_length = resp
            .content_length()
            .ok_or_else(|| HathError::ProxyDownloader {
                status: 502,
                message: "missing Content-Length".into(),
            })? as u64;

        // Java: check max_allowed_filesize before size match
        if content_length > config.max_allowed_filesize {
            return Err(HathError::ProxyDownloader {
                status: 502,
                message: format!(
                    "contentLength {} exceeds max allowed filesize {}",
                    content_length, config.max_allowed_filesize
                ),
            });
        }

        if content_length != hv_file.size as u64 {
            return Err(HathError::ProxyDownloader {
                status: 502,
                message: format!(
                    "size mismatch: expected {}, got {}",
                    hv_file.size, content_length
                ),
            });
        }

        let write_offset = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let notify = Arc::new(Notify::new());
        let body_done_notify = Arc::new(Notify::new());
        let download_done = Arc::new(AtomicBool::new(false));

        // Build the return value first, then move the Arcs into the spawn task
        // directly — avoids a second round of clone() for each field.
        let this = Self {
            content_length: hv_file.size as usize,
            content_type: hv_file.mime_type().to_string(),
            temp_file,
            write_offset,
            total_size: content_length,
            notify,
            body_done_notify,
            download_done,
        };

        // Spawn download task with inner retry matching Java's
        // do { ... } while(!streamThreadSuccess && --trycounter > 0)
        let wo = this.write_offset.clone();
        let tf = this.temp_file.clone();
        let not = this.notify.clone();
        let bdn = this.body_done_notify.clone();
        let dd = this.download_done.clone();
        let hash = hv_file.hash.clone();
        let expected_size = hv_file.size as u64;
        let fileid_owned = hv_file.fileid().clone();
        let cache_dir = config.cache_dir.clone();
        let retry_source = source.clone();
        let retry_client = client.clone();
        let retry_hath = hath_request.clone();

        tokio::spawn(async move {
            let mut stream_success = false;
            let mut final_digest = String::new();

            let mut should_retry = match Self::download_attempt(
                Ok(resp),
                &tf,
                expected_size,
                fileid_owned.as_str(),
                &wo,
                &not,
            )
            .await
            {
                DownloadAttemptResult::Success(digest) => {
                    stream_success = true;
                    final_digest = digest;
                    false
                }
                DownloadAttemptResult::Fatal => {
                    dd.store(true, Ordering::SeqCst);
                    not.notify_waiters();
                    return;
                }
                DownloadAttemptResult::Retry => {
                    wo.store(0, std::sync::atomic::Ordering::SeqCst);
                    true
                }
            };
            let mut tries_left = 2u32;

            while should_retry && tries_left > 0 {
                tracing::debug!(
                    "Proxy download: retrying... ({} tries left) for {}",
                    tries_left,
                    fileid_owned
                );
                tries_left -= 1;

                should_retry = match Self::download_attempt(
                    retry_client
                        .get(retry_source.clone())
                        .header("Hath-Request", &retry_hath)
                        .header(
                            "User-Agent",
                            format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
                        )
                        .send()
                        .await,
                    &tf,
                    expected_size,
                    fileid_owned.as_str(),
                    &wo,
                    &not,
                )
                .await
                {
                    DownloadAttemptResult::Success(digest) => {
                        stream_success = true;
                        final_digest = digest;
                        false
                    }
                    DownloadAttemptResult::Fatal => {
                        dd.store(true, Ordering::SeqCst);
                        not.notify_waiters();
                        return;
                    }
                    DownloadAttemptResult::Retry => {
                        // Reset write_offset on failure so body side knows to keep waiting.
                        wo.store(0, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                };
            }

            // Signal that the download has finished (success or failure).
            // The body reader checks this flag — if true and data is insufficient,
            // it terminates instead of waiting for the full 300s timeout.
            dd.store(true, Ordering::SeqCst);
            // Final wake so the body can read remaining chunks or see the done flag.
            not.notify_waiters();

            // Wait for the body to finish reading.
            let timeout = std::time::Duration::from_secs(300);
            tokio::select! {
                _ = bdn.notified() => {
                    tracing::info!(
                        "Proxy download: body done for {}",
                        fileid_owned
                    );
                }
                _ = tokio::time::sleep(timeout) => {
                    tracing::warn!(
                        "Proxy download: timeout waiting for body {}",
                        fileid_owned
                    );
                }
            }

            // Java: checkFinalizeDownloadedFile — only import on stream success
            if stream_success
                && final_digest == hash.as_str()
                && let Some(hv) = HVFile::from_fileid(fileid_owned.as_str())
            {
                let cache_path = hv.cache_path(&cache_dir);
                if let Ok(()) = utils::ensure_dir(cache_path.parent().unwrap())
                    && std::fs::copy(&tf, &cache_path).is_ok()
                    && let Some(ref cache) = cache_handler
                {
                    cache.register_proxy_file(&hv);
                }
            }
            utils::remove_file(&tf);
        });

        Ok(this)
    }

    async fn download_attempt(
        resp_result: std::result::Result<reqwest::Response, reqwest::Error>,
        temp_file: &Path,
        expected_size: u64,
        fileid: &str,
        write_offset: &Arc<std::sync::atomic::AtomicU64>,
        notify: &Arc<Notify>,
    ) -> DownloadAttemptResult {
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(temp_file)
            .await
        {
            Ok(f) => f,
            Err(_) => return DownloadAttemptResult::Fatal,
        };

        let mut resp = match resp_result {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("Proxy download: request failed for {}: {}", fileid, e);
                return DownloadAttemptResult::Retry;
            }
        };

        let mut sha1 = sha1::Sha1::new();
        let mut downloaded = 0u64;
        let download_start = std::time::Instant::now();

        loop {
            let chunk = match resp.chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    if downloaded == expected_size {
                        return DownloadAttemptResult::Success(utils::hex_encode(&sha1.finalize()));
                    }
                    tracing::warn!(
                        "Proxy download: premature EOF for {} ({} of {} bytes)",
                        fileid,
                        downloaded,
                        expected_size
                    );
                    return DownloadAttemptResult::Retry;
                }
                Err(e) => {
                    tracing::warn!(
                        "Proxy download: error for {} ({} of {} bytes): {}",
                        fileid,
                        downloaded,
                        expected_size,
                        e
                    );
                    return DownloadAttemptResult::Retry;
                }
            };

            if download_start.elapsed() > std::time::Duration::from_secs(300) {
                tracing::warn!("Proxy download: total time limit exceeded for {}", fileid);
                return DownloadAttemptResult::Retry;
            }

            sha1::Digest::update(&mut sha1, &chunk);
            if file.write_all(&chunk).await.is_err() {
                tracing::warn!("Proxy download: disk write error for {}", fileid);
                return DownloadAttemptResult::Retry;
            }
            downloaded += chunk.len() as u64;
            write_offset.store(downloaded, std::sync::atomic::Ordering::SeqCst);
            notify.notify_waiters();
        }
    }

    /// Get the current write offset (how many bytes have been downloaded).
    pub fn get_current_writeoff(&self) -> u64 {
        self.write_offset.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait for data to become available at or past the given offset.
    pub async fn wait_for_data(&self, offset: u64) -> Result<()> {
        let timeout = std::time::Duration::from_secs(300); // 5 min
        let start = std::time::Instant::now();

        while self.get_current_writeoff() <= offset {
            if start.elapsed() > timeout {
                return Err(HathError::Network("timeout waiting for proxy data".into()));
            }
            tokio::select! {
                _ = self.notify.notified() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
        }
        Ok(())
    }

    /// Fill a buffer with data from the temp file at the given offset.
    /// Returns the number of bytes read.
    pub fn fill_buffer(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let mut file = std::fs::File::open(&self.temp_file)?;
        file.seek(SeekFrom::Start(offset))?;
        let n = file.read(buf)?;
        Ok(n)
    }

    fn create_temp_file(hv_file: &HVFile, config: &Config) -> Result<PathBuf> {
        // Java creates the temp file during initialize(), before the response
        // body is returned. Keep that ordering so StreamingBody can open it
        // immediately without racing the background task.
        let temp_file = config.temp_dir.join(format!(
            "proxyfile_{}_{}",
            hv_file.fileid().as_str(),
            uuid::Uuid::now_v7()
        ));
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_file)
            .map_err(HathError::Io)?;
        Ok(temp_file)
    }
}

/// Build the shared reqwest::Client used for all proxy file downloads.
/// Applies image proxy settings from config if configured.
pub fn build_proxy_client(config: &Config) -> Result<Arc<Client>> {
    let mut builder = Client::builder()
        .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(30));
    if let (Some(proxy_type), Some(proxy_host), Some(proxy_port)) = (
        &config.image_proxy_type,
        &config.image_proxy_host,
        config.image_proxy_port,
    ) && let Ok(proxy_url) = build_proxy_url(proxy_type, proxy_host, proxy_port)
        && let Ok(proxy) = reqwest::Proxy::all(proxy_url.as_str())
    {
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map(Arc::new)
        .map_err(|e| HathError::Network(e.to_string()))
}

pub fn build_proxy_url(proxy_type: &str, proxy_host: &str, proxy_port: u16) -> Result<Url> {
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

    #[test]
    fn test_build_proxy_url_handles_ipv6_host() {
        let url = build_proxy_url("socks", "::1", 1080).unwrap();

        assert_eq!(url.as_str(), "socks://[::1]:1080/");
    }
}
