use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::utils;
use reqwest::{Client, Url};
use sha1::Digest;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, watch};

/// Why a proxy download failed, carried by `DownloadState::Failed`.
/// Kept in this module so it can evolve independently of `BodyIncompleteReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadFailReason {
    PrematureEof,
    NetworkError,
    Timeout,
    DiskWriteError,
    Sha1Mismatch,
    FinalizeFailed,
}

/// State published by download_task to StreamingBody via watch channel.
#[derive(Clone, Debug)]
pub enum DownloadState {
    /// Download in progress; value is bytes written to temp file so far.
    InProgress(u64),
    /// Download complete: SHA1 verified, file renamed to cache.
    Done,
    /// Download failed with a specific reason.
    Failed(DownloadFailReason),
}

/// Streaming proxy download: downloads from an upstream image server into a
/// temp file while publishing progress via a watch channel.
///
/// After construction the download runs in a background tokio task.
/// The body reads from the temp file, gated on the watch-published write offset.
pub struct ProxyFileDownloader {
    pub content_length: usize,
    pub content_type: String,
    /// Path to the temp file being written by download_task.
    pub temp_file: PathBuf,
    /// Watch receiver for download progress. Move into StreamingBody (GET) or drop (HEAD).
    pub watch_rx: watch::Receiver<DownloadState>,
    /// Send on this when the proxy body has finished transmitting (GET) or is not needed (HEAD).
    /// download_task waits for this signal before rename/delete — matching Java proxyThreadCompleted().
    pub proxy_done_tx: oneshot::Sender<()>,
}

impl ProxyFileDownloader {
    /// Try each source up to 3 times until a valid HTTP 200 response with
    /// matching Content-Length is obtained. Only then create the channel and
    /// spawn the download task. The caller never sees retry.
    pub async fn new(
        fileid: &str,
        sources: &[Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        client: &Arc<Client>,
    ) -> Result<Self> {
        let hv_file = HVFile::from_fileid(fileid)
            .ok_or_else(|| HathError::Parse(format!("invalid fileid: {}", fileid)))?;

        let hath_request = format!(
            "{}-{}",
            config.client_id.0,
            utils::sha1_string(&format!(
                "{}{}",
                config.client_key.as_str(),
                hv_file.fileid().as_str()
            ))
        );

        let mut last_err = None;

        for source in sources {
            for attempt in 0..3u32 {
                let resp_result = client
                    .get(source.clone())
                    .header("Hath-Request", &hath_request)
                    .header(
                        "User-Agent",
                        format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
                    )
                    .send()
                    .await;

                let resp = match resp_result {
                    Ok(r) => r,
                    Err(e) => {
                        let err = HathError::Network(e.to_string());
                        if attempt < 2 {
                            tracing::debug!(
                                "Proxy download attempt {} failed for {}: {}, retrying...",
                                attempt + 1,
                                source,
                                err
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        last_err = Some(err);
                        continue;
                    }
                };

                if resp.status() != reqwest::StatusCode::OK {
                    last_err = Some(HathError::ProxyDownloader {
                        status: 502,
                        message: format!("upstream returned {}", resp.status()),
                    });
                    continue;
                }

                let content_length = match resp.content_length() {
                    Some(n) => n,
                    None => {
                        let err = HathError::ProxyDownloader {
                            status: 502,
                            message: "missing Content-Length".into(),
                        };
                        last_err = Some(err);
                        continue;
                    }
                };

                if content_length > config.max_allowed_filesize {
                    last_err = Some(HathError::ProxyDownloader {
                        status: 502,
                        message: format!(
                            "contentLength {} exceeds max allowed filesize {}",
                            content_length, config.max_allowed_filesize
                        ),
                    });
                    continue;
                }

                if content_length != hv_file.size as u64 {
                    last_err = Some(HathError::ProxyDownloader {
                        status: 502,
                        message: format!(
                            "size mismatch: expected {}, got {}",
                            hv_file.size, content_length
                        ),
                    });
                    continue;
                }

                // Good response — create watch channel and spawn download task.
                let temp_file = Self::create_temp_file(&hv_file, config)?;
                let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0));
                let (proxy_done_tx, proxy_done_rx) = oneshot::channel();

                let hash = hv_file.hash.clone();
                let expected_size = hv_file.size as u64;
                let fileid_owned = hv_file.fileid().clone();
                let cache_dir = config.cache_dir.clone();
                let tf = temp_file.clone();

                tokio::spawn(async move {
                    Self::download_task(
                        resp,
                        watch_tx,
                        proxy_done_rx,
                        tf,
                        expected_size,
                        fileid_owned.as_str(),
                        hash.as_str(),
                        &cache_dir,
                        cache_handler.as_deref(),
                    )
                    .await;
                });

                return Ok(Self {
                    content_length: hv_file.size as usize,
                    content_type: hv_file.mime_type().to_string(),
                    temp_file,
                    watch_rx,
                    proxy_done_tx,
                });
            }
        }

        Err(last_err.unwrap_or_else(|| HathError::ProxyDownloader {
            status: 500,
            message: "all sources exhausted".into(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_task(
        mut resp: reqwest::Response,
        watch_tx: watch::Sender<DownloadState>,
        proxy_done_rx: oneshot::Receiver<()>,
        temp_file: PathBuf,
        expected_size: u64,
        fileid: &str,
        expected_hash: &str,
        cache_dir: &std::path::Path,
        cache_handler: Option<&CacheHandler>,
    ) {
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .open(&temp_file)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("Proxy download: cannot open temp file for {}: {}", fileid, e);
                utils::remove_file(&temp_file);
                let _ = watch_tx.send(DownloadState::Failed(DownloadFailReason::DiskWriteError));
                return;
            }
        };

        let mut sha1 = sha1::Sha1::new();
        let mut downloaded = 0u64;
        let download_start = std::time::Instant::now();
        let mut success = false;
        let mut fail_reason = DownloadFailReason::NetworkError;

        loop {
            let chunk = match resp.chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    if downloaded == expected_size {
                        success = true;
                    } else {
                        fail_reason = DownloadFailReason::PrematureEof;
                        tracing::warn!(
                            "Proxy download: premature EOF for {} ({} of {} bytes)",
                            fileid,
                            downloaded,
                            expected_size
                        );
                    }
                    break;
                }
                Err(e) => {
                    fail_reason = DownloadFailReason::NetworkError;
                    tracing::warn!(
                        "Proxy download: error for {} ({} of {} bytes): {}",
                        fileid,
                        downloaded,
                        expected_size,
                        e
                    );
                    break;
                }
            };

            if download_start.elapsed() > std::time::Duration::from_secs(300) {
                fail_reason = DownloadFailReason::Timeout;
                tracing::warn!("Proxy download: total time limit exceeded for {}", fileid);
                break;
            }

            sha1::Digest::update(&mut sha1, &chunk);

            if let Err(e) = file.write_all(&chunk).await {
                fail_reason = DownloadFailReason::DiskWriteError;
                tracing::warn!("Proxy download: disk write error for {}: {}", fileid, e);
                break;
            }
            downloaded += chunk.len() as u64;
            // Publish progress; body uses this to know how many bytes are safe to read.
            // Ignore send error — body may have dropped watch_rx (e.g. HEAD request).
            let _ = watch_tx.send(DownloadState::InProgress(downloaded));
        }

        // Wait for body to finish transmitting before finalizing the file.
        // Matches Java: checkFinalizeDownloadedFile() requires both streamThreadComplete
        // and proxyThreadComplete. Sender drop (client abort / HEAD) is also fine —
        // recv() returns Err which we ignore.
        let _ = proxy_done_rx.await;

        if success {
            let digest = utils::hex_encode(&sha1.finalize());
            if digest == expected_hash {
                if let Some(hv) = HVFile::from_fileid(fileid) {
                    let cache_path = hv.cache_path(cache_dir);
                    match utils::ensure_dir(cache_path.parent().unwrap()) {
                        Err(e) => {
                            tracing::warn!(
                                "Proxy download: cannot create cache dir for {}: {}",
                                fileid,
                                e
                            );
                            fail_reason = DownloadFailReason::FinalizeFailed;
                        }
                        Ok(()) => match tokio::fs::rename(&temp_file, &cache_path).await {
                            Ok(()) => {
                                tracing::info!(
                                    "Proxy download: cached {} ({} bytes)",
                                    fileid,
                                    expected_size
                                );
                                if let Some(cache) = cache_handler {
                                    cache.register_proxy_file(&hv);
                                }
                                let _ = watch_tx.send(DownloadState::Done);
                                return;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Proxy download: rename failed for {}: {}",
                                    fileid,
                                    e
                                );
                                fail_reason = DownloadFailReason::FinalizeFailed;
                            }
                        },
                    }
                }
            } else {
                tracing::warn!(
                    "Proxy download: SHA1 mismatch for {} (expected {}, got {})",
                    fileid,
                    expected_hash,
                    digest
                );
                fail_reason = DownloadFailReason::Sha1Mismatch;
            }
        }

        utils::remove_file(&temp_file);
        let _ = watch_tx.send(DownloadState::Failed(fail_reason));
    }

    fn create_temp_file(hv_file: &HVFile, config: &Config) -> Result<PathBuf> {
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
