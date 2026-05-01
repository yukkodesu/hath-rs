use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::utils;
use bytes::Bytes;
use reqwest::{Client, Url};
use sha1::Digest;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// Streaming proxy download: downloads from an upstream image server
/// while simultaneously serving data to the requesting client.
///
/// After construction the download runs in a background tokio task.
/// Chunks are delivered through an mpsc channel; the body polls the
/// receiver directly — no shared file, no AtomicU64, no Notify.
pub struct ProxyFileDownloader {
    pub content_length: usize,
    pub content_type: String,
    /// Receiving end of the download channel. Moved into StreamingBody.
    pub rx: mpsc::Receiver<Bytes>,
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

                // Good response — create channel and spawn download task.
                let temp_file = Self::create_temp_file(&hv_file, config)?;
                let (tx, rx) = mpsc::channel::<Bytes>(8);

                let hash = hv_file.hash.clone();
                let expected_size = hv_file.size as u64;
                let fileid_owned = hv_file.fileid().clone();
                let cache_dir = config.cache_dir.clone();
                let tf = temp_file.clone();

                tokio::spawn(async move {
                    Self::download_task(
                        resp,
                        tx,
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
                    rx,
                });
            }
        }

        Err(last_err.unwrap_or_else(|| HathError::ProxyDownloader {
            status: 500,
            message: "all sources exhausted".into(),
        }))
    }

    /// Stream `resp` to `tx` (for the body) and to `temp_file` (for cache).
    /// On success + SHA1 match: rename temp_file → cache and register.
    /// On any failure or body drop (tx.send returns Err): delete temp_file.
    async fn download_task(
        mut resp: reqwest::Response,
        tx: mpsc::Sender<Bytes>,
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
                return;
            }
        };

        let mut sha1 = sha1::Sha1::new();
        let mut downloaded = 0u64;
        let download_start = std::time::Instant::now();
        let mut success = false;

        loop {
            let chunk = match resp.chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    if downloaded == expected_size {
                        success = true;
                    } else {
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
                tracing::warn!("Proxy download: total time limit exceeded for {}", fileid);
                break;
            }

            sha1::Digest::update(&mut sha1, &chunk);

            // Write to temp file first, then send to body.
            // If write fails, abort (don't send corrupt data).
            if file.write_all(&chunk).await.is_err() {
                tracing::warn!("Proxy download: disk write error for {}", fileid);
                break;
            }
            downloaded += chunk.len() as u64;

            // Send to body. chunk is already Bytes — move it directly (zero-copy).
            // If body dropped rx, stop downloading.
            if tx.send(chunk).await.is_err() {
                tracing::debug!("Proxy download: body dropped, stopping for {}", fileid);
                utils::remove_file(&temp_file);
                return;
            }
        }

        // tx drops here — body will see Ready(None) on next poll.
        drop(tx);

        if success {
            let digest = utils::hex_encode(&sha1.finalize());
            if digest == expected_hash {
                if let Some(hv) = HVFile::from_fileid(fileid) {
                    let cache_path = hv.cache_path(cache_dir);
                    if let Ok(()) = utils::ensure_dir(cache_path.parent().unwrap()) {
                        match tokio::fs::rename(&temp_file, &cache_path).await {
                            Ok(()) => {
                                tracing::info!(
                                    "Proxy download: cached {} ({} bytes)",
                                    fileid,
                                    expected_size
                                );
                                if let Some(cache) = cache_handler {
                                    cache.register_proxy_file(&hv);
                                }
                                return; // temp_file renamed, don't delete
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Proxy download: rename failed for {}: {}",
                                    fileid,
                                    e
                                );
                            }
                        }
                    }
                }
            } else {
                tracing::warn!(
                    "Proxy download: SHA1 mismatch for {} (expected {}, got {})",
                    fileid,
                    expected_hash,
                    digest
                );
            }
        }

        utils::remove_file(&temp_file);
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
