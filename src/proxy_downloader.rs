use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::utils;
use reqwest::{Client, Url};
use sha1::Digest;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;

/// Streaming proxy download: downloads from an upstream image server
/// while simultaneously serving data to the requesting HTTPSession.
///
/// After construction, the download runs in a background tokio task.
/// The caller can create a [`crate::body::StreamingBody`] via
/// [`StreamingBody::new_proxy`] to stream data to the client as it arrives.
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
    success: Arc<std::sync::Mutex<bool>>,
}

impl ProxyFileDownloader {
    /// Initialize with a list of upstream source URLs.
    /// Returns a handle that can be used to stream data to the client.
    pub async fn new(
        fileid: &str,
        sources: &[Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
    ) -> Result<Self> {
        let hv_file = HVFile::from_fileid(fileid)
            .ok_or_else(|| HathError::Parse(format!("invalid fileid: {}", fileid)))?;

        let client = Client::builder()
            .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
            .build()
            .map_err(|e| HathError::Network(e.to_string()))?;

        let mut last_err = None;

        for source in sources {
            // Java: ProxyFileDownloader has inner retry loop (3 attempts per source)
            for attempt in 0..3u32 {
                match Self::try_source(&client, source, &hv_file, config, cache_handler.clone()).await {
                    Ok(this) => return Ok(this),
                    Err(e) => {
                        if attempt < 2 {
                            tracing::debug!(
                                "Proxy download attempt {} failed for {}: {}, retrying...",
                                attempt + 1, source, e
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

        Err(last_err.unwrap_or_else(|| HathError::Network("all sources exhausted".into())))
    }

    async fn try_source(
        client: &Client,
        source: &Url,
        hv_file: &HVFile,
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
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

        let mut resp = client
            .get(source.clone())
            .header("Hath-Request", &hath_request)
            .header(
                "User-Agent",
                format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
            )
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| HathError::Network(e.to_string()))?;

        let content_length = resp
            .content_length()
            .ok_or_else(|| HathError::Network("missing Content-Length".into()))?
            as u64;

        if content_length != hv_file.size as u64 {
            return Err(HathError::Network(format!(
                "size mismatch: expected {}, got {}",
                hv_file.size, content_length
            )));
        }

        // Create temp file
        let temp_file = config
            .temp_dir
            .join(format!("proxyfile_{}", hv_file.fileid().as_str()));
        let write_offset = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let notify = Arc::new(Notify::new());
        let body_done_notify = Arc::new(Notify::new());
        let success = Arc::new(std::sync::Mutex::new(false));

        let this = Self {
            content_length: hv_file.size as usize,
            content_type: hv_file.mime_type().to_string(),
            temp_file: temp_file.clone(),
            write_offset: write_offset.clone(),
            total_size: content_length,
            notify: notify.clone(),
            body_done_notify: body_done_notify.clone(),
            success,
        };

        // Spawn download task
        let wo = write_offset.clone();
        let tf = temp_file.clone();
        let not = notify.clone();
        let bdn = body_done_notify.clone();
        let hash = hv_file.hash.clone();
        let expected_size = hv_file.size as u64;
        let fileid_owned = hv_file.fileid();
        let cache_dir = config.cache_dir.clone();

        tokio::spawn(async move {
            let mut file = match tokio::fs::File::create(&tf).await {
                Ok(f) => f,
                Err(_) => {
                    not.notify_waiters();
                    return;
                }
            };

            let mut sha1 = sha1::Sha1::new();
            let mut downloaded = 0u64;

            loop {
                let chunk = match resp.chunk().await {
                    Ok(Some(data)) => data,
                    Ok(None) => break,
                    Err(_) => break,
                };
                sha1::Digest::update(&mut sha1, &chunk);
                if file.write_all(&chunk).await.is_err() {
                    break;
                }
                downloaded += chunk.len() as u64;
                wo.store(downloaded, std::sync::atomic::Ordering::SeqCst);
                not.notify_waiters();
            }

            drop(file);

            // Final wake so the body can read any remaining chunks.
            not.notify_waiters();

            // Wait for the body to finish reading, then delete temp.
            // Java: checkFinalizeDownloadedFile — proxyThreadComplete
            // notify_one() stores a permit, so even if body signaled first we'll wake.
            let timeout = std::time::Duration::from_secs(300);
            tokio::select! {
                _ = bdn.notified() => {},
                _ = tokio::time::sleep(timeout) => {
                    tracing::warn!(
                        "Proxy download: timeout waiting for body {}",
                        fileid_owned
                    );
                }
            }

            // Java: checkFinalizeDownloadedFile — import to cache after body finishes reading.
            // Use copy (not rename) so the body can still read from temp while we import.
            let digest = utils::hex_encode(&sha1.finalize());
            if downloaded == expected_size
                && digest == hash.as_str()
                && let Some(hv) = HVFile::from_fileid(fileid_owned.as_str())
            {
                let cache_path = hv.cache_path(&cache_dir);
                if let Ok(()) = utils::ensure_dir(cache_path.parent().unwrap())
                    && std::fs::copy(&tf, &cache_path).is_ok()
                        && let Some(ref cache) = cache_handler {
                            cache.register_proxy_file(&hv);
                        }
            }
            utils::remove_file(&tf);
        });

        Ok(this)
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

    /// Check if the download was successful.
    pub fn is_successful(&self) -> bool {
        *self.success.lock().unwrap()
    }
}
