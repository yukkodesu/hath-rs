use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::stats::Stats;
use crate::utils;
use reqwest::{Client, Url};
use sha1::Digest;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
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

/// State published by ProxyDownloadTask to StreamingBody via watch channel.
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
    /// Path to the temp file being written by ProxyDownloadTask.
    pub temp_file: PathBuf,
    /// Watch receiver for download progress. Move into StreamingBody (GET) or drop (HEAD).
    pub watch_rx: watch::Receiver<DownloadState>,
    /// Send on this when the proxy body has finished transmitting (GET) or is not needed (HEAD).
    /// ProxyDownloadTask waits for this signal before rename/delete — matching Java proxyThreadCompleted().
    pub proxy_done_tx: oneshot::Sender<()>,
}

trait ProxyReconnect {
    fn reconnect(&mut self) -> impl std::future::Future<Output = Result<reqwest::Response>> + Send;
}

struct LiveProxyReconnect {
    client: Arc<Client>,
    source_url: Url,
    hath_request: String,
    max_allowed_filesize: u64,
    expected_size: u64,
}

impl ProxyReconnect for LiveProxyReconnect {
    fn reconnect(&mut self) -> impl std::future::Future<Output = Result<reqwest::Response>> + Send {
        send_validated_proxy_request(
            &self.client,
            &self.source_url,
            &self.hath_request,
            self.max_allowed_filesize,
            self.expected_size,
        )
    }
}

struct ProxyDownloadTask<'a, R> {
    resp: reqwest::Response,
    watch_tx: watch::Sender<DownloadState>,
    proxy_done_rx: oneshot::Receiver<()>,
    temp_file: PathBuf,
    expected_size: u64,
    fileid: &'a str,
    expected_hash: &'a str,
    cache_dir: &'a Path,
    cache_handler: Option<&'a CacheHandler>,
    stats: &'a Stats,
    reconnect: R,
}

impl<R> ProxyDownloadTask<'_, R>
where
    R: ProxyReconnect + Send,
{
    async fn run(self) {
        let Self {
            resp,
            watch_tx,
            proxy_done_rx,
            temp_file,
            expected_size,
            fileid,
            expected_hash,
            cache_dir,
            cache_handler,
            stats,
            mut reconnect,
        } = self;

        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .open(&temp_file)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(
                    "Proxy download: cannot open temp file for {}: {}",
                    fileid,
                    e
                );
                utils::remove_file(&temp_file);
                let _ = watch_tx.send(DownloadState::Failed(DownloadFailReason::DiskWriteError));
                return;
            }
        };

        let mut success = false;
        let mut fail_reason = DownloadFailReason::NetworkError;
        let mut sha1 = sha1::Sha1::new();
        let mut next_resp = Some(resp);

        for attempt in 0..3 {
            if attempt > 0 {
                let _ = watch_tx.send(DownloadState::InProgress(0));
                let _ = file.seek(std::io::SeekFrom::Start(0)).await;
                let _ = file.set_len(0).await;

                tracing::debug!(
                    "Proxy download body-stage retry for {} ({} attempts left)",
                    fileid,
                    3 - attempt
                );

                match reconnect.reconnect().await {
                    Ok(r) => next_resp = Some(r),
                    Err(e) => {
                        fail_reason = DownloadFailReason::NetworkError;
                        tracing::warn!("Proxy download: reconnect failed for {}: {}", fileid, e);
                        continue;
                    }
                }
            }

            sha1 = sha1::Sha1::new();
            let mut downloaded = 0u64;
            let download_start = std::time::Instant::now();
            let mut resp = next_resp
                .take()
                .expect("proxy download attempt must have a validated response");

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

                if let Err(e) = ProxyFileDownloader::write_chunk_visible(&mut file, &chunk).await {
                    fail_reason = DownloadFailReason::DiskWriteError;
                    tracing::warn!("Proxy download: disk write error for {}: {}", fileid, e);
                    break;
                }
                downloaded += chunk.len() as u64;
                stats.record_bytes_rcvd(chunk.len() as u64);
                let _ = watch_tx.send(DownloadState::InProgress(downloaded));
            }

            if success {
                break;
            }
        }

        if success && let Err(e) = file.flush().await {
            success = false;
            fail_reason = DownloadFailReason::DiskWriteError;
            tracing::warn!("Proxy download: disk flush error for {}: {}", fileid, e);
        }
        drop(file);

        if success {
            stats.record_file_rcvd();
        } else {
            let _ = watch_tx.send(DownloadState::Failed(fail_reason));
            let _ = proxy_done_rx.await;
            utils::remove_file(&temp_file);
            return;
        }

        // Wait for body to finish transmitting before finalizing the file.
        // Matches Java: checkFinalizeDownloadedFile() requires both streamThreadComplete
        // and proxyThreadComplete. Sender drop (client abort / HEAD) is also fine —
        // recv() returns Err which we ignore.
        let _ = proxy_done_rx.await;

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
                    Ok(()) => {
                        match ProxyFileDownloader::move_temp_to_cache(&temp_file, &cache_path).await
                        {
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
            fail_reason = DownloadFailReason::Sha1Mismatch;
        }

        utils::remove_file(&temp_file);
        let _ = watch_tx.send(DownloadState::Failed(fail_reason));
    }
}

impl ProxyFileDownloader {
    /// Try each source once until a valid HTTP 200 response with matching
    /// Content-Length is obtained. Body-stage retry happens in the background
    /// download task after the downstream response is declared.
    pub async fn new(
        fileid: &str,
        sources: &[Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        stats: Arc<Stats>,
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
            let resp = match send_validated_proxy_request(
                client,
                source,
                &hath_request,
                config.max_allowed_filesize,
                hv_file.size as u64,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };

            // Good response — create watch channel and spawn download task.
            let temp_file = Self::create_temp_file(&hv_file, config)?;
            let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0));
            let (proxy_done_tx, proxy_done_rx) = oneshot::channel();

            let hash = hv_file.hash.clone();
            let expected_size = hv_file.size as u64;
            let fileid_owned = hv_file.fileid().clone();
            let cache_dir = config.cache_dir.clone();
            let tf = temp_file.clone();
            let stats = stats.clone();
            let client = client.clone();
            let retry_url = source.clone();
            let retry_hath = hath_request.clone();
            let max_allowed_filesize = config.max_allowed_filesize;

            tokio::spawn(async move {
                ProxyDownloadTask {
                    resp,
                    watch_tx,
                    proxy_done_rx,
                    temp_file: tf,
                    expected_size,
                    fileid: fileid_owned.as_str(),
                    expected_hash: hash.as_str(),
                    cache_dir: &cache_dir,
                    cache_handler: cache_handler.as_deref(),
                    stats: stats.as_ref(),
                    reconnect: LiveProxyReconnect {
                        client,
                        source_url: retry_url,
                        hath_request: retry_hath,
                        max_allowed_filesize,
                        expected_size,
                    },
                }
                .run()
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

        Err(last_err.unwrap_or_else(|| HathError::ProxyDownloader {
            status: 500,
            message: "all sources exhausted".into(),
        }))
    }

    async fn write_chunk_visible(file: &mut tokio::fs::File, chunk: &[u8]) -> io::Result<()> {
        file.write_all(chunk).await?;
        // Tokio file writes complete on a blocking worker after write() returns.
        // The watch offset must only advance once a separate reader can see the bytes.
        file.flush().await
    }

    async fn move_temp_to_cache(temp_file: &Path, cache_path: &Path) -> io::Result<()> {
        match tokio::fs::rename(temp_file, cache_path).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::debug!(
                    "Proxy download: rename failed from {} to {} ({}); falling back to copy",
                    temp_file.display(),
                    cache_path.display(),
                    e
                );
                Self::copy_temp_to_cache_and_delete(temp_file, cache_path).await
            }
        }
    }

    async fn copy_temp_to_cache_and_delete(temp_file: &Path, cache_path: &Path) -> io::Result<()> {
        tokio::fs::copy(temp_file, cache_path).await?;
        if let Err(e) = tokio::fs::remove_file(temp_file).await {
            tracing::warn!(
                "Proxy download: copied {} to cache but could not delete temp file: {}",
                temp_file.display(),
                e
            );
        }
        Ok(())
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
    let mut builder = proxy_client_builder();
    // Java: isImageProxyEnabled() checks host only; type defaults to
    // "socks"; port defaults to 1080 (socks) or 8080 (http).
    if let Some(proxy_host) = &config.image_proxy_host {
        let proxy_type = config.image_proxy_type.as_deref().unwrap_or("socks");
        let default_port = if proxy_type == "http" { 8080 } else { 1080 };
        let proxy_port = config.image_proxy_port.unwrap_or(default_port);
        if let Ok(proxy_url) = build_proxy_url(proxy_type, proxy_host, proxy_port)
            && let Ok(proxy) = reqwest::Proxy::all(proxy_url.as_str())
        {
            builder = builder.proxy(proxy);
        }
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

/// Reusable client building block: base builder with user-agent, connect,
/// and read timeouts matching Java ProxyFileDownloader connection settings.
fn proxy_client_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(30))
}

/// Send a Hath-Request-authenticated GET to an upstream image server.
/// User-Agent is set on the client at build time; only Hath-Request is per-call.
async fn send_proxy_request(
    client: &Client,
    url: &Url,
    hath_request: &str,
) -> reqwest::Result<reqwest::Response> {
    client
        .get(url.clone())
        .header("Hath-Request", hath_request)
        .header(
            "User-Agent",
            format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
        )
        .send()
        .await
}

async fn send_validated_proxy_request(
    client: &Client,
    url: &Url,
    hath_request: &str,
    max_allowed_filesize: u64,
    expected_size: u64,
) -> Result<reqwest::Response> {
    let resp = send_proxy_request(client, url, hath_request)
        .await
        .map_err(|e| HathError::Network(e.to_string()))?;
    validate_proxy_response(&resp, max_allowed_filesize, expected_size)?;
    Ok(resp)
}

fn validate_proxy_response(
    resp: &reqwest::Response,
    max_allowed_filesize: u64,
    expected_size: u64,
) -> Result<()> {
    if resp.status() != reqwest::StatusCode::OK {
        return Err(HathError::ProxyDownloader {
            status: 502,
            message: format!("upstream returned {}", resp.status()),
        });
    }

    let content_length = resp
        .content_length()
        .ok_or_else(|| HathError::ProxyDownloader {
            status: 502,
            message: "missing Content-Length".into(),
        })?;

    if content_length > max_allowed_filesize {
        return Err(HathError::ProxyDownloader {
            status: 502,
            message: format!(
                "contentLength {} exceeds max allowed filesize {}",
                content_length, max_allowed_filesize
            ),
        });
    }

    if content_length != expected_size {
        return Err(HathError::ProxyDownloader {
            status: 502,
            message: format!(
                "size mismatch: expected {}, got {}",
                expected_size, content_length
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::io::Read;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    #[test]
    fn test_build_proxy_url_handles_ipv6_host() {
        let url = build_proxy_url("socks", "::1", 1080).unwrap();

        assert_eq!(url.as_str(), "socks://[::1]:1080/");
    }

    #[tokio::test]
    async fn write_chunk_visible_makes_bytes_readable_before_progress_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy-write-visible");
        let mut writer = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();

        ProxyFileDownloader::write_chunk_visible(&mut writer, b"visible bytes")
            .await
            .unwrap();

        let mut out = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, b"visible bytes");
    }

    #[tokio::test]
    async fn copy_temp_to_cache_and_delete_removes_source_after_copy() {
        let dir = tempfile::tempdir().unwrap();
        let temp_path = dir.path().join("proxy-temp");
        let cache_path = dir.path().join("cache-file");
        std::fs::write(&temp_path, b"cache me").unwrap();

        ProxyFileDownloader::copy_temp_to_cache_and_delete(&temp_path, &cache_path)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&cache_path).unwrap(), b"cache me");
        assert!(!temp_path.exists());
    }

    fn test_response(
        status: u16,
        content_length: Option<u64>,
        body: impl Into<Vec<u8>>,
    ) -> reqwest::Response {
        let mut builder = http::Response::builder().status(status);
        if let Some(length) = content_length {
            builder = builder.header(http::header::CONTENT_LENGTH, length);
        }
        builder.body(body.into()).unwrap().into()
    }

    struct UnknownLenBody(Option<Bytes>);

    impl http_body::Body for UnknownLenBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.take().map(|data| Ok(http_body::Frame::data(data))))
        }
    }

    fn test_response_without_known_length(
        status: u16,
        body: impl Into<Vec<u8>>,
    ) -> reqwest::Response {
        http::Response::builder()
            .status(status)
            .body(reqwest::Body::wrap(UnknownLenBody(Some(Bytes::from(
                body.into(),
            )))))
            .unwrap()
            .into()
    }

    struct QueueReconnect {
        responses: Arc<Mutex<VecDeque<Result<reqwest::Response>>>>,
    }

    impl ProxyReconnect for QueueReconnect {
        fn reconnect(
            &mut self,
        ) -> impl std::future::Future<Output = Result<reqwest::Response>> + Send {
            std::future::ready(self.responses.lock().unwrap().pop_front().unwrap())
        }
    }

    const FINAL_BODY: &[u8] = b"0123456789abcdefghij";

    struct BodyRetryRun {
        state: DownloadState,
        cache_body: Vec<u8>,
        remaining_reconnects: usize,
    }

    impl BodyRetryRun {
        fn assert_done_with_final_body(self) {
            assert!(matches!(self.state, DownloadState::Done));
            assert_eq!(self.remaining_reconnects, 0);
            assert_eq!(self.cache_body, FINAL_BODY);
        }
    }

    fn final_body_response() -> Result<reqwest::Response> {
        Ok(test_response(
            200,
            Some(FINAL_BODY.len() as u64),
            FINAL_BODY.to_vec(),
        ))
    }

    fn upstream_status_error(message: &str) -> Result<reqwest::Response> {
        Err(HathError::ProxyDownloader {
            status: 502,
            message: message.into(),
        })
    }

    async fn run_body_retry(reconnects: VecDeque<Result<reqwest::Response>>) -> BodyRetryRun {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let temp_file = dir.path().join("proxy-temp");
        std::fs::write(&temp_file, []).unwrap();

        let final_hash = utils::sha1_bytes(FINAL_BODY);
        let fileid = format!("{}-{}-jpg", final_hash, FINAL_BODY.len());
        let hv = HVFile::from_fileid(&fileid).unwrap();
        let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0));
        let (proxy_done_tx, proxy_done_rx) = oneshot::channel();
        let stats = Stats::new();
        let reconnects = Arc::new(Mutex::new(reconnects));
        drop(proxy_done_tx);

        ProxyDownloadTask {
            resp: test_response(200, Some(FINAL_BODY.len() as u64), b"partial".to_vec()),
            watch_tx,
            proxy_done_rx,
            temp_file,
            expected_size: FINAL_BODY.len() as u64,
            fileid: &fileid,
            expected_hash: &final_hash,
            cache_dir: &cache_dir,
            cache_handler: None,
            stats: &stats,
            reconnect: QueueReconnect {
                responses: reconnects.clone(),
            },
        }
        .run()
        .await;

        BodyRetryRun {
            state: await_proxy_terminal_state(watch_rx).await,
            cache_body: std::fs::read(hv.cache_path(&cache_dir)).unwrap(),
            remaining_reconnects: reconnects.lock().unwrap().len(),
        }
    }

    async fn await_proxy_terminal_state(mut rx: watch::Receiver<DownloadState>) -> DownloadState {
        loop {
            let state = rx.borrow_and_update().clone();
            if matches!(state, DownloadState::Done | DownloadState::Failed(_)) {
                return state;
            }
            rx.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn body_retry_skips_invalid_reconnect_response_and_can_succeed() {
        run_body_retry(VecDeque::from([
            upstream_status_error("upstream returned 500 Internal Server Error"),
            final_body_response(),
        ]))
        .await
        .assert_done_with_final_body();
    }

    #[tokio::test]
    async fn body_retry_continues_after_reconnect_failure_and_can_succeed() {
        run_body_retry(VecDeque::from([
            Err(HathError::Network("connection closed".into())),
            final_body_response(),
        ]))
        .await
        .assert_done_with_final_body();
    }

    #[test]
    fn validated_proxy_response_rejects_wrong_content_length() {
        let response = test_response(200, Some(12), b"wrong length".to_vec());
        let err = validate_proxy_response(&response, 1_000_000, 20).unwrap_err();

        assert!(matches!(err, HathError::ProxyDownloader { .. }));
    }

    #[test]
    fn validated_proxy_response_rejects_status_and_missing_length() {
        let bad_status = test_response(500, Some(20), vec![b'x'; 20]);
        let missing_length = test_response_without_known_length(200, vec![b'x'; 20]);

        assert!(matches!(
            validate_proxy_response(&bad_status, 1_000_000, 20),
            Err(HathError::ProxyDownloader { .. })
        ));
        assert!(matches!(
            validate_proxy_response(&missing_length, 1_000_000, 20),
            Err(HathError::ProxyDownloader { .. })
        ));
    }
}
