//! Streaming HTTP body with optional per-chunk bandwidth throttling.
//!
//! Java equivalent: HTTPSession.write body loop + HTTPBandwidthMonitor.waitForQuota
//! Chunk size: 1460 bytes (Java Settings.TCP_PACKET_SIZE)
//!
//! Supports two data sources:
//! - `Static`: pre-loaded `Bytes` (for cached files, speedtest, text responses)
//! - `Proxy`: streaming from a temp file written by a background proxy download task

use bytes::{Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use rand::Rng;
use sha1::Digest;
use std::convert::Infallible;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

use crate::bandwidth::BandwidthMonitor;

/// Java Settings.TCP_PACKET_SIZE = 1460
const TCP_PACKET_SIZE: usize = 1460;

/// Where the body data comes from.
enum DataSource {
    /// Pre-loaded data (for small text responses).
    Static { data: Bytes },
    /// Generate random data on-the-fly per chunk (speedtest).
    Random,
    /// Streaming from a file with optional inline SHA1 verification.
    /// Java: HTTPResponseProcessorFile — reads chunks via FileChannel,
    /// computes SHA1 incrementally, deletes corrupt file in cleanup().
    File {
        file: std::fs::File,
        sha1: sha1::Sha1,
        expected_hash: String,
        /// Deleted after send if SHA1 mismatches.
        to_delete: Option<PathBuf>,
        cache_handler: Option<Arc<crate::cache::CacheHandler>>,
    },
    /// Proxy download: data is being written to a temp file by a background
    /// download task. We read it incrementally as it becomes available.
    Proxy {
        temp_file: PathBuf,
        write_offset: Arc<AtomicU64>,
        notify: Arc<Notify>,
        start_time: Instant,
        /// Wakes the download task when body finishes reading.
        body_done_notify: Arc<Notify>,
        /// Set by the download task when it finishes (success or failure).
        /// The body reader checks this to avoid waiting forever for
        /// data that will never arrive.
        download_done: Arc<AtomicBool>,
    },
}

/// A Hyper [`Body`] that streams data in chunks.
///
/// When `bwm` is [`None`], the entire remaining data is returned in a single
/// frame (equivalent to [`http_body_util::Full`]).
///
/// When `bwm` is [`Some`], data is split into 1460-byte chunks and each chunk
/// is throttled via [`BandwidthMonitor::wait_for_quota`] before being yielded.
///
/// For proxy mode, the body waits for the download task to write data to the
/// temp file before reading and yielding it.
pub struct StreamingBody {
    source: DataSource,
    offset: usize,
    total_size: usize,
    bwm: Option<Arc<BandwidthMonitor>>,
    /// Pending throttle future.
    throttle_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    /// Whether a throttle just completed (prevents double-throttle for same chunk).
    throttled: bool,
    /// Pending wait-for-data future (proxy mode only).
    wait_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl StreamingBody {
    /// Create a new static body from pre-loaded data (zero-copy for `Bytes::from_static`).
    ///
    /// * `data` - The full response body as `Bytes`. Use `Bytes::from_static(b"...")`
    ///   for static content, `Bytes::from(vec)` for owned data, or
    ///   `Bytes::copy_from_slice(s)` for borrowed slices.
    /// * `bwm`  - Optional bandwidth monitor for per-chunk throttling.
    pub fn new(data: Bytes, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        Self::from_bytes(data, bwm)
    }

    /// Zero-allocation empty body (for HEAD responses).
    pub fn empty() -> Self {
        Self::from_bytes(Bytes::new(), None)
    }

    /// Create a file-streaming body. Reads chunks incrementally, optionally
    /// verifying SHA1 and deleting corrupt files after the response is sent.
    /// Java: HTTPResponseProcessorFile with verifyFileIntegrity.
    pub fn new_file(
        path: PathBuf,
        total_size: usize,
        expected_hash: String,
        verify: bool,
        cache_handler: Option<Arc<crate::cache::CacheHandler>>,
        bwm: Option<Arc<BandwidthMonitor>>,
    ) -> std::io::Result<Self> {
        let file = std::fs::File::open(&path)?;
        Ok(Self {
            source: DataSource::File {
                file,
                sha1: sha1::Sha1::new(),
                expected_hash,
                to_delete: if verify { Some(path) } else { None },
                cache_handler,
            },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            wait_fut: None,
        })
    }

    /// Create a body that generates random data per-chunk (zero pre-allocation).
    /// Java: HTTPResponseProcessorSpeedtest.
    pub fn new_random(total_size: usize, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        Self {
            source: DataSource::Random,
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            wait_fut: None,
        }
    }

    fn from_bytes(data: Bytes, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        let total_size = data.len();
        Self {
            source: DataSource::Static { data },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            wait_fut: None,
        }
    }

    /// Create a proxy-streaming body.
    ///
    /// The body reads data from `temp_file` as the background download task
    /// writes to it. Progress is tracked via `write_offset` and signaled
    /// via `notify`.
    ///
    /// * `total_size` - Expected total file size in bytes.
    /// * `temp_file`  - Path to the temp file being written by the downloader.
    /// * `write_offset` - Atomic counter of bytes written so far.
    /// * `notify`     - Notified each time new data is written.
    /// * `body_done_notify` - Wakes the download task when body signals completion.
    /// * `bwm`        - Optional bandwidth monitor.
    pub fn new_proxy(
        total_size: usize,
        temp_file: PathBuf,
        write_offset: Arc<AtomicU64>,
        notify: Arc<Notify>,
        body_done_notify: Arc<Notify>,
        download_done: Arc<AtomicBool>,
        bwm: Option<Arc<BandwidthMonitor>>,
    ) -> Self {
        Self {
            source: DataSource::Proxy {
                temp_file,
                write_offset,
                notify,
                start_time: Instant::now(),
                body_done_notify,
                download_done,
            },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            wait_fut: None,
        }
    }
}

impl StreamingBody {
    /// Signal the download task that the body has finished reading.
    /// For File mode, verify SHA1 and delete corrupt file (Java: cleanup()).
    fn finish(&mut self) {
        match &mut self.source {
            DataSource::File { sha1, expected_hash, to_delete, cache_handler, .. } => {
                if let Some(path) = to_delete.take() {
                    let actual = crate::utils::hex_encode(&sha1.finalize_reset());
                    if actual != *expected_hash {
                        tracing::warn!(
                            "Corrupt file {:?} (expected {}, got {}); deleting from cache",
                            path, expected_hash, actual
                        );
                        if let Some(ch) = cache_handler {
                            // Extract fileid from path
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                let _ = ch.delete_file_from_cache(name);
                            }
                        }
                    }
                }
            }
            DataSource::Proxy { body_done_notify, .. } => {
                body_done_notify.notify_one();
            }
            DataSource::Static { .. } | DataSource::Random => {}
        }
    }
}

impl Drop for StreamingBody {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Body for StreamingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            // Step 1: Poll pending throttle future.
            // If it just completed, record that so we produce the chunk below
            // instead of starting another throttle for the same data.
            if let Some(ref mut fut) = self.throttle_fut {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.throttle_fut = None;
                        self.throttled = true;
                    }
                }
            }

            // Step 2: Check if done
            if self.offset >= self.total_size {
                // Signal download task that body has finished reading.
                // Java: proxyThreadCompleted() → proxyThreadComplete = true
                self.finish();
                return Poll::Ready(None);
            }

            // Step 3: Proxy mode — wait for data to be available.
            // Java: HTTPResponseProcessorProxy.getPreparedTCPBuffer()
            //   while (readThreshold > proxyDownloader.getCurrentWriteoff()) { sleep(10); ... }
            //
            // First, check whether we need to wait, using a short-lived borrow
            // so we can mutate self.wait_fut below without borrowing self.source.
            // Java: int nextReadThrehold = Math.min(getContentLength(), readoff + tcpBuffer.limit());
            //       while (nextReadThrehold > proxyDownloader.getCurrentWriteoff()) { sleep(10); }
            let need_wait = match &self.source {
                DataSource::Proxy { write_offset, .. } => {
                    let desired_end =
                        (self.offset + TCP_PACKET_SIZE).min(self.total_size) as u64;
                    write_offset.load(Ordering::SeqCst) < desired_end
                }
                DataSource::Static { .. } | DataSource::File { .. } | DataSource::Random => false,
            };

            // Clear stale wait_fut when data is now available.
            if !need_wait && self.wait_fut.is_some() {
                self.wait_fut = None;
            }

            if need_wait {
                // Re-borrow self.source to get notify + start_time + download_done.
                if let DataSource::Proxy { notify, start_time, download_done, .. } = &self.source {
                    // If the download task has completed (success or failure) and
                    // there isn't enough data, terminate immediately. No point
                    // waiting for data that will never arrive.
                    if download_done.load(Ordering::SeqCst) {
                        tracing::debug!(
                            "ProxyStreamingBody: download done, terminating at offset {}",
                            self.offset
                        );
                        return Poll::Ready(None);
                    }

                    // Check timeout (5 minutes, Java: timeout > 30000 * 10ms = 300s)
                    if start_time.elapsed() > Duration::from_secs(300) {
                        tracing::warn!(
                            "ProxyStreamingBody: timeout waiting for data at offset {}",
                            self.offset
                        );
                        return Poll::Ready(None);
                    }

                    // Start/poll wait future
                    if self.wait_fut.is_none() {
                        let n = notify.clone();
                        self.wait_fut = Some(Box::pin(async move {
                            n.notified().await;
                        }));
                    }
                    if let Some(ref mut fut) = self.wait_fut {
                        match fut.as_mut().poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(()) => {
                                self.wait_fut = None;
                            }
                        }
                    }
                }
                continue;
            }

            // Step 4: If throttling is active and we haven't just completed a
            // throttle cycle, start a new one for the next chunk.
            if !self.throttled && self.bwm.is_some() && self.throttle_fut.is_none() {
                let chunk_size = TCP_PACKET_SIZE.min(self.total_size - self.offset);
                let bwm = self.bwm.clone().unwrap();
                self.throttle_fut = Some(Box::pin(async move {
                    bwm.wait_for_quota(chunk_size).await;
                }));
                continue;
            }
            
            self.throttled = false;

            // Step 5: Produce the chunk
            // Static without throttling → send all remaining data at once.
            // Proxy always uses TCP_PACKET_SIZE chunks (matching Java HTTPSession.write loop).
            let chunk_size = match &self.source {
                DataSource::Static { .. } if self.bwm.is_none() => self.total_size - self.offset,
                _ => TCP_PACKET_SIZE,
            };
            let end = (self.offset + chunk_size).min(self.total_size);

            let offset = self.offset;
            let chunk: Bytes = match &mut self.source {
                DataSource::Static { data } => data.slice(offset..end),
                DataSource::Random => {
                    let actual_size = end - offset;
                    let mut buf = BytesMut::zeroed(actual_size);
                    rand::rng().fill_bytes(&mut buf);
                    buf.freeze()
                }
                DataSource::File { file, sha1, .. } => {
                    let actual_size = end - offset;
                    let mut buf = BytesMut::zeroed(actual_size);
                    match file.seek(SeekFrom::Start(offset as u64)) {
                        Ok(_) => match file.read(&mut buf[..actual_size]) {
                            Ok(0) => return Poll::Ready(None),
                            Ok(n) => {
                                sha1::Digest::update(sha1, &buf[..n]);
                                buf.truncate(n);
                                buf.freeze()
                            }
                            Err(e) => {
                                tracing::warn!("File body: read error at offset {}: {}", offset, e);
                                return Poll::Ready(None);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("File body: seek error at offset {}: {}", offset, e);
                            return Poll::Ready(None);
                        }
                    }
                }
                DataSource::Proxy { temp_file, .. } => {
                    let actual_size = end - offset;
                    let mut buf = BytesMut::zeroed(actual_size);
                    match std::fs::File::open(&*temp_file) {
                        Ok(mut file) => {
                            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                                return Poll::Ready(None);
                            }
                            match file.read(&mut buf[..actual_size]) {
                                Ok(0) => return Poll::Ready(None),
                                Ok(n) => {
                                    buf.truncate(n);
                                    buf.freeze()
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "ProxyStreamingBody: read error at offset {}: {}",
                                        offset, e
                                    );
                                    return Poll::Ready(None);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "ProxyStreamingBody: cannot open temp file {}: {}",
                                temp_file.display(),
                                e
                            );
                            return Poll::Ready(None);
                        }
                    }
                }
            };

            self.offset += chunk.len();

            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        if self.total_size > 0 {
            hint.set_exact(self.total_size as u64);
        }
        hint
    }
}
