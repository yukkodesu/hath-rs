//! Streaming HTTP body with optional per-chunk bandwidth throttling.
//!
//! Java equivalent: HTTPSession.write body loop + HTTPBandwidthMonitor.waitForQuota
//! Chunk size: 1460 bytes (Java Settings.TCP_PACKET_SIZE)
//!
//! Supports two data sources:
//! - `Static`: pre-loaded `Bytes` (for cached files, speedtest, text responses)
//! - `Proxy`: streaming from a temp file written by a background proxy download task

use bytes::{BufMut, Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use rand::{Rng, RngExt};
use sha1::Digest;
use std::convert::Infallible;
use std::future::Future;
use std::io::Read;
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
const FILE_BUFFER_SIZE: usize = 65_536;
const SPEEDTEST_RANDOM_LENGTH: usize = 8_192;

/// Where the body data comes from.
enum DataSource {
    /// Pre-loaded data (for small text responses).
    Static { data: Bytes },
    /// Generate speedtest chunks from a reusable random pool.
    Random { random_bytes: Box<[u8]> },
    /// Streaming from a file with optional inline SHA1 verification.
    /// Java: HTTPResponseProcessorFile — reads chunks via FileChannel,
    /// computes SHA1 incrementally, deletes corrupt file in cleanup().
    File {
        file: std::fs::File,
        file_buf: BytesMut,
        sha1: sha1::Sha1,
        expected_hash: String,
        /// Deleted after send if SHA1 mismatches.
        to_delete: Option<PathBuf>,
        cache_handler: Option<Arc<crate::cache::CacheHandler>>,
    },
    /// Proxy download: data is being written to a temp file by a background
    /// download task. We read it incrementally as it becomes available.
    Proxy {
        /// File handle opened once at construction; read sequentially.
        file: std::fs::File,
        file_buf: BytesMut,
        /// Kept for error messages only.
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
    completion: BodyCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyCompletion {
    Running,
    Complete,
    Incomplete(BodyIncompleteReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyIncompleteReason {
    Unknown,
    FileEndedEarly,
    FileReadError,
    ProxyEndedEarly,
    ProxyTimeout,
    ProxyReadError,
}

impl BodyIncompleteReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::FileEndedEarly => "file_ended_early",
            Self::FileReadError => "file_read_error",
            Self::ProxyEndedEarly => "proxy_ended_early",
            Self::ProxyTimeout => "proxy_timeout",
            Self::ProxyReadError => "proxy_read_error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyCompletionSnapshot {
    pub completion: BodyCompletion,
    pub offset: usize,
    pub total_size: usize,
}

pub trait BodyCompletionStatus {
    fn completion_status(&self) -> BodyCompletionSnapshot;
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

    /// Create a file-streaming body from an already-open file handle.
    /// The caller is responsible for opening the file and verifying its size.
    /// Java: HTTPResponseProcessorFile with verifyFileIntegrity.
    pub fn new_file(
        file: std::fs::File,
        path: PathBuf,
        total_size: usize,
        expected_hash: String,
        verify: bool,
        cache_handler: Option<Arc<crate::cache::CacheHandler>>,
        bwm: Option<Arc<BandwidthMonitor>>,
    ) -> Self {
        Self {
            source: DataSource::File {
                file,
                file_buf: BytesMut::with_capacity(FILE_BUFFER_SIZE),
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
            completion: BodyCompletion::Running,
        }
    }

    /// Create a body that serves random-looking chunks from a reusable 8 KiB pool.
    /// Java: HTTPResponseProcessorSpeedtest.
    pub fn new_random(total_size: usize, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        let mut random_bytes = vec![0; SPEEDTEST_RANDOM_LENGTH].into_boxed_slice();
        rand::rng().fill_bytes(&mut random_bytes);
        Self {
            source: DataSource::Random { random_bytes },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            wait_fut: None,
            completion: BodyCompletion::Running,
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
            completion: BodyCompletion::Running,
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
    ) -> std::io::Result<Self> {
        let file = std::fs::File::open(&temp_file)?;
        Ok(Self {
            source: DataSource::Proxy {
                file,
                file_buf: BytesMut::with_capacity(FILE_BUFFER_SIZE),
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
            completion: BodyCompletion::Running,
        })
    }
}

impl StreamingBody {
    /// Signal the download task that the body has finished reading.
    /// For File mode, verify SHA1 and delete corrupt file (Java: cleanup()).
    fn finish(&mut self) {
        self.finish_with(None);
    }

    fn finish_with(&mut self, reason: Option<BodyIncompleteReason>) {
        let completed = self.offset >= self.total_size;
        if self.completion == BodyCompletion::Running {
            self.completion = if completed {
                BodyCompletion::Complete
            } else {
                BodyCompletion::Incomplete(reason.unwrap_or(BodyIncompleteReason::Unknown))
            };
        }
        match &mut self.source {
            DataSource::File {
                sha1,
                expected_hash,
                to_delete,
                cache_handler,
                ..
            } => {
                // Java skips integrity verification if the remote client closes
                // early, because the digest only covers bytes that were sent.
                if !completed {
                    return;
                }
                if let Some(path) = to_delete.take() {
                    let actual = crate::utils::hex_encode(&sha1.finalize_reset());
                    if actual != *expected_hash {
                        tracing::warn!(
                            "Corrupt file {:?} (expected {}, got {}); deleting from cache",
                            path,
                            expected_hash,
                            actual
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
            DataSource::Proxy {
                body_done_notify, ..
            } => {
                body_done_notify.notify_one();
            }
            DataSource::Static { .. } | DataSource::Random { .. } => {}
        }
    }

    fn fill_file_buffer(
        file: &mut std::fs::File,
        file_buf: &mut BytesMut,
        need: usize,
    ) -> std::io::Result<usize> {
        Self::fill_file_buffer_limited(file, file_buf, need, usize::MAX)
    }

    fn fill_file_buffer_limited(
        file: &mut std::fs::File,
        file_buf: &mut BytesMut,
        need: usize,
        max_read: usize,
    ) -> std::io::Result<usize> {
        let mut remaining_read = max_read;
        while file_buf.len() < need && remaining_read > 0 {
            let read_target = FILE_BUFFER_SIZE
                .saturating_sub(file_buf.len())
                .max(need - file_buf.len())
                .min(remaining_read);
            file_buf.reserve(read_target);

            let spare = file_buf.chunk_mut();
            let to_read = read_target.min(spare.len());
            if to_read == 0 {
                break;
            }

            // SAFETY: `chunk_mut` returns spare, uninitialized capacity owned by
            // `file_buf`. `read` initializes exactly the returned byte count, and
            // `advance_mut` exposes only those initialized bytes.
            let dst = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr(), to_read) };
            let n = file.read(dst)?;
            if n == 0 {
                break;
            }
            remaining_read = remaining_read.saturating_sub(n);
            unsafe {
                file_buf.advance_mut(n);
            }
        }

        Ok(file_buf.len())
    }
}

impl BodyCompletionStatus for StreamingBody {
    fn completion_status(&self) -> BodyCompletionSnapshot {
        BodyCompletionSnapshot {
            completion: self.completion,
            offset: self.offset,
            total_size: self.total_size,
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
                DataSource::Proxy {
                    file_buf,
                    write_offset,
                    ..
                } => {
                    let desired = TCP_PACKET_SIZE.min(self.total_size - self.offset);
                    file_buf.len() < desired
                        && write_offset.load(Ordering::SeqCst)
                            < (self.offset + desired).min(self.total_size) as u64
                }
                DataSource::Static { .. } | DataSource::File { .. } | DataSource::Random { .. } => {
                    false
                }
            };

            // Clear stale wait_fut when data is now available.
            if !need_wait && self.wait_fut.is_some() {
                self.wait_fut = None;
            }

            if need_wait {
                // Check termination conditions first (short borrow on self.source).
                let early_exit = match &self.source {
                    DataSource::Proxy {
                        download_done,
                        start_time,
                        ..
                    } => {
                        if download_done.load(Ordering::SeqCst) {
                            tracing::debug!(
                                "ProxyStreamingBody: download done, terminating at offset {}",
                                self.offset
                            );
                            Some(BodyIncompleteReason::ProxyEndedEarly)
                        } else if start_time.elapsed() > Duration::from_secs(300) {
                            tracing::warn!(
                                "ProxyStreamingBody: timeout waiting for data at offset {}",
                                self.offset
                            );
                            Some(BodyIncompleteReason::ProxyTimeout)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                if let Some(reason) = early_exit {
                    self.finish_with(Some(reason));
                    return Poll::Ready(None);
                }

                // Re-borrow self.source to get notify for the wait future.
                if let DataSource::Proxy { notify, .. } = &self.source {
                    if self.wait_fut.is_none() {
                        let n = notify.clone();
                        self.wait_fut = Some(Box::pin(async move {
                            tokio::select! {
                                _ = n.notified() => {},
                                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                            }
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

            // Step 5: Produce the chunk.
            // Static without throttling → send all remaining data at once.
            // Proxy always uses TCP_PACKET_SIZE chunks (matching Java HTTPSession.write loop).
            let chunk_size = match &self.source {
                DataSource::Static { .. } if self.bwm.is_none() => self.total_size - self.offset,
                _ => TCP_PACKET_SIZE,
            };
            let end = (self.offset + chunk_size).min(self.total_size);

            let offset = self.offset;
            let total_size = self.total_size;

            // Separate early-termination from chunk production so that
            // self.finish() — which borrows self.source — is never called
            // while self.source is mutably borrowed by the match arm.
            enum ChunkResult {
                Data(Bytes),
                Done(BodyIncompleteReason),
            }

            let result = match &mut self.source {
                DataSource::Static { data } => ChunkResult::Data(data.slice(offset..end)),
                DataSource::Random { random_bytes } => {
                    let actual_size = end - offset;
                    let max_start = random_bytes.len().saturating_sub(actual_size);
                    let start = if max_start == 0 {
                        0
                    } else {
                        (rand::rng().random::<u32>() as usize) % (max_start + 1)
                    };
                    ChunkResult::Data(Bytes::copy_from_slice(
                        &random_bytes[start..start + actual_size],
                    ))
                }
                DataSource::File {
                    file,
                    file_buf,
                    sha1,
                    ..
                } => {
                    let actual_size = end - offset;
                    match Self::fill_file_buffer(file, file_buf, actual_size) {
                        Ok(0) => ChunkResult::Done(BodyIncompleteReason::FileEndedEarly),
                        Ok(available) => {
                            let n = actual_size.min(available);
                            let chunk = file_buf.split_to(n).freeze();
                            sha1::Digest::update(sha1, &chunk);
                            ChunkResult::Data(chunk)
                        }
                        Err(e) => {
                            tracing::warn!("File body: read error at offset {}: {}", offset, e);
                            ChunkResult::Done(BodyIncompleteReason::FileReadError)
                        }
                    }
                }
                DataSource::Proxy {
                    file,
                    file_buf,
                    temp_file,
                    write_offset,
                    ..
                } => {
                    let actual_size = end - offset;
                    let read_cursor = offset + file_buf.len();
                    let max_read = (write_offset.load(Ordering::SeqCst) as usize)
                        .min(total_size)
                        .saturating_sub(read_cursor);
                    // max_read == 0 means write_offset rolled back (retry in
                    // progress) or hasn't advanced past read_cursor yet.
                    // Loop back so need_wait re-evaluates rather than treating
                    // this as a genuine EOF.
                    if max_read == 0 {
                        continue;
                    }
                    match Self::fill_file_buffer_limited(file, file_buf, actual_size, max_read) {
                        Ok(0) => ChunkResult::Done(BodyIncompleteReason::ProxyEndedEarly),
                        Ok(available) => {
                            let n = actual_size.min(available);
                            let chunk = file_buf.split_to(n).freeze();
                            ChunkResult::Data(chunk)
                        }
                        Err(e) => {
                            tracing::warn!(
                                "ProxyStreamingBody: read error for {} at offset {}: {}",
                                temp_file.display(),
                                offset,
                                e
                            );
                            ChunkResult::Done(BodyIncompleteReason::ProxyReadError)
                        }
                    }
                }
            };

            match result {
                ChunkResult::Data(chunk) => {
                    self.offset += chunk.len();
                    return Poll::Ready(Some(Ok(Frame::data(chunk))));
                }
                ChunkResult::Done(reason) => {
                    self.finish_with(Some(reason));
                    return Poll::Ready(None);
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn file_body_skips_integrity_check_when_send_is_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cached-file");
        std::fs::write(&path, b"complete body").unwrap();
        let file = std::fs::File::open(&path).unwrap();

        let mut body = StreamingBody::new_file(
            file,
            path.clone(),
            "complete body".len(),
            "0000000000000000000000000000000000000000".to_string(),
            true,
            None,
            None,
        );
        body.offset = 4;
        body.finish();

        match &body.source {
            DataSource::File { to_delete, .. } => {
                assert!(to_delete.is_some());
            }
            _ => panic!("expected file data source"),
        }
    }

    #[tokio::test]
    async fn file_body_records_ended_early_when_file_is_shorter_than_declared() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short-file");
        std::fs::write(&path, b"short").unwrap();
        let file = std::fs::File::open(&path).unwrap();

        let mut body = StreamingBody::new_file(
            file,
            path,
            10,
            "0000000000000000000000000000000000000000".to_string(),
            false,
            None,
            None,
        );

        let frame = body.frame().await.unwrap().unwrap();
        assert_eq!(&frame.into_data().ok().unwrap()[..], b"short");
        assert!(body.frame().await.is_none());
        assert_eq!(
            body.completion_status().completion,
            BodyCompletion::Incomplete(BodyIncompleteReason::FileEndedEarly)
        );
    }

    #[tokio::test]
    async fn proxy_body_rechecks_write_offset_even_without_notify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy-file");
        std::fs::File::create(&path).unwrap();

        let write_offset = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());
        let body_done_notify = Arc::new(Notify::new());
        let download_done = Arc::new(AtomicBool::new(false));
        let mut body = StreamingBody::new_proxy(
            4,
            path.clone(),
            write_offset.clone(),
            notify,
            body_done_notify,
            download_done,
            None,
        )
        .unwrap();

        let reader = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(250), body.frame()).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(&path, b"data").unwrap();
        write_offset.store(4, Ordering::SeqCst);

        let frame = reader
            .await
            .unwrap()
            .expect("proxy body should wake by polling even without notify")
            .unwrap()
            .unwrap();
        let bytes = frame.into_data().ok().unwrap();
        assert_eq!(&bytes[..], b"data");
    }
}
