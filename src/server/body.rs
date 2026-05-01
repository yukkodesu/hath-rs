//! Streaming HTTP body with optional per-chunk bandwidth throttling.
//!
//! Java equivalent: HTTPSession.write body loop + HTTPBandwidthMonitor.waitForQuota
//! Chunk size: 1460 bytes (Java Settings.TCP_PACKET_SIZE)
//!
//! Supports two data sources:
//! - `Static`: pre-loaded `Bytes` (for cached files, speedtest, text responses)
//! - `Proxy`: streaming from an mpsc channel written by a background download task

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
use std::task::{Context, Poll};
use tokio::sync::mpsc;

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
    /// Proxy download: chunks arrive via mpsc channel from the background
    /// download task. `pending` holds leftover bytes when a channel chunk
    /// is larger than TCP_PACKET_SIZE and must be sliced across frames.
    Proxy {
        rx: mpsc::Receiver<Bytes>,
        /// Bytes received from the channel but not yet yielded to Hyper.
        pending: Option<Bytes>,
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
/// For proxy mode, the body receives chunks from an mpsc channel fed by the
/// background download task.
pub struct StreamingBody {
    source: DataSource,
    offset: usize,
    total_size: usize,
    bwm: Option<Arc<BandwidthMonitor>>,
    /// Pending throttle future.
    throttle_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    /// Whether a throttle just completed (prevents double-throttle for same chunk).
    throttled: bool,
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
    pub fn new(data: Bytes, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        Self::from_bytes(data, bwm)
    }

    /// Zero-allocation empty body (for HEAD responses).
    pub fn empty() -> Self {
        Self::from_bytes(Bytes::new(), None)
    }

    /// Create a file-streaming body from an already-open file handle.
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
            completion: BodyCompletion::Running,
        }
    }

    /// Create a body that serves random-looking chunks from a reusable 8 KiB pool.
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
            completion: BodyCompletion::Running,
        }
    }

    /// Create a proxy-streaming body backed by an mpsc channel.
    ///
    /// The download task sends `Bytes` chunks through `rx`; this body
    /// slices them into TCP_PACKET_SIZE frames (with optional BWM throttle).
    pub fn new_proxy(
        total_size: usize,
        rx: mpsc::Receiver<Bytes>,
        bwm: Option<Arc<BandwidthMonitor>>,
    ) -> Self {
        Self {
            source: DataSource::Proxy { rx, pending: None },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            completion: BodyCompletion::Running,
        }
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
                        if let Some(ch) = cache_handler
                            && let Some(name) = path.file_name().and_then(|n| n.to_str())
                        {
                            let _ = ch.delete_file_from_cache(name);
                        }
                    }
                }
            }
            DataSource::Static { .. } | DataSource::Random { .. } | DataSource::Proxy { .. } => {}
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
            if let Some(ref mut fut) = self.throttle_fut {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        self.throttle_fut = None;
                        self.throttled = true;
                    }
                }
            }

            // Step 2: Check if done.
            if self.offset >= self.total_size {
                self.finish();
                return Poll::Ready(None);
            }

            // Step 3: Proxy mode — poll channel for the next chunk.
            if let DataSource::Proxy { rx, pending } = &mut self.source {
                // If we have leftover bytes from a previous large chunk, serve
                // the next TCP_PACKET_SIZE slice without hitting the channel.
                if pending.is_none() {
                    match rx.poll_recv(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => {
                            // Channel closed: download task finished (or failed).
                            self.finish_with(Some(BodyIncompleteReason::ProxyEndedEarly));
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(chunk)) => {
                            *pending = Some(chunk);
                        }
                    }
                }
                // pending is now Some — fall through to throttle + slice below.
            }

            // Step 4: Throttle (applies to all sources including Proxy).
            if !self.throttled && self.bwm.is_some() && self.throttle_fut.is_none() {
                let chunk_size = TCP_PACKET_SIZE.min(self.total_size - self.offset);
                let bwm = self.bwm.clone().unwrap();
                self.throttle_fut = Some(Box::pin(async move {
                    bwm.wait_for_quota(chunk_size).await;
                }));
                continue;
            }
            self.throttled = false;

            // Step 5: Produce chunk.
            let offset = self.offset;
            let total_size = self.total_size;

            enum ChunkResult {
                Data(Bytes),
                Done(BodyIncompleteReason),
            }

            let bwm_is_none = self.bwm.is_none();
            let result = match &mut self.source {
                DataSource::Static { data } => {
                    let chunk_size = if bwm_is_none {
                        total_size - offset
                    } else {
                        TCP_PACKET_SIZE
                    };
                    let end = (offset + chunk_size).min(total_size);
                    ChunkResult::Data(data.slice(offset..end))
                }
                DataSource::Random { random_bytes } => {
                    let chunk_size = TCP_PACKET_SIZE;
                    let end = (offset + chunk_size).min(total_size);
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
                    let chunk_size = TCP_PACKET_SIZE;
                    let end = (offset + chunk_size).min(total_size);
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
                DataSource::Proxy { pending, .. } => {
                    let buf = pending.as_mut().expect("pending set in Step 3");
                    let n = TCP_PACKET_SIZE.min(buf.len()).min(total_size - offset);
                    let chunk = buf.split_to(n);
                    if buf.is_empty() {
                        *pending = None;
                    }
                    ChunkResult::Data(chunk)
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

    async fn collect_body(body: &mut StreamingBody) -> Vec<u8> {
        use std::future::poll_fn;
        let mut out = Vec::new();
        loop {
            let frame = poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await;
            match frame {
                Some(Ok(f)) => {
                    if let Ok(data) = f.into_data() {
                        out.extend_from_slice(&data);
                    }
                }
                _ => break,
            }
        }
        out
    }

    #[tokio::test]
    async fn proxy_body_streams_chunks_via_channel() {
        let (tx, rx) = mpsc::channel::<Bytes>(8);
        let mut body = StreamingBody::new_proxy(9, rx, None);

        tx.send(Bytes::from_static(b"hello")).await.unwrap();
        tx.send(Bytes::from_static(b"world")).await.unwrap();
        drop(tx);

        let collected = collect_body(&mut body).await;
        assert_eq!(collected.len(), 9);
        assert_eq!(
            body.completion_status().completion,
            BodyCompletion::Complete
        );
    }

    #[tokio::test]
    async fn proxy_body_records_incomplete_when_channel_closes_early() {
        let (tx, rx) = mpsc::channel::<Bytes>(8);
        let mut body = StreamingBody::new_proxy(100, rx, None);

        tx.send(Bytes::from_static(b"partial")).await.unwrap();
        drop(tx); // close before all 100 bytes sent

        collect_body(&mut body).await;
        assert_eq!(
            body.completion_status().completion,
            BodyCompletion::Incomplete(BodyIncompleteReason::ProxyEndedEarly)
        );
    }
}
