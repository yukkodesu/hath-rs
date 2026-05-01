# Proxy Watch-based Decoupling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple proxy download speed from body transmission speed using `tokio::sync::watch`, so download_task always runs to completion regardless of client behavior.

**Architecture:** download_task writes to a temp file and publishes progress via a `watch::Sender<DownloadState>`; StreamingBody reads from the temp file gated on the watch-published write offset, using the existing `fill_file_buffer_limited` logic. The two sides share no mutable state other than the file on disk.

**Tech Stack:** Rust, tokio::sync::watch, std::fs::File (sync read in poll_frame), tokio::fs (async write in download_task), existing BytesMut buffering.

---

## File map

| File | Change |
|------|--------|
| `src/proxy_downloader.rs` | Replace mpsc with watch; add `DownloadState`; rewrite `download_task`; update struct |
| `src/server/body.rs` | Replace `DataSource::Proxy` fields; add `wait_fut`; rewrite Proxy poll_frame branch |
| `src/server/response.rs` | Update `ProxyResponseParts` (swap `rx: mpsc::Receiver` for `file + watch_rx`) |
| `src/server/mod.rs` | Open temp file for reading; update HEAD path (drop watch_rx, no drain task); update GET path |

---

### Task 1: Add `DownloadState` and rewrite `ProxyFileDownloader`

**Files:**
- Modify: `src/proxy_downloader.rs`

- [ ] **Step 1: Replace imports and struct**

Replace the top of the file (imports + struct) with:

```rust
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
use tokio::sync::watch;

/// State published by download_task to StreamingBody via watch channel.
#[derive(Clone, Debug)]
pub enum DownloadState {
    /// Download in progress; value is bytes written to temp file so far.
    InProgress(u64),
    /// Download complete: SHA1 verified, file renamed to cache.
    Done,
    /// Download failed: network error, disk error, SHA1 mismatch, or rename failure.
    Failed,
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
}
```

- [ ] **Step 2: Update `new()` — replace channel creation with watch**

Inside `new()`, find the block starting with `// Good response — create channel and spawn download task.` and replace everything from that comment to `return Ok(Self { ... });`:

```rust
                // Good response — create channel and spawn download task.
                let temp_file = Self::create_temp_file(&hv_file, config)?;
                let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0));

                let hash = hv_file.hash.clone();
                let expected_size = hv_file.size as u64;
                let fileid_owned = hv_file.fileid().clone();
                let cache_dir = config.cache_dir.clone();
                let tf = temp_file.clone();

                tokio::spawn(async move {
                    Self::download_task(
                        resp,
                        watch_tx,
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
                });
```

- [ ] **Step 3: Rewrite `download_task` signature and body**

Replace the entire `download_task` function:

```rust
    /// Download `resp` into `temp_file`, publishing progress via `watch_tx`.
    /// On success + SHA1 match: rename temp_file → cache and send Done.
    /// On any failure: delete temp_file and send Failed.
    /// Never observes body state — runs to completion regardless of body drop.
    async fn download_task(
        mut resp: reqwest::Response,
        watch_tx: watch::Sender<DownloadState>,
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
                let _ = watch_tx.send(DownloadState::Failed);
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

            if let Err(e) = file.write_all(&chunk).await {
                tracing::warn!("Proxy download: disk write error for {}: {}", fileid, e);
                break;
            }
            downloaded += chunk.len() as u64;
            // Publish progress; body uses this to know how many bytes are safe to read.
            // Ignore send error — body may have dropped watch_rx (e.g. HEAD request).
            let _ = watch_tx.send(DownloadState::InProgress(downloaded));
        }

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
            }
        }

        utils::remove_file(&temp_file);
        let _ = watch_tx.send(DownloadState::Failed);
    }
```

- [ ] **Step 4: Verify it compiles (ignore body.rs / mod.rs errors for now)**

```bash
cargo check 2>&1 | grep "proxy_downloader"
```

Expected: no errors in `proxy_downloader.rs` itself (errors from `mod.rs` referencing `proxy.rx` are expected at this stage).

- [ ] **Step 5: Commit**

```bash
git add src/proxy_downloader.rs
git commit -m "refactor: replace mpsc with watch channel in ProxyFileDownloader"
```

---

### Task 2: Replace `DataSource::Proxy` in `StreamingBody`

**Files:**
- Modify: `src/server/body.rs`

- [ ] **Step 1: Update imports**

Replace the import block at the top of `src/server/body.rs`:

```rust
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
use tokio::sync::watch;

use crate::bandwidth::BandwidthMonitor;
use crate::proxy_downloader::DownloadState;
```

- [ ] **Step 2: Replace `DataSource::Proxy` variant**

Replace the old `Proxy { rx, pending }` variant with:

```rust
    /// Proxy download: body reads from the temp file written by download_task.
    /// `watch_rx` provides write_offset and completion state.
    /// `wait_fut` is a pending `watch_rx.changed()` future when we need to wait for more data.
    Proxy {
        file: std::fs::File,
        file_buf: BytesMut,
        /// For log messages only.
        temp_file: PathBuf,
        watch_rx: watch::Receiver<DownloadState>,
        wait_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    },
```

- [ ] **Step 3: Replace `new_proxy` constructor**

Replace the entire `new_proxy` method:

```rust
    /// Create a proxy-streaming body backed by a temp file + watch channel.
    ///
    /// `file` must be opened for reading at offset 0.
    /// `watch_rx` delivers write progress from the background download task.
    pub fn new_proxy(
        total_size: usize,
        file: std::fs::File,
        temp_file: PathBuf,
        watch_rx: watch::Receiver<DownloadState>,
        bwm: Option<Arc<BandwidthMonitor>>,
    ) -> Self {
        Self {
            source: DataSource::Proxy {
                file,
                file_buf: BytesMut::with_capacity(FILE_BUFFER_SIZE),
                temp_file,
                watch_rx,
                wait_fut: None,
            },
            offset: 0,
            total_size,
            bwm,
            throttle_fut: None,
            throttled: false,
            completion: BodyCompletion::Running,
        }
    }
```

- [ ] **Step 4: Update `finish_with` — Proxy arm is already a no-op, just update the pattern**

The match arm for `DataSource::Proxy` currently reads:
```rust
DataSource::Static { .. } | DataSource::Random { .. } | DataSource::Proxy { .. } => {}
```
This is already correct — no change needed.

- [ ] **Step 5: Rewrite the Proxy branch in `poll_frame`**

Replace Step 3 (the `if let DataSource::Proxy { rx, pending }` block) and Step 5's `DataSource::Proxy` arm. The full new `poll_frame` loop body (replace from `loop {` to the closing `}` of `poll_frame`):

```rust
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

            // Step 3: Proxy mode — check if enough data has been written to disk.
            if let DataSource::Proxy {
                watch_rx,
                wait_fut,
                temp_file,
                ..
            } = &mut self.source
            {
                let need = TCP_PACKET_SIZE.min(self.total_size - self.offset);

                // Sample the latest watch state without holding the borrow.
                let (write_offset, terminal) = {
                    let state = watch_rx.borrow_and_update();
                    match *state {
                        DownloadState::InProgress(n) => (n, false),
                        DownloadState::Done => (self.total_size as u64, true),
                        DownloadState::Failed => (0, true),
                    }
                };

                let have_enough = write_offset >= (self.offset + need) as u64;

                if !have_enough {
                    if terminal {
                        // Download finished but not enough data — real failure.
                        tracing::warn!(
                            "Proxy body: download ended early for {} (offset={} total={})",
                            temp_file.display(),
                            self.offset,
                            self.total_size,
                        );
                        self.finish_with(Some(BodyIncompleteReason::ProxyEndedEarly));
                        return Poll::Ready(None);
                    }
                    // Not enough data yet — wait for watch to fire.
                    if wait_fut.is_none() {
                        // Clone the receiver so we can move it into the async block
                        // without conflicting with the &mut self.source borrow.
                        // watch::Receiver::clone() is cheap (Arc increment).
                        let mut rx_clone = watch_rx.clone();
                        *wait_fut = Some(Box::pin(async move {
                            let _ = rx_clone.changed().await;
                        }));
                    }
                    if let Some(ref mut fut) = wait_fut {
                        match fut.as_mut().poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(()) => {
                                *wait_fut = None;
                            }
                        }
                    }
                    continue;
                }
                // Enough data on disk — clear any stale wait_fut and fall through to read.
                *wait_fut = None;
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
                DataSource::Proxy { file, file_buf, .. } => {
                    let chunk_size = TCP_PACKET_SIZE;
                    let end = (offset + chunk_size).min(total_size);
                    let actual_size = end - offset;
                    // Step 3 guarantees write_offset >= offset + actual_size, so
                    // fill_file_buffer_limited with max_read = total_size will not block.
                    match Self::fill_file_buffer(file, file_buf, actual_size) {
                        Ok(0) => ChunkResult::Done(BodyIncompleteReason::ProxyEndedEarly),
                        Ok(available) => {
                            let n = actual_size.min(available);
                            ChunkResult::Data(file_buf.split_to(n).freeze())
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Proxy body: file read error at offset {}: {}",
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
```

- [ ] **Step 6: Write two tests**

Replace the two proxy tests at the bottom of `mod tests`:

```rust
    #[tokio::test]
    async fn proxy_body_streams_full_file_via_watch() {
        use tokio::sync::watch;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy-file");
        std::fs::write(&path, b"helloworld").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let (tx, rx) = watch::channel(DownloadState::InProgress(10));

        let mut body = StreamingBody::new_proxy(10, file, path.clone(), rx, None);
        let collected = collect_body(&mut body).await;
        assert_eq!(collected, b"helloworld");
        assert_eq!(body.completion_status().completion, BodyCompletion::Complete);
        drop(tx);
    }

    #[tokio::test]
    async fn proxy_body_records_incomplete_when_download_fails() {
        use tokio::sync::watch;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy-partial");
        std::fs::write(&path, b"partial").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        // Declare total_size=100 but file only has 7 bytes and download fails.
        let (tx, rx) = watch::channel(DownloadState::InProgress(7));

        let mut body = StreamingBody::new_proxy(100, file, path.clone(), rx, None);

        // Advance: body will read 7 bytes then watch shows Failed.
        let handle = tokio::spawn(async move {
            collect_body(&mut body).await;
            body.completion_status().completion
        });

        // Signal failure after a short delay.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _ = tx.send(DownloadState::Failed);

        let completion = handle.await.unwrap();
        assert_eq!(
            completion,
            BodyCompletion::Incomplete(BodyIncompleteReason::ProxyEndedEarly)
        );
    }
```

- [ ] **Step 7: Run tests**

```bash
cargo test server::body 2>&1
```

Expected: all body tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/server/body.rs
git commit -m "refactor: replace DataSource::Proxy channel fields with watch + file"
```

---

### Task 3: Update `response.rs` and `mod.rs`

**Files:**
- Modify: `src/server/response.rs`
- Modify: `src/server/mod.rs`

- [ ] **Step 1: Update imports in `response.rs`**

Replace the import block at the top:

```rust
use super::body::StreamingBody;
use crate::bandwidth::BandwidthMonitor;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::proxy_downloader::DownloadState;
use bytes::Bytes;
use hyper::{Response, StatusCode, header};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;
```

- [ ] **Step 2: Update `ProxyResponseParts` and `proxy_response`**

Replace the `ProxyResponseParts` struct and `proxy_response` function:

```rust
/// Build a response for a proxy file download in progress.
/// The body reads from temp_file, gated on watch_rx progress.
/// Java: HTTPResponseProcessorProxy + ProxyFileDownloader.
pub struct ProxyResponseParts<'a> {
    pub content_type: &'a str,
    pub total_size: usize,
    pub temp_file: std::fs::File,
    pub temp_file_path: PathBuf,
    pub watch_rx: watch::Receiver<DownloadState>,
    pub bwm: Option<Arc<BandwidthMonitor>>,
}

pub fn proxy_response(parts: ProxyResponseParts<'_>) -> Result<Response<StreamingBody>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, parts.content_type)
        .header(header::CONNECTION, "close");
    if parts.total_size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, parts.total_size);
    }
    let body = StreamingBody::new_proxy(
        parts.total_size,
        parts.temp_file,
        parts.temp_file_path,
        parts.watch_rx,
        parts.bwm,
    );
    builder.body(body).map_err(HathError::Http)
}
```

- [ ] **Step 3: Update the proxy branch in `mod.rs`**

Replace the entire `if head_only { ... } else { ... }` block inside `Ok(proxy) =>`:

```rust
                                                if head_only {
                                                    // Drop watch_rx — download_task is unaffected
                                                    // and will run to completion, caching the file.
                                                    drop(proxy.watch_rx);
                                                    response::head_response(
                                                        mime,
                                                        proxy.content_length,
                                                    )
                                                } else {
                                                    let total_size = proxy.content_length;
                                                    let file = match std::fs::File::open(&proxy.temp_file) {
                                                        Ok(f) => f,
                                                        Err(e) => {
                                                            tracing::warn!(
                                                                "Proxy: cannot open temp file for {}: {}",
                                                                fileid,
                                                                e
                                                            );
                                                            return response::text_response(
                                                                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                                "proxy temp file unavailable",
                                                            );
                                                        }
                                                    };
                                                    let response = response::proxy_response(
                                                        response::ProxyResponseParts {
                                                            content_type: mime,
                                                            total_size,
                                                            temp_file: file,
                                                            temp_file_path: proxy.temp_file,
                                                            watch_rx: proxy.watch_rx,
                                                            bwm: bwm_for_request,
                                                        },
                                                    );
                                                    if response.is_ok() {
                                                        tracing::info!(
                                                            "Proxy download: returning body for {} ({} bytes, {})",
                                                            fileid,
                                                            total_size,
                                                            mime
                                                        );
                                                    }
                                                    response
                                                }
```

- [ ] **Step 4: Remove unused imports from `mod.rs`**

Remove `use tokio::sync::mpsc;` if present (it was only needed for the drain task). Also remove the drain task spawn if it's still there. The `Notify` import stays (used by `cert_refresh_notify`).

- [ ] **Step 5: Run full test suite**

```bash
cargo test 2>&1
```

Expected: all 39+ tests pass, no compile errors.

- [ ] **Step 6: Run clippy**

```bash
cargo clippy 2>&1
```

Expected: no new warnings beyond the pre-existing `too_many_arguments` and `dead_code` ones.

- [ ] **Step 7: Commit**

```bash
git add src/server/response.rs src/server/mod.rs
git commit -m "refactor: wire watch-based proxy body through response and mod"
```

---

### Task 4: Squash and verify

**Files:** none (git only)

- [ ] **Step 1: Verify git log**

```bash
git log --oneline -5
```

Expected: three new commits on top of `2f17322` (the spec commit).

- [ ] **Step 2: Run full build in release mode to catch any remaining issues**

```bash
cargo build --release 2>&1
```

Expected: compiles cleanly.

- [ ] **Step 3: Squash the three implementation commits**

```bash
git rebase -i HEAD~3
```

In the editor, mark the second and third commits as `squash`. Use this message:

```
refactor: replace mpsc proxy streaming with watch + temp file

download_task now writes at full speed to a temp file and publishes
progress via tokio::sync::watch<DownloadState>. StreamingBody reads from
the temp file gated on the watch offset, using fill_file_buffer_limited.

This decouples download speed from body consumption speed:
- Body slow / client abort: download_task runs to completion unaffected
- watch_rx drop (HEAD or abort): download_task continues, caches file
- No drain task, no mpsc backpressure, no shared mutable state

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

- [ ] **Step 4: Final test run after squash**

```bash
cargo test 2>&1
```

Expected: all tests pass.
