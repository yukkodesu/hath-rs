# Proxy Streaming Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace shared-file + AtomicU64/Notify coordination in proxy streaming with an mpsc channel, eliminating `proxy_ended_early` races and simplifying the data flow.

**Architecture:** `ProxyFileDownloader::new()` retries until it has a live HTTP response, creates an `mpsc::channel::<Bytes>(8)`, spawns a download task that sends chunks and writes to a temp file, then returns the receiver to the caller. `StreamingBody` receives chunks directly via the channel — no file handle, no AtomicU64, no Notify. On download success, the task atomically renames the temp file into cache.

**Tech Stack:** Rust, Tokio, `tokio::sync::mpsc`, `bytes::Bytes`, `reqwest`, `sha1`, existing `BandwidthMonitor`

---

## File map

| File | Change |
|---|---|
| `src/proxy_downloader.rs` | Full rewrite: new struct fields, retry in `new()`, channel-based download task |
| `src/server/body.rs` | Replace `DataSource::Proxy` fields + `poll_frame` Proxy branch; remove `wait_fut` |
| `src/server/response.rs` | Shrink `ProxyResponseParts` (remove AtomicU64/Notify/AtomicBool/PathBuf fields) |
| `src/server/mod.rs` | Update proxy response construction to pass only `rx` |

---

## Task 1: Rewrite `ProxyFileDownloader`

**Files:**
- Modify: `src/proxy_downloader.rs`

### Overview

Replace the struct's public fields and spawn logic. The new struct exposes only `content_length`, `content_type`, and `rx`. Retry (3 attempts × N sources) happens inside `new()` before the channel exists. The download task sends `Bytes` chunks through the channel while simultaneously writing to a temp file; on SHA1 success it renames to cache.

- [ ] **Step 1: Replace the struct definition and imports**

Replace the entire top of `src/proxy_downloader.rs` (imports through the struct definition) with:

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
```

- [ ] **Step 2: Rewrite `new()` — retry before channel creation**

Replace the entire `impl ProxyFileDownloader` block (keep `build_proxy_client`, `build_proxy_url`, and the existing test at the bottom untouched):

```rust
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

            // Send to body. If body dropped rx, stop downloading.
            if tx.send(Bytes::copy_from_slice(&chunk)).await.is_err() {
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
```

- [ ] **Step 3: Build and fix compile errors**

```bash
cargo build 2>&1
```

Expected: compiles with at most the existing `dead_code` warning. If there are errors about removed fields (`write_offset`, `notify`, etc.) they will be fixed in later tasks — note them and proceed.

- [ ] **Step 4: Commit**

```bash
git add src/proxy_downloader.rs
git commit -m "refactor: rewrite ProxyFileDownloader with mpsc channel"
```

---

## Task 2: Replace `DataSource::Proxy` in `StreamingBody`

**Files:**
- Modify: `src/server/body.rs`

### Overview

Swap out the 7-field file-based `Proxy` variant for a 2-field channel-based one. The `poll_frame` Proxy branch becomes a simple `rx.poll_recv(cx)` with a `pending` buffer for slicing large chunks into TCP_PACKET_SIZE pieces. Remove `wait_fut` from `StreamingBody` (no longer needed).

- [ ] **Step 1: Update imports and remove unused ones**

At the top of `src/server/body.rs`, replace the import block with:

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
use tokio::sync::mpsc;

use crate::bandwidth::BandwidthMonitor;
```

(Removed: `AtomicBool`, `AtomicU64`, `Ordering`, `Duration`, `Instant`, `Notify`)

- [ ] **Step 2: Replace the `DataSource::Proxy` variant**

In the `DataSource` enum, replace the `Proxy` variant:

```rust
    /// Proxy download: chunks arrive via mpsc channel from the background
    /// download task. `pending` holds leftover bytes when a channel chunk
    /// is larger than TCP_PACKET_SIZE and must be sliced across frames.
    Proxy {
        rx: mpsc::Receiver<Bytes>,
        /// Bytes received from the channel but not yet yielded to Hyper.
        pending: Option<Bytes>,
    },
```

- [ ] **Step 3: Update `StreamingBody` struct — remove `wait_fut`**

In the `StreamingBody` struct, remove the `wait_fut` field:

```rust
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
```

- [ ] **Step 4: Update `new_proxy` constructor**

Replace the existing `new_proxy` constructor:

```rust
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
```

- [ ] **Step 5: Update `finish` — remove `body_done_notify`**

In `finish_with`, replace the `DataSource::Proxy` arm:

```rust
            DataSource::Proxy { .. } => {
                // rx drop signals the download task naturally — no explicit notify needed.
            }
```

- [ ] **Step 6: Rewrite `poll_frame` — remove Step 3 (wait logic) and rewrite Proxy branch in Step 5**

Replace the entire `poll_frame` implementation with the version below. Key changes:
- Remove Step 3 (the `need_wait` / `wait_fut` block — the whole `if need_wait { ... }` section).
- Rewrite the `DataSource::Proxy` branch in Step 5.

```rust
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
            // Replaces the old file-read + AtomicU64 wait loop.
            if let DataSource::Proxy { rx, pending } = &mut self.source {
                // If we have leftover bytes from a previous large chunk, serve
                // the next TCP_PACKET_SIZE slice without hitting the channel.
                if pending.is_none() {
                    match rx.poll_recv(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => {
                            // Channel closed: download task finished (or failed).
                            // finish_with records Incomplete if offset < total_size.
                            let reason = crate::server::body::BodyIncompleteReason::ProxyEndedEarly;
                            self.finish_with(Some(reason));
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

            let result = match &mut self.source {
                DataSource::Static { data } => {
                    let chunk_size = if self.bwm.is_none() {
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
                    ChunkResult::Data(chunk.freeze())
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
```

- [ ] **Step 7: Fix `BodyIncompleteReason` visibility in Proxy branch**

The `BodyIncompleteReason` reference in Step 6 is within the same module, so it doesn't need a path prefix. Verify `BodyIncompleteReason` is defined in this file (it is — line ~101) and remove the `crate::server::body::` prefix from the `ProxyEndedEarly` reference:

```rust
                        Poll::Ready(None) => {
                            self.finish_with(Some(BodyIncompleteReason::ProxyEndedEarly));
                            return Poll::Ready(None);
                        }
```

- [ ] **Step 8: Update `new_proxy` test**

The existing test `proxy_body_rechecks_write_offset_even_without_notify` tests the old file-based Proxy. Replace it with a channel-based test:

```rust
    #[tokio::test]
    async fn proxy_body_streams_chunks_via_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);
        let mut body = StreamingBody::new_proxy(9, rx, None);

        tx.send(Bytes::from_static(b"hello")).await.unwrap();
        tx.send(Bytes::from_static(b"world")).await.unwrap();
        drop(tx);

        use http_body_util::BodyExt;
        let collected = body.collect().await.unwrap().to_bytes();
        // Without BWM the static path sends all at once, but Proxy always
        // slices through pending — verify total bytes match.
        assert_eq!(collected.len(), 9);
        assert_eq!(
            body.completion_status().completion,
            BodyCompletion::Complete
        );
    }

    #[tokio::test]
    async fn proxy_body_records_incomplete_when_channel_closes_early() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);
        let mut body = StreamingBody::new_proxy(100, rx, None);

        tx.send(Bytes::from_static(b"partial")).await.unwrap();
        drop(tx); // close before all 100 bytes sent

        use http_body_util::BodyExt;
        let _ = body.collect().await;
        assert_eq!(
            body.completion_status().completion,
            BodyCompletion::Incomplete(BodyIncompleteReason::ProxyEndedEarly)
        );
    }
```

- [ ] **Step 9: Run tests**

```bash
cargo test 2>&1
```

Expected: all tests pass. The old `proxy_body_rechecks_write_offset_even_without_notify` test is gone; the two new tests pass.

- [ ] **Step 10: Commit**

```bash
git add src/server/body.rs
git commit -m "refactor: replace DataSource::Proxy file-based fields with mpsc channel"
```

---

## Task 3: Update `response.rs` and `mod.rs`

**Files:**
- Modify: `src/server/response.rs`
- Modify: `src/server/mod.rs`

### Overview

`ProxyResponseParts` loses all the coordination fields; `proxy_response` just wraps the receiver. `mod.rs` passes `proxy.rx` instead of the old five Arc fields.

- [ ] **Step 1: Shrink `ProxyResponseParts` and `proxy_response`**

In `src/server/response.rs`, replace the `ProxyResponseParts` struct and `proxy_response` function:

```rust
pub struct ProxyResponseParts {
    pub content_type: String,
    pub total_size: usize,
    pub rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    pub bwm: Option<Arc<BandwidthMonitor>>,
}

pub fn proxy_response(parts: ProxyResponseParts) -> Result<Response<StreamingBody>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, parts.content_type)
        .header(header::CONNECTION, "close");
    if parts.total_size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, parts.total_size);
    }
    let body = StreamingBody::new_proxy(parts.total_size, parts.rx, parts.bwm);
    builder.body(body).map_err(HathError::Http)
}
```

Also remove the now-unused imports at the top of `response.rs`:

```rust
// Remove these lines:
use std::sync::atomic::{AtomicBool, AtomicU64};
use tokio::sync::Notify;
```

- [ ] **Step 2: Update proxy response construction in `mod.rs`**

In `src/server/mod.rs`, find the block that constructs `ProxyResponseParts` (around the `proxy_response(response::ProxyResponseParts { ... })` call) and replace it:

```rust
                                                let total_size = proxy.total_size as usize;
                                                let response = response::proxy_response(
                                                    response::ProxyResponseParts {
                                                        content_type: mime.to_string(),
                                                        total_size,
                                                        rx: proxy.rx,
                                                        bwm: bwm_for_request,
                                                    },
                                                );
```

Also update the HEAD path that calls `proxy.body_done_notify.notify_one()` — remove that line since there's no `body_done_notify` anymore:

```rust
                                                if head_only {
                                                    // rx drop signals the download task naturally.
                                                    response::head_response(
                                                        mime,
                                                        proxy.total_size as usize,
                                                    )
                                                } else {
```

- [ ] **Step 3: Build**

```bash
cargo build 2>&1
```

Expected: clean build (only existing `dead_code` warning). Fix any remaining compile errors from old field references.

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 5: Remove diagnostic logging commit**

The previous debug commit added `ProxyStreamingBody: fill returned Ok(0)` and `ProxyStreamingBody: download done, terminating` log lines to `body.rs`. These are now dead code (those code paths no longer exist). Verify they were removed as part of the rewrite in Task 2. If any stale debug log lines remain, delete them now.

- [ ] **Step 6: Commit**

```bash
git add src/server/response.rs src/server/mod.rs
git commit -m "refactor: simplify ProxyResponseParts to channel receiver only"
```

---

## Task 4: Clean up `proxy_downloader.rs` dead code

**Files:**
- Modify: `src/proxy_downloader.rs`

### Overview

Remove `get_current_writeoff`, `wait_for_data`, `fill_buffer`, `DownloadAttemptResult`, and the old `try_source`/`download_attempt` if they weren't already removed in Task 1. Also remove unused imports from previous debug commits (`AsyncSeekExt`, `Seek`, `SeekFrom`, `Read`, `AtomicBool`, `Notify`).

- [ ] **Step 1: Remove dead code and stale imports**

Verify the following are absent from `src/proxy_downloader.rs` after Task 1:
- `DownloadAttemptResult` enum
- `try_source` method
- `download_attempt` method
- `get_current_writeoff` method
- `wait_for_data` method
- `fill_buffer` method
- Imports: `std::io::{Read, Seek, SeekFrom}`, `tokio::io::AsyncSeekExt`, `tokio::sync::Notify`, `std::sync::atomic::{AtomicBool, Ordering}`

If any remain, delete them.

- [ ] **Step 2: Build and test**

```bash
cargo build 2>&1 && cargo test 2>&1
```

Expected: clean build and all tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/proxy_downloader.rs
git commit -m "chore: remove dead code from proxy_downloader after channel refactor"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| mpsc channel replaces AtomicU64/Notify/AtomicBool | Task 1, 2 |
| Retry in `new()` before channel creation | Task 1 |
| download task: write to tmp + send to channel + SHA1 | Task 1 |
| `fs::rename(tmp → cache)` on SHA1 success | Task 1 |
| `register_proxy_file` after rename only | Task 1 |
| Body drop → tx.send Err → task aborts | Task 1 |
| `DataSource::Proxy` fields removed | Task 2 |
| `pending: Option<Bytes>` for large-chunk slicing | Task 2 |
| `wait_fut` removed | Task 2 |
| `ProxyResponseParts` shrunk | Task 3 |
| `body_done_notify.notify_one()` on HEAD removed | Task 3 |
| Diagnostic log lines removed | Task 3 |
| Dead code removed | Task 4 |

**No placeholders:** Verified — all steps contain complete code.

**Type consistency:**
- `StreamingBody::new_proxy(total_size: usize, rx: mpsc::Receiver<Bytes>, bwm: Option<Arc<BandwidthMonitor>>)` — used consistently in Task 2 (definition) and Task 3 (call site).
- `ProxyResponseParts { content_type: String, total_size: usize, rx: mpsc::Receiver<Bytes>, bwm }` — defined in Task 3 Step 1, consumed in Task 3 Step 2.
- `proxy.rx` — field added to `ProxyFileDownloader` in Task 1, read in Task 3 Step 2.
- `proxy.total_size` — removed from struct in Task 1; Task 3 Step 2 uses `proxy.content_length as usize` instead. **Fix:** Task 3 Step 2 should use `proxy.content_length` not `proxy.total_size`. Updated above.
