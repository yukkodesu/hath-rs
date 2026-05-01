# Proxy Streaming: watch-based Decoupling

**Date:** 2026-05-01
**Status:** Approved

## Problem

The current mpsc channel design couples download speed to body consumption speed:

1. `tx.send(chunk).await` blocks when the channel buffer (8 slots) is full — if the client is slow, the download task stalls.
2. When the client aborts (body drops `rx`), the next `tx.send` returns `Err`, and the download task deletes the temp file and exits — the file is never cached.

## Goals

1. download_task runs at full speed regardless of body consumption rate.
2. Client abort does not interrupt the download; the file is still verified and cached.

## Design

### Core idea: watch channel for progress, file for data

Decouple the two sides completely:

- **download_task** only writes to a temp file and publishes progress via `tokio::sync::watch`. It has no knowledge of the body.
- **StreamingBody (Proxy)** reads from the temp file using the existing synchronous `fill_file_buffer_limited`, gated on the watch-published write offset.

### DownloadState

```rust
#[derive(Clone)]
enum DownloadState {
    InProgress(u64), // bytes written so far
    Done,            // download complete, SHA1 verified, rename succeeded
    Failed,          // any failure: network, disk, SHA1 mismatch, rename failure
}
```

`Done` means the file is fully written and verified. `Failed` means the download did not complete successfully; the body should stop and report `ProxyEndedEarly` if it hasn't sent all bytes yet.

### Data flow

```
ProxyFileDownloader::new()
  connect + validate Content-Length (retry loop, unchanged)
  create temp_file
  let (watch_tx, watch_rx) = watch::channel(DownloadState::InProgress(0))
  tokio::spawn(download_task(resp, watch_tx, temp_file, ...))
  return ProxyFileDownloader { content_length, content_type, temp_file, watch_rx }

download_task:
  open temp_file (write, tokio::fs)
  loop:
    chunk = resp.chunk().await
    file.write_all(&chunk).await
    downloaded += chunk.len()
    watch_tx.send(DownloadState::InProgress(downloaded))
  if success && sha1 ok:
    rename(temp → cache)
    cache.register_proxy_file()
    watch_tx.send(DownloadState::Done)
  else:
    remove temp_file
    watch_tx.send(DownloadState::Failed)

StreamingBody (Proxy) poll_frame:
  Step 3: check write_offset vs needed bytes
    - read latest watch value (borrow_and_update)
    - if write_offset >= offset + need → read from file (fill_file_buffer_limited)
    - elif state is Done or Failed → no more data coming
        - if offset >= total_size → finish (Complete)
        - else → finish_with(ProxyEndedEarly)
    - else → watch_rx.changed().await → Pending (register waker via poll)
  Step 4/5: throttle + yield chunk (unchanged)
```

### watch polling in poll_frame

`watch::Receiver` does not implement `Future` directly. The Proxy step uses a stored `Pin<Box<dyn Future<Output=()> + Send>>` (same slot as the existing `throttle_fut` pattern, but a separate field `wait_fut`) that wraps:

```rust
async move { let _ = watch_rx.changed().await; }
```

This future is created once when we need to wait, polled in Step 3, and cleared when it resolves. The borrow from `watch_rx.borrow_and_update()` is used immediately (not stored) so there is no borrow conflict.

### ProxyFileDownloader struct

```rust
pub struct ProxyFileDownloader {
    pub content_length: usize,
    pub content_type: String,
    pub temp_file: PathBuf,
    pub watch_rx: watch::Receiver<DownloadState>,
}
```

### DataSource::Proxy variant

```rust
Proxy {
    file: std::fs::File,
    file_buf: BytesMut,
    temp_file: PathBuf,                       // for log messages only
    watch_rx: watch::Receiver<DownloadState>,
    wait_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}
```

`file_buf` reuses the existing `BytesMut` buffering pattern from `DataSource::File`.

### HEAD path

Drop `watch_rx` immediately after `head_response()`. download_task does not observe the drop and runs to completion, caching the file normally. The drain task hack is eliminated.

### GET path

Pass `temp_file` (opened as `std::fs::File`) and `watch_rx` into `StreamingBody::new_proxy`. Body reads from file, gated on watch state.

### ProxyResponseParts

```rust
pub struct ProxyResponseParts<'a> {
    pub content_type: &'a str,
    pub total_size: usize,
    pub temp_file: std::fs::File,
    pub watch_rx: watch::Receiver<DownloadState>,
    pub bwm: Option<Arc<BandwidthMonitor>>,
}
```

`temp_file` is opened (read-only) in `mod.rs` before constructing `ProxyResponseParts`, and passed as `std::fs::File` so the body can read from offset 0.

## Error handling

| Scenario | Behaviour |
|----------|-----------|
| Network error mid-download | `watch_tx.send(Failed)`, body sees Failed, ProxyEndedEarly if incomplete |
| Disk write error | same as network error |
| SHA1 mismatch | temp deleted, `watch_tx.send(Failed)` |
| rename failure | temp deleted, `watch_tx.send(Failed)` |
| Client abort (body dropped) | watch_rx drops, download_task unaffected, completes normally |
| Body slow | download_task writes at full speed, body catches up via watch |
| 300s timeout | `watch_tx.send(Failed)`, body sees Failed |

## Files touched

- `src/proxy_downloader.rs` — replace mpsc with watch; add `DownloadState`; update `download_task` to send watch events; update struct
- `src/server/body.rs` — replace `DataSource::Proxy` fields; add `wait_fut`; rewrite Proxy poll_frame step
- `src/server/response.rs` — update `ProxyResponseParts`
- `src/server/mod.rs` — open temp_file for reading, pass to ProxyResponseParts; HEAD path drops watch_rx directly

## What is NOT changing

- Retry logic in `ProxyFileDownloader::new()`
- `create_temp_file`
- `build_proxy_client`
- SHA1 verification and cache import logic
- `fill_file_buffer_limited` (reused as-is)
- Access log / `BodyCompletionStatus` wiring
- Bandwidth throttling
- Java protocol semantics
