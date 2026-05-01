# Proxy Streaming Redesign

**Date:** 2026-05-01
**Status:** Approved

## Problem

The current proxy streaming implementation uses a shared temp file with `AtomicU64` + `Notify` to coordinate between the download task (writer) and the HTTP body (reader). This has caused recurring `proxy_ended_early` errors due to:

1. Retry truncating the temp file while the body's file handle cursor is mid-stream.
2. A TOCTOU race where `write_offset` resets to 0 between the `need_wait` check and the `max_read` computation.

The fundamental issue is that file-based coordination is fragile: two separate OS file descriptors, cursor state, and atomic synchronization are all required just to pass bytes from one async task to another.

## Design

### Core idea: mpsc channel instead of shared file

Replace the shared temp file + `AtomicU64`/`Notify`/`AtomicBool` coordination with a `tokio::sync::mpsc` channel. The download task sends `Bytes` chunks; the body receives them. The channel's built-in backpressure replaces the manual `write_offset` threshold checks.

### Retry strategy: connect-before-stream

Retry (both multi-source and per-source) happens entirely inside `ProxyFileDownloader::new()`, before the channel is created and before the body is returned to the caller. Once `new()` returns `Ok`, a live HTTP response is in hand, the channel exists, and the download task is spawned. The body side never knows retry happened.

This matches Java semantics: `ProxyFileDownloader.initialize()` loops over sources and returns only after a successful connection, then `run()` streams without retry awareness from the body side.

### Data flow

```
ProxyFileDownloader::new()
  for source in sources:
    for attempt in 0..3:
      resp = client.get(source).send().await  ← retry here, before channel
      if resp ok and Content-Length matches: break

  let (tx, rx) = mpsc::channel::<Bytes>(8)   ← channel created after success

  tokio::spawn(download_task(resp, tx, tmp_file, cache_path, sha1))
  return ProxyFileDownloader { rx, content_length, content_type }

download_task:
  open tmp_file (write)
  loop:
    chunk = resp.chunk().await
    tx.send(chunk).await          ← backpressure: blocks if body is slow
    file.write_all(&chunk).await  ← write to tmp_file
    sha1.update(&chunk)
  close tmp_file
  if sha1 ok:
    fs::rename(tmp_file, cache_path)
    cache.register_proxy_file(&hv)
  else:
    fs::remove_file(tmp_file)

StreamingBody (Proxy variant):
  poll_frame:
    rx.poll_recv(cx) → Bytes  ← no AtomicU64, no Notify, no file handle
    Poll::Ready(None)         ← channel closed = download finished, natural EOF
```

### Cache import chain

- Download task writes to `tmp/<uuid>` (same as today).
- On SHA1 success: `fs::rename(tmp → cache_path)` — atomic, no half-written files visible in cache.
- On SHA1 failure: delete tmp, don't import. Body already sent (Java behavior — no rollback).
- `cache.register_proxy_file()` called only after successful rename.

### StreamingBody changes

Replace `DataSource::Proxy` fields:

**Remove:**
- `file: std::fs::File`
- `file_buf: BytesMut`
- `write_offset: Arc<AtomicU64>`
- `notify: Arc<Notify>`
- `body_done_notify: Arc<Notify>`
- `download_done: Arc<AtomicBool>`
- `start_time: Instant`

**Add:**
- `rx: tokio::sync::mpsc::Receiver<Bytes>`
- `pending: Option<Bytes>` — leftover bytes from a chunk larger than TCP_PACKET_SIZE

`poll_frame` for Proxy:
```
if bwm throttle pending → wait
if pending.is_some() → slice TCP_PACKET_SIZE from it, return chunk
rx.poll_recv(cx):
  Pending → return Pending
  Ready(Some(bytes)) → store in pending, loop (apply throttle first if bwm)
  Ready(None) → finish(), return Ready(None)
```

### ProxyFileDownloader changes

**Remove:**
- `temp_file: PathBuf` (not exposed to caller)
- `write_offset: Arc<AtomicU64>`
- `notify: Arc<Notify>`
- `body_done_notify: Arc<Notify>`
- `download_done: Arc<AtomicBool>`
- `total_size: u64` (duplicates `content_length`)
- `fill_buffer()`, `wait_for_data()` (unused after redesign)

**Keep:**
- `content_length: usize`
- `content_type: String`

**Add:**
- `rx: tokio::sync::mpsc::Receiver<Bytes>`

The `ProxyResponseParts` struct in `response.rs` shrinks to just `{content_type, total_size, rx, bwm}`.

### Bandwidth throttling

Unchanged: `BandwidthMonitor::wait_for_quota(chunk_size)` is called per TCP_PACKET_SIZE chunk inside `poll_frame`, same as today.

### `body_done_notify` removal

Currently the download task waits up to 300s for `body_done_notify` before copying to cache. With channels this is unnecessary: when the body drops `rx`, the sender's `send().await` returns `Err(SendError)` on the next chunk. The download task detects this and aborts (no copy to cache). If the download finishes before the body drops (normal case), the channel drains naturally and the task proceeds to copy.

### Files touched

- `src/proxy_downloader.rs` — full rewrite of `ProxyFileDownloader` and `download_attempt`
- `src/server/body.rs` — replace `DataSource::Proxy` fields and `poll_frame` Proxy branch
- `src/server/response.rs` — shrink `ProxyResponseParts`
- `src/server/mod.rs` — update proxy response construction (fewer fields)
- Diagnostic logging added in previous commit can be removed

### What is NOT changing

- `try_source` / multi-source loop structure
- `create_temp_file` (still needed for tmp → rename pattern)
- `build_proxy_client` / proxy URL configuration
- Cache import SHA1 verification logic
- `register_proxy_file` in `CacheHandler`
- Access log / `BodyCompletionStatus` wiring
- Java protocol semantics
