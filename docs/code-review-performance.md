# Code Review: Performance & Memory Footprint

**Scope:** All source files under `src/`  
**Focus:** Performance, memory footprint, deadlock, lock scope, unnecessary clone/allocation (zero-copy)

---

## Critical

### C1 · Sync mutex held across blocking `fs::write` — `cache/mod.rs:449–489`

`save_persistent_data` acquires `std::sync::Mutex` locks and then calls `fs::write` while holding them. Since this runs on a Tokio worker thread (called from `client.rs:219` in an async context), blocking I/O under a sync mutex starves other tasks on the same thread.

```rust
// current — lock held across blocking write
{
    let ages = self.static_range_oldest.lock().unwrap();
    let ages_data = bincode::serialize(&*ages)?;
    fs::write(&ages_path, &ages_data)?;   // ← blocking I/O under lock
}
```

**Fix:** Serialize under the lock, drop the lock, then write the `Vec<u8>` to disk outside the lock scope.

---

### C2 · `std::thread::sleep` on Tokio worker thread — `cache/pruner.rs:138`

`execute_prune` calls `std::thread::sleep(Duration::from_millis(1000))` between file deletions. This blocks the entire Tokio worker thread for up to 1 second per deleted file, stalling all other tasks sharing that thread.

**Fix:** Either wrap `execute_prune` in `tokio::task::spawn_blocking`, or convert to `async` and use `tokio::time::sleep`.

---

### C3 · File opened on every 1460-byte chunk in proxy body — `body.rs:381–413`

`DataSource::Proxy`'s `poll_frame` calls `std::fs::File::open(&*temp_file)` on every chunk. For a 10 MB file this is ~7000 open/seek/read/close syscalls per request.

```rust
// current — open on every poll_frame call
let mut file = std::fs::File::open(&*temp_file)?;
file.seek(SeekFrom::Start(*read_offset))?;
```

**Fix:** Open the file once during `StreamingBody` construction and store the `File` handle inside `DataSource::Proxy`. Track read position to use sequential reads and avoid per-chunk seeks.

---

### C4 · New `reqwest::Client` per download request — `proxy_downloader.rs:55–71`, `downloader.rs:52–65`

`ProxyFileDownloader::new` and `FileDownloader::download` each create a fresh `reqwest::Client` (which spawns background tasks and creates a connection pool) on every invocation. The Java client reused a single `HttpClient`.

**Fix:** Add a shared `reqwest::Client` to `AppState` (or `RpcClient`) and pass it by `Arc` into both downloaders.

---

### C5 · New `reqwest::Client` per speed-test command — `server.rs:484–490`

`run_threaded_proxy_test` creates a new `reqwest::Client` every time a `threaded_proxy_test` command arrives, on the request-handler hot path.

**Fix:** Same as C4 — share a client from `AppState`.

---

## Important

### I1 · Redundant `Arc` clone after `load_full()` — `server.rs:163`

```rust
state.bandwidth_monitor.load_full().clone()
//                                  ^^^^^^ load_full() already returns a clone
```

`load_full()` already returns an owned `Arc`. The trailing `.clone()` increments the refcount a second time unnecessarily. Called on every HTTP request.

**Fix:** Remove `.clone()`.

---

### I2 · `to_string().to_lowercase()` allocation chain on every request — `server.rs:150–153`, `server.rs:967–1001`

```rust
let host_addr = client_ip.to_string().to_lowercase();       // 2 allocs
let is_rpc = config.rpc_servers.iter()
    .any(|s| s.to_string().to_lowercase() == host_addr);    // 2 allocs × N servers
```

Two allocations per request for `host_addr`, plus two more per RPC server entry per request for the comparison. The same pattern appears in both the request handler and the accept-loop task.

**Fix:** Since `rpc_servers` is already normalized to `IpAddr::V4` (after the C4 fix), `to_string()` is canonical. Pre-compute and store the string representations at config-parse time, or compare `IpAddr` values directly.

---

### I3 · `sha1_file` reads entire file into memory; double-read in `read_persistent_object` — `utils.rs:29–34`, `cache/mod.rs:398–429`

`sha1_file` reads the whole file into a `Vec<u8>` just to hash it. In `read_persistent_object`, the file is first read by `fs::read` (line ~415) and then `sha1_file` reads it again — two full reads of the same data.

**Fix:** Add a `sha1_bytes(&[u8]) -> String` helper and call it on the already-loaded buffer. In `save_persistent_data`, hash the serialized `Vec<u8>` directly (no disk re-read needed at all).

---

### I4 · Third redundant lock on `lru` mutex — `cache/mod.rs:475`

`save_persistent_data` acquires `self.lru` three times: once to serialize (line 463), once to write (same block), and a third time just to read `lru_clear_pointer` (line 475) — a value that was available in the second lock scope.

**Fix:** Capture `lru_clear_pointer` inside the second lock scope alongside the serialization.

---

### I5 · `String::replace` allocates on every connection — `server.rs:151–152`, `server.rs:997–998`

```rust
cfg.client_host.replace("::ffff:", "") == host_addr
```

`String::replace` always allocates a new `String` even when no substitution occurs (the common case). Called on every accepted connection.

**Fix:** Strip the `::ffff:` prefix from `client_host` once at config-apply time so the per-connection comparison is a plain `==`.

---

### I6 · `tokio::sync::Mutex` where `std::sync::Mutex` suffices — `bandwidth.rs`

`BwmInner` is protected by a `tokio::sync::Mutex` but the lock is never held across an `.await` point (the sleep happens after the lock is released). `tokio::sync::Mutex` has higher overhead than `std::sync::Mutex` (future allocation, yield point machinery).

**Fix:** Change `inner: tokio::sync::Mutex<BwmInner>` to `inner: std::sync::Mutex<BwmInner>`.

---

### I7 · Full `Config` clone (including large `HashMap`) on every RPC call — `rpc_client.rs:50–97`

Every RPC success/failure does:
```rust
let mut new = (**cfg).clone();   // clones static_ranges HashMap + all fields
new.rpc_current = Some(host);
self.config.store(Arc::new(new));
```

`Config` contains `HashMap<String, u8>` for `static_ranges` (can be large) and `Vec<IpAddr>` for `rpc_servers`. This full clone happens on every RPC round-trip just to update one `Option<String>` field.

**Fix:** Split `rpc_current` / `rpc_last_failed` into a separate `Arc<Mutex<RpcState>>`, or use `config.rcu(...)` (as `apply_server_response` already does).

---

### I8 · `hex_encode` allocates a `String` per byte — `utils.rs:37–39`

```rust
data.iter().map(|b| format!("{:02x}", b)).collect()
```

20 heap allocations for a SHA-1 digest. Called on every keystamp validation, file integrity check, and RPC signing.

**Fix:**
```rust
let mut out = String::with_capacity(data.len() * 2);
for b in data { write!(out, "{:02x}", b).unwrap(); }
out
```

---

### I9 · Request line string built then immediately re-parsed — `server.rs:165–172`

```rust
let request_line = format!("{} {} {:?}", req.method(), req.uri()..., req.version());
let request_type = request::parse_request(&request_line, client_ip, &config);
// parse_request then splits on spaces again
```

Method, URI, and version are already available as structured types on `Request`. Building a string just to split it is unnecessary.

**Fix:** Pass `req.method()`, `req.uri()`, and `req.version()` directly to `parse_request` and remove the intermediate `String`.

---

### I10 · Async `stat` + sync `open` = two syscalls with TOCTOU window — `response.rs:180–207`

`file_response` calls `tokio::fs::metadata` (stat) and then `StreamingBody::new_file` calls `std::fs::File::open` — two separate syscalls on the same path. The file could be deleted between them. `open` also implicitly stats the file.

**Fix:** Open the file first, then call `file.metadata()` on the open `File` to get the size. One syscall, no race window.

---

## Minor

### M1 · `save_persistent_data` triple-locks `lru` mutex — `cache/mod.rs`

Covered by C1 + I4 above. The third lock at line 475 is eliminated when `lru_clear_pointer` is captured in the second lock scope.

---

### M2 · `Instant` subtraction can panic on clock jump — `server.rs:96`

```rust
let elapsed_ms = (now - self.last_connect).as_millis() as u32;
```

`Instant` subtraction panics in debug mode if `now < self.last_connect` (e.g., after an NTP correction).

**Fix:** `now.checked_duration_since(self.last_connect).unwrap_or_default().as_millis() as u32`

---

### M3 · `RwLock` write lock for every byte-count update — `stats.rs:54–59`

`record_bytes_sent` acquires a write lock on `bytes_sent_history` to increment `hist[0]`, blocking all readers on every file chunk sent. `shift_bytes_sent_history` (every 10 s) genuinely needs exclusive access, but the per-chunk update does not.

**Fix:** Replace `RwLock<Vec<u64>>` with a fixed-size array of `AtomicU64`. The 10-second shift can still use a short exclusive lock or a seqlock pattern.

---

### M4 · `HashMap` allocated per request in `parse_additional` — `utils.rs:43–55`

`parse_additional` creates a `HashMap` and two `String`s per key-value pair. The map is used to look up 2–4 keys and then discarded.

**Fix:** For the common case (small number of keys), a linear scan over the raw `&str` avoids all allocations. Return an iterator or use a small stack-allocated structure.

---

### M5 · `HVFile::fileid()` allocates a `String` on every call — `hvfile.rs:51–69`

`fileid()` is called multiple times per request (cache path lookup, LRU marking, proxy temp file naming). Each call allocates a new `String`.

**Fix:** Cache the `FileId` as a field inside `HVFile` at parse time, or return `&str` where possible.

---

### M6 · Per-line `String` allocation for all RPC response lines — `rpc.rs:136–139`

```rust
lines: lines[1..].iter().map(|s| s.to_string()).collect(),
```

Allocates one `String` per response line. For `get_blacklist` responses this can be thousands of lines. The original `body: String` is already owned.

**Fix:** Keep the owned `body` string and store byte-range offsets, or use `Arc<str>` slices.

---

### M7 · Multiple intermediate `String`s per request in access log — `access_log.rs`

`access_log_prefix`, `access_log_request_info`, and `access_log_completion_line` each allocate a `String` on every HTTP request.

**Fix:** Use `tracing`'s structured fields (key-value pairs) instead of pre-built strings, or write directly into a single pre-allocated buffer.

---

### M8 · Sync mutex held during full `HashMap` scan in pruner — `cache/mod.rs:727–757`

`check_prune_action` holds `static_range_oldest` (`std::sync::Mutex`) for the entire `min_by_key` traversal across all static ranges. For large caches this is a long hold on a thread-blocking mutex.

**Fix:** Snapshot the needed data (clone the `HashMap` or collect keys/values) under the lock, drop the lock, then do the scan.

---

### M9 · Redundant `Arc` clones before task spawn in proxy downloader — `proxy_downloader.rs:185–197`

Before spawning the download task, multiple fields are cloned separately (`write_offset`, `temp_file`, `notify`, `body_done_notify`, `download_done`) even though the same values are stored in `this` which is returned. The task could capture a subset of `this` directly.

---

### M10 · `rand::rng()` called twice in RPC host selection — `config.rs:223–224`

```rust
let mut idx = (rand::rng().next_u32() ...) as isize;
let dir = if rand::rng().next_u32() & 1 == 0 { -1 } else { 1 };
```

Two separate thread-local RNG handle acquisitions where one suffices.

**Fix:** `let mut rng = rand::rng(); let idx = ...; let dir = ...;`

---

## Summary

| ID | Severity | File | Issue |
|----|----------|------|-------|
| C1 | **Critical** | `cache/mod.rs:449–489` | Sync mutex held across blocking `fs::write` in async context |
| C2 | **Critical** | `cache/pruner.rs:138` | `std::thread::sleep` blocks Tokio worker thread |
| C3 | **Critical** | `body.rs:381–413` | File opened on every 1460-byte chunk in proxy body |
| C4 | **Critical** | `proxy_downloader.rs:55`, `downloader.rs:52` | New `reqwest::Client` per download request |
| C5 | **Critical** | `server.rs:484` | New `reqwest::Client` per speed-test command |
| I1 | Important | `server.rs:163` | Redundant `Arc` clone after `load_full()` |
| I2 | Important | `server.rs:150–153`, `967–1001` | `to_string().to_lowercase()` allocation chain per request |
| I3 | Important | `utils.rs:29–34`, `cache/mod.rs:398–429` | `sha1_file` double-reads file; re-read avoidable |
| I4 | Important | `cache/mod.rs:475` | Third redundant lock on `lru` mutex |
| I5 | Important | `server.rs:151`, `997` | `String::replace` allocates on every connection |
| I6 | Important | `bandwidth.rs` | `tokio::sync::Mutex` where `std::sync::Mutex` suffices |
| I7 | Important | `rpc_client.rs:50–97` | Full `Config` clone (large `HashMap`) per RPC call |
| I8 | Important | `utils.rs:37–39` | `hex_encode` allocates per byte |
| I9 | Important | `server.rs:165–172` | Request line string built then immediately re-parsed |
| I10 | Important | `response.rs:180–207` | Async stat + sync open = two syscalls + TOCTOU |
| M1 | Minor | `cache/mod.rs` | Triple lock on `lru` (covered by C1+I4) |
| M2 | Minor | `server.rs:96` | `Instant` subtraction panics on clock jump |
| M3 | Minor | `stats.rs:54–59` | `RwLock` write lock for every byte-count update |
| M4 | Minor | `utils.rs:43–55` | `HashMap` allocated per request for `parse_additional` |
| M5 | Minor | `hvfile.rs:51–69` | `fileid()` allocates `String` on every call |
| M6 | Minor | `rpc.rs:136–139` | Per-line `String` allocation for all RPC response lines |
| M7 | Minor | `access_log.rs` | Multiple intermediate `String`s per request |
| M8 | Minor | `cache/mod.rs:727–757` | Sync mutex held during full `HashMap` scan |
| M9 | Minor | `proxy_downloader.rs:185–197` | Redundant `Arc` clones before task spawn |
| M10 | Minor | `config.rs:223–224` | `rand::rng()` called twice in RPC host selection |
