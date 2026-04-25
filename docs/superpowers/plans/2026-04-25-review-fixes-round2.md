# Review Fixes Round 2 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix protocol-level correctness and behavioral equivalence issues from the second REVIEW.md, bringing the Rust port closer to 1:1 with Java 1.6.5.

**Architecture:** Each task targets 1-2 specific files. Changes are independent and can be done in any order. Priority 1 tasks (RPC stickiness, cache hit logic) have the highest impact.

**Tech Stack:** Rust, Tokio, Hyper, reqwest, arc-swap, rand

**Source paths:**
- Rust: `src/`
- Java: `/Users/yukko/Code/HentaiAtHome/HentaiAtHome_1.6.5_src/src/hath/base/`

---

## File Structure Map

| Task | File(s) | Issue | Severity |
|------|---------|-------|----------|
| 1 | `rpc_client.rs` | RPC server stickiness | HIGH |
| 2 | `server.rs` | Cache hit: size check + LRU tracking | HIGH |
| 3 | `server.rs:566` | Cert always-download on startup | HIGH |
| 4 | `proxy_downloader.rs` | Proxy support + random temp filename | MEDIUM |
| 5 | `rpc.rs:114` | TEMPORARILY_UNAVAILABLE starts_with | MEDIUM |
| 6 | `client.rs` + `config.rs` | min_client_build fatal | MEDIUM |
| 7 | `request.rs` + `response.rs` + `server.rs` | HTTP 400/405 codes | MEDIUM |
| 8 | `downloader.rs` | Connection: Close header | LOW |
| 9 | `client.rs` | refresh_settings after client_start | LOW |

---

### Task 1: RPC Server Stickiness

**Problem:** No code ever sets `rpc_current = Some(...)`. `get_rpc_host()` returns a fresh random host every call. Java caches `rpcServerCurrent` after first successful RPC and reuses it.

**Files:**
- Modify: `src/rpc_client.rs:61-69`

**Java ref:** `Settings.java:627` — `rpcServerCurrent = candidate;`

- [ ] **Step 1: Persist selected RPC host on successful calls**

In `src/rpc_client.rs`, after line 65 (`new.rpc_current = None;`) and before line 69 (`return Ok(parsed);`), add persistence of the host on non-Null responses.

Current code (lines 61-69):
```rust
            if parsed.status == ResponseStatus::Null {
                let fail_host = parsed.fail_host.as_deref().unwrap_or(&host);
                let mut new = (**cfg).clone();
                new.rpc_last_failed = Some(fail_host.to_string());
                new.rpc_current = None;
                self.config.store(Arc::new(new));
            }

            return Ok(parsed);
```

Replace with:
```rust
            if parsed.status == ResponseStatus::Null {
                let fail_host = parsed.fail_host.as_deref().unwrap_or(&host);
                let mut new = (**cfg).clone();
                new.rpc_last_failed = Some(fail_host.to_string());
                new.rpc_current = None;
                self.config.store(Arc::new(new));
            } else {
                // Java: persist the selected RPC host so subsequent calls reuse it.
                // rpcServerCurrent is cached until cleared on failure or periodic reset.
                let mut new = (**cfg).clone();
                new.rpc_current = Some(host.clone());
                self.config.store(Arc::new(new));
            }

            return Ok(parsed);
```

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/rpc_client.rs
git commit -m "fix(rpc): persist selected RPC host for session stickiness

Java caches rpcServerCurrent after the first successful RPC call,
reusing the same host until failure or periodic reset. Previously
Rust never persisted the selection, re-randomizing on every call.
Now rpc_current is set to Some(host) after each non-Null response."
```

---

### Task 2: Cache Hit Logic — Size Check + Proxy Fallback + LRU Tracking

**Problem:** `server.rs:167` only checks `cache_path.exists()`, not file size. Java checks `file.exists() && file.length() == expectedSize`. Wrong-sized files should fall through to proxy, not return 500. Also missing `mark_recently_accessed()` call.

**Files:**
- Modify: `src/server.rs:165-172`

**Java ref:** `HTTPResponse.java:201-211`

- [ ] **Step 1: Add size check and LRU tracking to cache hit path**

In `src/server.rs` lines 165-172, replace:

```rust
                    } else if let Some(ref hv) = hv_file {
                        let cache_path = hv.cache_path(&config.cache_dir);
                        if cache_path.exists() {
                            state.stats.record_file_sent();
                            if !is_local && !is_rpc {
                                state.stats.record_bytes_sent(hv.size as u64);
                            }
                            response::file_response(hv, &config.cache_dir, bwm_for_request).await
                        } else {
```

With:

```rust
                    } else if let Some(ref hv) = hv_file {
                        let cache_path = hv.cache_path(&config.cache_dir);
                        // Java: check exists AND size before serving from cache.
                        // Wrong-sized files fall through to proxy download, not error.
                        let cache_hit = cache_path.exists()
                            && cache_path.metadata()
                                .map(|m| m.len() == hv.size as u64)
                                .unwrap_or(false);
                        if cache_hit {
                            // Java: markRecentlyAccessed — update LRU + file mtime.
                            state.cache.mark_recently_accessed(hv);
                            state.stats.record_file_sent();
                            if !is_local && !is_rpc {
                                state.stats.record_bytes_sent(hv.size as u64);
                            }
                            response::file_response(hv, &config.cache_dir, bwm_for_request).await
                        } else {
```

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors (method `mark_recently_accessed` already exists in `cache/mod.rs`)

- [ ] **Step 3: Commit**

```bash
git add src/server.rs
git commit -m "fix(server): add size check and LRU tracking to cache hit path

- Java checks file.length() == expectedSize before serving; Rust now
  does the same via metadata().len(). Wrong-sized files fall through
  to proxy fallback instead of producing 500 Internal Server Error.
- Call cache.mark_recently_accessed() on cache hit, matching Java's
  LRU bit update and conditional mtime refresh on the served file."
```

---

### Task 3: Certificate Always-Download on Startup

**Problem:** `server.rs:566` calls `build_tls_acceptor(&config, false)`. Java unconditionally downloads the cert on every server start.

**Files:**
- Modify: `src/server.rs:566`

**Java ref:** `HTTPServer.java:74-78` — unconditional download

- [ ] **Step 1: Change force_download to true at startup**

In `src/server.rs` line 566, replace:

```rust
    let (tls_acceptor, cert_expiry) = match build_tls_acceptor(&config, false).await {
```

With:

```rust
    // Java: always re-downloads the certificate on startup.
    let (tls_acceptor, cert_expiry) = match build_tls_acceptor(&config, true).await {
```

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/server.rs
git commit -m "fix(server): always download certificate on startup

Java unconditionally downloads hathcert.p12 on every server start.
Previously Rust only downloaded when the file didn't exist. Now
passes force_download=true at startup, matching Java behavior."
```

---

### Task 4: Proxy Downloader — Proxy Support + Random Temp Filename

**Problem A:** `try_source()` builds a reqwest Client with no proxy configuration. Java uses `Settings.getImageProxy()`.

**Problem B:** Temp filename is `proxyfile_<fileid>` — deterministic. Concurrent requests for same fileid conflict. Java uses `File.createTempFile()` with random suffix.

**Files:**
- Modify: `src/proxy_downloader.rs:49-52` (proxy)
- Modify: `src/proxy_downloader.rs:122-125` (temp file)

**Java ref:** `ProxyFileDownloader.java:77-84` (proxy), `ProxyFileDownloader.java:117` (temp file)

- [ ] **Step 1: Add proxy support and connect timeout to Client builder**

In `src/proxy_downloader.rs` lines 49-52, replace:

```rust
        let client = Client::builder()
            .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
            .build()
            .map_err(|e| HathError::Network(e.to_string()))?;
```

With:

```rust
        let mut builder = Client::builder()
            .user_agent(format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION))
            .connect_timeout(std::time::Duration::from_secs(5));

        // Java: proxy support via Settings.getImageProxy()
        if let (Some(proxy_type), Some(proxy_host), Some(proxy_port)) =
            (&config.image_proxy_type, &config.image_proxy_host, config.image_proxy_port)
        {
            let proxy_url = format!("{}://{}:{}", proxy_type, proxy_host, proxy_port);
            if let Ok(proxy) = reqwest::Proxy::all(&proxy_url) {
                builder = builder.proxy(proxy);
            }
        }

        let client = builder.build()
            .map_err(|e| HathError::Network(e.to_string()))?;
```

- [ ] **Step 2: Use random suffix for temp filename**

In `src/proxy_downloader.rs` lines 122-125, replace:

```rust
        let temp_file = config
            .temp_dir
            .join(format!("proxyfile_{}", hv_file.fileid().as_str()));
```

With:

```rust
        // Java: File.createTempFile("proxyfile_", "", tempDir) — random suffix prevents
        // concurrent requests for the same fileid from clobbering each other.
        let random_suffix: u32 = rand::rng().next_u32();
        let temp_file = config
            .temp_dir
            .join(format!("proxyfile_{}_{:08x}", hv_file.fileid().as_str(), random_suffix));
```

- [ ] **Step 3: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/proxy_downloader.rs
git commit -m "fix(proxy): add proxy support and random temp filenames

- Wire image_proxy_* config to reqwest Proxy, matching Java's
  Settings.getImageProxy() behavior.
- Add random hex suffix to temp filenames to prevent concurrent
  request collisions, matching Java's File.createTempFile()."
```

---

### Task 5: TEMPORARILY_UNAVAILABLE starts_with Match

**Problem:** `rpc.rs:114` uses Rust pattern matching (exact `==`). Java uses `split[0].startsWith("TEMPORARILY_UNAVAILABLE")`.

**Files:**
- Modify: `src/rpc.rs:114-118`

**Java ref:** `ServerResponse.java:74`

- [ ] **Step 1: Change to starts_with match**

In `src/rpc.rs` line 114, replace:

```rust
        "TEMPORARILY_UNAVAILABLE" => ServerResponse {
            status: ResponseStatus::Fail,
            fail_reason: "Temporarily Unavailable".into(),
            fail_code: Some("TEMPORARILY_UNAVAILABLE".into()),
```

With:

```rust
        s if s.starts_with("TEMPORARILY_UNAVAILABLE") => ServerResponse {
            status: ResponseStatus::Fail,
            fail_reason: "Temporarily Unavailable".into(),
            fail_code: Some(s.to_string()),
```

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/rpc.rs
git commit -m "fix(rpc): use starts_with for TEMPORARILY_UNAVAILABLE matching

Java uses split[0].startsWith() which matches suffixed variants like
'TEMPORARILY_UNAVAILABLE:reason'. Previously Rust used exact match
which would not recognize these extended forms."
```

---

### Task 6: min_client_build Should Be Fatal

**Problem:** `config.rs:242-246` only logs an error when the server requires a newer build. Java calls `dieWithError()` which terminates the client.

**Files:**
- Modify: `src/client.rs:77-78` (pre-check before applying settings)
- Modify: `src/config.rs:242-247` (remove log-only branch)

**Java ref:** `Settings.java:187-190` — `HentaiAtHomeClient.dieWithError(...)`

**Approach:** Check `min_client_build` in `client.rs` before calling `apply_setting`, and return a fatal error if the required build exceeds `CLIENT_BUILD`. Remove the log-only branch from `apply_setting`.

- [ ] **Step 1: Add min_client_build pre-check in login response handler**

In `src/client.rs`, after line 78 (login_resp check) and before line 79 (`config.rcu(...)`), insert:

```rust
    // Java: min_client_build is fatal if server requires a newer build.
    // Check before applying settings so we can return an error.
    for line in &login_resp.lines {
        if let Some((key, value)) = line.split_once('=') {
            if key.eq_ignore_ascii_case("min_client_build") {
                if let Ok(build) = value.parse::<i32>() {
                    if build > rpc::CLIENT_BUILD {
                        return Err(HathError::Fatal(format!(
                            "Your client is too old to connect to the Hentai@Home Network. \
                             Required build: {}, our build: {}. \
                             Please download a newer version of the client.",
                            build, rpc::CLIENT_BUILD
                        )));
                    }
                }
                break;
            }
        }
    }
```

Also add the same check in `refresh_settings` (Task 9) when it's implemented — in the `config.rcu` block that applies settings from the refresh response, add the same pre-check loop before iterating lines.

- [ ] **Step 2: Remove min_client_build from apply_setting**

In `src/config.rs` lines 242-247, replace:

```rust
            "min_client_build" => {
                if let Ok(build) = value.parse::<i32>()
                    && build > 178 {
                        tracing::error!("Client too old! Required build: {}, our build: 178", build);
                    }
            }
```

With just a comment:

```rust
            // min_client_build is checked before apply_setting (fatal if too old).
```

- [ ] **Step 3: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add src/client.rs src/config.rs
git commit -m "fix(config): make min_client_build fatal like Java

Java calls dieWithError() when the server requires a newer client
build. Previously Rust only logged an error and continued running.
Now the check is done before applying settings and returns a fatal
error, terminating the client."
```

---

### Task 7: HTTP Return Codes — 400 for Malformed /h + 405 for Illegal Method

**Problem A:** `request.rs:74` returns `NotFound` (404) for `< 4` URL parts. Java returns 400 (Bad Request).

**Problem B:** `request.rs:39-41` returns `NotFound` (404) for non-GET/HEAD methods. Java returns 405 (Method Not Allowed) + `Allow: GET, HEAD`.

**Files:**
- Modify: `src/request.rs` (enum + return values)
- Modify: `src/response.rs` (new `method_not_allowed_response`)
- Modify: `src/server.rs` (wire new variants)

- [ ] **Step 1: Add new RequestType variants and update parse functions**

In `src/request.rs`, find the `RequestType` enum definition and add two variants before the closing `}`:

```rust
    BadRequest,
    MethodNotAllowed,
```

In `src/request.rs` line 39-41, change method check:

```rust
    if !matches!(method.to_uppercase().as_str(), "GET" | "HEAD") {
        return RequestType::MethodNotAllowed;
    }
```

In `src/request.rs` line 74, change parse_file_serve:

```rust
    if url_parts.len() < 4 { return RequestType::BadRequest; }
```

- [ ] **Step 2: Add method_not_allowed_response to response.rs**

In `src/response.rs`, add after `bad_request_response()`:

```rust
pub fn method_not_allowed_response() -> Result<Response<StreamingBody>> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(header::ALLOW, "GET, HEAD")
        .header(header::CONTENT_TYPE, "text/html; charset=iso-8859-1")
        .header(header::CONNECTION, "close")
        .body(StreamingBody::new(b"Method Not Allowed".to_vec(), None))
        .map_err(HathError::Http)
}
```

- [ ] **Step 3: Wire new variants in server.rs**

In `src/server.rs`, find the match block around line 244-246 where `NotFound` and `Favicon`/`Robots` are handled. Add before `NotFound`:

```rust
                RequestType::BadRequest => response::bad_request_response(),
                RequestType::MethodNotAllowed => response::method_not_allowed_response(),
```

- [ ] **Step 4: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add src/request.rs src/response.rs src/server.rs
git commit -m "fix(http): match Java return codes for malformed requests

- Malformed /h/<fileid>/<additional> (<4 parts) now returns 400 Bad
  Request, matching Java. Previously returned 404.
- Non-GET/HEAD methods now return 405 Method Not Allowed with
  Allow: GET,HEAD header, matching Java. Previously returned 404."
```

---

### Task 8: FileDownloader — Connection: Close Header

**Problem:** Java `FileDownloader.java:167` sets `Connection: Close`. Rust `downloader.rs` does not.

**Files:**
- Modify: `src/downloader.rs:82-85`

**Java ref:** `FileDownloader.java:167` — `connection.setRequestProperty("Connection", "Close");`

- [ ] **Step 1: Add Connection: Close header**

In `src/downloader.rs` lines 82-85, replace:

```rust
        let mut resp = client.get(self.source.clone())
            .timeout(std::time::Duration::from_millis(self.timeout_ms))
            .send().await
            .map_err(|e| HathError::Network(e.to_string()))?;
```

With:

```rust
        let mut resp = client.get(self.source.clone())
            // Java: setRequestProperty("Connection", "Close")
            .header("Connection", "Close")
            .timeout(std::time::Duration::from_millis(self.timeout_ms))
            .send().await
            .map_err(|e| HathError::Network(e.to_string()))?;
```

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/downloader.rs
git commit -m "fix(downloader): add Connection: Close header to requests

Java FileDownloader sets Connection: Close to prevent connection
reuse. Previously Rust relied on reqwest's default keep-alive."
```

---

### Task 9: refresh_settings After client_start

**Problem:** Java calls `serverHandler.refreshServerSettings()` after `notifyStart()` (line 177 of `HentaiAtHomeClient.java`). Rust never calls `refresh_settings()` after startup.

**Files:**
- Modify: `src/client.rs` (after `stats.program_started()`)

**Java ref:** `HentaiAtHomeClient.java:177`

- [ ] **Step 1: Add refresh_settings after client_start success**

In `src/client.rs`, after line 165 (`stats.program_started();`) and before the initial blacklist fetch section, insert:

```rust
    // Java: refreshServerSettings() after notifyStart to check schedule.
    match rpc_client.refresh_settings().await {
        Ok(refresh_resp) if refresh_resp.status == ResponseStatus::Ok => {
            // Check min_client_build before applying other settings.
            for line in &refresh_resp.lines {
                if let Some((key, value)) = line.split_once('=') {
                    if key.eq_ignore_ascii_case("min_client_build") {
                        if let Ok(build) = value.parse::<i32>() {
                            if build > rpc::CLIENT_BUILD {
                                tracing::error!(
                                    "Server requires build {}, but client is build {}. \
                                     Continuing anyway (startup already completed).",
                                    build, rpc::CLIENT_BUILD
                                );
                            }
                        }
                        break;
                    }
                }
            }
            config.rcu(|current| {
                let mut new = (**current).clone();
                for line in &refresh_resp.lines {
                    if let Some((key, value)) = line.split_once('=') {
                        new.apply_setting(&key.to_lowercase(), value);
                    }
                }
                Arc::new(new)
            });
        }
        Ok(_) => {
            tracing::warn!("refresh_settings returned non-OK status after startup");
        }
        Err(e) => {
            tracing::warn!("Failed to refresh settings after startup: {}", e);
        }
    }
```

Note: `min_client_build` here is logged as an error but not fatal (we already passed `client_start`). This matches Java's behavior where `refreshServerSettings` after startup runs in the main loop and doesn't kill the process.

- [ ] **Step 2: Check compilation**

Run: `cargo check`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/client.rs
git commit -m "fix(client): refresh settings after client_start

Java calls refreshServerSettings() after notifyStart() to check
the client's schedule and pick up any settings changes. Previously
Rust only loaded settings during client_login. Now refreshes after
client_start completes successfully."
```

---

## Skipped (Low/No Impact)

| Issue | Reason |
|-------|--------|
| FileDownloader `max_dl_time_ms` unused | Also dead code in Java — `maxDLTime` is stored but never checked |
| Cache full rescan LRU bit init | Negligible impact — extra mtime update on first access only |
| Response header order | Hyper sets header order; changing would require custom serialization |
| Charset casing (`iso-8859-1`) | Both Java and Rust use the same casing; no difference found |
| Status reason phrase | Hyper uses canonical reason phrases which match Java's `getHTTPStatusHeader()` for standard codes |
| reqwest `.timeout()` vs per-read timeout | reqwest doesn't support per-read timeouts; total-request timeout is a reasonable approximation |

---

## Summary

| Task | Priority | Issue | Files |
|------|----------|-------|-------|
| 1 | HIGH | RPC stickiness | `rpc_client.rs` |
| 2 | HIGH | Cache hit size check + LRU | `server.rs` |
| 3 | HIGH | Cert always-download | `server.rs:566` |
| 4 | MEDIUM | Proxy + random temp file | `proxy_downloader.rs` |
| 5 | MEDIUM | TEMPORARILY_UNAVAILABLE starts_with | `rpc.rs:114` |
| 6 | MEDIUM | min_client_build fatal | `client.rs` + `config.rs` |
| 7 | MEDIUM | HTTP 400/405 codes | `request.rs` + `response.rs` + `server.rs` |
| 8 | LOW | Connection: Close | `downloader.rs` |
| 9 | LOW | refresh_settings after start | `client.rs` |
