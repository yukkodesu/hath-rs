# Hentai@Home Java 1.6.5 → Rust 1:1 Protocol Port — Code Review

**Review Date:** 2026-04-25

**Source Paths:**
- Rust: `src/`
- Java: `/Users/yukko/Code/HentaiAtHome/HentaiAtHome_1.6.5_src/src/hath/base/`

---

## Summary

| Severity | Count |
|----------|-------|
| **CRITICAL** | 1 |
| **HIGH** | 8 |
| **MEDIUM** | 12 |
| **LOW** | 6 |
| **NOTE** (intentional) | 9 |

---

## CRITICAL — Must Fix Before Use

### C1. Bandwidth Monitor is Non-Functional

**File:** `src/bandwidth.rs:42`

```rust
let now = Instant::now();
let now_millis = now.elapsed().as_millis() as u64;
```

`Instant::now().elapsed()` returns the duration since the Instant was *just created* — always ~0. This means `epoch_seconds` is always 0, `current_tick` is always 0, and all byte counters accumulate into `tick[0]`/`second[0]` forever. Once the per-tick quota is exceeded, **all subsequent requests sleep/retry indefinitely** because the tick never advances.

**Fix:** Replace with wall clock:
```rust
let now_millis = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64;
```
Or use `chrono::Utc::now().timestamp_millis()` since chrono is already a dependency.

---

## HIGH — Protocol-Level Divergences

### H1. KEY_EXPIRED Matching: `starts_with` vs `equals`

**File:** `src/rpc.rs:120`

```rust
first if first.starts_with("KEY_EXPIRED") => ...
```

**Java:** `ServerResponse.java:80` — `split[0].equals("KEY_EXPIRED")`

`starts_with` would also match `KEY_EXPIRED_SOMETHING`. Use `==` instead.

### H2. No KEY_EXPIRED Auto-Retry

**File:** `src/rpc_client.rs:24-46`

Java's `ServerResponse.getServerResponse()` (lines 80-84) catches `KEY_EXPIRED`, calls `retryhandler.refreshServerStat()`, and retries the RPC call with corrected server time. The Rust parser returns `ResponseStatus::Null` with `fail_code: "KEY_EXPIRED"`, but the caller treats all Null responses as server-failure — the client fails over to another RPC server instead of transparently recovering.

### H3. Connection Limit Off-by-One

**File:** `src/server.rs:629`

```rust
if active >= max_conns { // Rust: rejects at max
```

**Java:** `HTTPServer.java:247` — `sessionCount > maxConnections` (rejects strictly above max)

Rust allows `max_conns - 1` connections; Java allows `max_conns` connections.

### H4. Overload Notification Threshold Off-by-One

**File:** `src/server.rs:638`

```rust
if active >= (max_conns as f64 * 0.8) as u32 ... // >= 80%
```

**Java:** `HTTPServer.java:252` — `sessionCount > maxConnections * 0.8` (strictly above 80%)

Rust fires the overload notification 1 connection earlier.

### H5. RPC IPs Incorrectly Exempt from Bandwidth Throttling

**File:** `src/server.rs:141-145`

```rust
let bwm_for_request = if is_local || is_rpc { None } else { ... };
```

**Java:** `HTTPSession.java:160,202` — Only `localNetworkAccess` is exempt. RPC servers on non-local IPs are throttled. Rust exempts ALL RPC IPs regardless of network locality. This causes the Rust client to under-throttle traffic to RPC servers on different networks.

### H6. Header Bytes Hardcoded to 100

**File:** `src/server.rs:159`

```rust
bwm.wait_for_quota(100).await;
```

**Java:** `HTTPSession.java:161` — `bwm.waitForQuota(myThread, headerBytes.length)` — deducts the *actual* serialized header length. A typical response header is 200-400 bytes. Rust under-accounts bandwidth by 100-300+ bytes per response.

### H7. Proxy-Downloaded Files Not Registered in Cache

**File:** `src/proxy_downloader.rs:166-174`

Rust copies the temp file to the cache directory but does NOT call:
- `addFileToActiveCache` (so `cacheCount`/`cacheSize` are stale until next rescan)
- `markRecentlyAccessed` (file has no LRU bits set)
- `staticRangeOldest` update (static range tracking is incomplete)

**Java:** `CacheHandler.java:680-696` does all three.

### H8. No Per-Source Retry in Proxy Downloader

**File:** `src/proxy_downloader.rs:50-58`

Rust tries each source exactly once. Java's `ProxyFileDownloader.java:146-223` has an inner `do { ... } while(!streamThreadSuccess && --trycounter > 0)` retry loop (3 attempts per source).

---

## MEDIUM — Behavioral Differences with Functional Impact

### M1. RPC Server Failover: No Random Scan Direction

**File:** `src/config.rs:208-214`

```rust
idx = (idx + 1) % self.rpc_servers.len(); // always forward
```

**Java:** `Settings.java:613-631` — `scanDirection = Math.random() < 0.5 ? -1 : 1`, then `rpcServerSelector += scanDirection`

Rust always skips forward by 1; Java randomly walks forward or backward. When one server fails, Java distributes load more evenly across remaining servers.

### M2. Missing `%3d` URL Decoding

**File:** `src/request.rs:44-50`

`parse_additional` splits on `&` then `=`. Java's `HTTPResponse.java:151` first does `requestParts[1].replace("%3d", "=")` to decode URL-encoded equals signs. URLs with encoded `=` in the `additional` segment will fail in Rust.

### M3. 301 Redirect Missing Content-Type Header

**File:** `src/response.rs:61-66`

The `/favicon.ico` redirect response has no `Content-Type` header. Java's `HTTPSession.java:134` *always* sends `Content-Type: text/html; charset=iso-8859-1` regardless of status code. This is a header-level protocol divergence.

### M4. robots.txt Missing Charset Suffix

**File:** `src/response.rs:74`

```rust
header(header::CONTENT_TYPE, "text/plain")
```

**Java:** `HTTPResponseProcessorText.java:57` appends `; charset=iso-8859-1` → `text/plain; charset=iso-8859-1`

### M5. Speedtest Body Generation Differs

**File:** `src/response.rs:88-89`

Rust generates `size` random bytes in one allocation. Java generates 8,192 random bytes once and serves overlapping random windows. The byte streams differ; memory behavior differs (Rust allocates full test size, Java always uses 8KB).

### M6. Certificate Selected by Position, Not Alias

**File:** `src/server.rs:479`

Rust uses `pkcs12.cert` (first certificate). Java's `HTTPServer.java:89` uses `ks.getCertificate("hath.network")` selecting by alias. If the PKCS12 contains multiple certs, Rust picks the wrong one.

### M7. FAIL_CONNECT_TEST Treated as Fatal

**File:** `src/client.rs:139`

```rust
_ => return Err(HathError::Fatal(code)), // all start failures are fatal
```

**Java:** `ServerHandler.java:150-167` — `FAIL_CONNECT_TEST` prints troubleshooting info and returns `false`; the client stays running so the user can fix firewall/port forwarding.

### M8. Shutdown Order Reversed

**File:** `src/client.rs:270-278`

Rust: save persistent data → `client_stop` RPC → save client_login
Java: `client_stop` RPC → drain HTTP connections → save persistent data

If `client_stop` fails server-side and triggers a state change, Rust has already mutated local state that it can't reconcile. Additionally, Rust doesn't drain active HTTP connections before shutting down.

### M9. Server Socket Accept Errors Silently Continue

**File:** `src/server.rs:588-590`

Rust loops on accept errors. Java's `HTTPServer.java` calls `dieWithError` on `IOException`, shutting down the client (unless restarting). An underlying socket failure in Rust goes undetected.

### M10. No Proxy Support in FileDownloader

**File:** `src/downloader.rs:51`

The `allow_proxy` field exists but is never wired into reqwest client configuration. Java's `FileDownloader.java:153-164` uses `Settings.getImageProxy()` and passes a `Proxy` object to `source.openConnection(proxy)`.

### M11. Single Total Timeout Instead of Separate Connect/Read

**File:** `src/downloader.rs:73`

```rust
.timeout(std::time::Duration::from_millis(self.timeout_ms))
```

Java uses 5s connect timeout + 30s read timeout separately. Rust uses a single total timeout. A slow-to-connect server consumes Rust's entire timeout budget before any data transfers.

### M12. Pruning Trigger Logic Differs

**File:** `src/cache/mod.rs:621-631`

- Rust only prunes when `cache_size > limit`. Java also prunes when `limit - cache_size < 100MB` (near-limit reclaim).
- Rust's `fast_delete` triggers when `bytes_to_free > limit / 4`. Java always uses `fastDelete = true` when over limit.

---

## LOW — Cosmetic / Diagnostic / Logging Differences

### L1. Missing Copyright/Warranty Banner

**File:** `src/client.rs:44`

Java prints three lines including GPL copyright and warranty disclaimer. Rust prints only the version line. This is a legal concern — GPL requires prominent display of the warranty disclaimer.

### L2. Missing Cache Startup Checks

**File:** `src/cache/mod.rs` — `CacheHandler::new()` and `full_rescan`

Java checks at startup:
- Free disk space check (error if insufficient space, warning if <10%)
- Empty cache + static ranges check (dies if cache empty with 20+ ranges)
- LRU marking of recently-accessed files during rescan
- 30-second warning before cleanup if L1 dirs exceed static range count

None of these exist in Rust.

### L3. Cache Blacklist Fetch Failure Not Logged

**File:** `src/client.rs:249-264`

Java logs `"CacheHandler: Failed to retrieve file blacklist, will try again later."` on failure. Rust silently ignores.

### L4. Missing programStatus Tracking

**File:** `src/stats.rs`

Java tracks `programStatus` through lifecycle stages ("Logging in...", "Running", "Suspended", "Died"). Rust doesn't track this at all.

### L5. bytesSentHistory Always Initialized

**File:** `src/stats.rs:35`

Rust always initializes `vec![0u32; 361]`. Java only tracks bytes-sent history when the GUI is active (CLI mode silently discards it). Minor resource difference.

### L6. Missing Initial Blacklist Fetch at Startup

**File:** `src/client.rs`

Java calls `processBlacklist(259200)` at line 183 during startup (3-day delta). Rust doesn't fetch the blacklist until the periodic cycle starts.

---

## INTENTIONAL DIFFERENCES — Confirmed as Justified

### N1. Persistent Cache Uses Bincode (Not Java Serialization)

Format-incompatible. Migration path is documented: stop Java → start Rust (auto-rescans cache).

### N2. TLS Acceptor Swap via ArcSwapOption

Rust atomically swaps the TLS acceptor without restarting the server. Java restarts the entire HTTPServer. Advantage: no connection drain period. Disadvantage: no `client_suspend`/`still_alive` RPC notifications during refresh.

### N3. HTTP/1.1 Connection Lifecycle Managed by Hyper

No `HTTPSession` list, no connection-drain on shutdown. Hyper handles timeouts internally.

### N4. No HTTPSessionKiller

Confirmed — Hyper's built-in timeouts replace this.

### N5. No GUI

Confirmed — CLI-only. This is scoped out.

### N6. No Gallery Downloader

Confirmed — `/servercmd/start_downloader` returns empty 200 OK. `DownloaderFetch`/`DownloaderFailreport` action types are defined but unused.

### N7. CLI Credentials via `--client-id`/`--client-key` and `HATH_CLIENT_ID`/`HATH_CLIENT_KEY`

Rust adds CLI and env var credential support. Java has no equivalent. Non-breaking improvement.

### N8. Threaded Proxy Test Timeout Model

Rust uses `testtime + 5` total timeout; Java uses 10s connect + 60s read. Different timeout models due to reqwest vs Java HttpURLConnection.

### N9. Proxy Temp File Naming

Rust uses deterministic `proxyfile_{fileid}`; Java uses `File.createTempFile()`. Practical impact minimal since concurrent proxy downloads of the same file are unlikely.

---

## Section-by-Section Checklist

### 1. RPC Protocol — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| URL signing formula | PASS | `hentai@home-{act}-{add}-{cid}-{time}-{key}`, identical hex encoding |
| `server_stat` unsigned | PASS | Both skip all signature params |
| All other actions signed | PASS | 14 actions, 1:1 mapping, same strings |
| RPC server failover | **ISSUE** | Rust lacks random scan direction (H1-style) |
| Server time delta | PASS | Same computation |
| KEY_EXPIRED matching | **ISSUE** | `starts_with` vs `equals` (H1) |
| KEY_EXPIRED retry | **ISSUE** | No auto-retry in Rust (H2) |
| Response parsing | PASS | OK/NULL/TEMPORARILY_UNAVAILABLE/FAIL all handled |

### 2. Config & Settings — PASS

| Check | Verdict | Detail |
|-------|---------|--------|
| Priority order | PASS | CLI > env > file > server, with intentional CLI credential addition |
| client_login format | PASS | `int_id-20char_key`, same validation |
| Server-side settings | PASS | 30+ settings all applied |
| MAX_KEY_TIME_DRIFT | PASS | 300 seconds |
| Atomic config swap | PASS | `ArcSwap::rcu`, no RwLock across await |

### 3. TLS Certificate Lifecycle — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Cert download via RPC | PASS | Same URL, timeouts, filename `hathcert.p12` |
| PKCS12 password | PASS | Both use `client_key` |
| Cert alias selection | **ISSUE** | Position vs "hath.network" alias (M6) |
| Startup expiry check | PASS | Both reject if < 24h remaining |
| Periodic cert check | PASS | Every 5 minutes, same termination |
| refresh_certs swap | NOTE | ArcSwapOption vs full restart (N2) |
| TLS protocol | PASS | Both TLSv1.2 + TLSv1.3 |
| Hath-Request header | PASS | `cid-SHA1(key+fileid)` |

### 4. HTTP Request Routing — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Keystamp formula | PASS | `{ts}-{fileid}-{key}-hotlinkthis`, first 10 chars, case-insensitive |
| Keystamp window | PASS | 900 seconds (15 minutes) |
| Servercmd signing | PASS | `hentai@home-servercmd-{cmd}-{add}-{cid}-{time}-{key}` |
| All 7 commands | PASS | All response bodies match |
| Speedtest signing | PASS | `hentai@home-speedtest-{size}-{time}-{cid}-{key}` |
| /favicon.ico | **ISSUE** | Missing Content-Type header (M3) |
| /robots.txt | **ISSUE** | Missing charset suffix (M4) |
| Flood control | PASS | Hit formula, thresholds, staleness all match |
| Connection limiting | **ISSUE** | `>=` vs `>` (H3) |
| Overload notification | **ISSUE** | `>= 80%` vs `> 80%` (H4) |
| allow_normal_connections | PASS | Both start false, enable after startup |
| %3d decoding | **ISSUE** | Missing in Rust (M2) |

### 5. Response Headers & Body — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Date header | PASS | RFC 7231 / RFC 1123 format |
| Server header | PASS | `Genetic Lifeform and Distributed Open Server 1.6.5` |
| Connection: close | PASS | Always present |
| Content-Type always | **ISSUE** | Missing on 301 redirect (M3) |
| Cache-Control conditional | PASS | Only when body present |
| Content-Length conditional | PASS | Only when body present |
| Default Content-Type | PASS | `text/html; charset=iso-8859-1` |
| File MIME | PASS | From HVFile mapping |
| Speedtest body | **ISSUE** | Different generation strategy (M5) |
| Streaming chunk size | PASS | 1460 bytes |
| Header bytes deduction | **ISSUE** | Hardcoded 100 vs actual (H6) |

### 6. Cache Layer — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| HVFile cache path | PASS | `{hash[0..2]}/{hash[2..4]}/{fileid}` |
| LRU index/mask | PASS | chars 4-9 → index, char 9 → bit mask |
| markRecentlyAccessed | PASS | Return value semantics minor diff, no callers affected |
| cycleLRUCacheTable | PASS | 17 entries per tick, ~7 day full clear |
| Startup cleanup | **ISSUE** | Missing 30s warning, null check, progress (L2) |
| Full rescan | **ISSUE** | Missing free space check, empty-cache check, LRU marking (L2) |
| SHA-1 verification | PASS | 65536-byte buffer, `finalize_reset()` between files |
| Pruning cutoffs | PASS | Time constants identical |
| Pruning trigger | **ISSUE** | Less aggressive near limit (M12) |
| fast_delete | **ISSUE** | Different condition (M12) |
| I/O outside lock | PASS | Explicit phase separation |
| Persistent state | PASS | Same files, keys, SHA-1, crash safety |
| Blacklist | PASS | Fetch interval correct, failure not logged (L3) |

### 7. Bandwidth Throttling — CRITICAL BUG

| Check | Verdict | Detail |
|-------|---------|--------|
| Tick rate | PASS | 50 ticks/second, 20ms per tick |
| Bytes per tick calc | PASS | `ceil(throttle / 50)` |
| Three constraints | PASS | Current tick, 5-tick window, current second |
| Time reference | **CRITICAL** | `Instant::now().elapsed()` → BWM non-functional (C1) |
| Sleep on over-quota | PASS | 10ms async sleep |
| Local/RPC exemption | **ISSUE** | RPC IPs incorrectly exempt (H5) |

### 8. File Downloader — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Three modes | PASS | Memory (≤10MB), File, Discard |
| 3 retries + Content-Length | PASS | Both require Content-Length header |
| Timeout model | **ISSUE** | Single total vs separate connect/read (M11) |
| User-Agent | PASS | `Hentai@Home 1.6.5` |
| Proxy support | **ISSUE** | Not wired into reqwest (M10) |
| Hath-Request header | PASS | `cid-SHA1(key+fileid)` |
| Temp file naming | NOTE | Deterministic vs random (N9) |
| Cache import | **ISSUE** | Missing count/LRU/range updates (H7) |
| Per-source retry | **ISSUE** | No inner retry loop (H8) |
| Temp cleanup | PASS | Coordinates with body completion |

### 9. Main Loop & Lifecycle — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Startup sequence | PASS | Overall order correct |
| Initial blacklist | **ISSUE** | Missing at startup (L6) |
| Cache-empty message | **ISSUE** | Not printed (L2) |
| New-version warning | PASS | Not applicable (CLI-only) |
| LRU cycle interval | PASS | 10s |
| Flood control prune | PASS | 60s |
| still_alive heartbeat | PASS | 110s |
| Time + cert check | PASS | 300s (5 minutes) |
| RPC failure clear | PASS | 14400s (4 hours) |
| Blacklist fetch | PASS | 21600s (6 hours) |
| CachePruner tick | PASS | 1s |
| Shutdown order | **ISSUE** | Data before RPC, no connection drain (M8) |
| Shutdown hook | **ISSUE** | No signal handler equivalent to Java's ShutdownHook |

### 10. Error Handling & Logging — ISSUES FOUND

| Check | Verdict | Detail |
|-------|---------|--------|
| Fatal error shutdown | **ISSUE** | FAIL_CONNECT_TEST treated as fatal (M7) |
| Fatal error banners | **ISSUE** | Missing diagnostic output for FAIL_OTHER_CLIENT_CONNECTED |
| Non-fatal retry | **ISSUE** | KEY_EXPIRED retry not implemented (H2) |
| TLS/IO per-connection | PASS | Individual errors don't crash server |
| Accept errors | **ISSUE** | Silent retry vs `dieWithError` (M9) |
| Startup banner | **ISSUE** | Missing copyright/warranty text (L1) |
| Stats counters | PASS | Core counters mirrored |
| programStatus | **ISSUE** | Not tracked (L4) |
| bytesSentHistory init | **ISSUE** | Always initialized vs Java CLI no-op (L5) |

### 11. Intentional Differences — ALL CONFIRMED

| Difference | Verdict | Detail |
|------------|---------|--------|
| Bincode serialization | NOTE | Documented, migration via rescan (N1) |
| TLS acceptor swap | NOTE | No server restart needed (N2) |
| Hyper connection mgmt | NOTE | Replaces manual HTTPSession (N3) |
| No HTTPSessionKiller | NOTE | Hyper handles timeouts (N4) |
| No GUI | NOTE | Scoped out (N5) |
| No Gallery Downloader | NOTE | Scoped out (N6) |
| CLI credentials | NOTE | `--client-id`/`--client-key` + env vars (N7) |
| Proxy test timeout | NOTE | Different reqwest model (N8) |
| Proxy temp naming | NOTE | Deterministic naming (N9) |

---

## Top Priority Fixes

1. **`bandwidth.rs:42`** — Fix `Instant::now().elapsed()` → `SystemTime` wall clock (C1)
2. **`server.rs:141-145`** — Remove `is_rpc` from BWM exemption; only `is_local` should bypass throttling (H5)
3. **`server.rs:159`** — Deduct actual header byte count, not hardcoded 100 (H6)
4. **`server.rs:629`** — Change `>=` to `>` for connection limit check (H3)
5. **`rpc.rs:120`** + **`rpc_client.rs`** — Fix `starts_with` → exact match (H1) AND implement KEY_EXPIRED refresh-server-time + retry (H2)
6. **`server.rs:638`** — Change `>= 80%` to `> 80%` for overload notification (H4)
7. **`proxy_downloader.rs:166-174`** — Add cache registration after proxy download (H7)
8. **`proxy_downloader.rs:50-58`** — Add per-source retry loop (H8)
