# hath-rs Java Port Misbehave Audit

Date: 2026-05-02

Scope:

- Target parity: Hentai@Home Java 1.6.5 Build 178 -> Rust + Tokio 1:1 protocol port.
- Rust source: `src/`
- Java source: `../HentaiAtHome_1.6.5_src/src/hath/base/`
- Gallery downloader is out of scope.
- IPv6 dual-stack bind behavior is intentionally excluded from this audit.

Verification performed:

- `cargo test` in `hath-rs`: 42 passed, 0 failed.
- This document records findings only; no implementation fixes were made.

## Findings

### P0-01 Proxy body-stage retry is missing

Status: DONE

Rust retries while opening/validating an upstream proxy response, but once the body is being streamed to the downstream client, EOF or a network error marks the download failed and removes the temporary file. Java `ProxyFileDownloader` retries the body-read stage up to three attempts, resetting offsets and digest state between tries.

Remote-visible risk: high. A dispatcher/client can see a declared `Content-Length` followed by a truncated response or reset, which is a strong candidate for "misbehave" reports.

Rust references:

- `src/proxy_downloader.rs:83` - pre-body upstream retry loop.
- `src/proxy_downloader.rs:236` - body streaming loop.
- `src/proxy_downloader.rs:293` - failure path after body-stage error.

Java references:

- `ProxyFileDownloader.java:147` - `trycounter = 3`.
- `ProxyFileDownloader.java:211` - catch resets body-read state.
- `ProxyFileDownloader.java:222` - retry loop condition.

Suggested follow-up: reproduce with an upstream that closes mid-body after sending a valid `Content-Length`; compare Java retry behavior with Rust downstream response.

### P0-02 RPC timeout and retry semantics differ

Status: DONE

Java RPC uses `FileDownloader(serverConnectionURL, 3600000, 3600000)`, with a 5s connect timeout, 1h read timeout, and the `FileDownloader` retry loop. Rust RPC uses 30s connect/read timeouts and returns immediately on request/body failure.

Remote-visible risk: high. `client_start`, `still_alive`, `srfetch`, settings, blacklist, and shutdown notifications can fail earlier in Rust than in Java, making the client appear offline or unreliable.

Rust references:

- `src/rpc_client.rs:37` - 30s connect/read timeout.
- `src/rpc_client.rs:73` - single send path.
- `src/rpc_client.rs:86` - immediate failure return.

Java references:

- `ServerResponse.java:61` - RPC downloader timeout values.
- `FileDownloader.java:147` - retry loop.
- `FileDownloader.java:165` - 5s connect timeout.
- `FileDownloader.java:166` - read timeout argument.

Suggested follow-up: align RPC downloader retry/read-timeout semantics before investigating higher-level heartbeat behavior.

### P0-03 HTTP session timeout cleanup is not equivalent

Status: DONE

Java sets a 10s socket read timeout and periodically calls `nukeOldConnections()`. Sessions can be cleared after stale/idle conditions. Rust currently has `nuke_old_connections` as a stub and depends on Hyper/task lifecycle.

Remote-visible risk: high under slow or stuck peers. Stale sessions can occupy active connection slots longer than Java would allow, causing overload behavior or dispatch rejection.

Rust references:

- `src/server/mod.rs:834` - `nuke_old_connections` stub.

Java references:

- `HTTPSession.java:86` - socket read timeout.
- `HTTPSession.java:258` - timeout check entry point.
- `HTTPServer.java:201` - connection cleanup loop.
- `HentaiAtHomeClient.java:291` - periodic cleanup call.

Suggested follow-up: model Java's timeout rules explicitly around request read, response send, servercmd, and max session age.

### P0-04 Max connection limit has an async race

Status: DONE

Rust checks active connections with a load before spawning/serving, then increments later with `fetch_add`. Under a concurrent accept burst, multiple tasks can observe the same active count and pass the limit. Java's accept loop updates `sessionCount` synchronously.

Remote-visible risk: medium to high under bursts. Rust can exceed `max_connections`, changing overload and rejection behavior.

Rust references:

- `src/server/mod.rs:1008` - active connection increment happens after the earlier check.

Java references:

- `HTTPServer.java:247` - single-threaded max connection check.
- `HTTPServer.java:292` - synchronous session count update.

Suggested follow-up: make connection admission atomic or semaphore-based, including exact Java off-by-one behavior if needed.

### P1-05 Image proxy defaults differ

Status: DONE

Java enables the image proxy if `image_proxy_host` is set, defaulting type to `socks` and port to `1080` or `8080`. Rust only configures the proxy when type, host, and port are all present.

Remote-visible risk: medium. Environments relying on Java's defaults will direct-connect in Rust, causing proxy fetches or speed tests to fail differently.

Rust references:

- `src/proxy_downloader.rs:417` - requires `(type, host, port)`.
- `src/config.rs:355` - stores proxy fields independently.

Java references:

- `Settings.java:510` - proxy enabled by host.
- `Settings.java:518` - default type.
- `Settings.java:526` - default port.
- `Settings.java:540` - proxy construction.

Suggested follow-up: apply Java defaults when only host is configured.

### P1-06 Proxy downloader does not force connection close

Status: SKIPPED

Java disables HTTP keep-alive globally and `FileDownloader` sends `Connection: Close`. Rust RPC sends `Connection: close`, but the proxy downloader's shared reqwest client does not force close.

Remote-visible risk: medium. Upstream image servers and network middleboxes may observe different connection reuse/lifecycle behavior.

Rust references:

- `src/proxy_downloader.rs:410` - shared proxy client builder.

Java references:

- `HentaiAtHomeClient.java:85` - global `http.keepAlive=false`.
- `FileDownloader.java:167` - `Connection: Close`.

Suggested follow-up: decide whether proxy fetches must also force close to match Java exactly.

### P1-07 Generic downloader timeout semantics differ

Status: TODO

Rust `Downloader` uses reqwest's total request timeout. The `max_dl_time_ms` field exists but is not used. Java `FileDownloader` uses connect timeout plus read timeout semantics; a slow but progressing transfer can run longer.

Remote-visible risk: medium for startup/control paths, especially certificate download or other server-controlled downloads. Rust can fail earlier than Java.

Rust references:

- `src/downloader.rs:23` - `max_dl_time_ms`.
- `src/downloader.rs:61` - 5s connect timeout.
- `src/downloader.rs:99` - total request timeout.

Java references:

- `FileDownloader.java:165` - connect timeout.
- `FileDownloader.java:166` - read timeout.

Suggested follow-up: split connect/read/total timeouts to preserve Java behavior.

### P1-08 `threaded_proxy_test` timing behavior differs

Status: TODO

Java `threaded_proxy_test` uses `FileDownloader(source, 10000, 60000, true)`, inheriting read-timeout and retry semantics. Rust uses a 10s connect timeout and a whole-request 60s timeout.

Remote-visible risk: medium. Slow trickle, stalled reads, or transient upstream failures can produce different speed-test success/failure and timing.

Rust references:

- `src/server/mod.rs:645` - test client setup.
- `src/server/mod.rs:677` - timing/reporting path.

Java references:

- `HTTPResponse.java:105` - Java downloader invocation.

Suggested follow-up: test with slow/stalling local HTTP sources and compare returned servercmd result.

### P1-09 `static_ranges` does not update `static_range_count`

Status: TODO

Java increments `currentStaticRangeCount` while parsing `static_ranges`. Rust updates `static_ranges` but leaves `static_range_count` unchanged unless a separate `static_range_count` setting is present.

Remote-visible risk: medium and conditional on actual server response fields. If only `static_ranges` is sent, Rust can skip Java's fatal startup guard for empty cache plus many static ranges, which Java explicitly treats as a trust-risk condition.

Rust references:

- `src/config.rs:339` - parses `static_ranges`.
- `src/config.rs:347` - separate `static_range_count` handling.
- `src/cache/mod.rs:238` - startup guard uses `static_range_count`.

Java references:

- `Settings.java:310` - parses `static_ranges`.
- `Settings.java:317` - increments current range count.
- `Settings.java:322` - separate readout update.

Suggested follow-up: confirm current server startup payload, then mirror Java's count update behavior.

### P1-10 Persistent cache data is not deleted after load

Status: TODO

Rust defines `delete_persistent_data()` but does not call it. Java deletes persistent cache files during startup after attempting to load them, preventing stale `pcache_*` files from being reused repeatedly after crashes or partial startup.

Remote-visible risk: medium indirect. Stale cache metadata can affect cache count, range ages, pruning choices, and startup safety checks.

Rust references:

- `src/cache/mod.rs:515` - unused `delete_persistent_data`.

Java references:

- `CacheHandler.java:82` - deletes persistent data during startup.
- `CacheHandler.java:264` - delete implementation.

Suggested follow-up: match Java's delete-after-load lifecycle and add crash/stale-pcache tests.

### P1-11 Full cache rescan records empty or fully invalid ranges

Status: TODO

Rust full rescan can insert a range age even when every file in the range is invalid/deleted. Java deletes empty directories and only records ranges with valid files.

Remote-visible risk: medium indirect. Incorrect range age state can affect pruning order and cache churn.

Rust references:

- `src/cache/mod.rs:648` - range age insert.

Java references:

- `CacheHandler.java:468` - records range age only after valid files remain.

Suggested follow-up: add a fixture with an l2 directory containing only invalid files and compare resulting range-age map.

### P1-12 Cache pruner oldest timestamp includes deleted files

Status: TODO

Rust updates `oldest_last_modified` before deciding whether to delete a file. Java updates `oldestLastModified` only for files that remain after pruning.

Remote-visible risk: medium indirect. Rust can write a stale old timestamp for a range that has been partially pruned, causing repeated or excessive pruning.

Rust references:

- `src/cache/pruner.rs:116` - oldest timestamp update before deletion.
- `src/cache/pruner.rs:130` - file deletion path.

Java references:

- `CacheHandler.java:591` - oldest timestamp update for retained files.
- `CacheHandler.java:600` - range age update.

Suggested follow-up: compare prune results for a range containing both expired and retained files.

### P1-13 Cached HEAD does not open the file

Status: TODO

Rust returns a HEAD response for a cache hit without opening the cached file. Java initializes `HTTPResponseProcessorFile` for cached file responses, including HEAD, so an unreadable file can produce a 500.

Remote-visible risk: medium to low. This matters if file permissions or disk state change after cache indexing and the dispatcher probes with HEAD.

Rust references:

- `src/server/mod.rs:261` - cached HEAD response.
- `src/server/response.rs:178` - file response opens the file only for GET path.

Java references:

- `HTTPResponse.java:211` - file processor for cache hit.
- `HTTPResponse.java:321` - file processor initialization.
- `HTTPResponseProcessorFile.java:50` - file open during initialization.

Suggested follow-up: create a cache-hit file with unreadable permissions and compare Java/Rust HEAD status.

### P1-14 Shutdown and certificate-refresh quiesce windows differ

Status: TODO

Java suspends traffic, sleeps 5s, then `httpServerShutdown()` sleeps another 5s before shutdown/restart. Rust certificate refresh sleeps 5s and then stops the server; normal shutdown notifies the server, sleeps only 1s, then stops.

Remote-visible risk: medium during cert refresh or shutdown. Rust may reset or reject newly dispatched requests earlier than Java.

Rust references:

- `src/server/tls.rs:158` - 5s cert refresh sleep.
- `src/server/tls.rs:162` - stop server.
- `src/client.rs:236` - 1s normal shutdown sleep.

Java references:

- `HentaiAtHomeClient.java:228` - cert refresh pre-shutdown sleep.
- `HentaiAtHomeClient.java:461` - server shutdown method.
- `HentaiAtHomeClient.java:464` - additional shutdown sleep.

Suggested follow-up: decide whether Rust should preserve Java's two-stage quiesce delay.

### P2-15 Error response body and header formatting differ

Status: TODO

Java uses body text `An error has occurred. (code)` for generic error responses and `Allow: GET,HEAD`. Rust returns short reason text such as `Not Found`, `Permission Denied`, and uses `Allow: GET, HEAD`.

Remote-visible risk: low. Status codes are the main protocol signal, but exact 1:1 behavior differs.

Rust references:

- `src/server/response.rs:33` - error response helpers.
- `src/server/response.rs:54` - `Allow` header value.

Java references:

- `HTTPResponse.java:315` - generic error body.
- `HTTPResponse.java:318` - `Allow` header value.

Suggested follow-up: align body/header text if byte-level Java compatibility is desired.

### P2-16 Malformed numeric request parsing differs

Status: TODO

Rust parses malformed numeric fields with fallback behavior and can return normal invalid/403 responses. Java uses `Integer.parseInt` in request parsing; malformed values can throw and be handled at the session level instead.

Remote-visible risk: low. Valid dispatchers should not send malformed `servercmd` or speedtest numeric fields.

Rust references:

- `src/server/request.rs:110` - servercmd parse fallback.
- `src/server/request.rs:132` - speedtest parse fallback.

Java references:

- `HTTPResponse.java:247` - servercmd integer parse.
- `HTTPResponse.java:270` - speedtest size parse.
- `HTTPResponse.java:271` - speedtest time parse.

Suggested follow-up: add malformed request parity tests only after higher-risk protocol paths are fixed.

### P2-17 Absolute URI handling is case-sensitive in Rust

Status: TODO

Java strips absolute-form `http://host/path` using a case-insensitive pattern. Rust only strips lowercase `http://`.

Remote-visible risk: low. Most dispatch traffic should not use uppercase absolute URI schemes, but proxy-style requests can expose this.

Rust references:

- `src/server/request.rs:47` - lowercase `http://` stripping.

Java references:

- `HTTPResponse.java:32` - case-insensitive absolute URI pattern.

Suggested follow-up: add request parser tests for `HTTP://host/...`.

### P2-18 HVFile size numeric bounds differ

Status: TODO

Java's filename regex permits up to 10 digits for size, but parsing uses signed `Integer.parseInt`, so sizes above `2_147_483_647` are invalid. Rust stores size as `u32`, accepting values up to `4_294_967_295`.

Remote-visible risk: low. Normal H@H files should not hit this, but malformed or malicious fileids can diverge.

Rust references:

- `src/hvfile.rs` - `u32` size parse/storage.

Java references:

- `HVFile.java:158` - signed integer parse.

Suggested follow-up: decide whether strict Java numeric bounds matter for invalid request compatibility.

### P2-19 File logging disable flag appears inverted

Status: TODO

Rust's file logging filter appears to allow file logging when `disable_logging` is true. Java disables logging when the flag is set.

Remote-visible risk: low direct protocol risk. This is mainly a diagnostics problem: logs may be missing when investigating remote misbehavior.

Rust references:

- `src/logging.rs` - file logging filter.

Java references:

- `Settings.java:278` - disable logging setting.

Suggested follow-up: inspect `src/logging.rs` and add a tiny unit/integration check for the filter condition before changing it.

## Recommended Fix Order

1. Fix proxy body-stage retry and downstream failure behavior.
2. Align RPC downloader timeout/retry semantics.
3. Implement Java-equivalent HTTP session timeout cleanup and atomic connection admission.
4. Align proxy/downloader keep-alive, proxy defaults, and timeout semantics.
5. Fix cache metadata lifecycle and pruning parity.
6. Sweep low-risk parser/error-response compatibility differences.
