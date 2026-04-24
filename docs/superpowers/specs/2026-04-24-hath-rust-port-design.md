# Hentai@Home Java → Rust Port Design

## Context

Port the Hentai@Home (H@H) Java client v1.6.5 (Build 178) to Rust+Tokio with
1:1 protocol compatibility on the H@H network. The Rust client must be
indistinguishable from the Java client from the perspective of the central RPC
server and peer clients.

**Source**: `HentaiAtHome_1.6.5_src/` — 36 Java files, ~4000 lines, GPLv3.
**Target**: `hath-rs/` — existing skeleton Cargo project.

## Scope

**Phase 1 (this design)**:
- Full protocol compatibility (RPC, TLS HTTP server, file serving, proxy, speedtest)
- CLI-only (no GUI)
- Cache rescanned on first start (no Java serialization compat)
- Config via CLI args, env vars, `client_login` file

**Out of scope for Phase 1**:
- GUI
- Gallery Downloader (optional, can add later)
- Reading Java serialized cache state

## Architecture

```
src/
  main.rs            Entry point: arg parsing, init, start client
  config.rs          Settings: directories, RPC server selection, server config
  client.rs          Main loop: subsystem coordination, lifecycle
  rpc.rs             Server RPC protocol: URL signing, all actions
  server.rs          TLS Hyper service: connection management, flood control
  request.rs         HTTP route: /h/, /servercmd/, /t/, keystamp validation
  response/
    mod.rs           ResponseProcessor trait
    file.rs          Local cache file serving with optional SHA-1 verify
    proxy.rs         On-demand upstream download + pass-through
    speedtest.rs     Random data generator for speed tests
    text.rs          Plain text responses and error pages
  cache.rs           CacheHandler: LRU table, static ranges, import/export
  cache_pruner.rs    Background cache size monitoring and pruning
  hvfile.rs          File ID parsing, path mapping, MIME types
  downloader.rs      FileDownloader, ProxyFileDownloader
  bandwidth.rs       Token-bucket bandwidth throttle (50 tick/s)
  stats.rs           Metrics tracking (bytes, files, uptime, connections)
  logging.rs         Log output with rotation
  scheduler.rs       Periodic task scheduler for main loop intervals
  utils.rs           SHA-1, file I/O, key=value parsing
```

## Dependencies

| Crate | Purpose |
|---|---|
| `tokio` | Async runtime |
| `hyper` + `hyper-rustls` | HTTP/1.1 over TLS (server) |
| `rustls` + `rustls-pemfile` + `pkcs12` | TLS cert loading (all pure Rust, no C deps) |
| `reqwest` | Outbound HTTP client (RPC calls, file downloads) |
| `clap` | CLI argument parsing |
| `tracing` + `tracing-subscriber` + `tracing-appender` | Structured logging with rotation |
| `sha1` / `ring` | SHA-1 hashing |
| `serde` + `serde_json` / `bincode` | Cache state persistence |
| `dashmap` | Concurrent HashMap for flood control, static ranges |
| `tokio-util` | CancellationToken |
| `rand` | Speedtest random data |

**No C dependencies.** Entire TLS stack is pure Rust (rustls + ring).

## Shared State

```
ClientState {
    config: RwLock<Config>,
    cache: CacheHandler,
    stats: RwLock<Stats>,
    bandwidth_monitor: BandwidthMonitor,
    server_handler: ServerHandler,
    rpc: RpcClient,
    cert_refresh: watch::Sender<bool>,
    shutdown: CancellationToken,
}
```

All subsystems hold `Arc<ClientState>`. DashMap used for concurrent read-heavy maps
(flood control entries, static ranges). RwLock used for infrequently-mutated shared
values.

## Component Details

### 1. RPC Protocol (`rpc.rs`)

Strictly replicate the Java URL signing algorithm:

```
actkey = SHA1("hentai@home-" + act + "-" + add + "-" + cid + "-" + time + "-" + key)
url = "http://" + host + "/15/rpc?clientbuild=178&act=" + act
    + "&add=" + add + "&cid=" + cid + "&acttime=" + time + "&actkey=" + actkey
```

RPC actions: `server_stat`, `client_login`, `client_start`, `client_stop`,
`client_suspend`, `client_resume`, `still_alive`, `get_blacklist`, `client_settings`,
`srfetch`, `dlfetch`, `overload`.

Server response format:
```
OK            → response text lines follow
FAIL_CODE     → failure code
KEY_EXPIRED   → retry after refreshing server time
TEMPORARILY_UNAVAILABLE → RPC server offline, fail over
```

RPC server selection with failover: maintain list from `rpc_server_ip` setting,
randomize selection, avoid last-failed server, clear failure marker every ~4h.

### 2. Config (`config.rs`)

Priority: CLI args > env vars > `client_login` file > server-provided defaults.

`client_login` file format: `<int_id>-<20char_key>` (same as Java).

Server-side settings parsed from `key=value` lines sent during login and
`still_alive`. All settings from Java `Settings.updateSetting()` are supported.

Settings set once (not re-parsed from CLI args after login):
port, host, clientID, clientKey, all directory paths.

### 3. TLS HTTP Server (`server.rs`)

**Cert lifecycle**:
1. Download PKCS12 cert via RPC `get_cert` → save as `data/hathcert.p12`
2. Load with `pkcs12` crate, extract key + cert chain
3. Build `rustls::ServerConfig` with TLSv1.2 and TLSv1.3
4. Serve via `hyper::server::Builder` over `TcpListener`

**Cert refresh** (triggered by `/servercmd/refresh_certs`):
1. RPC `client_suspend` → close listener → wait for connections to drain (max 5 min)
2. Re-download and reload cert → open new listener → RPC `client_resume`

**Connection management**:
- Max connections = `20 + min(480, throttle_bytes / 10000)`
- At 80% capacity → send overload notification (30s cooldown)
- Flood control: 10+ connections in 5s window → 60s block per IP
- Localhost/private network IPs and RPC server IPs exempt from limits
- During startup (`allowNormalConnections = false`), only RPC server IPs accepted

### 4. Request Routing (`request.rs`)

| Path | Handler | Auth |
|---|---|---|
| `/h/<fileid>/<additional>/<filename>` | File or Proxy | keystamp: `|serverTime - ts| < 900 && sha1_prefix == SHA1(ts + "-" + fileid + "-" + key + "-hotlinkthis")[..10]` |
| `/servercmd/<cmd>/<add>/<time>/<key>` | Text (API) | SHA1 servercmd signature + RPC server IP check |
| `/t/<size>/<time>/<key>` | Speedtest | SHA1 speedtest signature |
| `/favicon.ico` | 301 redirect to e-hentai.org/favicon.ico | none |
| `/robots.txt` | `Disallow: /` | none |

Server commands: `still_alive`, `threaded_proxy_test`, `speed_test`,
`refresh_settings`, `start_downloader`, `refresh_certs`.

### 5. Response Processors

Trait:
```rust
trait ResponseProcessor: Send + Sync {
    fn content_type(&self) -> &str;
    fn content_length(&self) -> usize;
    fn header_fields(&self) -> Vec<(&str, &str)>;
    fn body_stream(self: Box<Self>) -> impl Stream<Item = Result<Bytes>> + Send;
}
```

**File processor**: Stream cached file in 1460-byte chunks via `tokio::fs::File`.
Optional inline SHA-1 verification (one check per 2s cooldown maximum).

**Proxy processor**: Download from upstream image server to temp file while
simultaneously serving to the requesting client. Producer-consumer coordination
via `tokio::sync::Notify`. On completion, verify SHA-1 and import to cache.

**Speedtest processor**: Generate `testsize` bytes of random data.

**Text processor**: Plain text with configurable content-type and custom headers.

**HTTP response header format** (must match Java exactly):
```
HTTP/1.1 <status>\r\n
[optional custom headers]\r\n
Date: <RFC 7231 date>\r\n
Server: Genetic Lifeform and Distributed Open Server 1.6.5\r\n
Connection: close\r\n
Content-Type: <mime>\r\n
Cache-Control: public, max-age=31536000\r\n  (if body present)
Content-Length: <len>\r\n  (if body present)
\r\n
```

### 6. Cache Layer

**HVFile**: Parses file IDs. Path: `{cachedir}/{hash[0..2]}/{hash[2..4]}/{fileid}`.

**LRU table**: `[u16; 1048576]` array. File ID chars 4-9 → array index,
char 9 → bit mask within the u16. Main loop clears 17 short entries every 10s
(complete clear in ~1 week). `markRecentlyAccessed()` sets the bit and conditionally
updates the file's `lastModified` timestamp.

**Persistence**: `pcache_info` (key=value text), `pcache_lru` (binary), `pcache_ages`
(binary). SHA-1 checksums on binary files. Format: serde + bincode.

**Pruning**: Background task every 1s. When cache exceeds limit, find the
static range with the oldest file, delete files within a recency window
(1 day to 30 days depending on age). Sleep 100ms (aggressive) or 1000ms between
deletions to avoid HDD fragmentation. Every 5 min, check free disk space.

**Startup**: Delete orphan temp files → try loading persistent state → if fail,
full directory rescan → check disk space → verify non-empty cache if static ranges
assigned → start pruner task.

### 7. Bandwidth Limiter

50-tick-per-second token bucket. Three constraint windows: current tick, 5-tick
sliding window, current second. All three must have quota before a write is
allowed. If over quota, sleep 10ms and retry. Replaces Java's busy-sleep with
`tokio::time::sleep`.

### 8. Downloader

**FileDownloader**: Uses `reqwest`. Three modes: memory (≤10MB), file, discard.
3 retries. Requires Content-Length header. Validates against maxAllowedFileSize.
Supports SOCKS/HTTP proxy. User-Agent: `Hentai@Home 1.6.5`.

**ProxyFileDownloader**: Downloads from upstream while serving to client.
`Hath-Request` header: `{cid}-{SHA1(clientKey + fileid)}`. Temp file as buffer.
Both download thread and send thread must complete before finalizing.

### 9. Main Loop (`client.rs` + `scheduler.rs`)

**Scheduler**: Manages periodic intervals via a HashMap keyed by Duration.
Each interval is reused across main loop iterations.

```rust
sched.periodic(secs(10)).tick()     // core: LRU cycle, nuke connections, shift stats
sched.periodic(secs(110)).tick()    // still_alive heartbeat
sched.periodic(secs(60)).tick()     // prune flood control
sched.periodic(mins(5)).tick()      // system time + cert expiry check
sched.periodic(hours(4)).tick()     // clear RPC server failure
sched.periodic(hours(6)).tick()     // process blacklist
```

Main loop uses `tokio::select!` with all interval branches and shutdown
token. Suspend state collapses select to only shutdown + cert_refresh branches.
Cert refresh triggered by `tokio::sync::watch` channel.

**Graceful shutdown** (SIGTERM/Ctrl+C):
1. Set shutdown flag → notify `client_stop` RPC
2. Close listener (stop accepting)
3. Drain existing connections (max 25s, report progress every 5s)
4. Save cache persistent state
5. Flush logs → exit

## Testing Strategy

- **Unit tests**: SHA-1 signing, HVFile parsing, config parsing, bandwidth limiter math, LRU bit operations, keystamp validation, RPC URL construction
- **Integration tests**: Cache startup/rescan, file serving (with mock filesystem), proxy download pipeline, main loop scheduling
- **Protocol compatibility tests**: Compare Rust and Java RPC URL outputs byte-for-byte; compare HTTP response headers; compare bandwidth limiter behavior with identical inputs
- **Live testing**: Run against the actual H@H network with a test client ID, verify server accepts the client, verify file serving to peers

## Migration Path

1. Start Rust client with empty/new cache on a test Client ID
2. Verify RPC communication and certificate download
3. Verify file serving (static range fetch from peers)
4. Compare metrics with Java client under same conditions
5. Production cutover: stop Java client, start Rust client with same cache directory (auto-rescans)
