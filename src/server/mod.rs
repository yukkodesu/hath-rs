mod access_log;
mod body;
mod request;
mod response;
mod tls;

use self::access_log::AccessLogService;
use self::body::StreamingBody;
use self::request::RequestType;
pub use self::tls::{spawn_cert_refresh_watcher, spawn_time_cert_check};
use crate::bandwidth::BandwidthMonitor;
use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::proxy_downloader::ProxyFileDownloader;
use crate::rpc_client::RpcClient;
use crate::stats::Stats;
use crate::utils;

use arc_swap::{ArcSwap, ArcSwapOption};
use hyper::body::Incoming;
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use openssl::ssl::SslContext;
use regex::Regex;
use reqwest::Url;
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};

/// Shared state accessible from all request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ArcSwap<Config>>,
    pub stats: Arc<Stats>,
    pub cache: Arc<CacheHandler>,
    pub rpc_client: Arc<RpcClient>,
    pub allow_normal_connections: Arc<std::sync::atomic::AtomicBool>,
    /// Flood control table (IP -> entry). Uses Arc<Mutex> for shared access.
    pub flood_control: Arc<Mutex<HashMap<String, FloodControlEntry>>>,
    /// TLS context that can be swapped at runtime (e.g. cert refresh).
    pub tls_acceptor: Arc<ArcSwapOption<SslContext>>,
    /// Certificate expiry as a Unix timestamp (seconds). Checked periodically;
    /// if the cert expires within 24 hours, the client shuts down (matches Java).
    pub cert_expiry: Arc<Mutex<Option<i64>>>,
    /// Bandwidth throttling monitor (shared across all connections).
    /// If throttle_bytes is 0, no throttling is applied (None).
    pub bandwidth_monitor: Arc<ArcSwapOption<BandwidthMonitor>>,
    /// Count of currently active connections (for max_connections enforcement).
    pub active_connections: Arc<std::sync::atomic::AtomicU32>,
    /// Monotonic connection/session id for Java-style access logs.
    pub next_conn_id: Arc<AtomicU32>,
    /// Timestamp of last overload notification (rate-limited to once per 30s).
    /// Java: ServerHandler.lastOverloadNotification
    pub last_overload_notification: Arc<Mutex<Option<Instant>>>,
    /// Flag: true when cert refresh (full server restart) is requested.
    /// Set by the refresh_certs RPC handler, cleared by the cert refresh watcher.
    pub do_cert_refresh: Arc<AtomicBool>,
    /// Wakes the cert refresh watcher when a refresh_certs command arrives.
    pub cert_refresh_notify: Arc<Notify>,
    /// Shutdown token for the currently-running server accept loop.
    /// Swapped during cert refresh to terminate the old listener and start a new one.
    pub server_shutdown_token: Arc<ArcSwapOption<tokio_util::sync::CancellationToken>>,
    /// Set to true by start_server() after the accept loop exits.
    /// The cert refresh watcher polls this to wait for the old server to terminate.
    pub server_terminated: Arc<AtomicBool>,
    /// Shared HTTP client for ProxyFileDownloader requests.
    /// 5s connect + 30s read timeout, reused across all proxy downloads.
    pub proxy_client: Arc<reqwest::Client>,
}

#[derive(Debug, Clone)]
pub struct FloodControlEntry {
    pub connect_count: u32,
    pub last_connect: Instant,
    pub block_until: Option<Instant>,
}

impl FloodControlEntry {
    pub fn is_blocked(&self) -> bool {
        self.block_until.is_some_and(|b| b > Instant::now())
    }

    pub fn is_stale(&self, now: Instant) -> bool {
        now.checked_duration_since(self.last_connect)
            .is_some_and(|d| d > Duration::from_secs(60))
    }

    /// Returns true if the connection should be allowed.
    pub fn hit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ms = now
            .checked_duration_since(self.last_connect)
            .unwrap_or_default()
            .as_millis() as u32;
        self.connect_count = self
            .connect_count
            .saturating_sub(elapsed_ms / 1000)
            .saturating_add(1);
        self.last_connect = now;

        if self.connect_count > 10 {
            self.block_until = Some(now + Duration::from_secs(60));
            false
        } else {
            true
        }
    }
}

/// RAII guard that decrements active_connections and updates Stats on drop.
/// Java: HTTPServer.removeHTTPSession() / connectionFinished()
struct ConnectionGuard {
    active_connections: Arc<std::sync::atomic::AtomicU32>,
    stats: Arc<Stats>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let prev = self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.stats.set_open_connections(prev.saturating_sub(1));
    }
}

static LOCAL_NETWORK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|169\.254\.|::1|0:0:0:0:0:0:0:1|fc|fd)")
        .expect("invalid regex")
});

fn configure_send_buffer(stream: &TcpStream, config: &Config) {
    if config.throttle_bytes == 0 {
        return;
    }

    let memory_cap = if config.use_less_memory {
        131_072
    } else {
        524_288
    };
    let throttle_cap = (0.2 * config.throttle_bytes as f64).round() as usize;
    let requested = memory_cap.min(throttle_cap);
    if requested == 0 {
        return;
    }

    if let Err(e) = socket2::SockRef::from(stream).set_send_buffer_size(requested) {
        tracing::debug!(
            "Failed to set TCP send buffer to {} bytes: {}",
            requested,
            e
        );
    }
}

pub struct HathService {
    pub state: AppState,
    pub remote_addr: SocketAddr,
}

impl Service<Request<Incoming>> for HathService {
    type Response = Response<StreamingBody>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let state = self.state.clone();
        let remote_addr = self.remote_addr;

        Box::pin(async move {
            // Load config once for this request (owned Arc, safe across .await)
            let config = state.config.load_full();

            // Normalize IPv4-mapped IPv6 to plain IPv4 (dual-stack listener).
            let client_ip = utils::normalize_ip(remote_addr.ip());

            // Determine if this is a local/RPC connection (skip bandwidth throttling)
            let host_addr = client_ip.to_string().to_lowercase();
            let is_local = LOCAL_NETWORK_RE.is_match(&host_addr) || config.client_host == host_addr;
            let is_rpc = config
                .rpc_servers
                .iter()
                .any(|s| s.to_string().to_lowercase() == host_addr);

            // Determine bandwidth monitor for this request.
            // Java: Only local connections skip throttling.
            // RPC servers on non-local IPs are still throttled.
            let bwm_for_request = if is_local {
                None
            } else {
                state.bandwidth_monitor.load_full()
            };

            let request_type = request::parse_request(
                req.method().as_str(),
                req.uri()
                    .path_and_query()
                    .map(|p| p.as_str())
                    .unwrap_or("/"),
                client_ip,
                &config,
            );

            // Clone BWM for later header throttling (bwm_for_request is consumed by response builders)
            let bwm_for_header = bwm_for_request.clone();

            let mut resp = match request_type {
                RequestType::FileServe {
                    fileid,
                    hv_file,
                    additional,
                    keystamp_valid,
                    head_only,
                } => {
                    // Java validates fileindex/xres BEFORE cache hit check
                    // (line 194 in HTTPResponse.processRequest). Even a cached
                    // file with missing/invalid arguments returns 404.
                    let fileindex_valid = additional
                        .fileindex
                        .as_deref()
                        .is_some_and(|v| v.parse::<u32>().is_ok());
                    let xres_valid = additional
                        .xres
                        .as_deref()
                        .is_some_and(|v| v == "org" || v.parse::<u32>().is_ok());

                    if !keystamp_valid {
                        response::forbidden_response()
                    } else if hv_file.is_none() || !fileindex_valid || !xres_valid {
                        response::not_found_response()
                    } else {
                        let hv = hv_file.as_ref().unwrap();
                        let fileindex = additional.fileindex.as_deref().unwrap();
                        let xres = additional.xres.as_deref().unwrap();
                        let cache_path = hv.cache_path(&config.cache_dir);
                        let cache_hit = cache_path.exists()
                            && cache_path
                                .metadata()
                                .map(|m| m.len() == hv.size as u64)
                                .unwrap_or(false);
                        if cache_hit {
                            // Java: if markRecentlyAccessed returns true (LRU bit was
                            // not set) and verification is not disabled/on cooldown,
                            // verify SHA1 inline and delete corrupt file in cleanup().
                            let recently_accessed = state.cache.mark_recently_accessed(hv, false);
                            // Java: Stats.fileSent() in HTTPResponseProcessorFile.initialize()
                            // (file opened successfully). bytesSent is recorded per actual
                            // bytes transmitted in the body's finish_with().
                            state.stats.record_file_sent();
                            if head_only {
                                response::head_response(hv.mime_type(), hv.size as usize)
                            } else {
                                let verify = recently_accessed
                                    && !config.disable_file_verification
                                    && !state.cache.is_file_verification_on_cooldown();
                                let stats = if !is_local && !is_rpc {
                                    Some(state.stats.clone())
                                } else {
                                    None
                                };
                                response::file_response(
                                    hv,
                                    &config.cache_dir,
                                    bwm_for_request,
                                    verify,
                                    Some(state.cache.clone()),
                                    stats,
                                )
                            }
                        } else {
                            match state
                                .rpc_client
                                .static_range_fetch(fileindex, xres, &fileid)
                                .await
                            {
                                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                                    let sources: Vec<Url> = sr
                                        .lines
                                        .iter()
                                        .filter(|s| !s.is_empty())
                                        .filter_map(|s| Url::parse(s).ok())
                                        .collect();
                                    if sources.is_empty() {
                                        response::not_found_response()
                                    } else {
                                        // Java creates HTTPResponseProcessorProxy and calls
                                        // initialize() for both GET and HEAD. The init result
                                        // (connecting to source, checking Content-Length/size)
                                        // determines the status code. HEAD then skips body.
                                        match ProxyFileDownloader::new(
                                            &fileid,
                                            &sources,
                                            &config,
                                            Some(state.cache.clone()),
                                            state.stats.clone(),
                                            &state.proxy_client,
                                        )
                                        .await
                                        {
                                            Ok(proxy) => {
                                                let mime = hv.mime_type();
                                                // Stats for proxy are recorded in body's finish_with()
                                                // after actual transmission — Java: proxyThreadCompleted().
                                                if head_only {
                                                    // Drop both channels — download_task treats
                                                    // proxy_done_rx recv Err as "body done" and
                                                    // proceeds to finalize (Java: HEAD still caches).
                                                    drop(proxy.watch_rx);
                                                    drop(proxy.proxy_done_tx);
                                                    response::head_response(
                                                        mime,
                                                        proxy.content_length,
                                                    )
                                                } else {
                                                    let total_size = proxy.content_length;
                                                    let proxy_stats = if !is_local && !is_rpc {
                                                        Some(state.stats.clone())
                                                    } else {
                                                        None
                                                    };
                                                    match std::fs::File::open(&proxy.temp_file) {
                                                        Err(e) => {
                                                            tracing::warn!(
                                                                "Proxy: cannot open temp file for {}: {}",
                                                                fileid,
                                                                e
                                                            );
                                                            // Signal done so download_task is not
                                                            // stuck waiting; it will delete temp file.
                                                            drop(proxy.proxy_done_tx);
                                                            response::text_response(
                                                                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                                "proxy temp file unavailable",
                                                            )
                                                        }
                                                        Ok(file) => {
                                                            let response = response::proxy_response(
                                                                response::ProxyResponseParts {
                                                                    content_type: mime,
                                                                    total_size,
                                                                    temp_file: file,
                                                                    temp_file_path: proxy.temp_file,
                                                                    watch_rx: proxy.watch_rx,
                                                                    proxy_done_tx: proxy
                                                                        .proxy_done_tx,
                                                                    stats: proxy_stats,
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
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    "Proxy download failed for {}: {}",
                                                    fileid,
                                                    e
                                                );
                                                if let crate::error::HathError::ProxyDownloader {
                                                    status,
                                                    message,
                                                } = e
                                                {
                                                    response::text_response(
                                                        hyper::StatusCode::from_u16(status).unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR),
                                                        &message,
                                                    )
                                                } else {
                                                    // Java: connection failures → 500
                                                    response::text_response(
                                                        hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                                        &e.to_string(),
                                                    )
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => response::not_found_response(),
                            }
                        }
                    }
                }
                RequestType::ServerCommand {
                    command,
                    additional,
                    valid,
                } => {
                    if valid {
                        handle_server_command(&command, &additional, &state, bwm_for_request).await
                    } else {
                        response::forbidden_response()
                    }
                }
                RequestType::SpeedTest {
                    testsize,
                    valid,
                    forbidden,
                    head_only,
                    ..
                } => {
                    if valid {
                        if !head_only && !is_local && !is_rpc {
                            state.stats.record_bytes_sent(testsize as u64);
                        }
                        if head_only {
                            // Java: speedtest inherits CONTENT_TYPE_DEFAULT = text/html
                            response::head_response(
                                "text/html; charset=iso-8859-1",
                                testsize as usize,
                            )
                        } else {
                            response::speedtest_response(testsize as usize, bwm_for_request)
                        }
                    } else if forbidden {
                        // Java: responseStatusCode = 403 for expired/invalid key
                        response::forbidden_response()
                    } else {
                        // Java: responseStatusCode = 400 for malformed URL (< 5 parts)
                        response::bad_request_response()
                    }
                }
                RequestType::Favicon => {
                    response::redirect_response("https://e-hentai.org/favicon.ico")
                }
                RequestType::Robots => response::robots_response(),
                RequestType::BadRequest => response::bad_request_response(),
                RequestType::MethodNotAllowed => response::method_not_allowed_response(),
                RequestType::NotFound => response::not_found_response(),
            };

            // Body chunk throttling is handled inside StreamingBody::poll_frame.

            // Add Server header to every response
            if let Ok(ref mut r) = resp {
                r.headers_mut().insert(
                    header::SERVER,
                    header::HeaderValue::from_static(
                        "Genetic Lifeform and Distributed Open Server 1.6.5",
                    ),
                );
                // Add Date header
                let date = chrono::Utc::now()
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string();
                if let Ok(v) = header::HeaderValue::from_str(&date) {
                    r.headers_mut().insert(header::DATE, v);
                }
            }

            // Header accounting/throttling: use actual serialized header bytes.
            // Java: bwm.waitForQuota(myThread, headerBytes.length) where headerBytes
            // is the full serialized HTTP response header; Stats.bytesSent records
            // the same header byte count for non-local clients.
            if let Ok(ref r) = resp {
                let reason_len = r.status().canonical_reason().map_or(0, |s| s.len());
                // Status line: "HTTP/1.1 XXX reason\r\n"
                let status_line_len = 13 + reason_len; // "HTTP/1.1 " + "XXX " + reason + "\r\n"
                let headers_len: usize = r
                    .headers()
                    .iter()
                    .map(|(k, v)| k.as_str().len() + 2 + v.as_bytes().len() + 2) // "Key: Value\r\n"
                    .sum();
                let total_header_bytes = status_line_len + headers_len + 2; // + trailing \r\n
                if let Some(ref bwm) = bwm_for_header {
                    bwm.wait_for_quota(total_header_bytes).await;
                }
                if !is_local {
                    state.stats.record_bytes_sent(total_header_bytes as u64);
                }
            }

            match resp {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::error!("Error building response: {}", e);
                    Ok(Response::builder()
                        .status(500)
                        .body(StreamingBody::new(
                            bytes::Bytes::from_static(b"Internal Server Error"),
                            None,
                        ))
                        .unwrap())
                }
            }
        })
    }
}

/// Helper for `threaded_proxy_test`: extract required params from Additional,
/// returning `INVALID_COMMAND` on missing/illegal values (matching Java's
/// NumberFormatException → catch → INVALID_COMMAND flow).
macro_rules! required_param {
    ($add:expr, $field:ident) => {
        match $add.$field.as_deref() {
            Some(v) => v,
            None => return response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
        }
    };
    ($add:expr, $field:ident, $T:ty) => {
        match $add.$field.as_deref().and_then(|v| v.parse::<$T>().ok()) {
            Some(v) => v,
            None => return response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
        }
    };
    ($add:expr, $field:ident, $T:ty, default $default:expr) => {
        match $add.$field.as_deref() {
            Some(v) => match v.parse::<$T>() {
                Ok(n) => n,
                Err(_) => return response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
            },
            None => $default,
        }
    };
}

/// Handle servercmd API commands. Must support all Java commands.
async fn handle_server_command(
    command: &str,
    additional: &str,
    state: &AppState,
    bwm: Option<Arc<BandwidthMonitor>>,
) -> crate::error::Result<Response<StreamingBody>> {
    match command.to_lowercase().as_str() {
        "still_alive" => response::text_response(
            hyper::StatusCode::OK,
            "I feel FANTASTIC and I'm still alive",
        ),
        "threaded_proxy_test" => {
            // Java: Integer.parseInt on missing/illegal params throws NFE,
            // caught by processRemoteAPICommand → returns "INVALID_COMMAND".
            let add = utils::parse_additional(additional);
            let hostname = required_param!(add, hostname);
            let protocol = required_param!(add, protocol);
            let port: u16 = required_param!(add, port, u16);
            let testsize: u64 = required_param!(add, testsize, u64);
            let testcount: u32 = required_param!(add, testcount, u32);
            let testtime: u32 = required_param!(add, testtime, u32);
            let testkey = add.testkey.as_deref().unwrap_or("");

            tracing::debug!(
                "Running speedtest against hostname={} protocol={} port={} testsize={} testcount={} testtime={} testkey={}",
                hostname,
                protocol,
                port,
                testsize,
                testcount,
                testtime,
                testkey
            );

            let result = run_threaded_proxy_test(
                hostname, protocol, port, testsize, testcount, testtime, testkey,
            )
            .await;

            tracing::debug!(
                "Ran speedtest against hostname={} testsize={} testcount={}, reporting successfulTests={} totalTimeMillis={}",
                hostname,
                testsize,
                testcount,
                result.0,
                result.1
            );

            response::text_response(
                hyper::StatusCode::OK,
                &format!("OK:{}-{}", result.0, result.1),
            )
        }
        "speed_test" => {
            // Java: additional is parsed as key=value pairs via Tools.parseAdditional();
            // testsize is read from addTable with default 1_000_000. No upper limit.
            let add = utils::parse_additional(additional);
            let testsize: usize = required_param!(add, testsize, usize, default 1_000_000);
            response::speedtest_response(testsize, bwm)
        }
        "refresh_settings" => {
            match state.rpc_client.refresh_settings().await {
                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                    crate::config::Config::apply_server_response(&state.config, &sr);
                    // Recreate bandwidth monitor if throttle_bytes changed
                    let cfg = state.config.load_full();
                    if cfg.throttle_bytes > 0 && !cfg.disable_bwm {
                        state
                            .bandwidth_monitor
                            .store(Some(Arc::new(BandwidthMonitor::new(cfg.throttle_bytes))));
                    } else {
                        state.bandwidth_monitor.store(None);
                    }
                    response::text_response(hyper::StatusCode::OK, "")
                }
                _ => response::text_response(hyper::StatusCode::OK, ""),
            }
        }
        "start_downloader" => response::text_response(hyper::StatusCode::OK, ""),
        "refresh_certs" => {
            // Java: client.setCertRefresh() — just set the flag, main loop does
            // the actual work (suspend → shutdown → restart → resume).
            state.do_cert_refresh.store(true, Ordering::Release);
            state.cert_refresh_notify.notify_one();
            response::text_response(hyper::StatusCode::OK, "")
        }
        _ => response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
    }
}

/// Run the threaded proxy test: spawn concurrent outbound GET requests
/// to {protocol}://{hostname}:{port}/t/{testsize}/{testtime}/{testkey}/{random_int}
/// and return (successful_tests, total_time_millis).
/// Java: HTTPResponse.processThreadedProxyTest()
async fn run_threaded_proxy_test(
    hostname: &str,
    protocol: &str,
    port: u16,
    testsize: u64,
    testcount: u32,
    testtime: u32,
    testkey: &str,
) -> (u32, u64) {
    use rand::RngExt;

    // Java: FileDownloader(source, 10000, 60000, true)
    // connectTimeout=5s, readTimeout=10s,
    // maxDLTime=60s is stored but not enforced, 3 retries.
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(10))
        .build();

    let Ok(client) = client else {
        return (0, 0);
    };

    let mut handles = Vec::with_capacity(testcount as usize);

    for _ in 0..testcount {
        let random_int: u32 = rand::rng().random();
        let Ok(url) = build_threaded_proxy_test_url(
            protocol, hostname, port, testsize, testtime, testkey, random_int,
        ) else {
            continue;
        };
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            run_threaded_proxy_test_download(&client, url, testsize).await
        }));
    }

    let mut successful = 0u32;
    let mut total_time_ms = 0u64;

    for handle in handles {
        if let Ok(Some(ms)) = handle.await {
            successful += 1;
            total_time_ms += ms;
        }
    }

    (successful, total_time_ms)
}

enum ThreadedProxyTestAttemptError {
    NotFound,
    Retryable,
}

async fn run_threaded_proxy_test_download(
    client: &reqwest::Client,
    url: Url,
    testsize: u64,
) -> Option<u64> {
    use std::time::Instant;

    for retries_left in (0..3).rev() {
        let result = async {
            let mut resp = client
                .get(url.clone())
                .header("Connection", "Close")
                .header(
                    hyper::header::USER_AGENT,
                    format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
                )
                .send()
                .await
                .map_err(|_| ThreadedProxyTestAttemptError::Retryable)?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(ThreadedProxyTestAttemptError::NotFound);
            }
            if !resp.status().is_success() {
                return Err(ThreadedProxyTestAttemptError::Retryable);
            }

            let content_length = resp
                .content_length()
                .ok_or(ThreadedProxyTestAttemptError::Retryable)?;
            let mut first_byte_at: Option<Instant> = None;
            let mut received = 0u64;

            while let Some(chunk) = resp
                .chunk()
                .await
                .map_err(|_| ThreadedProxyTestAttemptError::Retryable)?
            {
                if !chunk.is_empty() {
                    first_byte_at.get_or_insert_with(Instant::now);
                    received += chunk.len() as u64;
                }
            }

            if received != content_length {
                return Err(ThreadedProxyTestAttemptError::Retryable);
            }

            Ok((content_length >= testsize).then(|| {
                first_byte_at
                    .map(|start| start.elapsed().as_millis() as u64)
                    .unwrap_or(0)
            }))
        }
        .await;

        match result {
            Ok(download_time_ms) => return download_time_ms,
            Err(ThreadedProxyTestAttemptError::NotFound) => return None,
            Err(ThreadedProxyTestAttemptError::Retryable) => {
                tracing::warn!(
                    "Threaded proxy test failed (retrying, {} left)",
                    retries_left
                );
            }
        }
    }

    None
}

fn build_threaded_proxy_test_url(
    protocol: &str,
    hostname: &str,
    port: u16,
    testsize: u64,
    testtime: u32,
    testkey: &str,
    random_int: u32,
) -> Result<Url> {
    let mut url = Url::parse("http://hath.invalid/")
        .map_err(|e| HathError::Network(format!("invalid speedtest URL base: {}", e)))?;
    url.set_scheme(protocol)
        .map_err(|_| HathError::Network(format!("invalid speedtest protocol: {}", protocol)))?;
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        url.set_ip_host(ip)
            .map_err(|_| HathError::Network("invalid speedtest host".into()))?;
    } else {
        url.set_host(Some(hostname))
            .map_err(|e| HathError::Network(format!("invalid speedtest host: {}", e)))?;
    }
    url.set_port(Some(port))
        .map_err(|_| HathError::Network(format!("invalid speedtest port: {}", port)))?;
    url.set_path(&format!(
        "/t/{}/{}/{}/{}",
        testsize, testtime, testkey, random_int
    ));
    Ok(url)
}

/// Start one HTTP server generation. Returns a oneshot receiver that fires when
/// the server binds, and the generation shutdown token (to store in AppState).
pub fn start_server(
    state: AppState,
) -> (
    tokio::sync::oneshot::Receiver<std::result::Result<u16, String>>,
    tokio_util::sync::CancellationToken,
) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_shutdown_token = tokio_util::sync::CancellationToken::new();
    let server_shutdown = server_shutdown_token.clone();
    state.server_terminated.store(false, Ordering::Release);
    tokio::spawn(async move {
        if let Err(e) = run_server(state, server_shutdown, Some(ready_tx)).await {
            tracing::error!("Server error: {}", e);
        }
    });
    (ready_rx, server_shutdown_token)
}

/// Stop the current HTTP server generation and drain in-flight requests.
///
/// RPC lifecycle actions such as client_stop/client_suspend are intentionally
/// handled by callers. This helper only quiesces the local HTTP server.
pub async fn stop_server(state: &AppState) {
    tracing::info!("Stopping server");
    state
        .allow_normal_connections
        .store(false, Ordering::SeqCst);

    if let Some(token) = state.server_shutdown_token.load_full() {
        token.cancel();
    }

    for close_wait_cycles in 1..25 {
        let active = state.active_connections.load(Ordering::Relaxed);
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if close_wait_cycles % 5 == 0 {
            let remaining = 25 - close_wait_cycles;
            tracing::info!(
                "Waiting for {} request(s) to finish; will wait for another {} seconds",
                active,
                remaining
            );
        }
    }

    let mut wait_cycles = 0u32;
    loop {
        if state.server_terminated.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        wait_cycles += 1;
        if wait_cycles >= 60 {
            tracing::warn!("Server did not terminate after 300s");
            break;
        }
        if wait_cycles > 1 {
            tracing::info!(
                "Waiting for HTTPServer to fully terminate... (waited {} seconds)",
                wait_cycles * 5
            );
        }
    }
}

/// Spawn periodic flood control pruning (60s interval).
pub fn spawn_flood_control_pruner(state: AppState, shutdown: tokio_util::sync::CancellationToken) {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(60),
        move || {
            let state = state.clone();
            async move { prune_flood_control(&state).await }
        },
    ));
}

/// Prune stale flood control entries. Called periodically from main loop.
pub async fn prune_flood_control(state: &AppState) {
    let mut fc = state.flood_control.lock().await;
    let now = Instant::now();
    fc.retain(|_, entry| !entry.is_stale(now));
}

/// Nuke old connections (simplified — Hyper handles most connection lifecycle).
pub async fn nuke_old_connections(_state: &AppState) {
    // Hyper's http1::Builder doesn't expose connection tracking.
    // We rely on Hyper's built-in timeouts instead of Java's manual nuke.
}

/// Run one TLS HTTP server generation. Sends readiness via `ready_tx` after successful bind.
/// The generation stops only when its server shutdown token is cancelled.
async fn run_server(
    state: AppState,
    server_shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
    let config = state.config.load_full();

    // Build TLS acceptor (downloads cert if needed)
    // Java: always re-downloads the certificate on startup.
    let (tls_acceptor, cert_expiry) = match tls::build_tls_acceptor(&config, true).await {
        Ok((a, expiry)) => (a, expiry),
        Err(e) => {
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(e.to_string()));
            }
            return Err(e);
        }
    };

    // Create bandwidth monitor if throttling is enabled and not disabled.
    // Explicitly clear on server restart so cert refresh doesn't leak old monitor.
    if config.throttle_bytes > 0 && !config.disable_bwm {
        state
            .bandwidth_monitor
            .store(Some(Arc::new(BandwidthMonitor::new(config.throttle_bytes))));
    } else {
        state.bandwidth_monitor.store(None);
    }

    // Store in AppState so it can be refreshed at runtime
    state.tls_acceptor.store(Some(Arc::new(tls_acceptor)));
    *state.cert_expiry.lock().await = Some(cert_expiry);

    let port = config.client_port;
    // Bind to [::] (IPv6 dual-stack) which also accepts IPv4 on Linux/macOS.
    // Falls back to 0.0.0.0 (IPv4-only) if the OS doesn't support dual-stack.
    let addr_v6 = SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port));
    let listener = match TcpListener::bind(addr_v6).await {
        Ok(l) => l,
        Err(_) => {
            let addr_v4 = SocketAddr::from(([0, 0, 0, 0], port));
            TcpListener::bind(addr_v4).await.map_err(HathError::Io)?
        }
    };

    if let Some(tx) = ready_tx {
        let _ = tx.send(Ok(port));
    }

    tracing::info!("HTTPServer listening on port {}", port);

    loop {
        tokio::select! {
            _ = server_shutdown.cancelled() => break,
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Accept error: {}", e);
                        continue;
                    },
                };

                let state = state.clone();

                // Load current TLS context (may have been refreshed)
                let ssl_context = state.tls_acceptor.load_full()
                    .expect("TLS context not initialized");

                let conn_state = state.clone();
                tokio::spawn(async move {
                    // Normalize IPv4-mapped IPv6 (::ffff:a.b.c.d) → plain IPv4.
                    // Must happen inside the task so host_addr is available after
                    // the TLS handshake for the allow/flood-control checks below.
                    let host_addr = utils::normalize_ip(remote_addr.ip()).to_string().to_lowercase();

                    // --- TLS handshake first (matches Java: SSLServerSocket.accept()
                    //     completes the handshake before any policy checks are applied) ---
                    let ssl = match openssl::ssl::Ssl::new(ssl_context.as_ref()) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("SSL Ssl::new failed from {}: {:?}", remote_addr, e);
                            return;
                        }
                    };
                    let mut tls_stream = match tokio_openssl::SslStream::new(ssl, stream) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("SslStream::new failed from {}: {:?}", remote_addr, e);
                            return;
                        }
                    };
                    if let Err(e) = Pin::new(&mut tls_stream).accept().await {
                        let msg = e.to_string();
                        if msg.contains("connection reset") || msg.contains("unexpected EOF") {
                            tracing::debug!("TLS accept from {} closed early: {}", remote_addr, e);
                        } else {
                            tracing::warn!("TLS accept failed from {}: {:?}", remote_addr, e);
                        }
                        return;
                    }

                    // --- Post-handshake policy checks (Java: HTTPServer.run() order) ---
                    let cfg = conn_state.config.load();
                    configure_send_buffer(tls_stream.get_ref(), &cfg);
                    let is_local =
                        LOCAL_NETWORK_RE.is_match(&host_addr) || cfg.client_host == host_addr;
                    // Java: isValidRPCServer returns true when disableIPOriginCheck is set
                    let is_rpc = cfg.disable_ip_origin_check
                        || cfg.rpc_servers.iter().any(|s| s.to_string().to_lowercase() == host_addr);

                    let allow = conn_state.allow_normal_connections.load(Ordering::Relaxed);
                    if !allow && !is_rpc {
                        tracing::warn!(
                            "Rejecting connection from {} during startup (rpc_servers={:?})",
                            host_addr, cfg.rpc_servers
                        );
                        return;
                    }

                    if !is_local && !is_rpc && !cfg.disable_flood_control {
                        drop(cfg);
                        let mut fc = conn_state.flood_control.lock().await;
                        let entry = fc.entry(host_addr.clone()).or_insert_with(|| FloodControlEntry {
                            connect_count: 0,
                            last_connect: Instant::now(),
                            block_until: None,
                        });
                        if entry.is_blocked() || !entry.hit() {
                            tracing::warn!("Flood control activated for {}", host_addr);
                            return;
                        }
                    }

                    // Connection limiting for non-local, non-RPC traffic.
                    // Java: HTTPServer.run() checks sessionCount > maxConnections
                    // in the single-threaded accept loop.
                    // We use fetch_add as an atomic gate to avoid a race between
                    // the load check and the increment in the spawned task.
                    let mut _guard: Option<ConnectionGuard> = None;
                    if !is_local && !is_rpc {
                        let max_conns = conn_state.config.load().max_connections();
                        let prev = conn_state.active_connections.fetch_add(1, Ordering::Relaxed);
                        // Java uses > (not >=), so prev == max_conns is still accepted.
                        if prev > max_conns {
                            conn_state.active_connections.fetch_sub(1, Ordering::Relaxed);
                            tracing::warn!(
                                "Exceeded the maximum allowed number of incoming connections ({}).",
                                max_conns
                            );
                            return;
                        }
                        conn_state.stats.set_open_connections(prev + 1);
                        _guard = Some(ConnectionGuard {
                            active_connections: conn_state.active_connections.clone(),
                            stats: conn_state.stats.clone(),
                        });

                        if prev > (max_conns as f64 * 0.8) as u32 && prev > 0 {
                            tracing::warn!(
                                "Near connection limit: {} / {} active connections",
                                prev + 1, max_conns
                            );
                            let now = Instant::now();
                            let mut last = conn_state.last_overload_notification.lock().await;
                            let should_notify = last.is_none_or(|t| now - t >= Duration::from_secs(30));
                            if should_notify {
                                *last = Some(now);
                                drop(last);
                                let _ = conn_state.rpc_client.notify_overload().await;
                            }
                        }
                    }


                    let io = TokioIo::new(tls_stream);

                    let conn_id = conn_state.next_conn_id.fetch_add(1, Ordering::Relaxed) + 1;
                    let service = AccessLogService::new(
                        HathService {
                            state: conn_state,
                            remote_addr,
                        },
                        conn_id,
                        remote_addr,
                    );

                    if let Err(e) = http1::Builder::new()
                        .header_read_timeout(Duration::from_secs(10))
                        .timer(TokioTimer::new())
                        .serve_connection(io, service)
                        .await
                        && !e.to_string().contains("connection closed") {
                            tracing::debug!("HTTP connection error: {}", e);
                        }
                });
            }
        }
    }

    // Signal that the server has fully terminated (for cert refresh watcher).
    state.server_terminated.store(true, Ordering::Release);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threaded_proxy_test_url_handles_ipv6_host() {
        let url = build_threaded_proxy_test_url(
            "http",
            "::ffff:192.0.2.1",
            8443,
            1024,
            30,
            "testkey",
            12345,
        )
        .unwrap();

        assert_eq!(
            url.as_str(),
            "http://[::ffff:c000:201]:8443/t/1024/30/testkey/12345"
        );
    }
}
