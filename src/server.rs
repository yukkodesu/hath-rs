use crate::bandwidth::BandwidthMonitor;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::request::{self, RequestType};
use crate::response;
use crate::stats::Stats;
use crate::cache::CacheHandler;
use crate::rpc_client::RpcClient;
use crate::rpc::{self, Action};
use crate::utils;
use crate::proxy_downloader::ProxyFileDownloader;

use arc_swap::{ArcSwap, ArcSwapOption};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use crate::body::StreamingBody;
use hyper::header;
use openssl::asn1::Asn1Time;
use openssl::pkcs12::Pkcs12;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use reqwest::Url;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use regex::Regex;
use std::sync::LazyLock;

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
    /// TLS acceptor that can be swapped at runtime (e.g. cert refresh).
    pub tls_acceptor: Arc<ArcSwapOption<TlsAcceptor>>,
    /// Certificate expiry as a Unix timestamp (seconds). Checked periodically;
    /// if the cert expires within 24 hours, the client shuts down (matches Java).
    pub cert_expiry: Arc<Mutex<Option<i64>>>,
    /// Bandwidth throttling monitor (shared across all connections).
    /// If throttle_bytes is 0, no throttling is applied (None).
    pub bandwidth_monitor: Arc<ArcSwapOption<BandwidthMonitor>>,
    /// Count of currently active connections (for max_connections enforcement).
    pub active_connections: Arc<std::sync::atomic::AtomicU32>,
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
    pub server_restart_token: Arc<ArcSwapOption<tokio_util::sync::CancellationToken>>,
    /// Set to true by start_server() after the accept loop exits.
    /// The cert refresh watcher polls this to wait for the old server to terminate.
    pub server_terminated: Arc<AtomicBool>,
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
        self.last_connect < now - Duration::from_secs(60)
    }

    /// Returns true if the connection should be allowed.
    pub fn hit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ms = (now - self.last_connect).as_millis() as u32;
        self.connect_count = self.connect_count.saturating_sub(elapsed_ms / 1000).saturating_add(1);
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

pub struct HathService {
    pub state: AppState,
}

impl Service<Request<Incoming>> for HathService {
    type Response = Response<StreamingBody>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let state = self.state.clone();
        // Extract remote_addr from extensions (injected by accept loop)
        let remote_addr = req.extensions().get::<SocketAddr>().copied();

        Box::pin(async move {
            let client_ip = remote_addr.map(|a| a.ip()).unwrap_or_else(|| "0.0.0.0".parse().unwrap());
            // Load config once for this request (owned Arc, safe across .await)
            let config = state.config.load_full();

            // Determine if this is a local/RPC connection (skip bandwidth throttling)
            let host_addr = client_ip.to_string().to_lowercase();
            let is_local = LOCAL_NETWORK_RE.is_match(&host_addr)
                || config.client_host.replace("::ffff:", "") == host_addr;
            let is_rpc = config.rpc_servers.iter().any(|s| s.to_string().to_lowercase() == host_addr);

            // Determine bandwidth monitor for this request.
            // Java: Only local connections skip throttling.
            // RPC servers on non-local IPs are still throttled.
            let bwm_for_request = if is_local {
                None
            } else {
                state.bandwidth_monitor.load_full().clone()
            };

            // Build request line for parsing
            let request_line = format!(
                "{} {} {:?}",
                req.method(),
                req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/"),
                req.version()
            );

            let request_type = request::parse_request(&request_line, client_ip, &config);

            // Clone BWM for later header throttling (bwm_for_request is consumed by response builders)
            let bwm_for_header = bwm_for_request.clone();

            let mut resp = match request_type {
                RequestType::FileServe { fileid, hv_file, additional, keystamp_valid, head_only } => {
                    // Java validates fileindex/xres BEFORE cache hit check
                    // (line 194 in HTTPResponse.processRequest). Even a cached
                    // file with missing/invalid arguments returns 404.
                    let fileindex = additional.get("fileindex");
                    let xres = additional.get("xres");
                    let fileindex_valid = fileindex.is_some_and(|v| v.parse::<u32>().is_ok());
                    let xres_valid = xres.is_some_and(|v| v == "org" || v.parse::<u32>().is_ok());

                    if !keystamp_valid {
                        response::forbidden_response()
                    } else if hv_file.is_none() || !fileindex_valid || !xres_valid {
                        response::not_found_response()
                    } else {
                        let hv = hv_file.as_ref().unwrap();
                        let fileindex = fileindex.unwrap();
                        let xres = xres.unwrap();
                        let cache_path = hv.cache_path(&config.cache_dir);
                        let cache_hit = cache_path.exists()
                            && cache_path.metadata()
                                .map(|m| m.len() == hv.size as u64)
                                .unwrap_or(false);
                        if cache_hit {
                            state.cache.mark_recently_accessed(hv, false);
                            state.stats.record_file_sent();
                            if !is_local && !is_rpc {
                                state.stats.record_bytes_sent(hv.size as u64);
                            }
                            if head_only {
                                response::head_response(hv.mime_type(), hv.size as usize)
                            } else {
                                response::file_response(hv, &config.cache_dir, bwm_for_request).await
                            }
                        } else {
                            match state.rpc_client.static_range_fetch(fileindex, xres, &fileid).await {
                                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                                    let sources: Vec<Url> = sr.lines.iter()
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
                                        match ProxyFileDownloader::new(&fileid, &sources, &config, Some(state.cache.clone())).await {
                                            Ok(proxy) => {
                                                let mime = hv.mime_type();
                                                state.stats.record_file_sent();
                                                if !is_local && !is_rpc {
                                                    state.stats.record_bytes_sent(hv.size as u64);
                                                }
                                                if head_only {
                                                    // Java: requestCompleted() → proxyThreadCompleted()
                                                    // fires even for HEAD. Signal the download task
                                                    // that the body side is done so it can finalize
                                                    // immediately instead of waiting 300s.
                                                    proxy.body_done_notify.notify_one();
                                                    response::head_response(mime, proxy.total_size as usize)
                                                } else {
                                                    response::proxy_response(
                                                        mime,
                                                        proxy.total_size as usize,
                                                        proxy.temp_file,
                                                        proxy.write_offset,
                                                        proxy.notify,
                                                        proxy.body_done_notify,
                                                        proxy.download_done,
                                                        bwm_for_request,
                                                    )
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!("Proxy download failed for {}: {}", fileid, e);
                                                if let crate::error::HathError::ProxyDownloader { status, message } = e {
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
                RequestType::ServerCommand { command, additional, valid } => {
                    if valid {
                        handle_server_command(&command, &additional, &state, bwm_for_request).await
                    } else {
                        response::forbidden_response()
                    }
                }
                RequestType::SpeedTest { testsize, valid, forbidden, head_only, .. } => {
                    if valid {
                        if !head_only && !is_local && !is_rpc {
                            state.stats.record_bytes_sent(testsize as u64);
                        }
                        if head_only {
                            // Java: speedtest inherits CONTENT_TYPE_DEFAULT = text/html
                            response::head_response("text/html; charset=iso-8859-1", testsize as usize)
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
                RequestType::Favicon => response::redirect_response("https://e-hentai.org/favicon.ico"),
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
                    header::HeaderValue::from_static("Genetic Lifeform and Distributed Open Server 1.6.5")
                );
                // Add Date header
                let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
                if let Ok(v) = header::HeaderValue::from_str(&date) {
                    r.headers_mut().insert(header::DATE, v);
                }
            }

            // Header throttling: deduct actual serialized header bytes.
            // Java: bwm.waitForQuota(myThread, headerBytes.length) where headerBytes
            // is the full serialized HTTP response header.
            if let Some(ref bwm) = bwm_for_header
                && let Ok(ref r) = resp {
                    let reason_len = r.status().canonical_reason().map_or(0, |s| s.len());
                    // Status line: "HTTP/1.1 XXX reason\r\n"
                    let status_line_len = 13 + reason_len; // "HTTP/1.1 " + "XXX " + reason + "\r\n"
                    let headers_len: usize = r.headers().iter()
                        .map(|(k, v)| k.as_str().len() + 2 + v.as_bytes().len() + 2) // "Key: Value\r\n"
                        .sum();
                    let total_header_bytes = status_line_len + headers_len + 2; // + trailing \r\n
                    bwm.wait_for_quota(total_header_bytes).await;
                }

            match resp {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::error!("Error building response: {}", e);
                    Ok(Response::builder()
                        .status(500)
                        .body(StreamingBody::new(bytes::Bytes::from_static(b"Internal Server Error"), None))
                        .unwrap())
                }
            }
        })
    }
}

/// Helper for `threaded_proxy_test`: extract required params from add_table,
/// returning `INVALID_COMMAND` on missing/illegal values (matching Java's
/// NumberFormatException → catch → INVALID_COMMAND flow).
macro_rules! required_param {
    ($table:expr, $key:expr) => {
        match $table.get($key) {
            Some(v) => v,
            None => return response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
        }
    };
    ($table:expr, $key:expr, $T:ty) => {
        match $table.get($key).and_then(|v| v.parse::<$T>().ok()) {
            Some(v) => v,
            None => return response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
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
        "still_alive" => {
            response::text_response(hyper::StatusCode::OK, "I feel FANTASTIC and I'm still alive")
        }
        "threaded_proxy_test" => {
            // Java: Integer.parseInt on missing/illegal params throws NFE,
            // caught by processRemoteAPICommand → returns "INVALID_COMMAND".
            let add_table = utils::parse_additional(additional);
            let hostname = required_param!(add_table, "hostname");
            let protocol = required_param!(add_table, "protocol");
            let port: u16 = required_param!(add_table, "port", u16);
            let testsize: u64 = required_param!(add_table, "testsize", u64);
            let testcount: u32 = required_param!(add_table, "testcount", u32);
            let testtime: u32 = required_param!(add_table, "testtime", u32);
            let testkey = add_table.get("testkey")
                .map(|s| s.as_str()).unwrap_or("");

            tracing::debug!(
                "Running speedtest against hostname={} protocol={} port={} testsize={} testcount={} testtime={} testkey={}",
                hostname, protocol, port, testsize, testcount, testtime, testkey
            );

            let result = run_threaded_proxy_test(
                hostname, protocol, port, testsize, testcount, testtime, testkey,
            ).await;

            tracing::debug!(
                "Ran speedtest against hostname={} testsize={} testcount={}, reporting successfulTests={} totalTimeMillis={}",
                hostname, testsize, testcount, result.0, result.1
            );

            response::text_response(hyper::StatusCode::OK, &format!("OK:{}-{}", result.0, result.1))
        }
        "speed_test" => {
            // Java: additional is parsed as key=value pairs via Tools.parseAdditional();
            // testsize is read from addTable with default 1_000_000. No upper limit.
            let add_table = utils::parse_additional(additional);
            let testsize: usize = add_table.get("testsize")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000_000);
            response::speedtest_response(testsize, bwm)
        }
        "refresh_settings" => {
            match state.rpc_client.refresh_settings().await {
                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                    crate::config::Config::apply_server_response(&state.config, &sr);
                    // Recreate bandwidth monitor if throttle_bytes changed
                    let cfg = state.config.load_full();
                    if cfg.throttle_bytes > 0 && !cfg.disable_bwm {
                        state.bandwidth_monitor.store(Some(Arc::new(
                            BandwidthMonitor::new(cfg.throttle_bytes)
                        )));
                    } else {
                        state.bandwidth_monitor.store(None);
                    }
                    response::text_response(hyper::StatusCode::OK, "")
                }
                _ => response::text_response(hyper::StatusCode::OK, ""),
            }
        }
        "start_downloader" => {
            response::text_response(hyper::StatusCode::OK, "")
        }
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
    use std::time::Instant;
    use tokio::time::timeout;

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build();

    let Ok(client) = client else {
        return (0, 0);
    };

    let mut handles = Vec::with_capacity(testcount as usize);

    for _ in 0..testcount {
        let random_int: u32 = rand::rng().random();
        let url = format!(
            "{}://{}:{}/t/{}/{}/{}/{}",
            protocol, hostname, port, testsize, testtime, testkey, random_int
        );
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            let start = Instant::now();
            let total_timeout = Duration::from_secs(testtime as u64 + 5);
            let result = timeout(total_timeout, client.get(&url).send()).await;
            let elapsed_ms = start.elapsed().as_millis() as u64;

            match result {
                Ok(Ok(resp)) => {
                    if let Some(len) = resp.content_length()
                        && len >= testsize {
                            // Consume body (drain) to complete the request
                            let _ = resp.bytes().await;
                            Some(elapsed_ms)
                        } else {
                            None
                        }
                }
                _ => None,
            }
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

/// Build a TLS acceptor from the PKCS12 certificate.
/// Uses OpenSSL for PKCS12 parsing and cert expiry checking,
/// then converts to rustls types for the Hyper HTTP server.
/// If `force_download` is true, always re-download the cert from the RPC server.
/// Returns (acceptor, cert_expiry_unix_seconds).
async fn build_tls_acceptor(config: &Config, force_download: bool) -> Result<(TlsAcceptor, i64)> {
    let cert_path = config.data_dir.join("hathcert.p12");

    if force_download || !cert_path.exists() {
        let cert_url = rpc::make_rpc_url(Action::GetCertificate, "", config)?;
        let downloader = crate::downloader::FileDownloader::new(
            cert_url, 10000, 300000,
            crate::downloader::DownloadMode::File(cert_path.clone()),
            false,
        );
        downloader.download().await?;
    }

    let cert_data = std::fs::read(&cert_path)?;
    let pkcs12 = Pkcs12::from_der(&cert_data)?;
    let pkcs12 = pkcs12.parse2(config.client_key.as_str())?;

    let cert = pkcs12.cert.as_ref()
        .ok_or_else(|| HathError::Tls("no certificate in PKCS12".into()))?;
    let key = pkcs12.pkey.as_ref()
        .ok_or_else(|| HathError::Tls("no private key in PKCS12".into()))?;

    // Java: isCertExpired() — cert must not expire within the next 24 hours
    let one_day_from_now = Asn1Time::days_from_now(1)?;
    let not_after = cert.not_after();
    if not_after.compare(&one_day_from_now)? == std::cmp::Ordering::Less {
        tracing::error!(
            "The retrieved certificate is expired, or the system time is off by more than a day. \
             Correct the system time and try again."
        );
        return Err(HathError::CertExpired);
    }

    // Compute cert expiry as a Unix timestamp for periodic checks.
    let now_asn1 = Asn1Time::days_from_now(0)?;
    let diff = not_after.diff(&now_asn1)?;
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let cert_expiry_unix = now_unix + diff.days as i64 * 86400 + diff.secs as i64;

    // Convert cert and chain from OpenSSL X509 → DER → rustls CertificateDer
    let cert_der = cert.to_der()?;
    let mut cert_chain = vec![CertificateDer::from(cert_der)];
    if let Some(chain) = pkcs12.ca {
        for ca in chain {
            let ca_der = ca.to_der()?;
            cert_chain.push(CertificateDer::from(ca_der));
        }
    }

    // Convert private key: OpenSSL PKey → PEM PKCS#8 → rustls PrivatePkcs8KeyDer
    let key_pem = key.private_key_to_pem_pkcs8()?;
    let mut pem_reader = std::io::BufReader::new(key_pem.as_slice());
    let key_der = rustls_pemfile::pkcs8_private_keys(&mut pem_reader)
        .next()
        .ok_or_else(|| HathError::Tls("No private key found in PEM".into()))?
        .map_err(|e| HathError::Tls(format!("Failed to parse private key: {}", e)))?;
    let private_key = PrivateKeyDer::Pkcs8(key_der);

    // Build rustls ServerConfig with TLS 1.2 + 1.3 (matching Java)
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| HathError::Tls(format!("Failed to build TLS config: {}", e)))?;

    let acceptor = TlsAcceptor::from(Arc::new(config));
    Ok((acceptor, cert_expiry_unix))
}

/// Spawn the HTTP server. Returns a oneshot receiver that fires when
/// the server binds, and the restart token (to store in AppState).
pub fn spawn_server(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
) -> (
    tokio::sync::oneshot::Receiver<std::result::Result<u16, String>>,
    tokio_util::sync::CancellationToken,
) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let restart_token = tokio_util::sync::CancellationToken::new();
    let restart_clone = restart_token.clone();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = start_server(state, shutdown_clone, restart_clone, Some(ready_tx)).await {
            tracing::error!("Server error: {}", e);
        }
    });
    (ready_rx, restart_token)
}

/// Spawn the certificate refresh watcher.
/// Watches for refresh_certs RPC commands and performs a full server restart
/// (suspend → reject new connections → drain → shutdown old listener → restart → resume).
pub fn spawn_cert_refresh_watcher(
    state: AppState,
    rpc_client: Arc<RpcClient>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = state.cert_refresh_notify.notified() => {},
                _ = shutdown.cancelled() => break,
            }
            if !state.do_cert_refresh.load(Ordering::Acquire) {
                continue;
            }
            tracing::info!("Starting certificate refresh (full server restart)...");

            // 1. Suspend traffic
            match rpc_client.client_suspend().await {
                Ok(resp) if resp.status == rpc::ResponseStatus::Ok => {
                    tracing::info!("Suspend notification successful");
                }
                _ => {
                    tracing::warn!(
                        "Failed to contact server to suspend client traffic; will retry"
                    );
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    state.cert_refresh_notify.notify_one();
                    continue;
                }
            }

            // 2. Stop accepting new connections
            state.allow_normal_connections.store(false, Ordering::SeqCst);

            // 3. Wait 5s for in-flight requests to drain
            tokio::time::sleep(Duration::from_secs(5)).await;

            // 4. Reset terminated flag, then cancel restart token to stop old accept loop
            state.server_terminated.store(false, Ordering::Release);
            if let Some(token) = state.server_restart_token.load_full() {
                token.cancel();
            }

            // 5. Wait for old server to terminate (Java: up to 300s)
            let mut wait_cycles = 0u32;
            loop {
                if state.server_terminated.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                wait_cycles += 1;
                if wait_cycles >= 60 {
                    tracing::warn!(
                        "Server did not terminate after 300s, forcing restart"
                    );
                    break;
                }
                if wait_cycles > 1 {
                    tracing::info!(
                        "Waiting for HTTPServer to fully terminate... (waited {} seconds)",
                        wait_cycles * 5
                    );
                }
            }

            // 6. Wait 1s
            tokio::time::sleep(Duration::from_secs(1)).await;

            // 7. Restart server with new restart token
            let new_restart = tokio_util::sync::CancellationToken::new();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let server_state = state.clone();
            let new_restart_clone = new_restart.clone();
            let global_shutdown_clone = shutdown.clone();
            tokio::spawn(async move {
                if let Err(e) = start_server(
                    server_state,
                    global_shutdown_clone,
                    new_restart_clone,
                    Some(ready_tx),
                )
                .await
                {
                    tracing::error!("Server restart error: {}", e);
                }
            });
            state
                .server_restart_token
                .store(Some(Arc::new(new_restart)));

            // 8. Wait for new server to bind
            match ready_rx.await {
                Ok(Ok(port)) => {
                    tracing::info!(
                        "Server restarted successfully on port {}", port
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!("Server restart failed to bind: {}", e);
                    shutdown.cancel();
                    break;
                }
                Err(_) => {
                    tracing::error!(
                        "Server restart failed unexpectedly (oneshot dropped)"
                    );
                    shutdown.cancel();
                    break;
                }
            }

            // 9. Re-allow connections
            state.allow_normal_connections.store(true, Ordering::SeqCst);

            // 10. Resume traffic
            match rpc_client.still_alive(true).await {
                Ok(resp) if resp.status == rpc::ResponseStatus::Ok => {
                    tracing::info!("Resume notification successful");
                }
                Ok(resp) => {
                    let code = resp.fail_code.unwrap_or_default();
                    // Java: TERM_BAD_NETWORK → dieWithError (terminate client)
                    if code.starts_with("TERM_BAD_NETWORK") {
                        tracing::error!(
                            "Client is shutting down since the network is misconfigured; \
                             correct firewall/forwarding settings then restart the client."
                        );
                        shutdown.cancel();
                        break;
                    } else {
                        tracing::warn!("Failed stillAlive test: ({}) - will retry later", code);
                    }
                }
                Err(e) => {
                    tracing::warn!("Still-alive request failed: {}", e);
                }
            }

            state.do_cert_refresh.store(false, Ordering::Release);
            tracing::info!("Certificate refresh completed successfully");
        }
    });
}

/// Spawn periodic flood control pruning (60s interval).
pub fn spawn_flood_control_pruner(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(crate::utils::tick_every(shutdown, Duration::from_secs(60), move || {
        let state = state.clone();
        async move { prune_flood_control(&state).await }
    }));
}

/// Spawn periodic time check + cert expiry check (5min interval).
/// If time drift >24h or cert expires within 24h, triggers global shutdown.
pub fn spawn_time_cert_check(
    config: Arc<ArcSwap<Config>>,
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let global_shutdown = shutdown.clone();
    tokio::spawn(crate::utils::tick_every(shutdown, Duration::from_secs(300), move || {
        let config = config.clone();
        let state = state.clone();
        let shutdown_signal = global_shutdown.clone();
        async move {
            if config.load().server_time_delta.abs() > 86400 {
                tracing::warn!("System time off by >24h. Correct your system clock.");
            }
            if let Some(expiry) = *state.cert_expiry.lock().await {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                if expiry - now < 86400 {
                    tracing::error!(
                        "Either the system clock is significantly wrong, or something has \
                         gone wrong with certificate renewal. Check your system clock and \
                         internet connection, then restart the client manually."
                    );
                    shutdown_signal.cancel();
                }
            }
        }
    }));
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

/// Start the TLS HTTP server. Sends readiness via `ready_tx` after successful bind.
/// Listens for both `shutdown` (global/Ctrl+C) and `restart` (cert refresh).
pub async fn start_server(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
    restart: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
    let config = state.config.load_full();

    // Build TLS acceptor (downloads cert if needed)
    // Java: always re-downloads the certificate on startup.
    let (tls_acceptor, cert_expiry) = match build_tls_acceptor(&config, true).await {
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
        state.bandwidth_monitor.store(Some(Arc::new(
            BandwidthMonitor::new(config.throttle_bytes)
        )));
    } else {
        state.bandwidth_monitor.store(None);
    }

    // Store in AppState so it can be refreshed at runtime
    state.tls_acceptor.store(Some(Arc::new(tls_acceptor.clone())));
    *state.cert_expiry.lock().await = Some(cert_expiry);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.client_port));
    let port = config.client_port;
    let listener = TcpListener::bind(addr).await.map_err(HathError::Io)?;

    if let Some(tx) = ready_tx {
        let _ = tx.send(Ok(port));
    }

    tracing::info!("HTTPServer listening on port {}", port);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = restart.cancelled() => break,
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Accept error: {}", e);
                        continue;
                    },
                };

                let state = state.clone();
                let allow = state.allow_normal_connections.load(Ordering::Relaxed);

                // Load config for flood control / connection checks
                let cfg = state.config.load();

                // Flood control check for non-local, non-RPC traffic
                let host_addr = remote_addr.ip().to_string().to_lowercase();
                let is_local = LOCAL_NETWORK_RE.is_match(&host_addr)
                    || cfg.client_host.replace("::ffff:", "") == host_addr;
                // Java: isValidRPCServer returns true when disableIPOriginCheck is set
                let is_rpc = cfg.disable_ip_origin_check
                    || cfg.rpc_servers.iter().any(|s| s.to_string().to_lowercase() == host_addr);

                if !allow && !is_rpc {
                    drop(stream);
                    continue;
                }

                if !is_local && !is_rpc && !cfg.disable_flood_control {
                    drop(cfg); // release Guard before .await
                    let mut fc = state.flood_control.lock().await;
                    let entry = fc.entry(host_addr.clone()).or_insert_with(|| FloodControlEntry {
                        connect_count: 0,
                        last_connect: Instant::now(),
                        block_until: None,
                    });
                    if entry.is_blocked() || !entry.hit() {
                        tracing::warn!("Flood control activated for {}", host_addr);
                        continue;
                    }
                }

                // Connection limiting for non-local, non-RPC traffic
                // Java: HTTPServer.run() checks sessionCount vs maxConnections
                if !is_local && !is_rpc {
                    let max_conns = state.config.load().max_connections();
                    let active = state.active_connections.load(Ordering::Relaxed);

                    if active > max_conns {
                        tracing::warn!(
                            "Exceeded the maximum allowed number of incoming connections ({}).",
                            max_conns
                        );
                        drop(stream);
                        continue;
                    }

                    if active > (max_conns as f64 * 0.8) as u32 && active > 0 {
                        tracing::warn!(
                            "Near connection limit: {} / {} active connections",
                            active, max_conns
                        );
                        // Java: ServerHandler.notifyOverload() — rate-limited to once per 30s
                        let now = Instant::now();
                        let mut last = state.last_overload_notification.lock().await;
                        let should_notify = last.is_none_or(|t| now - t >= Duration::from_secs(30));
                        if should_notify {
                            *last = Some(now);
                            drop(last);
                            let _ = state.rpc_client.notify_overload().await;
                        }
                    }
                }

                // Increment active connections
                let prev = state.active_connections.fetch_add(1, Ordering::Relaxed);
                state.stats.set_open_connections(prev + 1);

                // Load current TLS acceptor (may have been refreshed)
                let acceptor = state.tls_acceptor.load_full()
                    .expect("TLS acceptor not initialized");

                let conn_state = state.clone();
                tokio::spawn(async move {
                    let _guard = ConnectionGuard {
                        active_connections: conn_state.active_connections.clone(),
                        stats: conn_state.stats.clone(),
                    };

                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("TLS accept failed: {}", e);
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);

                    let service = HathService { state: conn_state };

                    if let Err(e) = http1::Builder::new()
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
