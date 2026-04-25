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
use reqwest::Url;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;
use rustls::ServerConfig;
use regex::Regex;
use std::sync::LazyLock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// Shared state accessible from all request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ArcSwap<Config>>,
    pub stats: Arc<Stats>,
    pub cache: Arc<Mutex<CacheHandler>>,
    pub rpc_client: Arc<RpcClient>,
    pub allow_normal_connections: Arc<std::sync::atomic::AtomicBool>,
    /// Flood control table (IP -> entry). Uses Arc<Mutex> for shared access.
    pub flood_control: Arc<Mutex<HashMap<String, FloodControlEntry>>>,
    /// TLS acceptor that can be swapped at runtime (e.g. cert refresh).
    pub tls_acceptor: Arc<ArcSwapOption<TlsAcceptor>>,
    /// Bandwidth throttling monitor (shared across all connections).
    /// If throttle_bytes is 0, no throttling is applied (None).
    pub bandwidth_monitor: Arc<ArcSwapOption<BandwidthMonitor>>,
    /// Count of currently active connections (for max_connections enforcement).
    pub active_connections: Arc<std::sync::atomic::AtomicU32>,
    /// Timestamp of last overload notification (rate-limited to once per 30s).
    /// Java: ServerHandler.lastOverloadNotification
    pub last_overload_notification: Arc<Mutex<Option<Instant>>>,
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
            // Local/RPC connections skip throttling; others use the shared BWM.
            let bwm_for_request = if is_local || is_rpc {
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

            // Header throttling (Java: bwm.waitForQuota before writing header bytes)
            if let Some(ref bwm) = bwm_for_request {
                bwm.wait_for_quota(100).await;
            }

            let mut resp = match request_type {
                RequestType::FileServe { fileid, hv_file, additional, keystamp_valid } => {
                    if !keystamp_valid {
                        response::forbidden_response()
                    } else if let Some(ref hv) = hv_file {
                        let cache_path = hv.cache_path(&config.cache_dir);
                        if cache_path.exists() {
                            state.stats.record_file_sent();
                            if !is_local && !is_rpc {
                                state.stats.record_bytes_sent(hv.size as u64);
                            }
                            response::file_response(hv, &config.cache_dir, bwm_for_request).await
                        } else {
                            // Cache miss — try proxy fallback.
                            // Java: HTTPResponse.parseRequest() creates
                            // HTTPResponseProcessorProxy(fileid, sources).
                            let fileindex = additional.get("fileindex");
                            let xres = additional.get("xres");
                            if let (Some(fileindex), Some(xres)) = (fileindex, xres) {
                                match state.rpc_client.static_range_fetch(fileindex, xres, &fileid).await {
                                    Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                                        let sources: Vec<Url> = sr.lines.iter()
                                            .filter(|s| !s.is_empty())
                                            .filter_map(|s| Url::parse(s).ok())
                                            .collect();
                                        if !sources.is_empty() {
                                            match ProxyFileDownloader::new(&fileid, &sources, &config).await {
                                                Ok(proxy) => {
                                                    let mime = hv.mime_type();
                                                    state.stats.record_file_sent();
                                                    if !is_local && !is_rpc {
                                                        state.stats.record_bytes_sent(hv.size as u64);
                                                    }
                                                    response::proxy_response(
                                                        mime,
                                                        proxy.total_size as usize,
                                                        proxy.temp_file,
                                                        proxy.write_offset,
                                                        proxy.notify,
                                                        bwm_for_request,
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::warn!("Proxy download failed for {}: {}", fileid, e);
                                                    response::not_found_response()
                                                }
                                            }
                                        } else {
                                            response::not_found_response()
                                        }
                                    }
                                    _ => response::not_found_response(),
                                }
                            } else {
                                response::not_found_response()
                            }
                        }
                    } else {
                        response::not_found_response()
                    }
                }
                RequestType::ServerCommand { command, additional, valid } => {
                    if valid {
                        handle_server_command(&command, &additional, &state, bwm_for_request).await
                    } else {
                        response::forbidden_response()
                    }
                }
                RequestType::SpeedTest { testsize, valid, forbidden, .. } => {
                    if valid {
                        if !is_local && !is_rpc {
                            state.stats.record_bytes_sent(testsize as u64);
                        }
                        response::speedtest_response(testsize as usize, bwm_for_request)
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

            match resp {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::error!("Error building response: {}", e);
                    Ok(Response::builder()
                        .status(500)
                        .body(StreamingBody::new(b"Internal Server Error".to_vec(), None))
                        .unwrap())
                }
            }
        })
    }
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
            // Java: HTTPResponse.processThreadedProxyTest() parses addTable for
            // hostname/protocol/port/testsize/testcount/testtime/testkey,
            // spawns concurrent outbound GETs, returns OK:{successful}-{totalTimeMillis}.
            let add_table = utils::parse_additional(additional);
            let hostname = add_table.get("hostname")
                .map(|s| s.as_str()).unwrap_or("127.0.0.1");
            let protocol = add_table.get("protocol")
                .map(|s| s.as_str()).unwrap_or("http");
            let port: u16 = add_table.get("port")
                .and_then(|v| v.parse().ok()).unwrap_or(0);
            let testsize: u64 = add_table.get("testsize")
                .and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
            let testcount: u32 = add_table.get("testcount")
                .and_then(|v| v.parse().ok()).unwrap_or(1);
            let testtime: u32 = add_table.get("testtime")
                .and_then(|v| v.parse().ok()).unwrap_or(30);
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
                    state.config.rcu(|current| {
                        let mut new = (**current).clone();
                        for line in &sr.lines {
                            if let Some((key, value)) = line.split_once('=') {
                                new.apply_setting(&key.to_lowercase(), value);
                            }
                        }
                        Arc::new(new)
                    });
                    // Recreate bandwidth monitor if throttle_bytes changed
                    let cfg = state.config.load_full();
                    if cfg.throttle_bytes > 0 {
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
            // Java: client.setCertRefresh() just sets a flag; actual refresh is async in main loop.
            // Returns empty body with 200 OK regardless of outcome.
            let cfg = state.config.load_full();
            let state_clone = state.clone();
            tokio::spawn(async move {
                match build_tls_acceptor(&cfg, true).await {
                    Ok(new_acceptor) => {
                        state_clone.tls_acceptor.store(Some(Arc::new(new_acceptor)));
                        tracing::info!("Certificate refreshed successfully");
                    }
                    Err(e) => {
                        tracing::error!("Failed to refresh certificate: {}", e);
                    }
                }
            });
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
/// If `force_download` is true, always re-download the cert from the RPC server.
async fn build_tls_acceptor(config: &Config, force_download: bool) -> Result<TlsAcceptor> {
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
    let keystore = p12_keystore::KeyStore::from_pkcs12(
        &cert_data,
        config.client_key.as_str(),
        p12_keystore::Pkcs12ImportPolicy::Strict,
    ).map_err(|e| HathError::Tls(rustls::Error::General(e.to_string())))?;

    let (_, keychain) = keystore.private_key_chain()
        .ok_or_else(|| HathError::Tls(rustls::Error::General(
            "no private key in PKCS12".into()
        )))?;

    let certs: Vec<CertificateDer> = keychain.certs()
        .iter()
        .map(|c| CertificateDer::from(c.as_der().to_vec()))
        .collect();
    let key: PrivateKeyDer = PrivatePkcs8KeyDer::from(keychain.key().as_der().to_vec()).into();

    let tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(HathError::Tls)?;

    Ok(TlsAcceptor::from(Arc::new(tls_config)))
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
pub async fn start_server(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
    let config = state.config.load_full();

    // Build TLS acceptor (downloads cert if needed)
    let tls_acceptor = match build_tls_acceptor(&config, false).await {
        Ok(a) => a,
        Err(e) => {
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(e.to_string()));
            }
            return Err(e);
        }
    };

    // Create bandwidth monitor if throttling is enabled
    if config.throttle_bytes > 0 {
        state.bandwidth_monitor.store(Some(Arc::new(
            BandwidthMonitor::new(config.throttle_bytes)
        )));
    }

    // Store in AppState so it can be refreshed at runtime
    state.tls_acceptor.store(Some(Arc::new(tls_acceptor.clone())));

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
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let state = state.clone();
                let allow = state.allow_normal_connections.load(Ordering::Relaxed);

                // Load config for flood control / connection checks
                let cfg = state.config.load();

                // Flood control check for non-local, non-RPC traffic
                let host_addr = remote_addr.ip().to_string().to_lowercase();
                let is_local = LOCAL_NETWORK_RE.is_match(&host_addr)
                    || cfg.client_host.replace("::ffff:", "") == host_addr;
                let is_rpc = cfg.rpc_servers.iter().any(|s| s.to_string().to_lowercase() == host_addr);

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

                    if active >= max_conns {
                        tracing::warn!(
                            "Exceeded the maximum allowed number of incoming connections ({}).",
                            max_conns
                        );
                        drop(stream);
                        continue;
                    }

                    if active >= (max_conns as f64 * 0.8) as u32 && active > 0 {
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
                        Err(_) => return,
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

    Ok(())
}
