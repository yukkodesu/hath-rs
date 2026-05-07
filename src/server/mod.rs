mod admission;
mod body;
mod handler;
mod middleware;
mod proxy_transfer;
mod request;
mod response;
mod service;
mod threaded_proxy;
mod tls;

use self::admission::FloodControlEntry;
pub(crate) use self::admission::spawn_flood_control_pruner;
use self::middleware::access_log::AccessLogService;
pub(crate) use self::middleware::session::{SessionManager, spawn_session_reaper};
use self::service::HathService;
pub(crate) use self::tls::{spawn_cert_refresh_watcher, spawn_time_cert_check};
use crate::bandwidth::BandwidthMonitor;
use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc_client::RpcClient;
use crate::stats::Stats;
use crate::utils;

use arc_swap::{ArcSwap, ArcSwapOption};
use dashmap::DashMap;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use openssl::ssl::SslContext;
use regex::Regex;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};

/// Shared state accessible from all request handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: Arc<ArcSwap<Config>>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) cache: Arc<CacheHandler>,
    pub(crate) rpc_client: Arc<RpcClient>,
    pub(crate) allow_normal_connections: Arc<std::sync::atomic::AtomicBool>,
    /// Flood control table (IP -> entry). Sharded because it is touched by each incoming connection.
    pub(crate) flood_control: Arc<DashMap<String, FloodControlEntry>>,
    /// TLS context that can be swapped at runtime (e.g. cert refresh).
    pub(crate) tls_acceptor: Arc<ArcSwapOption<SslContext>>,
    /// Certificate expiry as a Unix timestamp (seconds). Checked periodically;
    /// if the cert expires within 24 hours, the client shuts down (matches Java).
    pub(crate) cert_expiry: Arc<Mutex<Option<i64>>>,
    /// Bandwidth throttling monitor (shared across all connections).
    /// If throttle_bytes is 0, no throttling is applied (None).
    pub(crate) bandwidth_monitor: Arc<ArcSwapOption<BandwidthMonitor>>,
    /// Connection/session lifecycle manager (max connections + timeout cleanup).
    pub(crate) session_manager: Arc<SessionManager>,
    /// Monotonic connection/session id for Java-style access logs.
    pub(crate) next_conn_id: Arc<AtomicU32>,
    /// Timestamp of last overload notification (rate-limited to once per 30s).
    /// Java: ServerHandler.lastOverloadNotification
    pub(crate) last_overload_notification: Arc<Mutex<Option<Instant>>>,
    /// Flag: true when cert refresh (full server restart) is requested.
    /// Set by the refresh_certs RPC handler, cleared by the cert refresh watcher.
    pub(crate) do_cert_refresh: Arc<AtomicBool>,
    /// Wakes the cert refresh watcher when a refresh_certs command arrives.
    pub(crate) cert_refresh_notify: Arc<Notify>,
    /// Shutdown token for the currently-running server accept loop.
    /// Swapped during cert refresh to terminate the old listener and start a new one.
    pub(crate) server_shutdown_token: Arc<ArcSwapOption<tokio_util::sync::CancellationToken>>,
    /// Set to true by start_server() after the accept loop exits.
    /// The cert refresh watcher polls this to wait for the old server to terminate.
    pub(crate) server_terminated: Arc<AtomicBool>,
    /// Shared HTTP client for ProxyFileDownloader requests.
    /// 5s connect + 30s read timeout, reused across all proxy downloads.
    pub(crate) proxy_client: Arc<reqwest::Client>,
}

static LOCAL_NETWORK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|169\.254\.|::1|0:0:0:0:0:0:0:1|fc|fd)")
        .expect("invalid regex")
});

/// Start one HTTP server generation. Returns a oneshot receiver that fires when
/// the server binds, and the generation shutdown token (to store in AppState).
pub(crate) fn start_server(
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
pub(crate) async fn stop_server(state: &AppState) {
    tracing::info!("Stopping server");
    state
        .allow_normal_connections
        .store(false, Ordering::SeqCst);

    if let Some(token) = state.server_shutdown_token.load_full() {
        token.cancel();
    }

    for close_wait_cycles in 1..25 {
        let active = state.session_manager.active_count();
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
                    let cfg = conn_state.config.load_full();
                    admission::configure_send_buffer(tls_stream.get_ref(), &cfg);
                    let admission = match admission::admit_connection(
                        &conn_state,
                        remote_addr,
                        &host_addr,
                        &cfg,
                    )
                    .await
                    {
                        admission::AdmissionOutcome::Accepted(admission) => admission,
                        admission::AdmissionOutcome::Rejected(reason) => {
                            let _ = reason;
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);
                    let conn_id = admission.conn_id;
                    let session_handle = admission.handle.clone();
                    let cancel = admission.cancel.clone();
                    let _session_guard = admission.guard;
                    let service = AccessLogService::new(
                        HathService {
                            state: conn_state.clone(),
                            remote_addr,
                            session: session_handle.clone(),
                        },
                        conn_id,
                        remote_addr,
                        Some(session_handle),
                    );

                    tokio::select! {
                        result = http1::Builder::new().serve_connection(io, service) => {
                            if let Err(e) = result
                                && !e.to_string().contains("connection closed") {
                                    tracing::debug!("HTTP connection error: {}", e);
                                }
                        }
                        _ = cancel.cancelled() => {
                            tracing::debug!("HTTP session {} timed out; closing connection", conn_id);
                        }
                    }
                });
            }
        }
    }

    // Signal that the server has fully terminated (for cert refresh watcher).
    state.server_terminated.store(true, Ordering::Release);
    Ok(())
}
