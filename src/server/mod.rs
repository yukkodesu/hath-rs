mod admission;
mod body;
mod connection;
mod handler;
mod middleware;
mod peer;
mod proxy_transfer;
mod request;
mod response;
mod service;
mod threaded_proxy;
mod tls;
mod transport;

use self::admission::FloodControlEntry;
pub(crate) use self::admission::spawn_flood_control_pruner;
pub(crate) use self::middleware::session::{SessionManager, spawn_session_reaper};
pub(crate) use self::tls::{spawn_cert_refresh_watcher, spawn_time_cert_check};
use crate::bandwidth::BandwidthMonitor;
use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::{HathError, Result};
use crate::gallery_downloader::GalleryDownloadSupervisor;
use crate::rpc_client::RpcClient;
use crate::stats::Stats;

use arc_swap::{ArcSwap, ArcSwapOption};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};

/// Shared state accessible from all request handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: Arc<ArcSwap<Config>>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) cache: Arc<CacheHandler>,
    pub(crate) rpc_client: Arc<RpcClient>,
    /// On-demand gallery queue consumer, kept separate from inbound serving.
    pub(crate) gallery_downloader: Arc<GalleryDownloadSupervisor>,
    pub(crate) allow_normal_connections: Arc<std::sync::atomic::AtomicBool>,
    /// Flood control table (IP -> entry). Sharded because it is touched by each incoming connection.
    pub(crate) flood_control: Arc<DashMap<String, FloodControlEntry>>,
    /// TLS acceptor that can be swapped at runtime (e.g. cert refresh).
    pub(crate) tls_acceptor: Arc<ArcSwapOption<tokio_rustls::TlsAcceptor>>,
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
    /// Serializes listener generations across certificate refresh and process exit.
    pub(crate) generation: Arc<Mutex<Option<ServerGeneration>>>,
    pub(crate) shutdown: crate::shutdown::Shutdown,
    pub(crate) transfers: crate::tasks::TransferTasks,
    /// Shared HTTP client for ProxyFileDownloader requests.
    /// 5s connect + 30s read timeout, reused across all proxy downloads.
    pub(crate) proxy_client: Arc<reqwest::Client>,
}

static LOCAL_NETWORK_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|169\.254\.|::1|0:0:0:0:0:0:0:1|fc|fd)")
        .expect("invalid regex")
});

/// Owns one listener generation; accepted connections are owned by its task.
pub(crate) struct ServerGeneration {
    stop: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

pub(crate) async fn start_server(state: AppState) -> Result<u16> {
    let mut generation = state.generation.lock().await;
    if state.shutdown.is_cancelled() {
        return Err(HathError::Shutdown);
    }
    if generation.is_some() {
        return Err(HathError::Config(
            "server generation is already running".into(),
        ));
    }
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let stop = tokio_util::sync::CancellationToken::new();
    let server_stop = stop.clone();
    let task_state = state.clone();
    let task = tokio::spawn(async move {
        use futures_util::FutureExt;
        match std::panic::AssertUnwindSafe(run_server(
            task_state.clone(),
            server_stop,
            Some(ready_tx),
        ))
        .catch_unwind()
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(HathError::Shutdown)) => {}
            Ok(Err(error)) => task_state.shutdown.fail(error),
            Err(_) => {
                task_state.transfers.invalidate();
                task_state
                    .shutdown
                    .fail(HathError::Fatal("server task panicked".into()));
            }
        }
    });
    *generation = Some(ServerGeneration { stop, task });
    tokio::select! {
        biased;
        result = ready_rx => {
            result.map_err(|_| HathError::Fatal("server startup task ended unexpectedly".into()))?
        }
        _ = state.shutdown.cancelled() => Err(HathError::Shutdown),
    }
}

/// Stops admission, then waits for the generation and its connections to finish.
pub(crate) async fn stop_server(state: &AppState, deadline: tokio::time::Instant) -> bool {
    state
        .allow_normal_connections
        .store(false, Ordering::SeqCst);
    let mut current = state.generation.lock().await;
    let Some(generation) = current.as_mut() else {
        return true;
    };
    generation.stop.cancel();
    let clean = match tokio::time::timeout_at(deadline, &mut generation.task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::warn!(%error, "Server task failed");
            false
        }
        Err(_) => {
            tracing::warn!("Server drain timed out; cancelling remaining connections");
            generation.task.abort();
            let _ = (&mut generation.task).await;
            false
        }
    };
    if !clean {
        state.transfers.invalidate();
    }
    *current = None;
    clean
}

/// Run one TLS HTTP server generation. Sends readiness via `ready_tx` after successful bind.
/// The generation stops only when its server shutdown token is cancelled.
async fn run_server(
    state: AppState,
    server_shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<Result<u16>>>,
) -> Result<()> {
    let config = state.config.load_full();

    // Build TLS acceptor (downloads cert if needed)
    // Java: always re-downloads the certificate on startup.
    let initialized = tokio::select! {
        biased;
        _ = server_shutdown.cancelled() => return Err(HathError::Shutdown),
        result = tls::build_tls_acceptor(&config, true) => result,
    };
    let (tls_acceptor, cert_expiry) = match initialized {
        Ok((a, expiry)) => (a, expiry),
        Err(e) => {
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(e));
                return Err(HathError::Shutdown);
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

    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = server_shutdown.cancelled() => break,
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    state.transfers.invalidate();
                    return Err(HathError::Fatal(format!("connection task failed: {error}")));
                }
            }
            (stream, remote_addr) = transport::accept_tcp(&listener) => {
                if let Some(connection) = connection::Connection::accept(&state, stream, remote_addr, &server_shutdown) {
                    connections.spawn(connection.serve());
                }
            }
        }
    }

    drop(listener);
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            state.transfers.invalidate();
            return Err(HathError::Fatal(format!("connection task failed: {error}")));
        }
    }
    Ok(())
}
