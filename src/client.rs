use crate::cache::{self, CacheHandler};
use crate::config::{CliArgs, Config};
use crate::error::{HathError, Result};
use crate::gallery_downloader::GalleryDownloadSupervisor;
use crate::rpc::{self, ResponseStatus};
use crate::rpc_client::{self, RpcClient};
use crate::server::{self, AppState};
use crate::stats::Stats;

use arc_swap::{ArcSwap, ArcSwapOption};
use clap::Parser;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Main client entry point. Follows Java HentaiAtHomeClient.run() lifecycle.
pub async fn run() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::load(args)?;

    // 1. Initialize directories
    config.initialize_directories()?;

    // 2. Start logging
    let config = Arc::new(ArcSwap::from_pointee(config));
    crate::logging::init_logging(&config.load().log_dir, config.clone())?;

    tracing::info!(
        "Hentai@Home {} (Build {}) starting up",
        rpc::CLIENT_VERSION,
        rpc::CLIENT_BUILD
    );
    tracing::info!("Copyright (c) 2008-2026, E-Hentai.org - all rights reserved.");
    tracing::info!(
        "This software comes with ABSOLUTELY NO WARRANTY. This is free software, and you are welcome to modify and redistribute it under the GPL v3 license."
    );

    let shutdown = CancellationToken::new();

    // Handle shutdown signals for graceful exit (Java: ShutdownHook).
    // On Unix: SIGINT (Ctrl+C), SIGTERM (docker stop / systemctl stop), SIGQUIT.
    // On Windows: ctrl_c() covers Ctrl+C, Ctrl+Break, and console close events.
    // SIGTERM must be caught on Unix — docker stop sends SIGTERM, and without a
    // handler the process is SIGKILL'd before persistent cache data can be saved.
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            s.cancel();
        });
    }

    // 3. Save client_login if newly provided via CLI
    if config.load().client_id.0 > 0 && !config.load().client_key.as_str().is_empty() {
        let _ = config.load().save_client_login();
    }

    // Validate credentials
    if config.load().client_id.0 < 1000 || config.load().client_key.as_str().len() != 20 {
        return Err(HathError::Config("Invalid credentials".into()));
    }

    // 5. Server stat: get server time, min build, RPC server list.
    // Java: refreshServerStat() applies these settings BEFORE client_login so
    // acttime/actkey in the login request use the corrected server time.
    let rpc_client = Arc::new(RpcClient::new(config.clone())?);
    tracing::info!("Getting initial stat from server...");

    let stat_resp = rpc_client.server_stat().await?;
    if stat_resp.status != ResponseStatus::Ok {
        return Err(HathError::Rpc(
            "Failed to get initial stat from server".into(),
        ));
    }

    Config::apply_server_response(&config, &stat_resp);

    // 6. Client login: get full settings (uses corrected server time from step 5)
    let login_resp = rpc_client.client_login().await?;
    if login_resp.status != ResponseStatus::Ok {
        let code = login_resp.fail_code.unwrap_or_default();
        return Err(HathError::Rpc(format!("Login failed: {}", code)));
    }

    Config::apply_server_response(&config, &login_resp);

    // 7. Init cache
    let stats = Arc::new(Stats::new());
    let cache = Arc::new(CacheHandler::new(
        config.clone(),
        stats.clone(),
        shutdown.clone(),
    )?);

    // 8. Build AppState and spawn HTTP server
    let allow_connections = Arc::new(AtomicBool::new(false));
    // Java: reportShutdown — set after successful notifyStart(), never cleared.
    // Used to decide whether to send client_stop on shutdown. allow_connections
    // is toggled during cert refresh, so it can't serve this purpose.
    let report_shutdown = Arc::new(AtomicBool::new(false));
    let flood_control = Arc::new(DashMap::new());

    let proxy_client = crate::proxy_downloader::build_proxy_client(&config.load())?;
    let gallery_downloader = GalleryDownloadSupervisor::new(
        config.clone(),
        rpc_client.clone(),
        stats.clone(),
        shutdown.clone(),
    );

    let app_state = AppState {
        config: config.clone(),
        stats: stats.clone(),
        cache: cache.clone(),
        rpc_client: rpc_client.clone(),
        gallery_downloader,
        allow_normal_connections: allow_connections.clone(),
        flood_control: flood_control.clone(),
        tls_acceptor: Arc::new(ArcSwapOption::const_empty()),
        cert_expiry: Arc::new(Mutex::new(None)),
        bandwidth_monitor: Arc::new(ArcSwapOption::const_empty()),
        session_manager: Arc::new(server::SessionManager::new(stats.clone())),
        next_conn_id: Arc::new(AtomicU32::new(0)),
        last_overload_notification: Arc::new(Mutex::new(None)),
        do_cert_refresh: Arc::new(AtomicBool::new(false)),
        cert_refresh_notify: Arc::new(Notify::new()),
        server_shutdown_token: Arc::new(ArcSwapOption::const_empty()),
        server_terminated: Arc::new(AtomicBool::new(false)),
        proxy_client,
    };

    let (ready_rx, server_shutdown_token) = server::start_server(app_state.clone());
    app_state
        .server_shutdown_token
        .store(Some(Arc::new(server_shutdown_token)));

    // Wait for server to bind before notifying the RPC server
    match ready_rx.await {
        Ok(Ok(_port)) => {}
        Ok(Err(e)) => {
            return Err(HathError::Config(format!("Server startup failed: {}", e)));
        }
        Err(_) => {
            return Err(HathError::Config(
                "Server startup failed unexpectedly".into(),
            ));
        }
    }

    // 9. notifyStart: tell server we're ready (this triggers connectivity test).
    // Java: if notifyStart() returns false, the main thread skips
    // allowNormalConnections and all periodic tasks, then waits for Ctrl+C.
    let start_resp = rpc_client.client_start().await?;
    let startup_ok = if start_resp.status == ResponseStatus::Ok {
        true
    } else {
        let code = start_resp.fail_code.unwrap_or_default();
        tracing::error!("Startup failure: {}", code);
        if code.starts_with("FAIL_OTHER_CLIENT_CONNECTED") || code.starts_with("FAIL_CID_IN_USE") {
            tracing::error!(
                "Another client with the same ID ({}) is already connected to the server. \
                 This can happen if the Java client is still running. \
                 If you are switching from the Java client, make sure to stop it first, \
                 then wait 5-10 minutes before starting this client.",
                config.load().client_id.0
            );
            return Err(HathError::Fatal(code));
        }
        if code.starts_with("FAIL_CONNECT_TEST") {
            tracing::error!(
                "FAIL_CONNECT_TEST: The server was unable to verify your connection. \
                 This usually means your port forwarding or firewall settings are incorrect. \
                 Please ensure port {} is accessible from the internet.",
                config.load().client_port
            );
            tracing::error!(
                "The client will remain running so you can diagnose firewall and port \
                 forwarding issues. Press Ctrl+C to exit."
            );
        } else {
            return Err(HathError::Fatal(format!(
                "Unexpected client_start failure: {}",
                code
            )));
        }
        false
    };

    let background_tasks = if startup_ok {
        // 10. Allow normal connections
        allow_connections.store(true, Ordering::SeqCst);
        report_shutdown.store(true, Ordering::SeqCst);
        stats.program_started();

        // Refresh settings after notifyStart
        match rpc_client.refresh_settings().await {
            Ok(refresh_resp) if refresh_resp.status == ResponseStatus::Ok => {
                Config::apply_server_response(&config, &refresh_resp);
            }
            Ok(_) => {
                tracing::warn!("refresh_settings returned non-OK status after startup");
            }
            Err(e) => {
                tracing::warn!("Failed to refresh settings after startup: {}", e);
            }
        }

        // Initial blacklist fetch (3-day delta, synchronous)
        cache::fetch_initial_blacklist(&rpc_client, &cache).await;

        tracing::info!("Startup completed successfully. Starting normal operation");

        // 11. Spawn periodic background tasks
        Some(spawn_background_tasks(
            cache.clone(),
            config.clone(),
            stats.clone(),
            app_state.clone(),
            rpc_client.clone(),
            shutdown.clone(),
        ))
    } else {
        None
    };

    // Wait for shutdown signal
    shutdown.cancelled().await;

    // Graceful shutdown (Java order: client_stop → stop listener → drain → save data).
    // Java: reportShutdown is only set after successful notifyStart().
    // Unlike allow_connections, it's never toggled during cert refresh.
    tracing::info!("Shutting down...");
    if report_shutdown.load(Ordering::Relaxed)
        && let Err(e) = rpc_client.client_stop().await
    {
        tracing::warn!("Failed to notify server about shutdown: {}", e);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    server::stop_server(&app_state).await;
    let background_tasks_stopped = if let Some(background_tasks) = background_tasks {
        background_tasks.join_for(Duration::from_secs(5)).await
    } else {
        true
    };
    if background_tasks_stopped {
        cache.save_persistent_data();
    } else {
        tracing::warn!(
            "Skipping persistent cache save because background shutdown was incomplete; \
             the next startup will rescan the cache"
        );
        cache.discard_persistent_data();
    }
    {
        let cfg = config.load();
        cfg.save_client_login().ok();
    }

    Ok(())
}

fn spawn_background_tasks(
    cache: Arc<CacheHandler>,
    config: Arc<ArcSwap<Config>>,
    stats: Arc<Stats>,
    app_state: AppState,
    rpc_client: Arc<RpcClient>,
    shutdown: CancellationToken,
) -> BackgroundTasks {
    let mut tasks = BackgroundTasks::new();
    tasks.track(
        "cache-pruner",
        cache::spawn_pruner(cache.clone(), config.clone(), shutdown.clone()),
    );
    tasks.track(
        "cache-periodic-stats",
        cache::spawn_periodic_stats(cache.clone(), stats.clone(), shutdown.clone()),
    );
    tasks.track(
        "flood-control-pruner",
        server::spawn_flood_control_pruner(app_state.clone(), shutdown.clone()),
    );
    tasks.track(
        "session-reaper",
        server::spawn_session_reaper(app_state.session_manager.clone(), shutdown.clone()),
    );
    tasks.track(
        "still-alive-heartbeat",
        rpc_client::spawn_still_alive_heartbeat(rpc_client.clone(), stats, shutdown.clone()),
    );
    tasks.track(
        "time-cert-check",
        server::spawn_time_cert_check(config, app_state.clone(), shutdown.clone()),
    );
    tasks.track(
        "rpc-failure-clearer",
        rpc_client::spawn_rpc_failure_clearer(rpc_client.clone(), shutdown.clone()),
    );
    tasks.track(
        "blacklist-fetcher",
        cache::spawn_blacklist_fetcher(rpc_client.clone(), cache, shutdown.clone()),
    );
    tasks.track(
        "cert-refresh-watcher",
        server::spawn_cert_refresh_watcher(app_state, rpc_client, shutdown),
    );
    tasks
}

struct BackgroundTasks {
    tasks: VecDeque<BackgroundTask>,
}

struct BackgroundTask {
    name: &'static str,
    handle: JoinHandle<()>,
}

impl BackgroundTasks {
    fn new() -> Self {
        Self {
            tasks: VecDeque::new(),
        }
    }

    fn track(&mut self, name: &'static str, handle: JoinHandle<()>) {
        self.tasks.push_back(BackgroundTask { name, handle });
    }

    async fn join_for(mut self, timeout: Duration) -> bool {
        if self.tasks.is_empty() {
            return true;
        }

        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);

        while let Some(mut task) = self.tasks.pop_front() {
            tokio::select! {
                result = &mut task.handle => {
                    if let Err(e) = result {
                        if !e.is_cancelled() {
                            tracing::warn!("Background task {} exited with error: {}", task.name, e);
                        }
                        self.abort_remaining();
                        return false;
                    }
                }
                _ = &mut deadline => {
                    tracing::warn!(
                        "Timed out waiting for background task {} to stop; aborting remaining tasks",
                        task.name
                    );
                    task.handle.abort();
                    self.abort_remaining();
                    return false;
                }
            }
        }
        true
    }

    fn abort_remaining(&mut self) {
        for task in self.tasks.drain(..) {
            task.handle.abort();
        }
    }
}

/// Wait for any shutdown signal.
/// Unix: SIGINT, SIGTERM, or SIGQUIT.
/// Windows: Ctrl+C, Ctrl+Break, or console close (all via ctrl_c()).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        let mut sigquit = signal(SignalKind::quit()).expect("failed to register SIGQUIT handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received, shutting down gracefully..."),
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down gracefully..."),
            _ = sigquit.recv() => tracing::info!("SIGQUIT received, shutting down gracefully..."),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Interrupt received, shutting down gracefully...");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn background_task_timeout_prevents_a_clean_shutdown_result() {
        let mut tasks = BackgroundTasks::new();
        tasks.track("never-finishes", tokio::spawn(std::future::pending()));

        assert!(!tasks.join_for(Duration::from_millis(1)).await);
    }
}
