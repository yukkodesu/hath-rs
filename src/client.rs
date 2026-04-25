use crate::config::{Config, CliArgs};
use crate::error::{HathError, Result};
use crate::cache::CacheHandler;
use crate::rpc::{self, ResponseStatus};
use crate::rpc_client::RpcClient;
use crate::server::{self, AppState, prune_flood_control};
use crate::stats::Stats;

use arc_swap::{ArcSwap, ArcSwapOption};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

/// Run `f` on each tick of an interval, until `shutdown` fires.
async fn tick_every<F, Fut>(shutdown: tokio_util::sync::CancellationToken, every: Duration, mut f: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut tick = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => f().await,
        }
    }
}

/// Main client entry point. Follows Java HentaiAtHomeClient.run() lifecycle.
pub async fn run() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::load(args)?;

    // 1. Initialize directories
    config.initialize_directories()?;

    // 2. Start logging
    crate::logging::init_logging(&config.log_dir, !config.disable_logging)?;

    tracing::info!("Hentai@Home {} (Build {}) starting up", rpc::CLIENT_VERSION, rpc::CLIENT_BUILD);

    let shutdown = tokio_util::sync::CancellationToken::new();

    // 3. Save client_login if newly provided via CLI
    if config.client_id.0 > 0 && !config.client_key.as_str().is_empty() {
        let _ = config.save_client_login();
    }

    // Validate credentials
    if config.client_id.0 < 1000 || config.client_key.as_str().len() != 20 {
        return Err(HathError::Config("Invalid credentials".into()));
    }

    // 4. Wrap config in ArcSwap for sharing
    let config = Arc::new(ArcSwap::from(Arc::new(config)));

    // 5. Server stat: get time and min build
    let rpc_client = Arc::new(RpcClient::new(config.clone())?);
    tracing::info!("Getting initial stat from server...");

    let stat_resp = rpc_client.server_stat().await?;
    if stat_resp.status != ResponseStatus::Ok {
        return Err(HathError::Rpc("Failed to get initial stat from server".into()));
    }

    // 6. Client login: get full settings
    tracing::info!("Reading client settings from server...");
    let login_resp = rpc_client.client_login().await?;
    if login_resp.status != ResponseStatus::Ok {
        let code = login_resp.fail_code.unwrap_or_default();
        return Err(HathError::Rpc(format!("Login failed: {}", code)));
    }

    // Apply server settings via rcu (clone → modify → atomic swap, zero unsafe)
    config.rcu(|current| {
        let mut new = (**current).clone();
        for line in &login_resp.lines {
            if let Some((key, value)) = line.split_once('=') {
                new.apply_setting(&key.to_lowercase(), value);
            }
        }
        Arc::new(new)
    });

    // 7. Init cache
    let stats = Arc::new(Stats::new());
    let cache = Arc::new(Mutex::new(CacheHandler::new(config.clone(), stats.clone())?));

    // 8. Download cert + start HTTP server
    let allow_connections = Arc::new(AtomicBool::new(false));
    let flood_control = Arc::new(Mutex::new(HashMap::new()));

    let app_state = AppState {
        config: config.clone(),
        stats: stats.clone(),
        cache: cache.clone(),
        rpc_client: rpc_client.clone(),
        allow_normal_connections: allow_connections.clone(),
        flood_control: flood_control.clone(),
        tls_acceptor: Arc::new(ArcSwapOption::const_empty()),
        bandwidth_monitor: Arc::new(ArcSwapOption::const_empty()),
        active_connections: Arc::new(AtomicU32::new(0)),
    };

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_shutdown = shutdown.clone();
    let server_state = app_state.clone();
    tokio::spawn(async move {
        if let Err(e) = server::start_server(server_state, server_shutdown, Some(ready_tx)).await {
            tracing::error!("Server error: {}", e);
        }
    });

    // Wait for server to bind before notifying the RPC server
    match ready_rx.await {
        Ok(Ok(_port)) => {}
        Ok(Err(e)) => {
            return Err(HathError::Config(format!("Server startup failed: {}", e)));
        }
        Err(_) => {
            return Err(HathError::Config("Server startup failed unexpectedly".into()));
        }
    }

    // 9. notifyStart: tell server we're ready (this triggers connectivity test)
    let start_resp = rpc_client.client_start().await?;
    if start_resp.status != ResponseStatus::Ok {
        let code = start_resp.fail_code.unwrap_or_default();
        tracing::error!("Startup failure: {}", code);
        if code.starts_with("FAIL_OTHER_CLIENT_CONNECTED") || code.starts_with("FAIL_CID_IN_USE") {
            return Err(HathError::Fatal(code));
        }
        return Err(HathError::Fatal(format!("Unexpected client_start failure: {}", code)));
    }

    // 10. Allow normal connections
    allow_connections.store(true, Ordering::SeqCst);
    stats.program_started();

    tracing::info!("Startup completed successfully. Starting normal operation");

    // 11. Spawn periodic background tasks

    // 10s: LRU cycle + shift stats
    {
        let cache = cache.clone();
        let stats = stats.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(10), move || {
            let cache = cache.clone();
            let stats = stats.clone();
            async move {
                if let Ok(mut c) = cache.try_lock() {
                    c.cycle_lru_cache_table();
                }
                stats.shift_bytes_sent_history();
            }
        }));
    }

    // 60s: prune flood control
    {
        let app_state = app_state.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(60), move || {
            let app_state = app_state.clone();
            async move { prune_flood_control(&app_state).await }
        }));
    }

    // 110s: still_alive
    {
        let rpc_client = rpc_client.clone();
        let stats = stats.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(110), move || {
            let rpc_client = rpc_client.clone();
            let stats = stats.clone();
            async move {
                if let Err(e) = rpc_client.still_alive(false).await {
                    tracing::warn!("Still-alive failed: {}", e);
                } else {
                    stats.record_server_contact();
                }
            }
        }));
    }

    // 5min: time check
    {
        let config = config.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(300), move || {
            let config = config.clone();
            async move {
                if config.load().server_time_delta.abs() > 86400 {
                    tracing::warn!("System time off by >24h. Correct your system clock.");
                }
            }
        }));
    }

    // 4h: clear RPC server failure
    {
        let config = config.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(14400), move || {
            let config = config.clone();
            async move {
                let cfg = config.load();
                if cfg.rpc_last_failed.is_some() {
                    let mut new = (**cfg).clone();
                    new.rpc_last_failed = None;
                    new.rpc_current = None;
                    config.store(Arc::new(new));
                }
            }
        }));
    }

    // 6h: fetch blacklist
    {
        let rpc_client = rpc_client.clone();
        let cache = cache.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(21600), move || {
            let rpc_client = rpc_client.clone();
            let cache = cache.clone();
            async move {
                if let Ok(resp) = rpc_client.get_blacklist(43200).await
                    && resp.status == ResponseStatus::Ok {
                        for fileid in &resp.lines {
                            if let Ok(mut c) = cache.try_lock() {
                                let _ = c.delete_file_from_cache(fileid);
                            }
                        }
                    }
            }
        }));
    }

    // Wait for shutdown signal
    shutdown.cancelled().await;

    // Graceful shutdown
    tracing::info!("Shutting down...");
    rpc_client.client_stop().await.ok();
    {
        let cfg = config.load();
        cfg.save_client_login().ok();
    }

    Ok(())
}
